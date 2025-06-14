# macro_harvester.py
# Версия: 1.4 (повышена надежность за счет requests.Session и таймаутов)

import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pandas as pd
import yfinance as yf
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# НОВОЕ: Создаем сессию requests для большей надежности
def get_requests_session() -> requests.Session:
    """
    Создает и настраивает сессию requests с User-Agent и механизмом retry.
    """
    session = requests.Session()
    # Маскируемся под обычный браузер
    session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    
    # Настраиваем механизм повторных попыток для всех HTTP-запросов
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

def get_yf_history(ticker: str, session: requests.Session, full_fetch: bool = False) -> Optional[pd.DataFrame]:
    """
    Получает дневную историю для одного тикера с Yahoo Finance.
    Использует переданную сессию requests для повышения надежности.

    Args:
        ticker: Тикер актива в формате Yahoo Finance.
        session: Настроенная сессия requests.
        full_fetch: Если True, загружает историю за 2 года.
                    Иначе, за последние 7 дней (с запасом).

    Returns:
        DataFrame с историей или None в случае критической ошибки.
    """
    logging.info(f"  - Запрос истории для {ticker} (Yahoo Finance)...")

    end_date = datetime.now() + timedelta(days=1)
    if full_fetch:
        start_date = end_date - timedelta(days=365 * 2)
        period_str = "2 года"
    else:
        start_date = end_date - timedelta(days=7)
        period_str = "7 дней"

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    try:
        asset = yf.Ticker(ticker, session=session)
        # Увеличиваем таймаут до 60 секунд
        hist = asset.history(
            start=start_date_str,
            end=end_date_str,
            interval="1d",
            timeout=60
        )

        if hist.empty:
            logging.warning(f"    - ⚠️ Для {ticker} не вернулась история с yfinance (период: {period_str}).")
            return None

        hist.reset_index(inplace=True)
        # Приводим названия столбцов к единому регистру для надежности
        hist.columns = [col.capitalize() for col in hist.columns]
        
        rename_map = {
            'Date': 'Date', 'Open': 'Open', 'High': 'High',
            'Low': 'Low', 'Close': 'Close', 'Volume': 'Volume'
        }
        hist.rename(columns=rename_map, inplace=True)

        hist['Date'] = pd.to_datetime(hist['Date']).dt.strftime('%Y-%m-%d')

        for col in ['Open', 'High', 'Low', 'Close']:
            if col in hist.columns:
                hist[col] = hist[col].apply(lambda x: float(x) if pd.notna(x) else None)
        if 'Volume' in hist.columns:
            hist['Volume'] = hist['Volume'].apply(lambda x: int(x) if pd.notna(x) else None)

        required_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        return hist[[col for col in required_cols if col in hist.columns]]

    except Exception as e:
        logging.error(f"    - ❌ КРИТИЧЕСКАЯ ОШИБКА при получении истории для {ticker}: {e}", exc_info=True)
        return None


def main_macro_updater(tickers_to_process: List[str], history_sheet, full_fetch: bool = False) -> None:
    """
    Основная функция для обновления макро-данных.
    """
    mode_str = "ПОЛНАЯ ИСТОРИЧЕСКАЯ ЗАГРУЗКА" if full_fetch else "Обновление"
    logging.info("\n" + "="*50)
    logging.info(f"--- 🌍 ASIPM-AI: {mode_str} Макро-данных v1.4 (Сверхнадежный) 🌍 ---")
    logging.info("="*50)

    # НОВОЕ: Создаем одну сессию на весь запуск
    session = get_requests_session()

    new_history_rows: List[List[Any]] = []
    for ticker in tickers_to_process:
        df = get_yf_history(ticker, session, full_fetch=full_fetch)
        
        # Пропускаем тикер, если данные не получены
        if df is None or df.empty:
            continue

        df.sort_values(by='Date', inplace=True)
        records_to_add = df.to_dict('records')
        
        for record in records_to_add:
            new_history_rows.append([
                record.get('Date', ''), 'D1', ticker,
                record.get('Open', ''), record.get('High', ''),
                record.get('Low', ''), record.get('Close', ''),
                record.get('Volume', '')
            ])
        # Добавляем небольшую паузу между запросами, чтобы не перегружать сервер
        time.sleep(1)

    if new_history_rows:
        logging.info(f"\n🔄 Найдено {len(new_history_rows)} новых макро-записей. Добавляю в 'History_OHLCV'...")
        history_sheet.append_rows(new_history_rows, value_input_option='USER_ENTERED')
        logging.info("✅ Макро-история успешно дополнена.")
    else:
        logging.info("✅ Новых макро-данных для добавления не найдено.")

    logging.info("--- 🏁 РАБОТА МАКРО-СБОРЩИКА ЗАВЕРШЕНА 🏁 ---")