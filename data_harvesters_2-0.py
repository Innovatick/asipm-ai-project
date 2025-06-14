# data_harvesters.py
# Версия: 2.0 (с поддержкой интервалов и работы по списку)

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import numpy as np

# =============================================================================
# --- БЛОК 1: КОНФИГУРАЦИЯ И ПОДКЛЮЧЕНИЕ ---
# =============================================================================
CREDS_FILE = 'credentials.json'
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1qBYS_DhGNsTo-Dnph3g_H27aHQOoY0EOcmCIKarb7Zc/"
SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

def get_gsheets_client(creds_file=CREDS_FILE, scope=SCOPE) -> gspread.Client | None:
    """Подключается к Google Sheets и возвращает объект клиента."""
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=scope)
        client = gspread.authorize(creds)
        logging.info("✅ Авторизация в Google Sheets прошла успешно.")
        return client
    except FileNotFoundError:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Файл credentials.json не найден.")
        return None
    except Exception as e:
        logging.error(f"❌ Ошибка авторизации Google: {e}")
        return None

# =============================================================================
# --- БЛОК 2: ФУНКЦИИ-СБОРЩИКИ ---
# =============================================================================
def get_moex_history(ticker: str, start_date: str, market: str, board: str, interval: int) -> pd.DataFrame:
    """
    Получает историю свечей (OHLCV) для одного тикера с MOEX с заданным интервалом.
    interval: 1, 10, 30 (минуты), 60 (час), 24 (день), 7 (неделя), 31 (месяц), 4 (квартал).
    """
    logging.info(f"  - Запрос истории для {ticker} ({market}/{board}) с даты {start_date}, интервал: {interval}...")
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/{market}/boards/{board}/securities/{ticker}.json?from={start_date}&interval={interval}&iss.meta=off"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json().get('history', {})
        if not data.get('data'):
            logging.warning(f"    - ⚠️ Для {ticker} не вернулась история (интервал: {interval}). Возможно, нет торгов.")
            return pd.DataFrame()

        cols = data['columns']
        df = pd.DataFrame(data['data'], columns=cols)
        required_cols = ['TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
        df = df[required_cols]
        df.rename(columns={'TRADEDATE': 'Date'}, inplace=True)
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"    - ❌ Сетевая ошибка при получении истории для {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"    - ❌ Неизвестная ошибка при получении истории для {ticker}: {e}")
        return pd.DataFrame()

# =============================================================================
# --- БЛОК 3: ГЛАВНАЯ ЛОГИКА ---
# =============================================================================
def main_history_updater(interval: int = 24, tickers_to_process: list[str] | None = None):
    """
    Основная функция для сбора и обновления исторических данных.
    Принимает интервал и необязательный список тикеров для обработки.
    """
    timeframe_map = {24: 'D1', 60: 'H1', 30: 'm30', 10: 'm10', 1: 'm1'}
    timeframe_label = timeframe_map.get(interval, f'm{interval}')

    logging.info("\n" + "="*50)
    logging.info(f"--- ✨ АСУП ИИ: Обновление Истории v2.0 (Интервал: {timeframe_label}) ✨ ---")
    logging.info("="*50)

    client = get_gsheets_client()
    if not client: return

    try:
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        holdings_sheet = spreadsheet.worksheet('Holdings')
        history_sheet = spreadsheet.worksheet('History_OHLCV')
    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Не могу открыть таблицу или листы. {e}")
        return

    holdings_df = pd.DataFrame(holdings_sheet.get_all_records())
    history_records = history_sheet.get_all_records()
    
    history_df = pd.DataFrame(history_records[1:], columns=history_records[0]) if len(history_records) > 1 else pd.DataFrame()
    if not history_df.empty:
        history_df_filtered = history_df[history_df['Timeframe'] == timeframe_label]
    else:
        history_df_filtered = pd.DataFrame()

    if tickers_to_process is None:
        logging.info("Режим 'Daily': обрабатываются все активы из листа Holdings.")
        tickers_to_iterate = holdings_df[holdings_df['Type'].isin(['Stock_MOEX', 'Bond_MOEX'])]['Ticker'].tolist()
    else:
        logging.info(f"Режим 'Intraday': обрабатываются тикеры из 'горячего списка' ({len(tickers_to_process)} шт).")
        tickers_to_iterate = tickers_to_process

    new_history_rows = []

    for ticker in tickers_to_iterate:
        asset_info = holdings_df[holdings_df['Ticker'] == ticker]
        if asset_info.empty:
            logging.warning(f"Тикер '{ticker}' из 'горячего списка' не найден в Holdings. Пропускаю.")
            continue
        
        asset_type = asset_info['Type'].iloc[0]
        market, board = ('shares', 'TQBR') if asset_type == 'Stock_MOEX' else ('bonds', 'TQOB')

        last_date_str = None
        if not history_df_filtered.empty and 'Ticker' in history_df_filtered.columns:
            ticker_dates = history_df_filtered[history_df_filtered['Ticker'] == ticker]['Date']
            if not ticker_dates.empty:
                last_date_str = ticker_dates.max()

        start_date = (datetime.strptime(last_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') if pd.notna(last_date_str) else (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        ticker_history_df = get_moex_history(ticker, start_date, market, board, interval)

        if not ticker_history_df.empty:
            ticker_history_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            ticker_history_df.fillna('', inplace=True)

            for _, row in ticker_history_df.iterrows():
                new_history_rows.append([row['Date'], timeframe_label, ticker, row['OPEN'], row['HIGH'], row['LOW'], row['CLOSE'], row['VOLUME']])

    if new_history_rows:
        logging.info(f"\n🔄 Найдено {len(new_history_rows)} новых записей для {timeframe_label}. Добавляю в 'History_OHLCV'...")
        history_sheet.append_rows(new_history_rows, value_input_option='USER_ENTERED')
        logging.info(f"✅ История успешно дополнена.")
    else:
        logging.info(f"✅ Новых исторических данных для {timeframe_label} не найдено.")

    logging.info("--- 🏁 РАБОТА ОБНОВИТЕЛЯ ИСТОРИИ ЗАВЕРШЕНА 🏁 ---")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main_history_updater(interval=24)