# data_harvesters.py
# Версия: 2.4 (исправлена инициализация логирования)

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import numpy as np
import xml.etree.ElementTree as ET

# ИЗМЕНЕНО: Инициализация логирования перенесена на уровень модуля
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("harvester.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ... (остальной код модуля без изменений) ...
# =============================================================================
# --- БЛОК 1: КОНФИГУРАЦИЯ И ПОДКЛЮЧЕНИЕ ---
# =============================================================================
CREDS_FILE = 'credentials.json'
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1qBYS_DhGNsTo-Dnph3g_H27aHQOoY0EOcmCIKarb7Zc/"
SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

def get_gsheets_client(creds_file=CREDS_FILE, scope=SCOPE) -> gspread.Client | None:
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
def get_cbr_history(ticker: str, start_date: str) -> pd.DataFrame:
    logging.info(f"  - Запрос истории для {ticker} (ЦБ РФ) с даты {start_date}...")
    currency_codes = {'USD/RUB': 'R01235', 'EUR/RUB': 'R01239', 'CNY/RUB': 'R01375'}
    currency_id = currency_codes.get(ticker)
    if not currency_id:
        logging.warning(f"    - ⚠️ Неизвестный код валюты для {ticker}")
        return pd.DataFrame()
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.now()
    url = f"http://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_dt.strftime('%d/%m/%Y')}&date_req2={end_dt.strftime('%d/%m/%Y')}&VAL_NM_RQ={currency_id}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        records = []
        for record in root.findall('Record'):
            date_str = record.get('Date')
            value_str = record.find('Value').text.replace(',', '.')
            records.append({'Date': datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d'), 'Close': float(value_str)})
        if not records:
            logging.warning(f"    - ⚠️ Для {ticker} (ЦБ РФ) не вернулась история.")
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df['Open'] = df['High'] = df['Low'] = df['Close']
        df['Volume'] = 0
        return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    except Exception as e:
        logging.error(f"    - ❌ Ошибка при получении истории от ЦБ РФ для {ticker}: {e}")
        return pd.DataFrame()

def get_moex_history(ticker: str, start_date: str, market: str, board: str, interval: int) -> pd.DataFrame:
    logging.info(f"  - Запрос истории для {ticker} (рынок: {market}, доска: {board}) с даты {start_date}, интервал: {interval}...")
    url = f"https://iss.moex.com/iss/history/engines/{market}/markets/shares/boards/{board}/securities/{ticker}.json?from={start_date}&interval={interval}&iss.meta=off"
    if market == 'currency':
        url = f"https://iss.moex.com/iss/history/engines/{market}/markets/selt/boards/{board}/securities/{ticker}.json?from={start_date}&interval={interval}&iss.meta=off"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json().get('history', {})
        if not data.get('data'):
            logging.warning(f"    - ⚠️ Для {ticker} не вернулась история (интервал: {interval}).")
            return pd.DataFrame()
        cols = data['columns']
        df = pd.DataFrame(data['data'], columns=cols)
        rename_map = {'TRADEDATE': 'Date', 'OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low', 'CLOSE': 'Close', 'VOLUME': 'Volume', 'VOLRUR': 'Volume'}
        df.rename(columns=rename_map, inplace=True)
        required_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        existing_cols = [col for col in required_cols if col in df.columns]
        df = df[existing_cols]
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
def main_history_updater(interval: int = 24, tickers_to_process: list[str] | None = None, full_fetch: bool = False):
    timeframe_map = {24: 'D1', 60: 'H1', 30: 'm30', 10: 'm10', 1: 'm1'}
    timeframe_label = timeframe_map.get(interval, f'm{interval}')
    
    mode_str = "ПОЛНАЯ ИСТОРИЧЕСКАЯ ЗАГРУЗКА" if full_fetch else f"Обновление (Интервал: {timeframe_label})"
    logging.info("\n" + "="*50)
    logging.info(f"--- ✨ АСУП ИИ: {mode_str} Истории v2.4 ✨ ---")
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
    
    history_df_filtered = pd.DataFrame()
    if not full_fetch:
        history_records = history_sheet.get_all_records()
        history_df = pd.DataFrame(history_records[1:], columns=history_records[0]) if len(history_records) > 1 else pd.DataFrame()
        if not history_df.empty:
            history_df_filtered = history_df[history_df['Timeframe'] == timeframe_label]
    
    if tickers_to_process is None:
        tickers_to_iterate = holdings_df['Ticker'].tolist()
    else:
        tickers_to_iterate = tickers_to_process
        
    new_history_rows = []
    for ticker in tickers_to_iterate:
        asset_info = holdings_df[holdings_df['Ticker'] == ticker]
        if asset_info.empty:
            logging.warning(f"Тикер '{ticker}' не найден в Holdings. Пропускаю.")
            continue
        
        start_date = (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d') # 2 года истории
        if not full_fetch:
            last_date_str = None
            if not history_df_filtered.empty and 'Ticker' in history_df_filtered.columns:
                ticker_dates = history_df_filtered[history_df_filtered['Ticker'] == ticker]['Date']
                if not ticker_dates.empty:
                    last_date_str = ticker_dates.max()
            if pd.notna(last_date_str):
                 start_date = (datetime.strptime(last_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        ticker_history_df = pd.DataFrame()
        asset_type = asset_info['Type'].iloc[0]
        if asset_type == 'Currency_CBR':
            ticker_history_df = get_cbr_history(ticker, start_date)
        else:
            market, board = None, None
            if asset_type == 'Stock_MOEX': market, board = 'stock', 'TQBR'
            elif asset_type == 'Bond_MOEX': market, board = 'stock', 'TQOB'
            elif asset_type == 'Currency_MOEX': market, board = 'currency', 'CETS'
            
            if all([market, board]):
                ticker_history_df = get_moex_history(ticker, start_date, market, board, interval)
            else:
                if asset_type != 'Macro_YF': # Игнорируем типы для другого сборщика
                    logging.warning(f"Неизвестный MOEX тип актива '{asset_type}' для тикера {ticker}. Пропускаю.")
                continue
                
        if not ticker_history_df.empty:
            ticker_history_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            ticker_history_df.fillna('', inplace=True)
            for _, row in ticker_history_df.iterrows():
                new_history_rows.append([row['Date'], timeframe_label, ticker, row.get('Open', ''), row.get('High', ''), row.get('Low', ''), row.get('Close', ''), row.get('Volume', '')])
                
    if new_history_rows:
        logging.info(f"\n🔄 Найдено {len(new_history_rows)} новых записей. Добавляю в 'History_OHLCV'...")
        history_sheet.append_rows(new_history_rows, value_input_option='USER_ENTERED')
        logging.info(f"✅ История успешно дополнена.")
    else:
        logging.info(f"✅ Новых исторических данных не найдено.")
        
    logging.info("--- 🏁 РАБОТА ОБНОВИТЕЛЯ ИСТОРИИ ЗАВЕРШЕНА 🏁 ---")

if __name__ == "__main__":
    # Этот блок остается для возможности ручного запуска с дефолтными параметрами
    main_history_updater(interval=24)