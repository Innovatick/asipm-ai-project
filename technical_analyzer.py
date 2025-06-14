# technical_analyzer.py
# Версия: 2.7 (Финальная: Исправлена критическая ошибка создания DataFrame)

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional

# Инициализация логирования на уровне модуля для надежности
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analyzer.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_worksheet(sheet_name: str) -> Optional[gspread.Worksheet]:
    """Подключается к Google Sheets и возвращает объект листа."""
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file'])
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBYS_DhGNsTo-Dnph3g_H27aHQOoY0EOcmCIKarb7Zc/")
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        logging.error(f"❌ Ошибка доступа к листу '{sheet_name}': {e}")
        return None

def calculate_indicators_and_state(df_for_calc: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Рассчитывает индикаторы на ЧИСТОМ DataFrame (только OHLC).
    """
    if df_for_calc.shape[0] < 50:
        return {}

    df_for_calc.ta.rsi(length=14, append=True)
    df_for_calc.ta.sma(length=20, append=True)
    df_for_calc.ta.sma(length=50, append=True)
    df_for_calc.ta.bbands(length=20, append=True)
    latest = df_for_calc.iloc[-1]

    rsi = latest.get('RSI_14')
    state = "Neutral"
    recommendation = "-"
    if pd.notna(rsi):
        warning_level = float(str(config.get('RSI_WARNING_LEVEL', 35)).replace(',', '.'))
        alert_level = float(str(config.get('RSI_ALERT_LEVEL', 30)).replace(',', '.'))
        proximity_pct = float(str(config.get('PROXIMITY_PERCENTAGE', 15)).replace(',', '.'))
        proximity_start_level = warning_level * (1 + proximity_pct / 100)
        if rsi < alert_level:
            state = "Oversold"
            recommendation = "Alert Sent"
        elif rsi < warning_level:
            state = "Warning"
            recommendation = "Monitor for reversal"
        elif rsi < proximity_start_level:
            state = "Proximity"
            recommendation = "Add to Hotlist?"

    def format_number(num: Optional[float]) -> str:
        if pd.notna(num):
            return f'{num:,.2f}'.replace(',', ' ').replace('.', ',')
        return 'N/A'

    return {
        'State': state, 'Recommendation': recommendation,
        'RSI_14': format_number(latest.get('RSI_14')),
        'MA_20': format_number(latest.get('SMA_20')),
        'MA_50': format_number(latest.get('SMA_50')),
        'BB_Upper': format_number(latest.get('BBU_20_2.0')),
        'BB_Lower': format_number(latest.get('BBL_20_2.0')),
    }

def main_analyzer() -> None:
    """Основная функция анализатора: читает всю историю и полностью пересчитывает аналитику."""
    logging.info("\n" + "="*50)
    logging.info(f"--- 🧠 ASIPM-AI: Технический Анализатор v2.7 (Корректный) 🧠 ---")
    logging.info("="*50)
    sheets = {name: get_worksheet(name) for name in ['History_OHLCV', 'Analysis', 'Config']}
    if not all(sheets.values()):
        logging.critical("Не удалось получить доступ к одному или нескольким листам Google. Завершение работы.")
        return

    logging.info("🔄 Читаю конфиги и ВСЮ историю для полного пересчета...")
    history_records = sheets['History_OHLCV'].get_all_records()
    configs_raw = sheets['Config'].get_all_records()
    config = {item['Parameter']: item['Value'] for item in configs_raw}

    if not history_records:
        logging.warning("Лист 'History_OHLCV' пуст. Анализ невозможен.")
        return

    # --- КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Правильный способ создания DataFrame из get_all_records() ---
    history_df = pd.DataFrame(history_records)
    # ------------------------------------------------------------------------------------

    cols_to_process = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in cols_to_process:
        if col in history_df.columns:
            history_df[col] = pd.to_numeric(history_df[col], errors='coerce')

    assets_to_analyze = history_df[['Ticker', 'Timeframe']].drop_duplicates().to_dict('records')
    logging.info(f"☑️ Найдено {len(assets_to_analyze)} уникальных пар (тикер/таймфрейм) для анализа.")

    all_analysis_results: List[List[Any]] = []
    for asset_key in assets_to_analyze:
        ticker, timeframe = asset_key['Ticker'], asset_key['Timeframe']
        logging.info(f"  - Анализирую {ticker} на {timeframe}...")

        ticker_history = history_df[(history_df['Ticker'] == ticker) & (history_df['Timeframe'] == timeframe)].copy()
        if ticker_history.empty: continue

        ticker_history['Date'] = pd.to_datetime(ticker_history['Date'])
        ticker_history.sort_values(by='Date', inplace=True)

        columns_for_calc = ['Open', 'High', 'Low', 'Close']
        if not all(col in ticker_history.columns for col in columns_for_calc):
            logging.warning(f"    - ⚠️ Пропускаю {ticker}, т.к. отсутствуют необходимые столбцы OHLC.")
            continue
        
        calculation_df = ticker_history[columns_for_calc].copy().dropna()
        
        analysis_result = calculate_indicators_and_state(calculation_df, config)
        if not analysis_result:
            logging.warning(f"    - ⚠️ Недостаточно данных для анализа {ticker} на {timeframe} (< 50 свечей).")
            continue

        new_row = [
            ticker, timeframe, analysis_result.get('State'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), analysis_result.get('RSI_14'),
            analysis_result.get('MA_20'), analysis_result.get('MA_50'),
            analysis_result.get('BB_Upper'), analysis_result.get('BB_Lower'),
            "N/A", analysis_result.get('Recommendation')
        ]
        all_analysis_results.append(new_row)
        logging.info(f"    - ✅ Анализ завершен. Состояние: {analysis_result.get('State')}, RSI: {analysis_result.get('RSI_14')}")

    if all_analysis_results:
        logging.info(f"\n🔄 Перезаписываю лист 'Analysis' {len(all_analysis_results)} строками...")
        try:
            headers = ['Ticker', 'Timeframe', 'State', 'Last_Update', 'RSI_14', 'MA_20', 'MA_50', 'BB_Upper', 'BB_Lower', 'Pattern_Found', 'Recommendation']
            sheets['Analysis'].clear()
            sheets['Analysis'].update(range_name='A1', values=[headers] + all_analysis_results)
            logging.info("✅✅✅ УСПЕХ! Лист 'Analysis' полностью пересобран и обновлен.")
        except Exception as e:
            logging.error(f"❌ ОШИБКА при записи в 'Analysis': {e}", exc_info=True)

    logging.info("--- 🏁 РАБОТА АНАЛИЗАТОРА ЗАВЕРШЕНА 🏁 ---")

if __name__ == "__main__":
    main_analyzer()