# main_runner.py
# Версия: 2.4 (Добавлена фильтрация по столбцу 'Watch' в Holdings)

import logging
import sys
import argparse
import pandas as pd
from typing import List

# --- Блок импорта ---
try:
    # ИЗМЕНЕНО: data_harvesters теперь импортируется без main_history_updater,
    # так как вся логика управления будет здесь.
    from data_harvesters import main_history_updater, get_gsheets_client, SPREADSHEET_URL
    from macro_harvester import main_macro_updater
    from technical_analyzer import main_analyzer
    from alerter import main_alerter
except ImportError as e:
    print(f"Критическая ошибка: не удалось импортировать модули. Ошибка: {e}")
    sys.exit(1)

# --- Блок логирования ---
LOG_FILE = 'asipm_main_log.txt'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_hot_watchlist(holdings_df: pd.DataFrame, analysis_df: pd.DataFrame, config: dict) -> list[str]:
    # ... (функция без изменений) ...
    logging.info("--- Формирование 'горячего списка' для внутридневного мониторинга ---")
    
    priority_tickers = set(holdings_df[holdings_df['Priority'].isin(['Strategic', 'Promising'])]['Ticker'])
    logging.info(f"Приоритетные тикеры (Strategic, Promising): {priority_tickers}")

    proximity_tickers = set()
    if not analysis_df.empty and 'RSI_14' in analysis_df.columns:
        analysis_df['RSI_14_NUM'] = pd.to_numeric(analysis_df['RSI_14'].astype(str).str.replace(',', '.'), errors='coerce')
        
        warning_level = float(str(config.get('RSI_WARNING_LEVEL', 35)).replace(',', '.'))
        proximity_pct = float(str(config.get('PROXIMITY_PERCENTAGE', 15)).replace(',', '.'))
        proximity_start_level = warning_level * (1 + proximity_pct / 100)
        
        proximity_tickers = set(analysis_df[
            (analysis_df['RSI_14_NUM'] < proximity_start_level) &
            (analysis_df['RSI_14_NUM'] >= warning_level)
        ]['Ticker'])
        logging.info(f"Тикеры в 'зоне внимания' (RSI < {proximity_start_level:.2f}): {proximity_tickers}")

    hot_list = list(priority_tickers.union(proximity_tickers))
    logging.info(f"Итоговый 'горячий список': {hot_list}")
    return hot_list


def run_pipeline(mode: str, interval: int, fetch_mode: str):
    """Основной конвейер для запуска всех этапов обработки данных."""
    logging.info("="*20 + f" ЗАПУСК КОНВЕЙЕРА ASIPM-AI (Режим: {mode}, Интервал: {interval}, Загрузка: {fetch_mode}) " + "="*20)
    
    is_full_fetch = (fetch_mode == 'full')

    # --- ЭТАП 0: ПОДГОТОВКА ---
    client = get_gsheets_client()
    if not client: sys.exit(1)
    try:
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        holdings_sheet = spreadsheet.worksheet('Holdings')
        analysis_sheet = spreadsheet.worksheet('Analysis')
        config_sheet = spreadsheet.worksheet('Config')
        history_sheet = spreadsheet.worksheet('History_OHLCV')
        
        holdings_records = holdings_sheet.get_all_records()
        holdings_df = pd.DataFrame(holdings_records)
        # ИЗМЕНЕНО: Приводим столбец Watch к строковому типу для надежного сравнения
        if 'Watch' in holdings_df.columns:
            holdings_df['Watch'] = holdings_df['Watch'].astype(str).str.upper()

        analysis_records = analysis_sheet.get_all_records()
        configs_raw = config_sheet.get_all_records()
        config = {item['Parameter']: item['Value'] for item in configs_raw}
        analysis_df = pd.DataFrame(analysis_records[1:], columns=analysis_records[0]) if len(analysis_records) > 1 else pd.DataFrame()

    except Exception as e:
        logging.error(f"Критическая ошибка на этапе подготовки: {e}", exc_info=True)
        sys.exit(1)

    # --- ОПРЕДЕЛЕНИЕ СПИСКОВ ТИКЕРОВ ДЛЯ ОБРАБОТКИ ---
    # ИЗМЕНЕНО: Логика вынесена наверх и учитывает столбец 'Watch'

    # 1. Тикеры для макро-сборщика (только в daily режиме)
    macro_tickers_to_process = []
    if mode == 'daily':
        macro_tickers_to_process = holdings_df[
            (holdings_df['Type'] == 'Macro_YF') & 
            (holdings_df['Watch'] == 'TRUE')
        ]['Ticker'].tolist()

    # 2. Тикеры для основного сборщика
    harvester_tickers_to_process = []
    if mode == 'daily':
        # В ежедневном режиме берем все отслеживаемые российские активы
        harvester_tickers_to_process = holdings_df[
            (holdings_df['Type'].isin(['Stock_MOEX', 'Bond_MOEX', 'Currency_MOEX', 'Currency_CBR'])) &
            (holdings_df['Watch'] == 'TRUE')
        ]['Ticker'].tolist()
    elif mode == 'intraday' and not is_full_fetch:
        # Внутри дня - только "горячий список"
        harvester_tickers_to_process = get_hot_watchlist(holdings_df, analysis_df, config)


    # --- ЗАПУСК МАКРО-СБОРЩИКА ---
    if macro_tickers_to_process:
        main_macro_updater(
            tickers_to_process=macro_tickers_to_process, 
            history_sheet=history_sheet, 
            full_fetch=is_full_fetch
        )
    else:
        logging.info("Макро-тикеры для обработки не найдены (проверьте Holdings: Type='Macro_YF' и Watch='TRUE').")

    
    # --- ЭТАП 1: ОСНОВНОЙ СБОРЩИК ДАННЫХ ---
    if harvester_tickers_to_process:
        try:
            main_history_updater(
                interval=interval, 
                tickers_to_process=harvester_tickers_to_process, 
                full_fetch=is_full_fetch
            )
        except Exception as e:
            logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА на этапе Сбора Данных: {e}", exc_info=True)
            sys.exit(1)
    else:
        logging.info("Основные тикеры для обработки не найдены. Пропускаем основной сборщик.")


    # --- ЭТАП 2 и 3 (АНАЛИЗ И АЛЕРТЫ) ---
    try:
        logging.info("--- Этап 2: Запуск Технического Анализатора ---")
        main_analyzer()
        logging.info("--- Этап 2: Технический Анализатор УСПЕШНО завершил работу. ---")
    except Exception as e:
        logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА на этапе Анализа Данных: {e}", exc_info=True)
        sys.exit(1)

    try:
        logging.info("--- Этап 3: Запуск Алертера ---")
        main_alerter(interval=interval)
        logging.info("--- Этап 3: Алертер УСПЕШНО завершил работу. ---")
    except Exception as e:
        logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА на этапе Отправки Алертов: {e}", exc_info=True)
        sys.exit(1)

    logging.info("="*20 + " КОНВЕЙЕР ASIPM-AI УСПЕШНО ЗАВЕРШЕН " + "="*20)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Запускает конвейер ASIPM-AI.")
    parser.add_argument('--mode', type=str, choices=['daily', 'intraday'], default='daily', help='Режим запуска: daily (все активы) или intraday (горячий список).')
    parser.add_argument('--interval', type=int, default=24, help='Интервал свечей в минутах (24 для дня).')
    parser.add_argument('--fetch-mode', type=str, choices=['delta', 'full'], default='delta', help="Режим загрузки: 'delta' для новых данных, 'full' для полной истории.")
    args = parser.parse_args()
    
    if args.fetch_mode == 'full' and args.mode != 'daily':
        print("Ошибка: Полная историческая загрузка (--fetch-mode full) возможна только в ежедневном режиме (--mode daily).")
        sys.exit(1)
        
    run_pipeline(mode=args.mode, interval=args.interval, fetch_mode=args.fetch_mode)