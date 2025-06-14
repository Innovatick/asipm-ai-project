# alerter.py
# Версия: 1.9.2 (исправлена инициализация логирования)

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
import logging
from datetime import datetime

# ИЗМЕНЕНО: Инициализация логирования перенесена на уровень модуля
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("alerter.log", encoding='utf-8'),
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

def get_worksheet(sheet_name):
    """Подключается к Google Sheets и возвращает объект листа."""
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        logging.error(f"❌ Ошибка доступа к листу '{sheet_name}': {e}")
        return None

# =============================================================================
# --- БЛОК 2: ЛОГИКА ОТПРАВКИ АЛЕРТОВ ---
# =============================================================================
def escape_markdown(text: str) -> str:
    """Экранирует специальные символы MarkdownV2 для безопасной отправки в Telegram."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

def send_telegram_alert(bot_token, chat_id, message):
    """Отправляет сообщение в Telegram и возвращает True в случае успеха."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {'chat_id': chat_id, 'text': message, 'parse_mode': 'MarkdownV2'}
    try:
        response = requests.post(url, json=params, timeout=10)
        response.raise_for_status()
        logging.info(f"    >> ✅ Алерт успешно отправлен в Telegram!")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"    >> ❌ Ошибка отправки алерта в Telegram: {e}")
        if e.response:
            logging.error(f"    >> ❌ Ответ сервера: {e.response.text}")
        return False

# =============================================================================
# --- БЛОК 3: ГЛАВНАЯ ЛОГИКА АЛЕРТЕРА ---
# =============================================================================
def main_alerter(interval: int = 24):
    """
    Основная функция алертера. Ищет сигналы для заданного интервала.
    """
    timeframe_map = {24: 'D1', 60: 'H1', 30: 'm30', 10: 'm10', 1: 'm1'}
    timeframe_label = timeframe_map.get(interval, f'm{interval}')
    
    logging.info("\n" + "="*50)
    logging.info(f"--- 🔔 АСУП ИИ: Система Оповещений v1.9.2 (Таймфрейм: {timeframe_label}) 🔔 ---")
    logging.info("="*50)
    
    config_sheet = get_worksheet('Config')
    analysis_sheet = get_worksheet('Analysis')
    if not all([config_sheet, analysis_sheet]): 
        logging.critical("Не удалось получить доступ к одному или нескольким листам Google. Завершение работы.")
        return

    logging.info("🔄 Читаю настройки и данные для анализа...")
    configs_raw = config_sheet.get_all_records()
    configs = {item['Parameter']: item['Value'] for item in configs_raw}
    analysis_data = analysis_sheet.get_all_records()
    
    if len(analysis_data) < 2:
        logging.info("ℹ️ Лист 'Analysis' пуст. Пропускаю.")
        return

    analysis_df = pd.DataFrame(analysis_data[1:], columns=analysis_data[0])
    
    analysis_df_filtered = analysis_df[analysis_df['Timeframe'] == timeframe_label].copy()
    if analysis_df_filtered.empty:
        logging.info(f"ℹ️ Нет данных для анализа на таймфрейме {timeframe_label}. Пропускаю.")
        return

    bot_token = configs.get('TELEGRAM_BOT_TOKEN')
    chat_id = configs.get('TELEGRAM_CHAT_ID')
    
    logging.info(f"⚙️ Проверяю условия для таймфрейма {timeframe_label}...")
    
    alerts_to_send = analysis_df_filtered[
        (analysis_df_filtered['State'] == 'Oversold') &
        (analysis_df_filtered['Recommendation'] != 'Alert Sent')
    ]
    
    if alerts_to_send.empty:
        logging.info("✅ Новых сигналов 'Oversold' для отправки не найдено.")
    else:
        logging.info(f"Найдено {len(alerts_to_send)} новых сигналов 'Oversold'. Отправка...")
        for index, alert_row in alerts_to_send.iterrows():
            ticker = alert_row['Ticker']
            rsi_value = float(str(alert_row['RSI_14']).replace(',', '.'))
            
            safe_ticker = escape_markdown(ticker)
            now_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = (
                f"🚨 *СИГНАЛ: ПЕРЕПРОДАННОСТЬ ({timeframe_label})*\n\n"
                f"*{safe_ticker}* вошел в зону перепроданности\n\n"
                f"*Текущий RSI\\(14\\):* `{rsi_value:.2f}`\n"
                f"*Время сигнала:* `{now_time_str}`\n\n"
                f"*Рекомендация:* Искать точку для покупки на таймфрейме {timeframe_label}\\."
            )

            if send_telegram_alert(bot_token, chat_id, message):
                logging.info(f"  - Алерт по {ticker} отправлен. Анализатор обновит статус при следующем запуске.")

    logging.info("--- 🏁 РАБОТА СИСТЕМЫ ОПОВЕЩЕНИЙ ЗАВЕРШЕНА 🏁 ---")

if __name__ == "__main__":
    main_alerter(interval=24)