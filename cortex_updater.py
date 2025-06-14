# cortex_updater.py
# Версия: 0.1 (MVP - Надежный логгер)
# Назначение: Читает файл с логом сессии и дописывает его в Google-таблицу "Project_Exocortex".

import gspread
from google.oauth2.service_account import Credentials
import logging
from datetime import datetime

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- КОНФИГУРАЦИЯ ---
GSHEETS_CREDS = 'credentials.json'
# ВАЖНО: Укажите URL вашего файла "Project_Exocortex"
EXOCORTEX_URL = "https://docs.google.com/spreadsheets/d/ВАШ_ID_ТАБЛИЦЫ/"
LOG_FILE_PATH = 'session_log.txt'

def main():
    """Основная функция скрипта."""
    logging.info("--- 🧠 Запуск логгера Экзокортекса ---")

    # 1. Чтение файла с диалогом
    try:
        with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f:
            session_text = f.read()
        if not session_text.strip():
            logging.warning("Файл session_log.txt пуст. Запись не требуется.")
            return
    except FileNotFoundError:
        logging.error(f"Файл лога не найден: {LOG_FILE_PATH}")
        return

    # 2. Подключение к Google Sheets
    try:
        creds = Credentials.from_service_account_file(GSHEETS_CREDS, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(EXOCORTEX_URL)
        dailies_sheet = spreadsheet.worksheet('Dailies')
    except Exception as e:
        logging.error(f"Ошибка подключения к Google Sheets: {e}")
        return

    # 3. Запись данных
    try:
        today_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Добавляем новую строку с датой и полным текстом диалога
        dailies_sheet.append_row([today_str, session_text])
        logging.info(f"✅ Сессия от {today_str} успешно записана в 'Dailies'.")
    except Exception as e:
        logging.error(f"Ошибка записи данных в лист 'Dailies': {e}")

    logging.info("--- ✅ Работа логгера завершена ---")

if __name__ == "__main__":
    main()