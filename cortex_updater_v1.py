# cortex_updater.py
# Версия: 1.0
# Назначение: Автоматизирует обновление базы знаний "Project_Exocortex"

import gspread
from google.oauth2.service_account import Credentials
import json
import logging
from datetime import datetime

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- КОНФИГУРАЦИЯ ---
GSHEETS_CREDS = 'credentials.json'
# ВАЖНО: Укажите URL вашего нового файла "Project_Exocortex"
EXOCORTEX_URL = "https://docs.google.com/spreadsheets/d/1lQbJvGKSuVjC09ui6v6Gc0npnO7HzuQzAefQBMWqTxU/"
LOG_FILE_PATH = 'session_log.txt'

def get_gsheets_client():
    """Подключается к Google Sheets и возвращает объект клиента."""
    try:
        creds = Credentials.from_service_account_file(GSHEETS_CREDS, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logging.error(f"Ошибка авторизации Google: {e}")
        return None

def read_session_log(file_path: str) -> str:
    """Читает текстовый файл с логом сессии."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logging.error(f"Файл лога не найден: {file_path}")
        return ""

def get_structured_data_from_ai(text: str) -> dict:
    """
    (ЗАГЛУШКА) Эта функция будет отправлять текст AI и получать структурированный JSON.
    Пока она возвращает тестовые данные.
    """
    logging.info("Отправка текста диалога AI для структурирования...")
    
    # В будущем здесь будет реальный вызов API LLM
    # Системный промпт будет примерно таким:
    # "Проанализируй диалог. Выдели: 1. Ключевые темы. 2. Принятые решения. 3. Новые идеи. 4. Извлеченные уроки. Верни результат в формате JSON."
    
    # Возвращаем тестовый JSON для отладки
    mock_response = {
        "themes": "Отладка Дашборда, Концепция 'Экзокортекса'",
        "decisions": [
            {"decision": "Перейти к созданию 'Экзокортекса'", "reason": "Потеря контекста и ошибки в GSheets"}
        ],
        "ideas": [
            {"idea": "AI-Приоритизатор для Todoist", "impact": 9, "effort": 6, "status": "К рассмотрению"}
        ],
        "knowledge": [
            {"category": "Google Sheets", "lesson": "Функция MAP не работает с QUERY внутри LAMBDA."}
        ]
    }
    logging.info("Получен структурированный ответ от AI.")
    return mock_response

def update_exocortex(data: dict, gsheet_client):
    """Обновляет листы в Google-таблице 'Project_Exocortex'."""
    try:
        spreadsheet = gsheet_client.open_by_url(EXOCORTEX_URL)
        today_str = datetime.now().strftime("%Y-%m-%d")

        # Обновляем лист Dailies
        dailies_sheet = spreadsheet.worksheet('Dailies')
        # Здесь мы бы добавили полный текст диалога, но для примера добавим только темы
        dailies_sheet.append_row([today_str, data.get("themes", "")])
        logging.info("Лист 'Dailies' обновлен.")
        
        # Добавляем другие обновления... (реализуем на следующих шагах)

        return True
    except Exception as e:
        logging.error(f"Ошибка записи в Google Sheets: {e}")
        return False

def main():
    """Основная функция скрипта."""
    logging.info("--- 🧠 Запуск обновления Экзокортекса 🧠 ---")
    
    session_text = read_session_log(LOG_FILE_PATH)
    if not session_text:
        return

    structured_data = get_structured_data_from_ai(session_text)
    
    gs_client = get_gsheets_client()
    if not gs_client:
        return
        
    update_exocortex(structured_data, gs_client)
    
    logging.info("--- ✅ Обновление Экзокортекса завершено ---")

if __name__ == "__main__":
    main()