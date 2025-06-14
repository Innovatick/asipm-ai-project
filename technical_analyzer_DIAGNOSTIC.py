# technical_analyzer_DIAGNOSTIC.py
# Цель: Вывести состояние данных на каждом этапе обработки.
# Этот скрипт ничего не записывает в Google Sheets.

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from io import StringIO
from typing import Optional

print("--- 🔬 ASIPM-AI: ЗАПУСК ДИАГНОСТИКИ АНАЛИЗАТОРА 🔬 ---")

def get_worksheet(sheet_name: str) -> Optional[gspread.Worksheet]:
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file'])
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBYS_DhGNsTo-Dnph3g_H27aHQOoY0EOcmCIKarb7Zc/")
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"❌ Ошибка доступа к листу '{sheet_name}': {e}")
        return None

# --- ЭТАП 1: ЗАГРУЗКА ДАННЫХ ---
print("\n--- ЭТАП 1: ЗАГРУЗКА ДАННЫХ ---")
history_sheet = get_worksheet('History_OHLCV')
if not history_sheet:
    raise SystemExit("Не удалось загрузить лист History_OHLCV. Прерывание.")

history_records = history_sheet.get_all_records()
print(f"✅ Загружено {len(history_records)} записей из Google Sheets.")
print("Первые 3 записи в сыром виде (из gspread):")
print(history_records[:3])

# --- ЭТАП 2: СОЗДАНИЕ ПЕРВИЧНОГО DATAFRAME ---
print("\n--- ЭТАП 2: СОЗДАНИЕ ПЕРВИЧНОГО DATAFRAME ---")
history_df = pd.DataFrame(history_records)
print("✅ Первичный DataFrame создан.")
print("Типы данных (dtypes) СРАЗУ ПОСЛЕ создания DataFrame:")
print(history_df.dtypes)
print("\nПервые 5 строк первичного DataFrame (history_df.head()):")
print(history_df.head().to_markdown())

# --- ЭТАП 3: ПОПЫТКА ПРЕОБРАЗОВАНИЯ В ЧИСЛА ---
print("\n--- ЭТАП 3: ПОПЫТКА ПРЕОБРАЗОВАНИЯ В ЧИСЛА ---")
cols_to_process = ['Open', 'High', 'Low', 'Close', 'Volume']
for col in cols_to_process:
    if col in history_df.columns:
        print(f"  - Обрабатываю столбец '{col}'...")
        # Принудительно заменяем пустые строки на None, чтобы to_numeric их понял
        history_df[col] = history_df[col].replace('', None)
        history_df[col] = pd.to_numeric(history_df[col], errors='coerce')
    else:
        print(f"  - ⚠️ Столбец '{col}' не найден в DataFrame.")

print("✅ Цикл преобразования завершен.")
print("\nТипы данных (dtypes) ПОСЛЕ попытки преобразования:")
print(history_df.dtypes)
print("\nПервые 5 строк DataFrame ПОСЛЕ преобразования (history_df.head()):")
print(history_df.head().to_markdown())

# --- ЭТАП 4: ПРОВЕРКА ДАННЫХ ДЛЯ ОДНОГО ТИКЕРА ---
print("\n--- ЭТАП 4: ПРОВЕРКА ДАННЫХ ДЛЯ ОДНОГО ТИКЕРА (BZ=F) ---")
ticker_to_check = "BZ=F"
ticker_history = history_df[history_df['Ticker'] == ticker_to_check].copy()

print(f"✅ Отфильтровано {len(ticker_history)} строк для тикера {ticker_to_check}.")
print(f"Типы данных (dtypes) в отфильтрованном DataFrame для {ticker_to_check}:")
buffer = StringIO()
ticker_history.info(buf=buffer)
info_str = buffer.getvalue()
print(info_str)

print(f"\nПоследние 5 строк для {ticker_to_check} (перед расчетами):")
print(ticker_history.tail().to_markdown())

print("\n--- 🔬 ДИАГНОСТИКА ЗАВЕРШЕНА 🔬 ---")