# main_runner.py
# Версия: 1.2 (с корректным выходом при ошибке)

import logging
import sys # <-- ДОБАВЛЕНО
from datetime import datetime

try:
    from data_harvesters import main_history_updater
    from technical_analyzer import main_analyzer
    from alerter import main_alerter
except ImportError as e:
    print(f"Критическая ошибка: не удалось импортировать модули. Ошибка: {e}")
    sys.exit(1) # <-- ИЗМЕНЕНО

# ... (блок логирования остается без изменений) ...
LOG_FILE = 'asipm_main_log.txt'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def run_pipeline():
    """Основной конвейер для запуска всех этапов обработки данных."""
    logging.info("="*20 + " ЗАПУСК КОНВЕЙЕРА ASIPM-AI " + "="*20)

    try:
        logging.info("--- Этап 1: Запуск Сборщика Данных ---")
        main_history_updater()
        logging.info("--- Этап 1: Сборщик Данных УСПЕШНО завершил работу. ---")
    except Exception as e:
        logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА на этапе Сбора Данных: {e}", exc_info=True)
        logging.info("="*20 + " КОНВЕЙЕР ОСТАНОВЛЕН ИЗ-ЗА ОШИБКИ " + "="*20)
        sys.exit(1) # <-- ИЗМЕНЕНО

    try:
        logging.info("--- Этап 2: Запуск Технического Анализатора ---")
        main_analyzer()
        logging.info("--- Этап 2: Технический Анализатор УСПЕШНО завершил работу. ---")
    except Exception as e:
        logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА на этапе Анализа Данных: {e}", exc_info=True)
        logging.info("="*20 + " КОНВЕЙЕР ОСТАНОВЛЕН ИЗ-ЗА ОШИБКИ " + "="*20)
        sys.exit(1) # <-- ИЗМЕНЕНО

    try:
        logging.info("--- Этап 3: Запуск Алертера ---")
        main_alerter()
        logging.info("--- Этап 3: Алертер УСПЕШНО завершил работу. ---")
    except Exception as e:
        logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА на этапе Отправки Алертов: {e}", exc_info=True)
        logging.info("="*20 + " КОНВЕЙЕР ОСТАНОВЛЕН ИЗ-ЗА ОШИБКИ " + "="*20)
        sys.exit(1) # <-- ИЗМЕНЕНО

    logging.info("="*20 + " КОНВЕЙЕР ASIPM-AI УСПЕШНО ЗАВЕРШЕН " + "="*20)


if __name__ == "__main__":
    run_pipeline()