# izhbet/publisher.py
"""
Точка входа для публикации прогнозов из таблицы statistics.
Упрощенный launcher без сложной бизнес-логики.
"""

import sys
import logging
from typing import Optional

# Добавляем путь к модулям
sys.path.append('.')

from publisher.simple_service import SimplePublisherService
from publisher.cli import PublisherCLI

logger = logging.getLogger(__name__)


def main():
    """Точка входа в приложение."""
    try:
        # Создаем упрощенный сервис
        service = SimplePublisherService()
        
        # Парсим аргументы командной строки
        cli = PublisherCLI()
        time_frame = cli.get_time_frame_from_args()
        year = cli.get_year_from_args()
        
        # Выводим заголовок
        cli.log_run_header(time_frame, year)
        
        # Выполняем режим
        service.execute_mode(time_frame, year)
        
        # Выводим сообщение о завершении
        cli.log_completion()
        
    except Exception as e:
        logger.error(f'Критическая ошибка в main(): {e}')
        sys.exit(1)


if __name__ == "__main__":
    main()