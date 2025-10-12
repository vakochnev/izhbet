"""
Командная строка для публикации конформных прогнозов.
Обрабатывает аргументы командной строки и валидирует входные данные.
"""

import logging
import sys
from typing import Optional, Tuple

from core.constants import TIME_FRAME

logger = logging.getLogger(__name__)


class PublisherCLI:
    """Обработчик командной строки для публикации конформных прогнозов."""

    @staticmethod
    def show_help():
        """Показывает справку по использованию."""
        help_text = """
Использование: python publisher.py [TIME_FRAME] [YEAR]

Параметры:
  TIME_FRAME    Временной диапазон для публикации:
                - TODAY     : Прогнозы на сегодня + итоги за вчера
                - ALL_TIME  : Прогнозы и итоги за весь период
                
  YEAR          Год турнира (опционально)
                - Если не указан, используется значение по умолчанию

Примеры:
  python publisher.py TODAY
  python publisher.py ALL_TIME
  python publisher.py TODAY 2025
  python publisher.py ALL_TIME 2025

Справка:
  python publisher.py -h
  python publisher.py --help
  python publisher.py help
        """
        print(help_text)

    @staticmethod
    def get_time_frame_from_args() -> str:
        """Получает параметр TIME_FRAME из аргументов командной строки.
        
        Returns:
            str: Выбранный временной диапазон (TODAY по умолчанию)
        """
        try:
            time_frame = sys.argv[1]
            
            # Обработка справки
            if time_frame in ['-h', '--help', 'help']:
                PublisherCLI.show_help()
                sys.exit(0)
            
            if time_frame not in TIME_FRAME:
                logger.warning(f'Неизвестный параметр времени: {time_frame}. Используется TODAY')
                return 'TODAY'
            return time_frame
        except IndexError:
            logger.info('Параметр времени не указан. Используется TODAY')
            return 'TODAY'

    @staticmethod
    def get_year_from_args() -> Optional[str]:
        """Получает параметр года/даты из аргументов командной строки.
        
        Returns:
            str: Год турнира или дата (None если не указан)
        """
        try:
            year = sys.argv[2]
            logger.info(f'Указан параметр: {year}')
            return year
        except IndexError:
            logger.info('Параметр не указан. Используется значение по умолчанию')
            return None

    @staticmethod
    def parse_arguments() -> Tuple[str, Optional[str]]:
        """Парсит все аргументы командной строки.
        
        Returns:
            Tuple[str, Optional[str]]: (time_frame, year)
        """
        time_frame = PublisherCLI.get_time_frame_from_args()
        year = PublisherCLI.get_year_from_args()
        return time_frame, year

    @staticmethod
    def log_run_header(time_frame: str, year: Optional[str]) -> None:
        """Выводит единообразный заголовок запуска в логах.
        
        Args:
            time_frame: Режим работы
            year: Год или дата (опционально)
        """
        logger.info('='*60)
        logger.info('ЗАПУСК ПУБЛИКАЦИИ КОНФОРМНЫХ ПРОГНОЗОВ')
        logger.info(f'Режим работы: {time_frame}')
        
        if year:
            if time_frame in ['QUALITY', 'QUALITY_OUTCOMES']:
                logger.info(f'Дата: {year}')
            elif time_frame == 'FUNNEL':
                logger.info(f'Год анализа: {year}')
            else:
                logger.info(f'Год турнира: {year}')
        else:
            if time_frame == 'QUALITY':
                logger.info('Дата: сегодня')
            elif time_frame == 'QUALITY_OUTCOMES':
                logger.info('Дата: вчера')
            elif time_frame == 'FUNNEL':
                logger.info('Период анализа: последние 90 дней')
            else:
                logger.info('Год турнира: текущий сезон')
        
        from datetime import datetime
        logger.info(f'Время запуска: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        logger.info('='*60)

    @staticmethod
    def log_completion() -> None:
        """Выводит сообщение о завершении работы."""
        from datetime import datetime
        logger.info('='*60)
        logger.info('ПУБЛИКАЦИЯ КОНФОРМНЫХ ПРОГНОЗОВ ЗАВЕРШЕНА УСПЕШНО')
        logger.info(f'Время завершения: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        logger.info('='*60)

    @staticmethod
    def log_error(error: Exception) -> None:
        """Выводит сообщение об ошибке.
        
        Args:
            error: Исключение, которое произошло
        """
        logger.error(f'Ошибка при публикации конформных прогнозов: {error}')
        logger.error('='*60)
