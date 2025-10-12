# izhbet/publisher/simple_service.py
"""
Упрощенный сервис для публикации прогнозов из таблицы statistics.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from publisher.statistics_publisher import StatisticsPublisher

logger = logging.getLogger(__name__)


class SimplePublisherService:
    """
    Упрощенный сервис для публикации готовых прогнозов.
    
    Отвечает ТОЛЬКО за публикацию из таблицы statistics:
    - TODAY: публикация прогнозов на сегодня + итогов вчера
    - ALL_TIME: публикация статистики за весь период
    
    НЕ создает прогнозы - только публикует готовые данные!
    """
    
    def __init__(self):
        """Инициализация сервиса."""
        self.publisher = StatisticsPublisher()
        logger.info('Упрощенный сервис публикации инициализирован')
    
    def execute_today(self) -> None:
        """Выполняет режим TODAY - прогнозы на сегодня + итоги вчера."""
        logger.info('Выполнение режима TODAY')
        
        try:
            # Публикуем прогнозы на сегодня (качественные + обычные)
            self.publisher.publish_today_forecasts_and_outcomes()
            
            logger.info('Режим TODAY выполнен успешно - прогнозы и итоги опубликованы')
            
        except Exception as e:
            logger.error(f'Ошибка при выполнении режима TODAY: {e}')
            raise
    
    def execute_all_time(self, year: Optional[str] = None) -> None:
        """Выполняет режим ALL_TIME - публикация статистики за весь период."""
        logger.info(f'Выполнение режима ALL_TIME для года {year or "все время"}')
        
        try:
            # Публикуем статистику за весь период
            success = self.publisher.publish_all_time_statistics(year)
            
            if success:
                logger.info('Режим ALL_TIME выполнен успешно - статистика опубликована')
            else:
                logger.error('Ошибка при выполнении режима ALL_TIME')
                raise Exception('Не удалось опубликовать статистику за весь период')
            
        except Exception as e:
            logger.error(f'Ошибка при выполнении режима ALL_TIME: {e}')
            raise
    
    def execute_mode(self, time_frame: str, year: Optional[str] = None) -> None:
        """Выполняет указанный режим публикации.
        
        Args:
            time_frame: Режим работы (TODAY, ALL_TIME)
            year: Год или дата (опционально)
        """
        logger.info(f'Выполнение режима {time_frame} с параметром {year or "по умолчанию"}')
        
        if time_frame == 'TODAY':
            self.execute_today()
        elif time_frame == 'ALL_TIME':
            self.execute_all_time(year)
        else:
            raise ValueError(f'Неподдерживаемый режим: {time_frame}')


def create_simple_service() -> SimplePublisherService:
    """
    Создает и настраивает упрощенный сервис публикации.
    
    Returns:
        SimplePublisherService: Настроенный сервис
    """
    return SimplePublisherService()
