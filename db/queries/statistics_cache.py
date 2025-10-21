"""
Кеширование статистики для ускорения публикации отчетов.
"""

import logging
from typing import Dict, Any, Optional
from functools import lru_cache

from db.queries.statistics_metrics import (
    get_historical_accuracy_regular,
    get_recent_accuracy,
    get_calibration,
    get_stability,
    get_confidence_bounds
)

logger = logging.getLogger(__name__)


# LRU кеш для статистики (maxsize=1024 - для ~100 типов прогнозов)
@lru_cache(maxsize=1024)
def get_complete_statistics_cached(
    forecast_type: str,
    forecast_subtype: str,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Кешированная версия get_complete_statistics.
    
    Кеш сбрасывается при перезапуске приложения.
    Для долгоживущих процессов используйте clear_statistics_cache().
    
    Args:
        forecast_type: Тип прогноза
        forecast_subtype: Подтип прогноза
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Dict со всеми метриками
    """
    try:
        # Получаем историческую точность
        hist = get_historical_accuracy_regular(
            forecast_type, forecast_subtype, championship_id, sport_id
        )
        
        # Получаем точность последних 10
        recent = get_recent_accuracy(
            forecast_type, forecast_subtype, 10, championship_id, sport_id
        )
        
        # Получаем калибровку
        calibration = get_calibration(
            forecast_type, forecast_subtype, championship_id, sport_id
        )
        
        # Получаем стабильность
        stability = get_stability(
            forecast_type, forecast_subtype, 90, championship_id, sport_id
        )
        
        # Получаем границы уверенности
        bounds = get_confidence_bounds(
            forecast_type, forecast_subtype, championship_id, sport_id
        )
        
        # Объединяем все метрики
        return {
            'calibration': calibration,
            'stability': stability,
            'confidence': bounds.get('confidence', 0.5),
            'uncertainty': bounds.get('uncertainty', 0.5),
            'lower_bound': bounds.get('lower_bound', 0.0),
            'upper_bound': bounds.get('upper_bound', 1.0),
            'historical_correct': hist.get('correct', 0),
            'historical_total': hist.get('total', 0),
            'historical_accuracy': hist.get('accuracy', 0.0),
            'recent_correct': recent.get('correct', 0),
            'recent_accuracy': recent.get('accuracy', 0.0)
        }
    except Exception as e:
        logger.error(f'Ошибка при получении статистики для {forecast_type}/{forecast_subtype}: {e}')
        return {
            'calibration': 0.75,
            'stability': 0.80,
            'confidence': 0.75,
            'uncertainty': 0.25,
            'lower_bound': 0.5,
            'upper_bound': 0.9,
            'historical_correct': 0,
            'historical_total': 0,
            'historical_accuracy': 0.0,
            'recent_correct': 0,
            'recent_accuracy': 0.0
        }


def clear_statistics_cache() -> None:
    """Очищает кеш статистики."""
    get_complete_statistics_cached.cache_clear()
    logger.info('Кеш статистики очищен')


def get_cache_info() -> Dict[str, Any]:
    """Возвращает информацию о кеше."""
    info = get_complete_statistics_cached.cache_info()
    return {
        'hits': info.hits,
        'misses': info.misses,
        'maxsize': info.maxsize,
        'currsize': info.currsize,
        'hit_rate': info.hits / (info.hits + info.misses) if (info.hits + info.misses) > 0 else 0.0
    }

