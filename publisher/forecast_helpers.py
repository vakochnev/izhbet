"""
Вспомогательные функции для работы с типами и описаниями прогнозов.
"""

import logging
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)


# Маппинг feature -> forecast_type
FEATURE_TYPE_MAPPING = {
    1: 'WIN_DRAW_LOSS',
    2: 'OZ',
    3: 'GOAL_HOME',
    4: 'GOAL_AWAY',
    5: 'TOTAL',
    6: 'TOTAL_HOME',
    7: 'TOTAL_AWAY',
    8: 'TOTAL_AMOUNT',
    9: 'TOTAL_HOME_AMOUNT',
    10: 'TOTAL_AWAY_AMOUNT'
}

# Описания типов прогнозов
TYPE_DESCRIPTIONS = {
    'WIN_DRAW_LOSS': 'WIN_DRAW_LOSS',
    'OZ': 'OZ (Обе забьют)',
    'GOAL_HOME': 'GOAL_HOME (Гол хозяев)',
    'GOAL_AWAY': 'GOAL_AWAY (Гол гостей)',
    'TOTAL': 'TOTAL (Общий тотал)',
    'TOTAL_HOME': 'TOTAL_HOME (Тотал хозяев)',
    'TOTAL_AWAY': 'TOTAL_AWAY (Тотал гостей)',
    'TOTAL_AMOUNT': 'TOTAL_AMOUNT (Общий тотал)',
    'TOTAL_HOME_AMOUNT': 'TOTAL_HOME_AMOUNT (Тотал хозяев)',
    'TOTAL_AWAY_AMOUNT': 'TOTAL_AWAY_AMOUNT (Тотал гостей)'
}


def get_feature_type(feature: int) -> str:
    """
    Получает тип прогноза по коду feature.
    
    Args:
        feature: Код feature (1-10)
        
    Returns:
        str: Тип прогноза или 'Unknown Feature X'
    """
    return FEATURE_TYPE_MAPPING.get(feature, f'Unknown Feature {feature}')


def get_forecast_type_subtype(feature: int, outcome: str) -> Tuple[str, str]:
    """
    Получает тип и подтип прогноза из feature кода и outcome.
    
    Args:
        feature: Код feature (1-10)
        outcome: Outcome из таблицы (например, "п1", "обе забьют - да")
        
    Returns:
        tuple: (forecast_type, forecast_subtype)
    """
    try:
        forecast_type = get_feature_type(feature)
        
        if forecast_type.startswith('Unknown'):
            return (forecast_type, 'UNKNOWN')
        
        # Определяем подтип на основе outcome
        if outcome and outcome != 'Unknown':
            outcome_lower = outcome.lower().strip()
            
            # WIN_DRAW_LOSS (feature 1)
            if feature == 1:
                if 'п1' in outcome_lower:
                    return (forecast_type, 'П1')
                elif 'х' in outcome_lower:
                    return (forecast_type, 'X')
                elif 'п2' in outcome_lower:
                    return (forecast_type, 'П2')
            
            # OZ (feature 2)
            elif feature == 2:
                if 'да' in outcome_lower:
                    return (forecast_type, 'ДА')
                elif 'нет' in outcome_lower:
                    return (forecast_type, 'НЕТ')
            
            # GOAL_HOME (feature 3)
            elif feature == 3:
                if 'да' in outcome_lower:
                    return (forecast_type, 'ДА')
                elif 'нет' in outcome_lower:
                    return (forecast_type, 'НЕТ')
            
            # GOAL_AWAY (feature 4)
            elif feature == 4:
                if 'да' in outcome_lower:
                    return (forecast_type, 'ДА')
                elif 'нет' in outcome_lower:
                    return (forecast_type, 'НЕТ')
            
            # TOTAL (feature 5)
            elif feature == 5:
                if 'тб' in outcome_lower or 'больше' in outcome_lower:
                    return (forecast_type, 'БОЛЬШЕ')
                elif 'тм' in outcome_lower or 'меньше' in outcome_lower:
                    return (forecast_type, 'МЕНЬШЕ')
            
            # TOTAL_HOME (feature 6)
            elif feature == 6:
                if 'ит1б' in outcome_lower or 'больше' in outcome_lower:
                    return (forecast_type, 'БОЛЬШЕ')
                elif 'ит1м' in outcome_lower or 'меньше' in outcome_lower:
                    return (forecast_type, 'МЕНЬШЕ')
            
            # TOTAL_AWAY (feature 7)
            elif feature == 7:
                if 'ит2б' in outcome_lower or 'больше' in outcome_lower:
                    return (forecast_type, 'БОЛЬШЕ')
                elif 'ит2м' in outcome_lower or 'меньше' in outcome_lower:
                    return (forecast_type, 'МЕНЬШЕ')
            
            # Регрессионные прогнозы (8, 9, 10)
            elif feature in [8, 9, 10]:
                return (forecast_type, outcome.upper())
        
        # Если не удалось определить подтип
        return (forecast_type, outcome.upper() if outcome else 'UNKNOWN')
        
    except Exception as e:
        logger.error(f'Ошибка при определении типа прогноза для feature {feature}, outcome {outcome}: {e}')
        return (f'Unknown Feature {feature}', 'UNKNOWN')


def get_feature_description(feature: int, outcome: str) -> str:
    """
    Получает описание прогноза по feature коду и outcome.
    
    Args:
        feature: Код feature (1-10)
        outcome: Outcome из таблицы
        
    Returns:
        str: Описание прогноза
    """
    try:
        forecast_type, forecast_subtype = get_forecast_type_subtype(feature, outcome)
        
        if forecast_type.startswith('Unknown'):
            return f'Unknown Feature {feature}'
        
        description = TYPE_DESCRIPTIONS.get(forecast_type, forecast_type)
        
        # Добавляем подтип
        if forecast_subtype and forecast_subtype != 'UNKNOWN':
            description += f': {forecast_subtype}'
        
        return description
        
    except Exception as e:
        logger.error(f'Ошибка при получении описания feature {feature} с outcome {outcome}: {e}')
        return f'Feature {feature}'


def get_empty_statistics() -> Dict[str, Any]:
    """
    Возвращает пустую статистику при отсутствии данных.
    
    Returns:
        Dict: Словарь с дефолтными значениями метрик
    """
    return {
        'calibration': 75.0,
        'stability': 80.0,
        'confidence': 75.0,
        'uncertainty': 25.0,
        'lower_bound': 0.5,
        'upper_bound': 0.9,
        'historical_correct': 0,
        'historical_total': 0,
        'historical_accuracy': 0.0,
        'recent_correct': 0,
        'recent_accuracy': 0.0
    }

