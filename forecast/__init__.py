# izhbet/forecast/__init__.py
"""
Модуль прогнозирования.

Содержит:
- conformal_publication: Конформные прогнозы с интервалами неопределенности
- forecast: Форматирование и валидация прогнозов
- conformal_predictor: Конформное прогнозирование (перенесено из processing)
- neural_conformal: Нейронное конформное прогнозирование (перенесено из processing)
- improved_conformal_predictor: Улучшенный предиктор (перенесено из processing)
"""

from .conformal_publication import ConformalForecastGenerator
from .forecast import ForecastFormatter
from .conformal_processor import ConformalProcessor

# Убираем импорты, которые вызывают циклические зависимости
# from .conformal_predictor import ConformalPredictor
# from .neural_conformal import NeuralConformalPredictor
# from .improved_conformal_predictor import ImprovedConformalPredictor

__all__ = [
    'ConformalForecastGenerator', 
    'ForecastFormatter',
    'ConformalProcessor'
]
