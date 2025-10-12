# izhbet/forecast/conformal_processor.py
"""
Интегрированный процессор конформного прогнозирования.
Объединяет все компоненты конформного прогнозирования в единый интерфейс.
"""

import logging
from typing import List, Optional, Dict, Any
import pandas as pd

from .conformal_predictor import ConformalPredictor
from .neural_conformal import NeuralConformalPredictor
from .improved_conformal_predictor import ImprovedConformalPredictor
from db.queries.forecast import (
    get_tournament_ids_with_predictions,
    get_predictions_for_tournament,
    get_training_predictions,
    get_training_targets
)
from db.storage.forecast import save_conformal_outcome
from db.storage.statistic import save_conformal_outcome_with_statistics
from config import Session_pool

logger = logging.getLogger(__name__)


class ConformalProcessor:
    """
    Интегрированный процессор конформного прогнозирования.
    
    Объединяет все компоненты конформного прогнозирования:
    - ConformalPredictor - базовое конформное прогнозирование
    - NeuralConformalPredictor - нейронное конформное прогнозирование  
    - ImprovedConformalPredictor - улучшенный предиктор с ансамблем
    """
    
    def __init__(self, confidence_level: float = 0.95):
        self.confidence_level = confidence_level
        self.conformal_predictor = ConformalPredictor(confidence_level)
        self.improved_predictor = ImprovedConformalPredictor()
        self.is_trained = False
        
    def train_conformal_predictor(self) -> bool:
        """
        Обучает конформный предиктор на основе существующих прогнозов.
        
        Returns:
            bool: True если обучение успешно, False иначе
        """
        logger.info('Обучение интегрированного конформного предиктора')
        
        try:
            # Обучаем базовый конформный предиктор
            if not self.conformal_predictor.train_conformal_predictor():
                logger.error('Не удалось обучить базовый конформный предиктор')
                return False
            
            # Загружаем улучшенные модели (опционально)
            try:
                self.improved_predictor.load_models()
                logger.info('Улучшенные модели загружены')
            except Exception as e:
                logger.warning(f'Не удалось загрузить улучшенные модели: {e}')
                logger.info('Продолжаем без улучшенных моделей')
            
            self.is_trained = True
            logger.info('Интегрированный конформный предиктор успешно обучен')
            return True
            
        except Exception as e:
            logger.error(f'Ошибка при обучении интегрированного конформного предиктора: {e}')
            return False
    
    def create_conformal_predictions(self, tournament_ids: List[int]) -> Dict[str, Any]:
        """
        Создает конформные прогнозы для списка чемпионатов.
        
        Args:
            tournament_ids: Список ID чемпионатов для обработки
            
        Returns:
            Dict[str, Any]: Результаты обработки
        """
        if not self.is_trained:
            logger.error('Конформный предиктор не обучен. Сначала вызовите train_conformal_predictor()')
            return {'success': False, 'error': 'Предиктор не обучен'}
        
        if not tournament_ids:
            logger.warning('Список чемпионатов пуст')
            return {'success': False, 'error': 'Нет чемпионатов для обработки'}
        
        logger.info(f'Создание конформных прогнозов для {len(tournament_ids)} чемпионатов')

        # Делегируем базовому конформному предиктору, который сохраняет результаты в БД
        try:
            return self.conformal_predictor.create_conformal_predictions(tournament_ids)
        except Exception as e:
            logger.error(f'Ошибка делегирования созданию конформных прогнозов: {e}')
            return {'success': False, 'error': str(e)}
    
    def get_tournament_ids(self) -> List[int]:
        """
        Получает список ID турниров с прогнозами.
        
        Returns:
            List[int]: Список ID турниров
        """
        try:
            return get_tournament_ids_with_predictions()
        except Exception as e:
            logger.error(f'Ошибка при получении списка турниров: {e}')
            return []
    
    def process_season_conformal_forecasts(self, year: Optional[str] = None) -> bool:
        """
        Обрабатывает конформные прогнозы для сезона.
        
        Args:
            year: Год сезона (если None, используется текущий сезон)
            
        Returns:
            bool: True если обработка успешна, False иначе
        """
        try:
            logger.info(f'Обработка конформных прогнозов для сезона {year or "текущий"}')
            
            # Если предиктор не обучен, обучаем его
            if not self.is_trained:
                logger.info('Предиктор не обучен, начинаем обучение...')
                if not self.train_conformal_predictor():
                    logger.error('Не удалось обучить предиктор')
                    return False
            
            # Получаем список турниров
            tournament_ids = self.get_tournament_ids()
            
            if not tournament_ids:
                logger.warning('Нет турниров для обработки')
                return False
            
            # Создаем конформные прогнозы
            result = self.create_conformal_predictions(tournament_ids)
            
            if result.get('success', False):
                logger.info('Обработка конформных прогнозов для сезона завершена успешно')
                return True
            else:
                logger.error(f'Ошибка обработки конформных прогнозов для сезона: {result.get("error", "Неизвестная ошибка")}')
                return False
                
        except Exception as e:
            logger.error(f'Критическая ошибка при обработке конформных прогнозов для сезона: {e}')
            return False


def create_conformal_processor(confidence_level: float = 0.95) -> ConformalProcessor:
    """
    Создает и настраивает интегрированный процессор конформного прогнозирования.
    
    Args:
        confidence_level: Уровень доверия для конформного прогнозирования
        
    Returns:
        ConformalProcessor: Настроенный процессор
    """
    processor = ConformalProcessor(confidence_level)
    
    # Обучаем предиктор
    if not processor.train_conformal_predictor():
        logger.error('Не удалось создать интегрированный процессор конформного прогнозирования')
        return None
    
    return processor
