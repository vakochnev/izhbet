"""
Интегрированный конформный предиктор для обработки прогнозов.
"""
import logging
from typing import List, Optional, Dict, Any
import pandas as pd
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor

from forecast.neural_conformal import NeuralConformalPredictor, NeuralConformalAnalyzer
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

def process_tournament_conformal(tournament_id: int, conformal_predictor: NeuralConformalPredictor) -> str:
    """
    Обрабатывает конформное прогнозирование для одного чемпионата.
    
    Args:
        tournament_id: ID чемпионата
        conformal_predictor: Обученный конформный предиктор
        
    Returns:
        str: Результат обработки
    """
    try:
        logger.info(f'Обработка конформного прогнозирования для чемпионата {tournament_id}')
        
        with Session_pool() as db_session:
            # Загружаем прогнозы для обработки
            predictions = get_predictions_for_tournament(db_session, tournament_id)
            
            # Безопасная проверка на пустой DataFrame
            if predictions is None or len(predictions) == 0:
                logger.warning(f'Нет прогнозов для чемпионата {tournament_id}')
                return f'Нет прогнозов для чемпионата {tournament_id}'
            
            # Создаем анализатор
            analyzer = NeuralConformalAnalyzer(db_session, conformal_predictor)
            
            # Обрабатываем каждый прогноз
            successful_predictions = 0
            failed_predictions = 0
            
            for idx, prediction_dict in enumerate(predictions):
                try:
                    # Анализируем прогноз с конформными интервалами
                    result = analyzer.analyze_prediction(prediction_dict)
                    
                    if 'error' not in result:
                        # Сохраняем результат в таблицу outcomes и интегрируем в statistics
                        if save_conformal_outcome_with_statistics(db_session, result):
                            successful_predictions += 1
                        else:
                            failed_predictions += 1
                    else:
                        failed_predictions += 1
                        
                except Exception as e:
                    failed_predictions += 1
                    logger.error(f'Ошибка при обработке прогноза {idx + 1}: {e}')
                    continue
            
            result_msg = f'Чемпионат {tournament_id}: успешно {successful_predictions}, ошибок {failed_predictions}'
            logger.info(result_msg)
            return result_msg
            
    except Exception as e:
        error_msg = f'Ошибка при обработке чемпионата {tournament_id}: {e}'
        logger.error(error_msg)
        return error_msg


class ConformalPredictor:
    """
    Интегрированный конформный предиктор для обработки прогнозов.
    """
    
    def __init__(self, confidence_level: float = 0.95):
        self.confidence_level = confidence_level
        self.conformal_predictor = None
        self.is_trained = False

    def train_conformal_predictor(self) -> bool:
        """
        Обучает конформный предиктор на основе существующих прогнозов.
        
        Returns:
            bool: True если обучение успешно, False иначе
        """
        logger.info('Обучение конформного предиктора на основе существующих прогнозов')
        
        try:
            # Загружаем данные для обучения
            predictions = get_training_predictions()
            targets = get_training_targets()
            
            # Безопасная проверка на пустые DataFrame
            predictions_empty = predictions is None or len(predictions) == 0
            targets_empty = targets is None or len(targets) == 0
            
            if predictions_empty or targets_empty:
                logger.warning(f'Недостаточно данных для обучения (прогнозы: {len(predictions) if predictions is not None else 0}, таргеты: {len(targets) if targets is not None else 0})')
                return False
            
            # Приводим данные к формату pandas.DataFrame
            try:
                # predictions уже DataFrame, используем его напрямую
                predictions_df = predictions.copy()
                
                # targets уже DataFrame, используем его напрямую
                outcomes_df = targets.copy()
                
                logger.info(f'Подготовлены данные для обучения: {len(predictions_df)} прогнозов, {len(outcomes_df)} исходов')
            except Exception as conv_err:
                logger.error(f'Ошибка приведения обучающих данных к DataFrame: {conv_err}')
                return False

            # Создаем конформный предиктор
            self.conformal_predictor = NeuralConformalPredictor(self.confidence_level)
            
            # Обучаем предиктор
            self.conformal_predictor.fit(predictions_df, outcomes_df)
            self.is_trained = True
            
            logger.info('Конформный предиктор успешно обучен')
            return True
            
        except Exception as e:
            logger.error(f'Ошибка при обучении конформного предиктора: {e}')
            self.is_trained = False
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
        
        try:
            # Используем многопроцессорную обработку
            results = self._process_tournaments_parallel(tournament_ids)
            
            # Анализируем результаты
            successful = sum(1 for r in results if "успешно" in r and "ошибок 0" in r)
            failed = len(results) - successful
            
            logger.info(f'Конформное прогнозирование завершено. Успешно: {successful}, Ошибок: {failed}')
            
            return {
                'success': True,
                'total_tournaments': len(tournament_ids),
                'successful': successful,
                'failed': failed,
                'results': results
            }
            
        except Exception as e:
            logger.error(f'Ошибка при создании конформных прогнозов: {e}')
            return {'success': False, 'error': str(e)}

    def _process_tournaments_parallel(self, tournament_ids: List[int]) -> List[str]:
        """
        Обрабатывает чемпионаты параллельно.
        
        Args:
            tournament_ids: Список ID чемпионатов
            
        Returns:
            List[str]: Результаты обработки
        """
        logger.info(f'Запуск многопоточной обработки {len(tournament_ids)} чемпионатов')
        
        def _worker(tid: int) -> str:
            return process_tournament_conformal(tid, self.conformal_predictor)
        
        with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            results = list(executor.map(_worker, tournament_ids))
        
        return results

    def get_tournament_ids(self) -> List[int]:
        """
        Получает список ID чемпионатов с прогнозами для обработки.
        
        Returns:
            List[int]: Список ID чемпионатов
        """
        return get_tournament_ids_with_predictions()
