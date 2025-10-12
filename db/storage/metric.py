# izhbet/db/storage/metric.py

import logging
from datetime import datetime
from typing import Any, Dict

from db.models import Metric
from config import Session_pool, Session
from db.base import DBSession

logger = logging.getLogger(__name__)


def save_metrics(
        championship_id: int,
        championship_name: str,
        payload: Dict[str, Any]
) -> None:
    """
    Сохранение метрик обучения модели в базу данных (таблица metrics).
    Использует upsert логику - обновляет существующую запись или создает новую.

    Args:
        championship_id: ID чемпионата
        championship_name: Название чемпионата
        payload: Данные метрик обучения, словарь с model_name, metrics, model_info
    """
    try:
        model_name: str = None
        model_type: str = None
        accuracy = precision = recall = f1_score = None
        precision_binary = recall_binary = f1_binary = None

        if isinstance(payload, dict):
            model_name = payload.get('model_name')
            metrics_block = payload.get('metrics', {})
            model_info = payload.get('model_info', {})
            model_type = (model_info or {}).get('model_type')
            
            # Метрики для классификации
            accuracy = (metrics_block or {}).get('accuracy')
            precision = (metrics_block or {}).get('precision')
            recall = (metrics_block or {}).get('recall')
            f1_score = (metrics_block or {}).get('f1_score')
            precision_binary = (metrics_block or {}).get('precision_binary')
            recall_binary = (metrics_block or {}).get('recall_binary')
            f1_binary = (metrics_block or {}).get('f1_binary')
            
            # Метрики для регрессии
            mae = (metrics_block or {}).get('mae')
            mse = (metrics_block or {}).get('mse')
            rmse = (metrics_block or {}).get('rmse')
            r2 = (metrics_block or {}).get('r2')
            min_error = (metrics_block or {}).get('min_error')
            max_error = (metrics_block or {}).get('max_error')

        # Формируем kwargs только по существующим в модели колонкам
        model_columns = set(getattr(Metric, '__table__').columns.keys())
        candidate_kwargs = {
            'championship_id': championship_id,
            'championship_name': championship_name,
            'model_name': model_name,
            'model_type': model_type,
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'precision_binary': precision_binary,
            'recall_binary': recall_binary,
            'f1_binary': f1_binary,
            'mae': mae,
            'mse': mse,
            'rmse': rmse,
            'r2': r2,
            'min_error': min_error,
            'max_error': max_error,
            'training_date': datetime.now(),
        }

        filtered_kwargs = {k: v for k, v in candidate_kwargs.items() if k in model_columns}

        with Session_pool() as session:
            db_session = DBSession(session)
            
            # Ищем существующую запись по championship_id, model_name, model_type
            existing_metric = db_session.query(Metric).filter(
                Metric.championship_id == championship_id,
                Metric.model_name == model_name,
                Metric.model_type == model_type
            ).first()
            
            if existing_metric:
                # Обновляем существующую запись
                for key, value in filtered_kwargs.items():
                    if key not in ['id', 'created_at']:  # Не обновляем ID и created_at
                        setattr(existing_metric, key, value)
                existing_metric.updated_at = datetime.now()
                logger.info(f"Метрики обучения обновлены для чемпионата {championship_name}, модели {model_name}")
            else:
                # Создаем новую запись
                training_metrics = Metric(**filtered_kwargs)
                db_session.add_model(training_metrics)
                logger.info(f"Метрики обучения созданы для чемпионата {championship_name}, модели {model_name}")
            
            db_session.commit()

    except Exception as e:
        logger.error(f"Ошибка сохранения метрик обучения: {e}")


