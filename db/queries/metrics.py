# izhbet/db/queries/metrics.py
"""
Запросы для работы с метриками моделей из таблицы metrics.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy import desc, func

from config import Session_pool
from db.models import Metric

logger = logging.getLogger(__name__)


def get_metrics_by_championship(
    championship_id: Optional[int] = None,
    days: int = 30
) -> List[Dict[str, Any]]:
    """
    Получение метрик по чемпионату за указанный период.
    
    Args:
        championship_id: ID чемпионата (если None - все чемпионаты)
        days: Количество дней для анализа
        
    Returns:
        Список метрик
    """
    results = []
    
    with Session_pool() as session:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            query = session.query(Metric).filter(
                Metric.training_date >= cutoff_date
            )
            
            if championship_id:
                query = query.filter(Metric.championship_id == championship_id)
            
            metrics_records = query.order_by(desc(Metric.training_date)).all()
            
            for record in metrics_records:
                results.append({
                    'id': record.id,
                    'championship_id': record.championship_id,
                    'championship_name': record.championship_name,
                    'model_name': record.model_name,
                    'model_type': record.model_type,
                    'accuracy': record.accuracy,
                    'precision': record.precision,
                    'recall': record.recall,
                    'f1_score': record.f1_score,
                    'precision_binary': record.precision_binary,
                    'recall_binary': record.recall_binary,
                    'f1_binary': record.f1_binary,
                    'mae': getattr(record, 'mae', None),
                    'mse': getattr(record, 'mse', None),
                    'rmse': getattr(record, 'rmse', None),
                    'r2': getattr(record, 'r2', None),
                    'min_error': getattr(record, 'min_error', None),
                    'max_error': getattr(record, 'max_error', None),
                    'training_date': record.training_date,
                    'created_at': record.created_at
                })
                
        except Exception as e:
            logger.error(f"Ошибка получения метрик по чемпионату: {e}")
    
    return results


def get_available_championships() -> List[Dict[str, Any]]:
    """
    Получение списка доступных чемпионатов с метриками.
    
    Returns:
        Список чемпионатов с базовой информацией
    """
    results = []
    
    with Session_pool() as session:
        try:
            # Получаем уникальные чемпионаты с последними метриками
            subquery = session.query(
                Metric.championship_id,
                Metric.championship_name,
                func.max(Metric.training_date).label('last_training')
            ).group_by(
                Metric.championship_id,
                Metric.championship_name
            ).subquery()
            
            query = session.query(
                subquery.c.championship_id,
                subquery.c.championship_name,
                subquery.c.last_training,
                func.count(Metric.id).label('total_metrics')
            ).join(
                Metric, 
                Metric.championship_id == subquery.c.championship_id
            ).group_by(
                subquery.c.championship_id,
                subquery.c.championship_name,
                subquery.c.last_training
            ).order_by(desc(subquery.c.last_training))
            
            records = query.all()
            
            for record in records:
                results.append({
                    'id': record.championship_id,
                    'name': record.championship_name,
                    'last_training': record.last_training,
                    'total_metrics': record.total_metrics
                })
                
        except Exception as e:
            logger.error(f"Ошибка получения списка чемпионатов: {e}")
    
    return results


def get_models_by_championship(championship_id: int) -> List[Dict[str, Any]]:
    """
    Получение моделей для конкретного чемпионата.
    
    Args:
        championship_id: ID чемпионата
        
    Returns:
        Список моделей с их метриками
    """
    results = []
    
    with Session_pool() as session:
        try:
            # Группируем по моделям и получаем последние метрики
            subquery = session.query(
                Metric.model_name,
                func.max(Metric.training_date).label('last_training')
            ).filter(
                Metric.championship_id == championship_id
            ).group_by(Metric.model_name).subquery()
            
            query = session.query(Metric).join(
                subquery,
                (Metric.model_name == subquery.c.model_name) &
                (Metric.training_date == subquery.c.last_training)
            ).filter(
                Metric.championship_id == championship_id
            )
            
            # Сортировка: классификация по accuracy desc, регрессия по mae asc
            from sqlalchemy import case
            query = query.order_by(
                case(
                    (Metric.model_type == 'classification', Metric.accuracy.desc()),
                    (Metric.model_type == 'regression', Metric.mae.asc()),
                    else_=Metric.accuracy.desc()
                )
            )
            
            records = query.all()
            
            for record in records:
                results.append({
                    'model_name': record.model_name,
                    'model_type': record.model_type,
                    'accuracy': record.accuracy,
                    'precision': record.precision,
                    'recall': record.recall,
                    'f1_score': record.f1_score,
                    'mae': getattr(record, 'mae', None),
                    'mse': getattr(record, 'mse', None),
                    'rmse': getattr(record, 'rmse', None),
                    'r2': getattr(record, 'r2', None),
                    'min_error': getattr(record, 'min_error', None),
                    'max_error': getattr(record, 'max_error', None),
                    'training_date': record.training_date
                })
                
        except Exception as e:
            logger.error(f"Ошибка получения моделей для чемпионата {championship_id}: {e}")
    
    return results


def get_metrics_history(
    championship_id: int,
    model_name: Optional[str] = None,
    days: int = 90
) -> List[Dict[str, Any]]:
    """
    Получение истории метрик для чемпионата и модели.
    
    Args:
        championship_id: ID чемпионата
        model_name: Имя модели (если None - все модели)
        days: Количество дней для анализа
        
    Returns:
        Список исторических метрик
    """
    results = []
    
    with Session_pool() as session:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            query = session.query(Metric).filter(
                Metric.championship_id == championship_id,
                Metric.training_date >= cutoff_date
            )
            
            if model_name:
                query = query.filter(Metric.model_name == model_name)
            
            records = query.order_by(Metric.training_date).all()
            
            for record in records:
                results.append({
                    'model_name': record.model_name,
                    'accuracy': record.accuracy,
                    'precision': record.precision,
                    'recall': record.recall,
                    'f1_score': record.f1_score,
                    'mae': getattr(record, 'mae', None),
                    'mse': getattr(record, 'mse', None),
                    'rmse': getattr(record, 'rmse', None),
                    'r2': getattr(record, 'r2', None),
                    'min_error': getattr(record, 'min_error', None),
                    'max_error': getattr(record, 'max_error', None),
                    'training_date': record.training_date
                })
                
        except Exception as e:
            logger.error(f"Ошибка получения истории метрик: {e}")
    
    return results


def get_championship_stats(championship_id: int) -> Dict[str, Any]:
    """
    Получение сводной статистики по чемпионату.
    
    Args:
        championship_id: ID чемпионата
        
    Returns:
        Словарь со статистикой
    """
    stats = {
        'total_models': 0,
        'total_predictions': 0,
        'avg_accuracy': 0.0,
        'best_model': 'N/A',
        'worst_model': 'N/A',
        'stability': 'Низкая'
    }
    
    with Session_pool() as session:
        try:
            # Получаем последние метрики для каждой модели
            subquery = session.query(
                Metric.model_name,
                func.max(Metric.training_date).label('last_training')
            ).filter(
                Metric.championship_id == championship_id
            ).group_by(Metric.model_name).subquery()
            
            query = session.query(Metric).join(
                subquery,
                (Metric.model_name == subquery.c.model_name) &
                (Metric.training_date == subquery.c.last_training)
            ).filter(
                Metric.championship_id == championship_id
            )
            
            records = query.all()
            
            if records:
                # Собираем метрики для всех моделей
                model_scores = []
                model_names = []
                
                for record in records:
                    if record.model_name:
                        model_names.append(record.model_name)
                        
                        # Для классификации используем accuracy
                        if record.accuracy is not None and record.accuracy > 0:
                            model_scores.append(record.accuracy)
                        # Для регрессии используем обратное значение MAE
                        elif hasattr(record, 'mae') and record.mae is not None and record.mae > 0:
                            # Адаптивная нормализация MAE в зависимости от типа модели
                            mae_value = record.mae
                            model_name = record.model_name or ''
                            
                            # Разные пороги нормализации для разных типов моделей
                            if 'total_amount' in model_name:
                                max_mae = 5.0
                            elif 'total_home' in model_name or 'total_away' in model_name:
                                max_mae = 3.0
                            else:
                                max_mae = 2.0
                            
                            # Нормализуем MAE к [0,1] с адаптивным порогом
                            normalized_mae = min(mae_value / max_mae, 1.0)
                            accuracy_score = 1 - normalized_mae
                            model_scores.append(accuracy_score)
                
                stats['total_models'] = len(records)
                stats['avg_accuracy'] = sum(model_scores) / len(model_scores) if model_scores else 0.0
                
                if model_scores and model_names:
                    # Находим лучшую и худшую модель
                    best_idx = model_scores.index(max(model_scores))
                    worst_idx = model_scores.index(min(model_scores))
                    stats['best_model'] = model_names[best_idx]
                    stats['worst_model'] = model_names[worst_idx]
                    
                    # Определяем стабильность
                    if len(model_scores) > 1:
                        std_dev = (sum([(x - stats['avg_accuracy']) ** 2 for x in model_scores]) / len(model_scores)) ** 0.5
                        if std_dev < 0.1:
                            stats['stability'] = 'Высокая'
                        elif std_dev < 0.2:
                            stats['stability'] = 'Средняя'
                        else:
                            stats['stability'] = 'Низкая'
                            
        except Exception as e:
            logger.error(f"Ошибка получения статистики чемпионата {championship_id}: {e}")
    
    return stats
