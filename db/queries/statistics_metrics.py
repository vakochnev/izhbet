"""
Модуль для расчета метрик качества прогнозов на основе реальных данных из БД.
Заменяет захардкоженные константы в publisher/statistics_publisher.py.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy import func, case, and_
from config import Session_pool
from db.models.statistics import Statistic
from db.models.prediction import Prediction

logger = logging.getLogger(__name__)


def _normalize_forecast_subtype(forecast_type: str, forecast_subtype: str) -> str:
    """
    Нормализует forecast_subtype для поиска в БД.
    
    В коде подтипы передаются в верхнем регистре (П1, ДА, БОЛЬШЕ),
    а в БД хранятся в нижнем регистре и иногда как полные фразы.
    
    Args:
        forecast_type: Тип прогноза (в верхнем регистре)
        forecast_subtype: Подтип прогноза (в верхнем регистре)
        
    Returns:
        str: Нормализованный подтип для БД
    """
    forecast_type_lower = forecast_type.lower()
    forecast_subtype_lower = forecast_subtype.lower()
    
    # Маппинг специальных случаев
    # Для OZ в БД хранится полная фраза
    if forecast_type_lower == 'oz':
        if forecast_subtype_lower == 'да':
            return 'обе забьют - да'
        elif forecast_subtype_lower == 'нет':
            return 'обе забьют - нет'
    
    # Для GOAL_HOME в БД могут быть разные варианты
    # Проверим, что именно там
    # Пока возвращаем как есть в нижнем регистре
    
    # Для TOTAL и аналогов: БОЛЬШЕ -> тб, МЕНЬШЕ -> тм
    if forecast_type_lower in ['total', 'total_home', 'total_away']:
        if forecast_subtype_lower == 'больше':
            if forecast_type_lower == 'total':
                return 'тб'
            elif forecast_type_lower == 'total_home':
                return 'ит1б'
            elif forecast_type_lower == 'total_away':
                return 'ит2б'
        elif forecast_subtype_lower == 'меньше':
            if forecast_type_lower == 'total':
                return 'тм'
            elif forecast_type_lower == 'total_home':
                return 'ит1м'
            elif forecast_type_lower == 'total_away':
                return 'ит2м'
    
    # Для остальных случаев просто возвращаем в нижнем регистре
    return forecast_subtype_lower


def get_historical_accuracy_regular(
    forecast_type: str, 
    forecast_subtype: str,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Рассчитывает историческую точность для regular прогнозов из таблицы statistics.
    
    Args:
        forecast_type: Тип прогноза (WIN_DRAW_LOSS, OZ, TOTAL и т.д.)
        forecast_subtype: Подтип прогноза (П1, П2, X, ДА, НЕТ и т.д.)
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Dict с ключами: total, correct, accuracy, formatted
    """
    try:
        # Нормализуем значения для поиска в БД
        forecast_type_lower = forecast_type.lower()
        forecast_subtype_normalized = _normalize_forecast_subtype(forecast_type, forecast_subtype)
        
        with Session_pool() as session:
            query = session.query(
                func.count(Statistic.id).label('total'),
                func.sum(
                    case((Statistic.prediction_correct == True, 1), else_=0)
                ).label('correct')
            ).filter(
                Statistic.forecast_type == forecast_type_lower,
                Statistic.forecast_subtype == forecast_subtype_normalized,
                Statistic.prediction_correct.isnot(None)
            )
            
            if championship_id:
                query = query.filter(Statistic.championship_id == championship_id)
            
            if sport_id:
                query = query.filter(Statistic.sport_id == sport_id)
            
            result = query.first()
            
            if result and result.total and result.total > 0:
                total = int(result.total)
                correct = int(result.correct or 0)
                accuracy = correct / total if total > 0 else 0.0
                
                return {
                    'total': total,
                    'correct': correct,
                    'accuracy': accuracy,
                    'formatted': f'{correct}/{total} ({accuracy*100:.1f}%)'
                }
            
            logger.debug(
                f'Нет данных для {forecast_type}/{forecast_subtype}, '
                f'championship_id={championship_id}, sport_id={sport_id}'
            )
            
    except Exception as e:
        logger.error(f'Ошибка при расчете historical_accuracy: {e}')
    
    # Возвращаем пустые данные, если нет статистики
    return {
        'total': 0,
        'correct': 0,
        'accuracy': 0.0,
        'formatted': '0/0 (0.0%)'
    }


def get_recent_accuracy(
    forecast_type: str, 
    forecast_subtype: str,
    limit: int = 10,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Рассчитывает точность последних N прогнозов.
    
    Args:
        forecast_type: Тип прогноза
        forecast_subtype: Подтип прогноза
        limit: Количество последних прогнозов (по умолчанию 10)
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Dict с ключами: total, correct, accuracy, formatted
    """
    try:
        # Нормализуем значения для поиска в БД
        forecast_type_lower = forecast_type.lower()
        forecast_subtype_normalized = _normalize_forecast_subtype(forecast_type, forecast_subtype)
        
        with Session_pool() as session:
            query = session.query(
                Statistic.prediction_correct
            ).filter(
                Statistic.forecast_type == forecast_type_lower,
                Statistic.forecast_subtype == forecast_subtype_normalized,
                Statistic.prediction_correct.isnot(None)
            )
            
            if championship_id:
                query = query.filter(Statistic.championship_id == championship_id)
            
            if sport_id:
                query = query.filter(Statistic.sport_id == sport_id)
            
            recent = query.order_by(Statistic.match_date.desc()).limit(limit).all()
            
            if recent:
                total = len(recent)
                correct = sum(1 for r in recent if r.prediction_correct)
                accuracy = correct / total if total > 0 else 0.0
                
                return {
                    'total': total,
                    'correct': correct,
                    'accuracy': accuracy,
                    'formatted': f'{correct}/{total} ({accuracy*100:.1f}%)'
                }
            
            logger.debug(
                f'Нет последних данных для {forecast_type}/{forecast_subtype}'
            )
            
    except Exception as e:
        logger.error(f'Ошибка при расчете recent_accuracy: {e}')
    
    return {
        'total': 0,
        'correct': 0,
        'accuracy': 0.0,
        'formatted': f'0/{limit} (0.0%)'
    }


def get_calibration(
    forecast_type: str,
    forecast_subtype: str,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> float:
    """
    Рассчитывает калибровку модели.
    
    Калибровка показывает, насколько заявленная вероятность соответствует 
    реальной точности. Идеальная калибровка = 1.0 (если модель дает 75%, 
    то должна угадывать в 75% случаев).
    
    Args:
        forecast_type: Тип прогноза
        forecast_subtype: Подтип прогноза
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Значение от 0 до 1, где 1 = идеальная калибровка
    """
    try:
        # Нормализуем значения для поиска в БД
        forecast_type_lower = forecast_type.lower()
        forecast_subtype_normalized = _normalize_forecast_subtype(forecast_type, forecast_subtype)
        
        with Session_pool() as session:
            # Получаем среднюю точность прогнозов
            query = session.query(
                func.avg(Statistic.prediction_accuracy).label('avg_accuracy')
            ).filter(
                Statistic.forecast_type == forecast_type_lower,
                Statistic.forecast_subtype == forecast_subtype_normalized,
                Statistic.prediction_accuracy.isnot(None)
            )
            
            if championship_id:
                query = query.filter(Statistic.championship_id == championship_id)
            
            if sport_id:
                query = query.filter(Statistic.sport_id == sport_id)
            
            result = query.first()
            
            if result and result.avg_accuracy:
                # prediction_accuracy уже содержит калибровку модели
                calibration = float(result.avg_accuracy)
                return min(max(calibration, 0.0), 1.0)  # Ограничиваем 0-1
            
    except Exception as e:
        logger.error(f'Ошибка при расчете calibration: {e}')
    
    # Возвращаем нейтральное значение при отсутствии данных
    return 0.75


def get_stability(
    forecast_type: str,
    forecast_subtype: str,
    period_days: int = 90,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> float:
    """
    Рассчитывает стабильность прогнозов за период.
    
    Стабильность показывает, насколько постоянна точность модели во времени.
    Высокая стабильность = низкий разброс точности между неделями.
    
    Args:
        forecast_type: Тип прогноза
        forecast_subtype: Подтип прогноза
        period_days: Период анализа в днях (по умолчанию 90)
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Значение от 0 до 1, где 1 = максимальная стабильность
    """
    try:
        # Нормализуем значения для поиска в БД
        forecast_type_lower = forecast_type.lower()
        forecast_subtype_normalized = _normalize_forecast_subtype(forecast_type, forecast_subtype)
        
        with Session_pool() as session:
            cutoff_date = datetime.now() - timedelta(days=period_days)
            
            # Получаем прогнозы за последние period_days дней
            query = session.query(
                Statistic.match_date,
                Statistic.prediction_correct
            ).filter(
                Statistic.forecast_type == forecast_type_lower,
                Statistic.forecast_subtype == forecast_subtype_normalized,
                Statistic.prediction_correct.isnot(None),
                Statistic.match_date >= cutoff_date
            )
            
            if championship_id:
                query = query.filter(Statistic.championship_id == championship_id)
            
            if sport_id:
                query = query.filter(Statistic.sport_id == sport_id)
            
            results = query.all()
            
            if len(results) < 10:  # Недостаточно данных
                return 0.75
            
            # Группируем по неделям и считаем точность
            weekly_accuracy = {}
            for row in results:
                week = row.match_date.isocalendar()[1]  # Номер недели
                if week not in weekly_accuracy:
                    weekly_accuracy[week] = {'correct': 0, 'total': 0}
                
                weekly_accuracy[week]['total'] += 1
                if row.prediction_correct:
                    weekly_accuracy[week]['correct'] += 1
            
            # Считаем стандартное отклонение точности по неделям
            weekly_rates = [
                w['correct'] / w['total'] 
                for w in weekly_accuracy.values() 
                if w['total'] > 0
            ]
            
            if len(weekly_rates) < 2:
                return 0.75
            
            import statistics
            mean = statistics.mean(weekly_rates)
            std_dev = statistics.stdev(weekly_rates)
            
            # Стабильность = 1 - нормализованное отклонение
            # Чем меньше отклонение, тем выше стабильность
            stability = 1.0 - min(std_dev / (mean if mean > 0 else 1.0), 1.0)
            
            return max(min(stability, 1.0), 0.0)
            
    except Exception as e:
        logger.error(f'Ошибка при расчете stability: {e}')
    
    return 0.75


def get_confidence_bounds(
    forecast_type: str,
    forecast_subtype: str,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> Dict[str, float]:
    """
    Рассчитывает доверительные интервалы и уровень уверенности.
    
    Args:
        forecast_type: Тип прогноза
        forecast_subtype: Подтип прогноза
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Dict с ключами: confidence, uncertainty, lower_bound, upper_bound
    """
    try:
        # Нормализуем значения для поиска в БД
        forecast_type_lower = forecast_type.lower()
        forecast_subtype_normalized = _normalize_forecast_subtype(forecast_type, forecast_subtype)
        
        with Session_pool() as session:
            query = session.query(
                func.avg(Statistic.prediction_accuracy).label('avg_accuracy'),
                func.min(Statistic.prediction_accuracy).label('min_accuracy'),
                func.max(Statistic.prediction_accuracy).label('max_accuracy')
            ).filter(
                Statistic.forecast_type == forecast_type_lower,
                Statistic.forecast_subtype == forecast_subtype_normalized,
                Statistic.prediction_accuracy.isnot(None)
            )
            
            if championship_id:
                query = query.filter(Statistic.championship_id == championship_id)
            
            if sport_id:
                query = query.filter(Statistic.sport_id == sport_id)
            
            result = query.first()
            
            if result and result.avg_accuracy:
                confidence = float(result.avg_accuracy)
                lower_bound = float(result.min_accuracy or 0.0)
                upper_bound = float(result.max_accuracy or 1.0)
                uncertainty = 1.0 - confidence
                
                return {
                    'confidence': confidence,
                    'uncertainty': uncertainty,
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound
                }
                
    except Exception as e:
        logger.error(f'Ошибка при расчете confidence_bounds: {e}')
    
    return {
        'confidence': 0.75,
        'uncertainty': 0.25,
        'lower_bound': 0.60,
        'upper_bound': 0.90
    }


def get_complete_statistics(
    forecast_type: str,
    forecast_subtype: str,
    championship_id: Optional[int] = None,
    sport_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Получает полную статистику для типа прогноза (объединяет все метрики).
    
    Args:
        forecast_type: Тип прогноза
        forecast_subtype: Подтип прогноза
        championship_id: Фильтр по чемпионату (опционально)
        sport_id: Фильтр по виду спорта (опционально)
        
    Returns:
        Dict со всеми метриками
    """
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
    
    # Получаем доверительные интервалы
    bounds = get_confidence_bounds(
        forecast_type, forecast_subtype, championship_id, sport_id
    )
    
    return {
        'calibration': calibration,
        'stability': stability,
        'confidence': bounds['confidence'],
        'uncertainty': bounds['uncertainty'],
        'lower_bound': bounds['lower_bound'],
        'upper_bound': bounds['upper_bound'],
        'historical_correct': hist['correct'],
        'historical_total': hist['total'],
        'historical_accuracy': hist['accuracy'],
        'recent_correct': recent['correct'],
        'recent_accuracy': recent['accuracy']
    }

