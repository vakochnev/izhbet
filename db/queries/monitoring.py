# izhbet/db/queries/monitoring.py
"""
Запросы для мониторинга моделей с поддержкой чемпионатов.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy import and_, desc, func, case, text
from sqlalchemy.sql.expression import cast
from sqlalchemy import Float

from db.base import DBSession
from db.models import (
    Prediction, Match, Feature, ChampionShip, Metric
)

logger = logging.getLogger(__name__)

def get_recent_predictions(lookback_period: timedelta) -> Dict[str, List[Dict]]:
    """
    Получение последних предсказаний за указанный период.

    Args:
        lookback_period: Период для поиска предсказаний

    Returns:
        Словарь с предсказаниями по моделям
    """
    results = {}

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - lookback_period

            # Получаем предсказания за указанный период
            predictions = (
                session.query(Prediction)
                .filter(Prediction.created_at >= cutoff_date)
                .order_by(desc(Prediction.created_at))
                .all()
            )

            # Группируем по имени модели
            for prediction in predictions:
                model_name = prediction.model_name
                if model_name not in results:
                    results[model_name] = []

                results[model_name].append({
                    'prediction_id': prediction.id,
                    'match_id': prediction.match_id,
                    'model_name': prediction.model_name,
                    'prediction': prediction.prediction,
                    'probability': prediction.probability,
                    'confidence': prediction.confidence,
                    'created_at': prediction.created_at
                })

        except Exception as e:
            logger.error(f"Ошибка получения предсказаний: {e}")

    return results

def get_actual_results(lookback_period: timedelta) -> Dict[int, Dict]:
    """
    Получение фактических результатов матчей за указанный период.

    Args:
        lookback_period: Период для поиска результатов

    Returns:
        Словарь с фактическими результатами по match_id
    """
    results = {}

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - lookback_period

            # Получаем матчи с результатами за указанный период
            matches = (
                session.query(Match)
                .filter(
                    and_(
                        Match.gameData >= cutoff_date,
                        Match.numOfHeadsHome.isnot(None),  # Есть результат
                        Match.numOfHeadsAway.isnot(None)
                    )
                )
                .all()
            )

            # Получаем фичи для этих матчей
            match_ids = [match.id for match in matches]
            features = (
                session.query(Feature)
                .filter(Feature.match_id.in_(match_ids))
                .all()
            )

            # Создаем словарь фич по match_id
            features_dict = {feature.match_id: feature for feature in features}

            # Формируем результаты
            for match in matches:
                feature = features_dict.get(match.id)
                if feature:
                    results[match.id] = {
                        'match_id': match.id,
                        'goals_home': match.numOfHeadsHome,
                        'goals_away': match.numOfHeadsAway,
                        'game_date': match.gameData,
                        'championship_id': match.championship_id
                    }

        except Exception as e:
            logger.error(f"Ошибка получения результатов матчей: {e}")

    return results

def get_model_performance_stats(
        model_name: str,
        days: int = 30,
        championship_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Получение статистики производительности модели.

    Args:
        model_name: Название модели
        days: Количество дней для анализа
        championship_id: ID чемпионата для фильтрации (опционально)

    Returns:
        Статистика производительности
    """
    stats = {}

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            # Базовый запрос
            query = session.query(func.count(Prediction.id)).filter(
                and_(
                    Prediction.model_name == model_name,
                    Prediction.created_at >= cutoff_date
                )
            )

            # Фильтр по чемпионату
            if championship_id:
                query = query.join(Match, Prediction.match_id == Match.id).filter(
                    Match.championship_id == championship_id
                )

            total_predictions = query.scalar()

            # Для моделей классификации считаем точность
            if model_name in ['win_draw_loss', 'oz', 'total', 'goal_home', 'goal_away']:
                correct_predictions = session.query(func.count(Prediction.id)).join(
                    Feature, Prediction.match_id == Feature.match_id
                ).join(Match, Prediction.match_id == Match.id).filter(
                    and_(
                        Prediction.model_name == model_name,
                        Prediction.created_at >= cutoff_date,
                        # Условия корректности для разных моделей
                        case([
                            (Prediction.model_name == 'win_draw_loss',
                            (Prediction.model_name == 'oz',
                            (Prediction.model_name == 'total',
                            (Prediction.model_name == 'goal_home',
                            (Prediction.model_name == 'goal_away',
                        ])
                    )
                )

                if championship_id:
                    correct_predictions = correct_predictions.filter(
                        Match.championship_id == championship_id
                    )

                correct_count = correct_predictions.scalar()
                accuracy = correct_count / total_predictions if total_predictions > 0 else 0

                stats.update({
                    'total_predictions': total_predictions,
                    'correct_predictions': correct_count,
                    'accuracy': accuracy
                })

            # Для регрессии считаем среднюю ошибку
            elif model_name in ['total_amount', 'total_home_amount', 'total_away_amount']:
                error_query = session.query(
                    func.avg(
                        func.abs(cast(Prediction.prediction, Float) -
                                 cast(getattr(Feature, f'feature_{model_name}'), Float))
                    )
                ).join(Feature, Prediction.match_id == Feature.match_id).join(
                    Match, Prediction.match_id == Match.id
                ).filter(
                    and_(
                        Prediction.model_name == model_name,
                        Prediction.created_at >= cutoff_date,
                        Prediction.prediction.isnot(None),
                        getattr(Feature, f'feature_{model_name}').isnot(None)
                    )
                )

                if championship_id:
                    error_query = error_query.filter(Match.championship_id == championship_id)

                mae = error_query.scalar() or 0

                stats.update({
                    'total_predictions': total_predictions,
                    'mae': mae
                })

        except Exception as e:
            logger.error(f"Ошибка получения статистики для модели {model_name}: {e}")

    return stats

def get_prediction_accuracy_by_model(
        days: int = 30,
        championship_id: Optional[str] = None
) -> Dict[str, float]:
    """
    Получение точности предсказаний по всем моделям.

    Args:
        days: Количество дней для анализа
        championship_id: ID чемпионата для фильтрации (опционально)

    Returns:
        Словарь с точностью по моделям
    """
    accuracy_stats = {}

    models = ['win_draw_loss', 'oz', 'total', 'total_amount',
              'goal_home', 'goal_away', 'total_home', 'total_away']

    for model_name in models:
        stats = get_model_performance_stats(model_name, days, championship_id)
        if 'accuracy' in stats:
            accuracy_stats[model_name] = stats['accuracy']
        elif 'mae' in stats:
            accuracy_stats[model_name] = stats['mae']

    return accuracy_stats

def get_recent_predictions_with_actuals(
        lookback_period: timedelta,
        model_name: Optional[str] = None,
        championship_id: Optional[str] = None
) -> List[Dict]:
    """
    Получение предсказаний с фактическими результатами.

    Args:
        lookback_period: Период для поиска
        model_name: Фильтр по модели (опционально)
        championship_id: Фильтр по чемпионату (опционально)

    Returns:
        Список предсказаний с результатами
    """
    results = []

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - lookback_period

            # Базовый запрос
            query = (
                session.query(Prediction, Match, Feature, ChampionShip)
                .join(Match, Prediction.match_id == Match.id)
                .join(Feature, Prediction.match_id == Feature.match_id)
                .join(ChampionShip, Match.championship_id == ChampionShip.id)
                .filter(
                    and_(
                        Prediction.created_at >= cutoff_date,
                        Match.numOfHeadsHome.isnot(None),
                        Match.numOfHeadsAway.isnot(None)
                    )
                )
            )

            if model_name:
                query = query.filter(Prediction.model_name == model_name)

            if championship_id:
                query = query.filter(Match.championship_id == championship_id)

            records = query.order_by(desc(Prediction.created_at)).all()

            for prediction, match, feature, championship in records:
                result = {
                    'prediction_id': prediction.id,
                    'match_id': prediction.match_id,
                    'model_name': prediction.model_name,
                    'predicted_value': prediction.prediction,
                    'probability': prediction.probability,
                    'confidence': prediction.confidence,
                    'actual_goals_home': match.numOfHeadsHome,
                    'actual_goals_away': match.numOfHeadsAway,
                    'game_date': match.gameData,
                    'created_at': prediction.created_at,
                    'championship_id': match.championship_id,
                    'championship_name': championship.championshipName
                }

                # Добавляем feature значения в зависимости от типа модели
                if prediction.model_name == 'win_draw_loss':

                elif prediction.model_name == 'oz':

                elif prediction.model_name == 'total':

                elif prediction.model_name == 'goal_home':

                elif prediction.model_name == 'goal_away':

                elif prediction.model_name == 'total_amount':
                    # Для регрессии считаем абсолютную ошибку
                    if (prediction.prediction is not None and
                        try:
                            result['absolute_error'] = abs(
                            )
                        except (ValueError, TypeError):
                            result['absolute_error'] = None

                elif prediction.model_name == 'total_home_amount':
                    if (prediction.prediction is not None and
                        try:
                            result['absolute_error'] = abs(
                            )
                        except (ValueError, TypeError):
                            result['absolute_error'] = None

                elif prediction.model_name == 'total_away_amount':
                    if (prediction.prediction is not None and
                        try:
                            result['absolute_error'] = abs(
                            )
                        except (ValueError, TypeError):
                            result['absolute_error'] = None

                results.append(result)

        except Exception as e:
            logger.error(f"Ошибка получения предсказаний с результатами: {e}")

    return results

def get_daily_accuracy_stats(
        days: int = 7,
        championship_id: Optional[str] = None
) -> Dict[str, List[Dict]]:
    """
    Получение ежедневной статистики точности.

    Args:
        days: Количество дней для анализа
        championship_id: ID чемпионата для фильтрации (опционально)

    Returns:
        Статистика точности по дням
    """
    daily_stats = {}

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            # Базовый запрос
            query = session.query(
                func.date(Prediction.created_at).label('prediction_date'),
                Prediction.model_name,
                func.count(Prediction.id).label('total_predictions'),
                func.sum(
                    case([
                        (and_(
                            Prediction.model_name == 'win_draw_loss',
                        ), 1),
                        (and_(
                            Prediction.model_name == 'oz',
                        ), 1),
                        (and_(
                            Prediction.model_name == 'total',
                        ), 1),
                        (and_(
                            Prediction.model_name == 'goal_home',
                        ), 1),
                        (and_(
                            Prediction.model_name == 'goal_away',
                        ), 1)
                    ], else_=0)
                ).label('correct_predictions')
            ).join(Feature, Prediction.match_id == Feature.match_id).join(
                Match, Prediction.match_id == Match.id
            ).filter(
                and_(
                    Prediction.created_at >= cutoff_date,
                    Match.numOfHeadsHome.isnot(None),
                    Match.numOfHeadsAway.isnot(None)
                )
            )

            # Фильтр по чемпионату
            if championship_id:
                query = query.filter(Match.championship_id == championship_id)

            results = query.group_by(
                func.date(Prediction.created_at),
                Prediction.model_name
            ).order_by(desc('prediction_date')).all()

            for date, model_name, total, correct in results:
                date_str = date.strftime('%Y-%m-%d')
                if date_str not in daily_stats:
                    daily_stats[date_str] = []

                accuracy = correct / total if total > 0 else 0

                daily_stats[date_str].append({
                    'model_name': model_name,
                    'total_predictions': total,
                    'correct_predictions': correct,
                    'accuracy': accuracy
                })

        except Exception as e:
            logger.error(f"Ошибка получения ежедневной статистики: {e}")

    return daily_stats

def get_model_usage_stats(
        days: int = 30,
        championship_id: Optional[str] = None
) -> Dict[str, int]:
    """
    Получение статистики использования моделей.

    Args:
        days: Количество дней для анализа
        championship_id: ID чемпионата для фильтрации (опционально)

    Returns:
        Количество предсказаний по моделям
    """
    usage_stats = {}

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            query = session.query(
                Prediction.model_name,
                func.count(Prediction.id).label('prediction_count')
            ).filter(Prediction.created_at >= cutoff_date)

            # Фильтр по чемпионату
            if championship_id:
                query = query.join(Match, Prediction.match_id == Match.id).filter(
                    Match.championship_id == championship_id
                )

            results = query.group_by(Prediction.model_name).order_by(
                desc('prediction_count')
            ).all()

            for model_name, count in results:
                usage_stats[model_name] = count

        except Exception as e:
            logger.error(f"Ошибка получения статистики использования: {e}")

    return usage_stats

def get_championship_list() -> List[Dict]:
    """
    Получение списка всех чемпионатов с активными предсказаниями.

    Returns:
        Список словарей с информацией о чемпионатах
    """
    championships = []

    with DBSession() as session:
        try:
            # Чемпионаты с предсказаниями за последние 30 дней
            cutoff_date = datetime.now() - timedelta(days=30)

            results = session.query(
                ChampionShip.id,
                ChampionShip.championshipName,
                func.count(Prediction.id).label('prediction_count')
            ).join(Match, ChampionShip.id == Match.championship_id).join(
                Prediction, Match.id == Prediction.match_id
            ).filter(
                Prediction.created_at >= cutoff_date
            ).group_by(
                ChampionShip.id, ChampionShip.championshipName
            ).order_by(desc('prediction_count')).all()

            for champ_id, champ_name, count in results:
                championships.append({
                    'id': champ_id,
                    'name': champ_name,
                    'prediction_count': count
                })

        except Exception as e:
            logger.error(f"Ошибка получения списка чемпионатов: {e}")

    return championships

def get_championship_performance_stats(
        championship_id: str,
        days: int = 30
) -> Dict[str, Any]:
    """
    Получение сводной статистики по чемпионату.

    Args:
        championship_id: ID чемпионата
        days: Количество дней для анализа

    Returns:
        Сводная статистика чемпионата
    """
    stats = {
        'championship_id': championship_id,
        'models': {},
        'summary': {}
    }

    # Получаем статистику по всем моделям для чемпионата
    models = ['win_draw_loss', 'oz', 'total', 'total_amount',
              'goal_home', 'goal_away', 'total_home', 'total_away']

    total_predictions = 0
    total_correct = 0
    model_stats = {}

    for model_name in models:
        model_stat = get_model_performance_stats(model_name, days, championship_id)
        if model_stat:
            model_stats[model_name] = model_stat
            total_predictions += model_stat.get('total_predictions', 0)
            total_correct += model_stat.get('correct_predictions', 0)

    # Рассчитываем общую точность
    overall_accuracy = total_correct / total_predictions if total_predictions > 0 else 0

    stats['models'] = model_stats
    stats['summary'] = {
        'total_predictions': total_predictions,
        'total_correct': total_correct,
        'overall_accuracy': overall_accuracy,
        'active_models': len([m for m in model_stats if model_stats[m].get('total_predictions', 0) > 0])
    }

    return stats

def get_championships_comparison(days: int = 30) -> List[Dict]:
    """
    Сравнение производительности моделей между чемпионатами.

    Args:
        days: Количество дней для анализа

    Returns:
        Список с сравнением чемпионатов
    """
    comparison_data = []

    # Получаем список чемпионатов
    championships = get_championship_list()

    for championship in championships:
        champ_id = championship['id']
        champ_stats = get_championship_performance_stats(champ_id, days)

        if champ_stats['summary']['total_predictions'] > 0:
            comparison_data.append({
                'championship_id': champ_id,
                'championship_name': championship['name'],
                'total_predictions': champ_stats['summary']['total_predictions'],
                'overall_accuracy': champ_stats['summary']['overall_accuracy'],
                'active_models': champ_stats['summary']['active_models']
            })

    # Сортируем по точности
    comparison_data.sort(key=lambda x: x['overall_accuracy'], reverse=True)

    return comparison_data

#===
def get_training_metrics_history(
        championship_id: Optional[int] = None,
        days: int = 90
) -> List[Dict]:
    """
    Получение истории метрик обучения.

    Args:
        championship_id: ID чемпионата (опционально)
        days: Количество дней для анализа

    Returns:
        Список метрик обучения
    """
    results = []

    with DBSession() as session:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            query = session.query(ModelTrainingMetrics).filter(
                ModelTrainingMetrics.training_date >= cutoff_date
            )

            if championship_id:
                query = query.filter(ModelTrainingMetrics.championship_id == championship_id)

            metrics_records = query.order_by(desc(ModelTrainingMetrics.training_date)).all()

            for record in metrics_records:
                results.append({
                    'id': record.id,
                    'championship_id': record.championship_id,
                    'championship_name': record.championship_name,
                    'training_date': record.training_date,
                    'metrics_data': json.loads(record.metrics_data),
                    'created_at': record.created_at
                })

        except Exception as e:
            logger.error(f"Ошибка получения истории метрик обучения: {e}")

    return results

def get_last_training_date(championship_id: int) -> Optional[datetime]:
    """
    Получение даты последнего обучения для чемпионата.

    Args:
        championship_id: ID чемпионата

    Returns:
        Дата последнего обучения или None
    """
    with DBSession() as session:
        try:
            last_training = session.query(ModelTrainingMetrics).filter(
                ModelTrainingMetrics.championship_id == championship_id
            ).order_by(desc(ModelTrainingMetrics.training_date)).first()

            return last_training.training_date if last_training else None

        except Exception as e:
            logger.error(f"Ошибка получения даты последнего обучения: {e}")
            return None
