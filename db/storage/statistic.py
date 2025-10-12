"""
Хранилище для работы со статистикой прогнозов.
"""

import logging
from datetime import datetime, date
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from db.models.statistics import Statistic
from db.models.outcome import Outcome
from db.models.prediction import Prediction
from db.models.match import Match
from db.models.championship import ChampionShip
from db.models.sport import Sport
from db.storage.forecast import save_conformal_outcome
# from forecast.quality_selector import is_quality_outcome  # Циклический импорт
from config import Session_pool, DBSession

logger = logging.getLogger(__name__)


def _is_quality_outcome(forecast_type: str, probability: Optional[float], confidence: Optional[float]) -> bool:
    """
    Локальная функция для проверки качества outcome (избегаем циклического импорта).
    """
    # Пороги качества (можно вынести в конфиг)
    quality_thresholds = {
        'win_draw_loss': {'min_probability': 0.0, 'min_confidence': 0.0},
        'oz': {'min_probability': 0.0, 'min_confidence': 0.0},
        'total': {'min_probability': 0.0, 'min_confidence': 0.0},
        'total_home': {'min_probability': 0.0, 'min_confidence': 0.0},
        'total_away': {'min_probability': 0.0, 'min_confidence': 0.0},
    }
    
    cfg = quality_thresholds.get(forecast_type)
    if not cfg:
        return False

    try:
        prob = float(probability) if probability is not None else 0.0
    except Exception:
        prob = 0.0

    try:
        conf = float(confidence) if confidence is not None else 0.0
    except Exception:
        conf = 0.0

    return prob >= cfg['min_probability'] and conf >= cfg['min_confidence']


def _map_feature_to_type(feature_code: int) -> str:
    mapping = {
        1: 'win_draw_loss',
        2: 'oz',
        3: 'goal_home',
        4: 'goal_away',
        5: 'total',
        6: 'total_home',
        7: 'total_away',
        8: 'total_amount',
        9: 'total_home_amount',
        10: 'total_away_amount',
    }
    return mapping.get(int(feature_code), 'unknown')


def _map_feature_to_type_and_model(feature_code: int) -> tuple[str, str]:
    forecast_type = _map_feature_to_type(feature_code)
    if forecast_type in ['total_amount', 'total_home_amount', 'total_away_amount']:
        return forecast_type, 'regression'
    if forecast_type == 'unknown':
        return forecast_type, 'unknown'
    return forecast_type, 'classification'

def save_conformal_outcome_with_statistics(db_session: Session, result: Dict[str, Any]) -> bool:
    """
    Расширенная версия save_conformal_outcome с автоматической интеграцией в statistics.
    
    Args:
        db_session: Сессия базы данных
        result: Результат конформного анализа
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        # 1. Сохраняем в outcomes (существующая логика)
        success = save_conformal_outcome(db_session, result)
        
        if not success:
            logger.error("Ошибка сохранения в outcomes")
            return False
        
        # 2. Интегрируем в statistics (новая логика)
        match_id = result['match_id']
        
        # Получаем последние созданные outcomes для этого матча
        latest_outcomes = (
            db_session.query(Outcome)
            .filter(Outcome.match_id == match_id)
            .order_by(Outcome.id.desc())
            .all()
        )
        
        if not latest_outcomes:
            logger.warning(f"Не найдены outcomes для матча {match_id}")
            return True  # outcomes сохранены, но statistics не созданы
        
        # Интегрируем каждый outcome по правилу качества
        for outcome in latest_outcomes:
            try:
                # Пропускаем низкокачественные исходы
                forecast_type = _map_feature_to_type(outcome.feature)
                if not _is_quality_outcome(forecast_type, outcome.probability, outcome.confidence):
                    continue

                integrate_outcome_to_statistics(db_session, outcome)
            except Exception as e:
                logger.error(f"Ошибка интеграции outcome {outcome.id}: {e}")
                continue
        
        logger.info(f"Успешно интегрированы {len([o for o in latest_outcomes if _is_quality_outcome(_map_feature_to_type(o.feature), o.probability, o.confidence)])} outcomes в statistics")
        
        # 3. Обновляем результаты матча в statistics (если матч завершен)
        try:
            match = db_session.query(Match).filter(Match.id == match_id).first()
            if match and match.numOfHeadsHome is not None and match.numOfHeadsAway is not None:
                logger.info(f"Обновляем результаты матча {match_id}: {match.numOfHeadsHome}:{match.numOfHeadsAway}")
                update_match_results(match_id, match.numOfHeadsHome, match.numOfHeadsAway)
        except Exception as e:
            logger.warning(f"Не удалось обновить результаты матча {match_id}: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка в save_conformal_outcome_with_statistics: {e}")
        return False


def integrate_outcome_to_statistics(db_session: Session, outcome: Outcome) -> bool:
    """
    Интегрирует один outcome в statistics.
    
    Args:
        db_session: Сессия базы данных
        outcome: Запись из таблицы outcomes
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        # Проверяем, не интегрирован ли уже
        existing = db_session.query(Statistic).filter(Statistic.outcome_id == outcome.id).first()
        
        if existing:
            logger.debug(f"Outcome {outcome.id} уже интегрирован")
            return True
        
        # Получаем связанные данные
        match = db_session.query(Match).filter(Match.id == outcome.match_id).first()
        if not match:
            logger.error(f"Match {outcome.match_id} не найден")
            return False
        
        championship = db_session.query(ChampionShip).filter(ChampionShip.id == match.tournament_id).first()
        if not championship:
            logger.error(f"Championship {match.tournament_id} не найден")
            return False
        
        sport = db_session.query(Sport).filter(Sport.id == championship.sport_id).first()
        if not sport:
            logger.error(f"Sport {championship.sport_id} не найден")
            return False
        
        # Определяем тип прогноза
        forecast_type, model_type = _map_feature_to_type_and_model(outcome.feature)
        forecast_subtype = outcome.outcome or 'unknown'
        
        # Вычисляем результат матча
        actual_result, actual_value = calculate_actual_result(match)
        
        # Подбираем связанное предсказание (последнее по времени для этого матча)
        linked_prediction_id = None
        try:
            pred = (
                db_session.query(Prediction)
                .filter(Prediction.match_id == outcome.match_id)
                .order_by(Prediction.created_at.desc())
                .first()
            )
            if pred:
                linked_prediction_id = pred.id
        except Exception:
            linked_prediction_id = None

        # Создаем запись в statistics через ORM
        statistic = Statistic(
            outcome_id=outcome.id,
            prediction_id=linked_prediction_id,
            match_id=outcome.match_id,
            championship_id=match.tournament_id,
            sport_id=championship.sport_id,
            match_date=match.gameData.date() if match.gameData else date.today(),
            match_round=getattr(match, 'tour', None),
            match_stage=getattr(match, 'stage', None),
            forecast_type=forecast_type,
            forecast_subtype=forecast_subtype,
            model_name='conformal_predictor',
            model_version='1.0',
            model_type=model_type,
            # В actual_result теперь сохраняем бинарный успех (1/0),
            # но на этапе интеграции результата матча может не быть — отложим до обновления
            actual_result=None,
            actual_value=actual_value,
            prediction_correct=None,  # Будет вычислено позже
            prediction_accuracy=None,  # Будет вычислено позже
            prediction_error=None,  # Будет вычислено позже
            prediction_residual=None,  # Будет вычислено позже
            coefficient=None,
            potential_profit=None,
            actual_profit=None
        )
        
        db_session.add(statistic)
        db_session.commit()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка интеграции outcome {outcome.id}: {e}")
        return False


def integrate_prediction_to_statistics(db_session: Session, prediction_id: int) -> bool:
    """
    Интегрирует один prediction в statistics.
    
    Args:
        db_session: Сессия базы данных
        prediction_id: ID записи из таблицы predictions
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        from db.models.prediction import Prediction
        
        # Получаем prediction
        prediction = db_session.query(Prediction).filter(Prediction.id == prediction_id).first()
        if not prediction:
            logger.error(f"Prediction {prediction_id} не найден")
            return False
        
        # Проверяем, не интегрирован ли уже
        existing = db_session.query(Statistic).filter(Statistic.prediction_id == prediction_id).first()
        
        if existing:
            logger.debug(f"Prediction {prediction_id} уже интегрирован")
            return True
        
        # Получаем связанные данные
        match = db_session.query(Match).filter(Match.id == prediction.match_id).first()
        if not match:
            logger.error(f"Match {prediction.match_id} не найден")
            return False
        
        championship = db_session.query(ChampionShip).filter(ChampionShip.id == match.tournament_id).first()
        if not championship:
            logger.error(f"Championship {match.tournament_id} не найден")
            return False
        
        sport = db_session.query(Sport).filter(Sport.id == championship.sport_id).first()
        if not sport:
            logger.error(f"Sport {championship.sport_id} не найден")
            return False
        
        # Для predictions используем общий тип
        forecast_type = 'prediction'
        forecast_subtype = 'general'
        model_type = 'classification'
        
        # Вычисляем результат матча
        actual_result, actual_value = calculate_actual_result(match)
        
        # Создаем запись в statistics через ORM
        statistic = Statistic(
            outcome_id=None,
            prediction_id=prediction_id,
            match_id=prediction.match_id,
            championship_id=match.tournament_id,
            sport_id=championship.sport_id,
            match_date=match.gameData.date() if match.gameData else date.today(),
            match_round=getattr(match, 'tour', None),
            match_stage=getattr(match, 'stage', None),
            forecast_type=forecast_type,
            forecast_subtype=forecast_subtype,
            model_name=prediction.model_name or 'keras_model',
            model_version='1.0',
            model_type=model_type,
            actual_result=actual_result,
            actual_value=actual_value,
            prediction_correct=None,  # Будет вычислено позже
            prediction_accuracy=None,  # Будет вычислено позже
            prediction_error=None,  # Будет вычислено позже
            prediction_residual=None,  # Будет вычислено позже
            coefficient=None,
            potential_profit=None,
            actual_profit=None
        )
        
        db_session.add(statistic)
        db_session.commit()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка интеграции prediction {prediction_id}: {e}")
        return False


def calculate_actual_result(match: Match) -> tuple[Optional[str], Optional[float]]:
    """Вычисляет фактический результат матча."""
    if not match.numOfHeadsHome or not match.numOfHeadsAway:
        return None, None
    
    goal_home = int(match.numOfHeadsHome)
    goal_away = int(match.numOfHeadsAway)
    
    # Определяем результат матча
    if goal_home > goal_away:
        actual_result = 'home_win'
    elif goal_home < goal_away:
        actual_result = 'away_win'
    else:
        actual_result = 'draw'
    
    # Для регрессии (total_amount) возвращаем сумму голов
    actual_value = float(goal_home + goal_away)
    
    return actual_result, actual_value


def update_match_results(match_id: int, goal_home: int, goal_away: int) -> bool:
    """
    Обновляет статистику после завершения матча.
    
    Args:
        match_id: ID матча
        goal_home: Голы домашней команды
        goal_away: Голы гостевой команды
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        with Session_pool() as db_session:
            # Определяем результат матча
            if goal_home > goal_away:
                actual_result = 'home_win'
            elif goal_home < goal_away:
                actual_result = 'away_win'
            else:
                actual_result = 'draw'
            
            actual_value = float(goal_home + goal_away)
            
            # Обновляем все записи статистики для этого матча через ORM
            statistics = db_session.query(Statistic).filter(Statistic.match_id == match_id).all()
            
            for statistic in statistics:
                statistic.actual_value = actual_value

                # Пересчитываем качество прогнозов и устанавливаем бинарный actual_result
                if statistic.outcome_id:
                    outcome = db_session.query(Outcome).filter(Outcome.id == statistic.outcome_id).first()
                    if outcome:
                        is_success = False
                        
                        if statistic.forecast_type == 'win_draw_loss':
                            # П1, Х, П2 -> home_win, draw, away_win
                            outcome_mapping = {'п1': 'home_win', 'х': 'draw', 'п2': 'away_win'}
                            expected_result = outcome_mapping.get(outcome.outcome, 'unknown')
                            is_success = (expected_result == actual_result)
                            
                        elif statistic.forecast_type == 'oz':
                            # "обе забьют - да/нет" - определяем по голам
                            both_scored = (goal_home > 0 and goal_away > 0)
                            if outcome.outcome == 'обе забьют - да':
                                is_success = both_scored
                            elif outcome.outcome == 'обе забьют - нет':
                                is_success = not both_scored
                            
                        elif statistic.forecast_type == 'total':
                            # ТБ/ТМ - сравниваем с общим количеством голов
                            if outcome.outcome == 'тб':
                                is_success = (actual_value > outcome.forecast)
                            elif outcome.outcome == 'тм':
                                is_success = (actual_value < outcome.forecast)
                            
                        elif statistic.forecast_type == 'total_home':
                            # ИТ1Б/ИТ1М - сравниваем с голами домашней команды
                            if outcome.outcome == 'ит1б':
                                is_success = (goal_home > outcome.forecast)
                            elif outcome.outcome == 'ит1м':
                                is_success = (goal_home < outcome.forecast)
                            
                        elif statistic.forecast_type == 'total_away':
                            # ИТ2Б/ИТ2М - сравниваем с голами гостевой команды
                            if outcome.outcome == 'ит2б':
                                is_success = (goal_away > outcome.forecast)
                            elif outcome.outcome == 'ит2м':
                                is_success = (goal_away < outcome.forecast)
                        
                        statistic.prediction_correct = is_success
                        statistic.actual_result = is_success  # boolean: True/False
                        statistic.prediction_accuracy = 1.0 if is_success else 0.0
            
            db_session.commit()
            
            logger.info(f"Обновлена статистика для матча {match_id}: {actual_result} ({goal_home}:{goal_away})")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка обновления результатов матча {match_id}: {e}")
        return False