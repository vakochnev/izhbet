import logging
import os
from typing import Dict, Any, List

from db.queries.prediction import get_prediction_match_id
from db.models import Prediction, Outcome
from db.models.match import Match
from core.constants import SIZE_TOTAL, SIZE_ITOTAL, SPR_SPORTS
from core.logger_message import MEASSGE_LOG
from config import Session_pool

logger = logging.getLogger(__name__)


def save_forecast(db_session, forecasts, match_id):
    try:
        prediction_db = (
            get_prediction_match_id(
                db_session,
                match_id=match_id
            )
        )
        if prediction_db is None:
            prediction_db = Prediction()

        prediction_db.match_id = match_id

        # Переходный режим: выключаем запись устаревших полей в Prediction
        # Установите LEGACY_PREDICTION_WRITE=true, чтобы временно включить обратную совместимость
        if os.getenv('LEGACY_PREDICTION_WRITE', 'false').lower() == 'true':
            prediction_db.outcome_forecast_win_draw_loss = (
                forecasts['win_draw_loss']['forecast']
            )
            prediction_db.forecast_probability_win_draw_loss = (
                forecasts['win_draw_loss']['probability']
            )
            prediction_db.forecast_confidence_win_draw_loss = (
                forecasts['win_draw_loss']['confidence']
            )

            prediction_db.outcome_forecast_oz = (
                forecasts['oz']['forecast']
            )
            prediction_db.forecast_probability_oz = (
                forecasts['oz']['probability']
            )
            prediction_db.forecast_confidence_oz = (
                forecasts['oz']['confidence']
            )

            prediction_db.outcome_forecast_total = (
                forecasts['total']['forecast']
            )
            prediction_db.forecast_probability_total = (
                forecasts['total']['probability']
            )
            prediction_db.forecast_confidence_total = (
                forecasts['total']['confidence']
            )

            prediction_db.outcome_forecast_goal_home = (
                forecasts['goal_home']['forecast']
            )
            prediction_db.forecast_probability_goal_home = (
                forecasts['goal_home']['probability']
            )
            prediction_db.forecast_confidence_goal_home = (
                forecasts['goal_home']['confidence']
            )

            prediction_db.outcome_forecast_goal_away = (
                forecasts['goal_away']['forecast']
            )
            prediction_db.forecast_probability_goal_away = (
                forecasts['goal_away']['probability']
            )
            prediction_db.forecast_confidence_goal_away = (
                forecasts['goal_away']['confidence']
            )

            prediction_db.outcome_forecast_total_home = (
                forecasts['total_home']['forecast']
            )
            prediction_db.forecast_probability_total_home = (
                forecasts['total_home']['probability']
            )
            prediction_db.forecast_confidence_total_home = (
                forecasts['total_home']['confidence']
            )

            prediction_db.outcome_forecast_total_away = (
                forecasts['total_away']['forecast']
            )
            prediction_db.forecast_probability_total_away = (
                forecasts['total_away']['probability']
            )
            prediction_db.forecast_confidence_total_away = (
                forecasts['total_away']['confidence']
            )

            prediction_db.outcome_forecast_total_amount = (
                forecasts['total_amount']['forecast']
            )
            prediction_db.forecast_probability_total_amount = (
                forecasts['total_amount']['probability']
            )
            prediction_db.forecast_confidence_total_amount = (
                forecasts['total_amount']['confidence']
            )

            prediction_db.outcome_forecast_total_home_amount = (
                forecasts['total_home_amount']['forecast']
            )
            prediction_db.forecast_probability_total_home_amount = (
                forecasts['total_home_amount']['probability']
            )
            prediction_db.forecast_confidence_total_home_amount = (
                forecasts['total_home_amount']['confidence']
            )

            prediction_db.outcome_forecast_total_away_amount = (
                forecasts['total_away_amount']['forecast']
            )
            prediction_db.forecast_probability_total_away_amount = (
                forecasts['total_away_amount']['probability']
            )
            prediction_db.forecast_confidence_total_away_amount = (
                forecasts['total_away_amount']['confidence']
            )

        db_session.add_model(prediction_db)
        db_session.commit()

        logger.debug(MEASSGE_LOG['saving_db_prediction'])

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных в PREDICTION: {e}'
        )


def save_conformal_outcome(db_session, result: Dict[str, Any]) -> bool:
    """
    Сохраняет конформный прогноз в таблицу outcomes.
    
    Args:
        db_session: Сессия базы данных
        result: Результат конформного анализа
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        match_id = result['match_id']
        
        # Маппинг типов прогнозов на числовые коды
        feature_mapping = {
            'win_draw_loss': 1,
            'oz': 2,
            'goal_home': 3,
            'goal_away': 4,
            'total': 5,
            'total_home': 6,
            'total_away': 7,
            'total_amount': 8,
            'total_home_amount': 9,
            'total_away_amount': 10
        }
        
        # Сохраняем каждый тип прогноза
        for forecast_type, forecast_data in result.items():
            if forecast_type == 'match_id' or 'error' in forecast_data:
                continue
            
            # Получаем числовой код для типа прогноза
            feature_code = feature_mapping.get(forecast_type, -999)
            
            # Рассчитываем границы интервала
            lower_bound, upper_bound = _calculate_prediction_bounds(forecast_data)
            
            # Для регрессионных типов также формируем категоризированный исход (ТБ/ТМ и ИТ1Б/ИТ1М, ИТ2Б/ИТ2М)
            # и корректно сохраняем числовой прогноз
            is_regression = forecast_type in ('total_amount', 'total_home_amount', 'total_away_amount')
            forecast_numeric = None
            outcome_text = str(forecast_data.get('forecast', ''))
            try:
                if is_regression:
                    forecast_numeric = float(forecast_data.get('forecast')) if forecast_data.get('forecast') is not None else None
                    if forecast_numeric is not None:
                        # Вычисляем динамические пороги по виду спорта/лиге
                        threshold_total, threshold_itotal = _get_dynamic_thresholds(db_session, match_id)
                        if forecast_type == 'total_amount':
                            threshold = threshold_total
                            outcome_text = 'ТБ' if forecast_numeric >= threshold else 'ТМ'
                        elif forecast_type == 'total_home_amount':
                            threshold = threshold_itotal
                            outcome_text = 'ИТ1Б' if forecast_numeric >= threshold else 'ИТ1М'
                        elif forecast_type == 'total_away_amount':
                            threshold = threshold_itotal
                            outcome_text = 'ИТ2Б' if forecast_numeric >= threshold else 'ИТ2М'
                else:
                    # для классификации forecast в исходных данных — это уже метка исхода
                    forecast_numeric = None
            except Exception:
                # В случае ошибок парсинга оставляем исходный outcome_text
                pass
            
            # Upsert: если запись существует для (match_id, feature), обновляем; иначе создаем
            existing = (
                db_session.query(Outcome)
                .filter(Outcome.match_id == match_id, Outcome.feature == feature_code)
                .one_or_none()
            )

            if existing is None:
                outcome = Outcome(
                    match_id=match_id,
                    feature=feature_code,
                    forecast=(float(forecast_numeric) if (is_regression and forecast_numeric is not None) else float(forecast_data.get('probability', 0.0))),
                    outcome=outcome_text,
                    probability=float(forecast_data.get('probability', 0.0)),
                    confidence=float(forecast_data.get('confidence', 0.0)),
                    uncertainty=float(forecast_data.get('uncertainty', 0.0)),
                    lower_bound=lower_bound,
                    upper_bound=upper_bound
                )
                db_session.add(outcome)
            else:
                existing.forecast = (float(forecast_numeric) if (is_regression and forecast_numeric is not None) else float(forecast_data.get('probability', 0.0)))
                existing.outcome = outcome_text
                existing.probability = float(forecast_data.get('probability', 0.0))
                existing.confidence = float(forecast_data.get('confidence', 0.0))
                existing.uncertainty = float(forecast_data.get('uncertainty', 0.0))
                existing.lower_bound = lower_bound
                existing.upper_bound = upper_bound
        
        db_session.commit()
        logger.debug(f'Конформный прогноз для матча {match_id} сохранен')
        return True
        
    except Exception as e:
        logger.error(f'Ошибка при сохранении конформного прогноза: {e}')
        db_session.rollback()
        return False


def save_conformal_outcomes_batch(db_session, results: List[Dict[str, Any]]) -> int:
    """
    Сохраняет несколько конформных прогнозов в таблицу outcomes.
    
    Args:
        db_session: Сессия базы данных
        results: Список результатов конформного анализа
        
    Returns:
        int: Количество успешно сохраненных прогнозов
    """
    successful = 0
    for result in results:
        if save_conformal_outcome(db_session, result):
            successful += 1
    
    logger.info(f'Сохранено конформных прогнозов: {successful} из {len(results)}')
    return successful


def _calculate_prediction_bounds(forecast_data: Dict[str, Any]) -> tuple:
    """
    Рассчитывает нижнюю и верхнюю границы интервала прогноза.
    
    Args:
        forecast_data: Данные прогноза с confidence и uncertainty
        
    Returns:
        tuple: (lower_bound, upper_bound)
    """
    try:
        confidence = float(forecast_data.get('confidence', 0.0))
        uncertainty = float(forecast_data.get('uncertainty', 0.0))
        probability = float(forecast_data.get('probability', 0.0))
        
        # Для классификационных прогнозов границы основаны на вероятности и неопределенности
        if confidence > 0 and uncertainty > 0:
            # Нижняя граница: вероятность минус половина неопределенности
            lower_bound = max(0.0, probability - (uncertainty * 0.5))
            # Верхняя граница: вероятность плюс половина неопределенности
            upper_bound = min(1.0, probability + (uncertainty * 0.5))
        else:
            # Если нет данных о неопределенности, используем стандартные границы
            lower_bound = max(0.0, probability - 0.1)
            upper_bound = min(1.0, probability + 0.1)
        
        return lower_bound, upper_bound
        
    except Exception as e:
        logger.error(f'Ошибка при расчете границ интервала: {e}')
        return 0.0, 1.0


def _get_dynamic_thresholds(db_session, match_id: int) -> tuple:
    """
    Возвращает пороги (total, itotal) для матча, используя настройки по виду спорта.
    Если матч не найден или данные отсутствуют — возвращает дефолтные 2.5 и 1.5.
    """
    try:
        match: Match = (
            db_session.query(Match)
            .filter(Match.id == match_id)
            .one_or_none()
        )
        if not match or match.sport_id is None:
            return 2.5, 1.5
        sport_key = SPR_SPORTS.get(match.sport_id)
        if not sport_key:
            return 2.5, 1.5
        total = SIZE_TOTAL.get(sport_key, 2.5)
        itotal = SIZE_ITOTAL.get(sport_key, 1.5)
        return total, itotal
    except Exception:
        return 2.5, 1.5
