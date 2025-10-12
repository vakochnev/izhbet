import logging
from typing import Any, Dict, Optional, List
from config import Session_pool, DBSession

from db.queries.prediction import get_prediction_match_id
from db.models import Prediction
from core.logger_message import MEASSGE_LOG

logger = logging.getLogger(__name__)


def save_prediction(db_session, predictions):
    try:
        logger.debug(f"Сохранение предсказаний для {len(predictions)} матчей")
        if predictions:
            first_match_id = list(predictions.keys())[0]
            logger.debug(f"Структура данных для матча {first_match_id}: {list(predictions[first_match_id].keys())}")
        
        for match_id in predictions:
            prediction_db = (
                get_prediction_match_id(db_session, match_id)
            )
            if prediction_db is None:
                prediction_db = Prediction()
                prediction_db.match_id = match_id

            prediction_db.teamHome_id = (
                predictions[match_id]
                ['teamHome_id']
            )
            prediction_db.teamAway_id = (
                predictions[match_id]
                ['teamAway_id']
            )

            prediction_db.model_name = 'KERAS'

            # Убираем старую проверку win_draw_loss, так как теперь используем отдельные ключи
            # prediction_db.forecast_win_draw_loss больше не используется

            #=== сохраняем вероятности по своим подмоделям напрямую
            # ожидаем, что batch_predict вернул по one-hot моделям отдельные записи
            wdl_home = predictions[match_id].get('win_draw_loss_home_win', {})
            wdl_draw = predictions[match_id].get('win_draw_loss_draw', {})
            wdl_away = predictions[match_id].get('win_draw_loss_away_win', {})

            try:
                prediction_db.win_draw_loss_home_win = (wdl_home.get('probabilities') or [None])[0]
            except Exception:
                prediction_db.win_draw_loss_home_win = None
            try:
                prediction_db.win_draw_loss_draw = (wdl_draw.get('probabilities') or [None])[0]
            except Exception:
                prediction_db.win_draw_loss_draw = None
            try:
                prediction_db.win_draw_loss_away_win = (wdl_away.get('probabilities') or [None])[0]
            except Exception:
                prediction_db.win_draw_loss_away_win = None
            #===

            # Убираем старую проверку oz, так как теперь используем отдельные ключи oz_yes и oz_no
            # prediction_db.forecast_oz больше не используется
            
            # Используем отдельные ключи для oz_yes и oz_no
            oz_yes_data = predictions[match_id].get('oz_yes', {})
            oz_no_data = predictions[match_id].get('oz_no', {})
            
            try:
                prediction_db.oz_yes = (oz_yes_data.get('probabilities') or [None])[0]
            except Exception:
                prediction_db.oz_yes = None
            try:
                prediction_db.oz_no = (oz_no_data.get('probabilities') or [None])[0]
            except Exception:
                prediction_db.oz_no = None

            # Проверяем наличие ключа goal_home
            if 'goal_home' not in predictions[match_id]:
                logger.error(f"Ключ 'goal_home' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_goal_home = (
                predictions[match_id]
                    ['goal_home']['prediction']
            )
            
            goal_home_probs = predictions[match_id]['goal_home']['probabilities']
            if len(goal_home_probs) > 1:
                prediction_db.goal_home_yes = goal_home_probs[1]
                prediction_db.goal_home_no = goal_home_probs[0]
            else:
                logger.warning(f"Недостаточно элементов в probabilities для goal_home матча {match_id}")
                prediction_db.goal_home_yes = 0.0
                prediction_db.goal_home_no = 0.0

            # Проверяем наличие ключа goal_away
            if 'goal_away' not in predictions[match_id]:
                logger.error(f"Ключ 'goal_away' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_goal_away = (
                predictions[match_id]
                    ['goal_away']['prediction']
            )
            
            goal_away_probs = predictions[match_id]['goal_away']['probabilities']
            if len(goal_away_probs) > 1:
                prediction_db.goal_away_yes = goal_away_probs[1]
                prediction_db.goal_away_no = goal_away_probs[0]
            else:
                logger.warning(f"Недостаточно элементов в probabilities для goal_away матча {match_id}")
                prediction_db.goal_away_yes = 0.0
                prediction_db.goal_away_no = 0.0

            # Проверяем наличие ключа total
            if 'total' not in predictions[match_id]:
                logger.error(f"Ключ 'total' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_total = (
                predictions[match_id]
                    ['total']['prediction']
            )
            
            total_probs = predictions[match_id]['total']['probabilities']
            if len(total_probs) > 1:
                prediction_db.total_yes = total_probs[1]
                prediction_db.total_no = total_probs[0]
            else:
                logger.warning(f"Недостаточно элементов в probabilities для total матча {match_id}")
                prediction_db.total_yes = 0.0
                prediction_db.total_no = 0.0

            # Проверяем наличие ключа total_home
            if 'total_home' not in predictions[match_id]:
                logger.error(f"Ключ 'total_home' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_total_home = (
                predictions[match_id]
                    ['total_home']['prediction']
            )
            
            total_home_probs = predictions[match_id]['total_home']['probabilities']
            if len(total_home_probs) > 1:
                prediction_db.total_home_yes = total_home_probs[1]
                prediction_db.total_home_no = total_home_probs[0]
            else:
                logger.warning(f"Недостаточно элементов в probabilities для total_home матча {match_id}")
                prediction_db.total_home_yes = 0.0
                prediction_db.total_home_no = 0.0

            # Проверяем наличие ключа total_away
            if 'total_away' not in predictions[match_id]:
                logger.error(f"Ключ 'total_away' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_total_away = (
                predictions[match_id]
                    ['total_away']['prediction']
            )
            
            total_away_probs = predictions[match_id]['total_away']['probabilities']
            if len(total_away_probs) > 1:
                prediction_db.total_away_yes = total_away_probs[1]
                prediction_db.total_away_no = total_away_probs[0]
            else:
                logger.warning(f"Недостаточно элементов в probabilities для total_away матча {match_id}")
                prediction_db.total_away_yes = 0.0
                prediction_db.total_away_no = 0.0

            # Проверяем наличие ключа total_amount
            if 'total_amount' not in predictions[match_id]:
                logger.error(f"Ключ 'total_amount' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_total_amount = (
                predictions[match_id]
                    ['total_amount']['prediction']
            )
            
            # Проверяем наличие ключа total_home_amount
            if 'total_home_amount' not in predictions[match_id]:
                logger.error(f"Ключ 'total_home_amount' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_total_home_amount = (
                predictions[match_id]
                    ['total_home_amount']['prediction']
            )
            
            # Проверяем наличие ключа total_away_amount
            if 'total_away_amount' not in predictions[match_id]:
                logger.error(f"Ключ 'total_away_amount' отсутствует в данных для матча {match_id}")
                continue
                
            prediction_db.forecast_total_away_amount = (
                predictions[match_id]
                    ['total_away_amount']['prediction']
            )

            db_session.add_model(prediction_db)
            db_session.commit()

            logger.debug(MEASSGE_LOG['saving_db_prediction'])

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных турнирной таблицы: {e}'
        )
