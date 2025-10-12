import logging
from math import isnan

from db.models import Outcome
from db.queries.outcome import get_outcome_match_outcome
from core.logger_message import MEASSGE_LOG
from config import Session_pool, DBSession


logger = logging.getLogger(__name__)


def save_outcome(outcomes):
    db = DBSession(Session_pool())
    for _, row in outcomes.iterrows():
        logger.info(
            f'Сохранение прогнозов и исходов матча: {row.match_id}.'
        )
        try:
            if isnan(row.feature):
                row.feature = -999
        except TypeError as err:
            logger.info(
                f'В поле row[feature] не корректное значение. '
                f'Ошибка: {err}'
            )

        outcome_db = get_outcome_match_outcome(
            match_id=row['match_id'],
            outcome=row['outcome']
        )
        try:
            if outcome_db is None:
                outcome_db = Outcome()

            outcome_db.match_id = row.match_id
            outcome_db.outcome = row.outcome
            outcome_db.feature = row.feature
            outcome_db.forecast = row.forecast
            outcome_db.probability = row.probability
            outcome_db.confidence = row.confidence
            outcome_db.accuracy_home = row.accuracy_home
            outcome_db.accuracy_away = row.accuracy_away

            db.add_model(outcome_db)
            db.commit_session()

            logger.debug(MEASSGE_LOG['saving_db_outcome'])

        except Exception as e:
            logger.critical(
                f'Ошибка при сохранении данных '
                f'в OUTCOME: {e}'
            )

        finally:
            pass
