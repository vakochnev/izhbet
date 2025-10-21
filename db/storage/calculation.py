import logging
import time
from sqlalchemy.exc import OperationalError

from db.queries.standing import (
    get_standing_data_id,
    #delete_standings_by_match_ids,
    get_model_columns,
    object_to_dict
)
from db.queries.feature import get_feature_match_id_prefix
from db.models import Standing, Feature
from core.constants import DROP_FIELD_BLOWOUTS


logger = logging.getLogger(__name__)


def save_standing(self, tournament_id, standings):
    batch_size = 50
    processed = 0
    
    try:
        logger.info(
            f'Сохранение данных STANDINGS по турниру: {tournament_id}'
        )
        
        # Отключаем autoflush для избежания блокировок
        with self.db_session._session.no_autoflush:
            for key in standings:
                match_team = key.split('_')
                match_id = int(match_team[0])
                team_id = int(match_team[1])

                standing_db = (
                    self.db_session.query(Standing)
                    .filter(
                        Standing.match_id == match_id,
                        Standing.team_id == team_id
                    )
                    .first()
                )

                if standing_db is None:
                    standing_db = Standing()

                standings[key].match_id = match_id
                standings[key].team_id = team_id

                for k, v in standings[key].__dict__.items():
                    # if k in DROP_FIELD_BLOWOUTS:
                    #     continue
                    if not k.startswith('_'):
                        setattr(standing_db, k, v)

                self.db_session.add_model(standing_db)
                processed += 1
                
                # Коммитим пакетами с retry
                if processed % batch_size == 0:
                    for retry in range(3):
                        try:
                            self.db_session.commit()
                            logger.debug(f'Сохранено {processed} standings')
                            time.sleep(0.01)
                            break
                        except OperationalError as e:
                            if '1205' in str(e) and retry < 2:
                                wait = (retry + 1) * 0.3
                                logger.warning(f'Блокировка standings, retry {retry + 1}/3')
                                self.db_session.rollback()
                                time.sleep(wait)
                            else:
                                raise
            
            # Финальный коммит
            if processed % batch_size != 0:
                for retry in range(3):
                    try:
                        self.db_session.commit()
                        logger.debug(f'Финальное сохранение: {processed} standings')
                        break
                    except OperationalError as e:
                        if '1205' in str(e) and retry < 2:
                            wait = (retry + 1) * 0.3
                            logger.warning(f'Блокировка при финальном коммите standings, retry {retry + 1}/3')
                            self.db_session.rollback()
                            time.sleep(wait)
                        else:
                            raise

    except Exception as err:
        logger.critical(
            f'Ошибка при сохранении данных в STANDING: {err}'
        )
        self.db_session.rollback()


def save_feature(self, tournament_id, features):
    batch_size = 50
    processed = 0
    
    try:
        logger.info(
            f'Сохранение данных FEATURE по турниру: {tournament_id}'
        )
        
        # Отключаем autoflush для избежания блокировок
        with self.db_session._session.no_autoflush:
            for match_id in features:
                for prefix in features[match_id]:

                    feature_db = (
                        self.db_session.query(Feature)
                        .filter(
                            Feature.match_id == int(match_id),
                            Feature.prefix == prefix
                        )
                        .first()
                    )
                    
                    if feature_db is None:
                        feature_db = Feature()

                    # ВАЖНО: Принудительно устанавливаем match_id и prefix из внешней области,
                    # не даем им затираться при копировании полей ниже
                    feature_db.match_id = int(match_id)
                    feature_db.prefix = prefix

                    # Копируем только бизнес-поля, исключая системные/идентификационные
                    for k, v in features[match_id][prefix].__dict__.items():
                        if k.startswith('_'):
                            continue
                        if k in ('id', 'match_id', 'prefix', 'created_at', 'updated_at'):
                            continue
                        setattr(feature_db, k, v)

                    self.db_session.add_model(feature_db)
                    processed += 1
                    
                    # Коммитим пакетами с retry
                    if processed % batch_size == 0:
                        for retry in range(3):
                            try:
                                self.db_session.commit()
                                logger.debug(f'Сохранено {processed} features')
                                time.sleep(0.01)
                                break
                            except OperationalError as e:
                                if '1205' in str(e) and retry < 2:
                                    wait = (retry + 1) * 0.3
                                    logger.warning(f'Блокировка features, retry {retry + 1}/3')
                                    self.db_session.rollback()
                                    time.sleep(wait)
                                else:
                                    raise
            
            # Финальный коммит
            if processed % batch_size != 0:
                for retry in range(3):
                    try:
                        self.db_session.commit()
                        logger.debug(f'Финальное сохранение: {processed} features')
                        break
                    except OperationalError as e:
                        if '1205' in str(e) and retry < 2:
                            wait = (retry + 1) * 0.3
                            logger.warning(f'Блокировка при финальном коммите features, retry {retry + 1}/3')
                            self.db_session.rollback()
                            time.sleep(wait)
                        else:
                            raise

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных в FEATURES: {e}'
        )
        self.db_session.rollback()
