import logging

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
    try:
        logger.info(
            f'Сохранение данных STANDINGS по турниру: {tournament_id}'
        )
        for key in standings:
            match_team = key.split('_')
            match_id = int(match_team[0])
            team_id = int(match_team[1])

            standing_db = (
                get_standing_data_id(
                    self.db_session,
                    match_id,
                    team_id
                )
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
            self.db_session.commit()

    except Exception as err:
        logger.critical(
            f'Ошибка при сохранении данных в STANDING: {err}'
        )


def save_feature(self, tournament_id, features):
    try:
        logger.info(
            f'Сохранение данных FEATURE по турниру: {tournament_id}'
        )
        for match_id in features:
            for prefix in features[match_id]:

                feature_db = (
                    get_feature_match_id_prefix(
                        db_session=self.db_session,
                        match_id=match_id,
                        prefix=prefix
                    )
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

            self.db_session.commit()

    except AttributeError as e:
        logger.critical(
            f'Ошибка при сохранении данных в FEATURES: {e}'
        )
