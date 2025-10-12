from db.models import Feature
from config import Session_pool, DBSession
from core.constants import NOT_IN_FEATURE, TARGET_FIELDS


def get_feature_match_id_prefix(
        db_session: DBSession,
        match_id: int,
        prefix: str
) -> Feature:
    return (
        db_session.query(Feature).
            filter(Feature.match_id == match_id).
            filter(Feature.prefix == prefix).first()
    )


def get_feature_match_all() -> list:
    with Session_pool() as session:
        return session.query(Feature).all()


def get_feature_match_ids(
        db_session: DBSession,
        match_ids: list[int]
) -> list[Feature]:
    """
    Возвращает по одной записи на каждый match_id, объединяя фичи из
    нескольких строк (по prefix) в одну запись. Имя каждой фичи дополняется
    суффиксом _{prefix}.

    Пример: было: general_games_played + prefix=home → стало: general_games_played_home
    """
    rows = (
        db_session.query(Feature).
        filter(Feature.match_id.in_(match_ids)).all()
    )

    # Группируем по match_id
    by_match: dict[int, list[Feature]] = {}
    for row in rows:
        by_match.setdefault(row.match_id, []).append(row)

    # Ключи, которые не считаются признаками и не переименовываются
    # Используем константы из core.constants
    non_feature_keys = (
            set(NOT_IN_FEATURE) | set(TARGET_FIELDS) |
            {'prefix', 'created_at', 'updated_at', 'matchs'}
    )

    class FeatureMerged:
        def __init__(self, data: dict):
            self._data = data

        def as_dict(self) -> dict:
            return self._data

    merged: list[FeatureMerged] = []

    for match_id, items in by_match.items():
        aggregated: dict = {'match_id': match_id}

        for item in items:
            # Получаем словарь полей исходной сущности
            try:
                data = item.as_dict()
            except Exception:
                # Fallback: используем атрибуты модели напрямую, если нет as_dict
                data = {c.name: getattr(item, c.name) for c in item.__table__.columns}

            prefix = data.get('prefix') or getattr(item, 'prefix', None)
            if prefix is None:
                # Если нет префикса, просто пропускаем переименование и добавление суффикса
                prefix = 'none'

            for key, value in data.items():
                if key in non_feature_keys:
                    # Сохраняем полезные нефичевые поля один раз (если нужны)
                    # if key in FEATURE_FIELDS
                    if key != 'prefix':
                        aggregated.setdefault(key, value)
                    continue

                # Переименовываем фичу с учетом префикса
                new_key = f"{key}_{prefix}"
                aggregated[new_key] = value

        merged.append(FeatureMerged(aggregated))

    return merged


def get_match_in_feature_all(
        db_session: DBSession,
        match_ids: list[int]
) -> list[Feature]:
    return (
        db_session.query(Feature).filter(
            Feature.match_id.in_(match_ids)
        ).all()
    )


def get_match_in_feature_all_pool(
        match_ids: list[int]
) -> list[Feature]:
    with Session_pool as db_session:
        return (
            db_session.query(Feature).filter(
                Feature.match_id.in_(match_ids)
            ).all()
        )
