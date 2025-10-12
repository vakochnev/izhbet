from db.models import Standing
from config import Session_pool, Session


def get_standing_data_id(
    db_session,
    match_id,
    team_id
) -> Standing:
    # with Session_pool() as db:
    return (
        db_session.query(Standing).
            filter(
                Standing.match_id == match_id,
                Standing.team_id == team_id
            ).
            first()
    )


def get_standing_id(team_id) -> Standing:
    with Session_pool() as session:
        return (
            session.query(Standing).
                filter(Standing.team_id == team_id).
                first()
        )

# def delete_standings_by_match_ids(self, db_session, match_ids):
#     try:
#         db_session.query(Standing).filter(
#             Standing.match_id.in_([int(x) for x in match_ids])
#         ).delete(synchronize_session=False)
#     except Exception as e:
#         raise


def get_model_columns(model):
    """Возвращает список колонок модели"""
    return [c.key for c in model.__table__.columns]


def object_to_dict(obj, columns):
    """
    Преобразует объект в словарь только с указанными полями.
    Пробует приводить значения к int или float, если возможно.
    """
    result = {}
    for col in columns:
        value = getattr(obj, col, None)

        if value is None:
            result[col] = None
            continue

        try:
            # Попробуем привести к числу
            if '.' in str(value):
                result[col] = float(value)
            else:
                result[col] = int(value)
        except (ValueError, TypeError):
            result[col] = value

    return result