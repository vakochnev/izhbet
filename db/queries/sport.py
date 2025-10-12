from db.models import Sport
from config import Session_pool, db_session, db_session_pool

SPR_SPORTS = {1: 'Футбол', 4: 'Хоккей'}


def get_sport_id(db_session, sport_id: int) -> Sport:
    return (
        db_session.query(Sport).filter(
            Sport.id == sport_id
        ).first()
    )


def get_sport_id_pool(sport_id: int) -> Sport:
    with Session_pool() as db_session:
        return (
            db_session_pool.query(Sport).filter(
                Sport.id == sport_id
            ).first()
        )


def get_sport_all() -> list:
    with Session_pool() as session:
        return session.query(Sport).all()


def get_sport_count() -> int:
    with Session_pool() as session:
        return session.query(Sport).count()
