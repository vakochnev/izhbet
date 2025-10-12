from db.models.period import Period
from config import Session_pool


def get_period_id(
    db_session, match_id: int, period: str
) -> Period:
    a= (
        db_session.query(Period).
        filter(Period.match_id == match_id).
        filter(Period.period == period).
        first()
    )
    return a


def get_period(period_id: int) -> Period:
    with Session_pool() as session:
        return (
            session.query(Period).filter(
                Period.match_id == period_id
            ).one()
        )


def get_period_all() -> list:
    with Session_pool() as session:
        return session.query(Period).all()
