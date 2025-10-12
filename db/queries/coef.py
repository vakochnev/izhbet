from db.models import Coef
from config import Session_pool


def get_coef_id(db_session, coef_id: int) -> Coef:
    return (
        db_session.query(Coef).filter(
            Coef.match_id == coef_id
        ).first()
    )


def get_coef(coef_idid: int) -> Coef:
    with Session_pool() as session:
        return (
            session.query(Coef).filter(
                Coef.match_id == coef_idid
            ).one()
        )


def get_coef_all() -> list:
    with Session_pool() as session:
        return session.query(Coef).all()
