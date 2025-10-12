from db.models import Country
from core.constants import SPR_COUNTRY_TOP
from config import Session_pool, db_session, db_session_pool


def get_country_id(db_session, country_id: int) -> Country:
    return (
        db_session.query(Country).
        filter(Country.id == country_id).first()
    )


def get_country_id_pool(country_id: int) -> Country:
    with Session_pool() as db_session:
        return (
            db_session.query(Country).
            filter(Country.id == country_id).first()
        )


def get_country_all() -> list:
    with Session_pool() as session:
        return session.query(Country).all()


def get_country_count() -> int:
    with Session_pool() as session:
        return session.query(Country).count()


def is_country_top(country_id: int) -> bool:
    for data in SPR_COUNTRY_TOP:
        if data['catId'] == country_id:
            return True
    return False
