from db.models import ChampionShip, Sport
from core.constants import SPR_COUNTRY_TOP
from config import Session_pool


def get_championship_id(
    db_session,
    championship_id: int
) -> ChampionShip:
    return (
        db_session.query(ChampionShip).
        filter(ChampionShip.id == championship_id).
        first()
    )


def get_championship_all() -> list:
    with Session_pool() as session:
        return session.query(ChampionShip).all()


def get_championship_count() -> int:
    with Session_pool() as session:
        return session.query(ChampionShip).count()


def select_championship_all() -> list:
    with Session_pool() as session:
        return session.query(ChampionShip).join(Sport).all()


def get_championship_league(
    championship_id: int
) -> ChampionShip:
    with Session_pool() as session:
        return (
            session.query(ChampionShip).
            filter(ChampionShip.id == championship_id).all()
        )


def is_chmpionship_top(championship_id: int) -> bool:
    for data in SPR_COUNTRY_TOP:
        if data['id'] == championship_id:
            return True
    return False


def get_championship_season():
    with Session_pool() as session:
        season = session.query(ChampionShip.curSeason).all()
        return [s[0] for s in season]
