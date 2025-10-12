from config import Session_pool, db_session, db_session_pool
from db.models import Tournament
from db.queries.sport import get_sport_id
from db.queries.country import get_country_id


def get_tournament_id(
    db_session,
    tournament_id: int
) -> Tournament:
    return (
        db_session.query(Tournament).
            filter(Tournament.id == tournament_id).first()
    )


# def get_tournament_id_(
#     tournament_id: int
# ) -> Tournament:
#     a = (
#         db_session.query(Tournament).
#             filter(Tournament.id == tournament_id).first()
#     )
#     return a

def get_tournament_id_pool(tournament_id: int) -> Tournament:
    with Session_pool() as session:
        return (
            session.query(Tournament).
                filter(Tournament.id == tournament_id).first()
        )


def get_championship_id(
    championship_id: int,
    data: int
) -> Tournament:
    with Session_pool() as session:
        return (
            session.query(Tournament).filter(
                Tournament.championship_id == championship_id,
                Tournament.startTournament <= data,
                Tournament.endTournament >= data)
            .first()
        )


def get_tournament_all() -> list[Tournament]:
    with Session_pool() as session:
        return session.query(Tournament).all()


def get_season_tournament(season) -> list[Tournament]:
    with Session_pool() as session:
        return (
            session.query(Tournament).
                filter(Tournament.id.in_(season)).
                #order_by(Tournament.championships[0].country_id).
                all()
        )


def get_past_tournament(season) -> list[Tournament]:
    with Session_pool() as session:
        return (
            session.query(Tournament).
            filter(Tournament.id.notin_(season))
        ).all()


def get_tournament_count() -> int:
    with Session_pool() as session:
        return session.query(Tournament).count()


def get_info_shot_tournament(tournament_id) -> str:
    with Session_pool() as session:
        tournament = session.query(Tournament).filter(
            Tournament.championship_id == tournament_id
        ).first()

        vs = get_sport_id(
            sport_id=tournament.sport_id
        ).sportName

        co = get_country_id(
            country_id=tournament.championships[0].country_id
        ).countryName

        ne = tournament.championships[0].championshipName

        return f'{vs} {co} {ne}'
