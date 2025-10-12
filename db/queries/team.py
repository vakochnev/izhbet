from db.models import Team, ChampionShip
from config import Session_pool, db_session_pool


def get_team_id(db_session, team_id: int) -> Team:
    return (
        db_session.query(Team).filter(
            Team.id == team_id
        ).first()
    )


def get_team_id_pool(team_id: int) -> Team:
    with Session_pool() as db:
        return (
            db.query(Team).filter(
                Team.id == team_id
            ).first()
        )


def get_team(team_id: int) -> Team:
    with Session_pool() as session:
        return (
            session.query(Team).filter(
                Team.id == team_id
            ).one()
        )


def get_team_tournament_count(team_id: int) -> int:
    with Session_pool() as session:
        return (
            session.query(Team).
                filter(Team.championships.id == team_id).
                count()
        )


def select_team_all() -> list:
    with Session_pool() as session:
        return (
            session.query(Team).join(ChampionShip).all()
        )


def select_team_tournament(team_id: int) -> list:
    with Session_pool() as session:
        return (
            session.query(Team).
                filter(Team.championships.championship_id == team_id)
            .all()
        )


def get_team_name(team_id: int) -> Team:
    with Session_pool() as session:
        return (
            session.query(Team).filter(
                Team.team_id == team_id
            ).one()
        )


def get_team_all() -> list:
    with Session_pool() as session:
        return session.query(Team).all()
