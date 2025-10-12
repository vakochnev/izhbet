from db.models.goal import Goal
from config import Session_pool


def get_goal_id(db_session, goal_id: int) -> Goal:
    return (
        db_session.query(Goal).filter(
            Goal.match_id == goal_id
        ).first()
    )


def get_goal(goal_id: int) -> Goal:
    with Session_pool() as session:
        return (
            session.query(Goal).filter(
                Goal.match_id == goal_id
            ).one()
        )


def get_goal_all() -> list:
    with Session_pool() as session:
        return session.query(Goal).all()
