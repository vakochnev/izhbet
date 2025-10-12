import datetime as dt
from sqlalchemy import asc
from typing import Tuple, Dict, Any, List, Union

from db.models import Prediction, Match
from config import Session_pool, Session, DBSession


def get_prediction_match_id(
    db_session: Session,
    match_id: int
) -> List[Prediction]:
    return (
        db_session.query(Prediction).
            filter(Prediction.match_id == match_id).
            first()
    )


def get_prediction_match_all() -> list[Prediction]:
    with Session_pool() as session:
        return (
            session.query(Prediction).
            order_by(asc(Prediction.id)).all()
        )


def get_match_in_prediction_all(
        db_session: DBSession,
        match_ids: list[int]
) -> list[Prediction]:
    return (
        db_session.query(Prediction).filter(
            Prediction.match_id.in_(match_ids)
        ).all()
    )


def get_match_in_prediction_all_pool(
        match_ids: list[int]
) -> list[Prediction]:
    with Session_pool() as db_session:
        return (
            db_session.query(Prediction).filter(
                Prediction.match_id.in_(match_ids)
            ).all()
        )


def get_prediction_matchs(all=True, tournament_id=None):
    with Session_pool() as db_session:
        today = dt.datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        if all:
            query = db_session.query(Prediction.match_id)
        else:
            query = db_session.query(Prediction.match_id).join(Match).filter(
                Match.gameData < today
            )

        if tournament_id is not None:
            query = query.join(Match).filter(Match.tournament_id == tournament_id)

        return [x.match_id for x in query.all()]
