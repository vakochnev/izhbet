from db.models import Outcome, Match, Team, ChampionShip, Sport
from config import Session_pool
from sqlalchemy import func
import pandas as pd
from datetime import date
import logging

logger = logging.getLogger(__name__)


def get_outcome_match_outcome(
    match_id: int,
    outcome: str
) -> Outcome:
    with Session_pool() as session:
        return (
            session.query(Outcome).
                filter(Outcome.match_id == match_id).
                filter(Outcome.outcome == outcome).
            first()
        )


def get_outcome_all():
    with Session_pool() as session:
        return session.query(Outcome).all()


def get_outcomes_for_date(target_date: date) -> pd.DataFrame:
    """
    Получает outcomes за указанную дату.
    
    Args:
        target_date: Дата для фильтрации
        
    Returns:
        pd.DataFrame: DataFrame с outcomes за дату
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Outcome.id,
            Outcome.match_id,
            Outcome.feature,
            Outcome.forecast,
            Outcome.outcome,
            Outcome.probability,
            Outcome.confidence,
            Outcome.uncertainty,
            Outcome.lower_bound,
            Outcome.upper_bound,
            Outcome.created_at,
            Match.gameData,
            Match.tournament_id,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName.label('championshipName'),
            Sport.sportName.label('sportName')
        ).join(
            Match, Outcome.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).filter(
            func.date(Match.gameData) == target_date
        ).order_by(
            Outcome.created_at.desc(), Match.gameData
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} outcomes на {target_date}')
        return df


def get_all_outcomes() -> pd.DataFrame:
    """
    Получает все outcomes.
    
    Returns:
        pd.DataFrame: DataFrame со всеми outcomes
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Outcome.id,
            Outcome.match_id,
            Outcome.feature,
            Outcome.forecast,
            Outcome.outcome,
            Outcome.probability,
            Outcome.confidence,
            Outcome.uncertainty,
            Outcome.lower_bound,
            Outcome.upper_bound,
            Outcome.created_at,
            Match.gameData,
            Match.tournament_id,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName.label('championshipName'),
            Sport.sportName.label('sportName')
        ).join(
            Match, Outcome.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).order_by(
            Outcome.created_at.desc(), Match.gameData
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} записей из всех outcomes')
        return df


def get_outcomes_for_today() -> pd.DataFrame:
    """
    Получает outcomes на сегодня.
    
    Returns:
        pd.DataFrame: DataFrame с outcomes на сегодня
    """
    from datetime import datetime
    today = datetime.now().date()
    return get_outcomes_for_date(today)
