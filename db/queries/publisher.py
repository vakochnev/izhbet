"""
Модуль для запросов данных, связанных с публикацией прогнозов.
Содержит все ORM-запросы для модуля publisher.
"""

import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import pandas as pd

from sqlalchemy.orm import Session, aliased
from sqlalchemy import and_, func, desc
from config import Session_pool

from db.models.outcome import Outcome
from db.models.match import Match
from db.models.target import Target
from db.models.team import Team
from db.models.championship import ChampionShip
from db.models.sport import Sport

logger = logging.getLogger(__name__)


def get_all_tournaments(year: Optional[str] = None) -> List[int]:
    """
    Получает список всех турниров.
    
    Args:
        year: Год турнира (опционально)
        
    Returns:
        Список ID турниров
    """
    with Session_pool() as session:
        query = session.query(ChampionShip.id)
        
        if year:
            query = query.filter(ChampionShip.year == year)
        
        tournaments = query.all()
        return [t.id for t in tournaments]


def get_tournament_by_id(tournament_id: int) -> Optional[Dict[str, Any]]:
    """
    Получает информацию о турнире по ID.
    
    Args:
        tournament_id: ID турнира
        
    Returns:
        Словарь с информацией о турнире или None
    """
    with Session_pool() as session:
        tournament = session.query(ChampionShip).filter(
            ChampionShip.id == tournament_id
        ).first()
        
        if tournament:
            return tournament.as_dict()
        return None


def get_matches_by_tournament_and_date(tournament_id: int, match_date: date) -> pd.DataFrame:
    """
    Получает матчи турнира на указанную дату.
    
    Args:
        tournament_id: ID турнира
        match_date: Дата матчей
        
    Returns:
        DataFrame с матчами
    """
    with Session_pool() as session:
        # Создаем алиасы для таблиц команд
        TeamHome = aliased(Team)
        TeamAway = aliased(Team)
        
        query = session.query(
            Match.id,
            Match.gameData,
            Match.tournament_id,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.teamName.label('teamHome_name'),
            TeamAway.teamName.label('teamAway_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).filter(
            and_(
                Match.tournament_id == tournament_id,
                func.date(Match.gameData) == match_date
            )
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        
        logger.info(f"Загружено {len(df)} матчей турнира {tournament_id} на {match_date}")
        return df


def get_outcomes_by_match_ids(match_ids: List[int]) -> pd.DataFrame:
    """
    Получает исходы прогнозов для указанных матчей.
    
    Args:
        match_ids: Список ID матчей
        
    Returns:
        DataFrame с исходами прогнозов
    """
    with Session_pool() as session:
        query = session.query(Outcome).filter(
            Outcome.match_id.in_(match_ids)
        )
        
        result = query.all()
        df = pd.DataFrame([outcome.as_dict() for outcome in result])
        
        logger.info(f"Загружено {len(df)} исходов прогнозов для {len(match_ids)} матчей")
        return df


def get_targets_by_match_ids(match_ids: List[int]) -> pd.DataFrame:
    """
    Получает целевые переменные для указанных матчей.
    
    Args:
        match_ids: Список ID матчей
        
    Returns:
        DataFrame с целевыми переменными
    """
    with Session_pool() as session:
        query = session.query(Target).filter(
            Target.match_id.in_(match_ids)
        )
        
        result = query.all()
        df = pd.DataFrame([target.as_dict() for target in result])
        
        logger.info(f"Загружено {len(df)} целевых переменных для {len(match_ids)} матчей")
        return df