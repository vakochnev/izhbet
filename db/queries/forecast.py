"""
Модуль для запросов данных, связанных с модулем forecast.
Содержит все ORM-запросы для модуля forecast.
"""

import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import pandas as pd

from sqlalchemy.orm import Session, aliased
from sqlalchemy import and_, func, desc, distinct
from config import Session_pool

from db.models.outcome import Outcome
from db.models.prediction import Prediction
from db.models.match import Match
from db.models.target import Target
from db.models.team import Team
from db.models.championship import ChampionShip
from db.models.sport import Sport

logger = logging.getLogger(__name__)


def get_conformal_forecasts_for_today() -> pd.DataFrame:
    """
    Загружает конформные прогнозы на сегодняшние матчи.
    
    Returns:
        DataFrame с прогнозами на сегодня
    """
    with Session_pool() as db_session:
        # Создаем алиасы для таблиц команд
        TeamHome = aliased(Team)
        TeamAway = aliased(Team)
        
        query = db_session.query(
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
            Outcome.updated_at,
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
            Sport.sportName,
            Target.target_win_draw_loss_home_win,
            Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win,
            Target.target_oz_both_score,
            Target.target_oz_not_both_score,
            Target.target_goal_home_yes,
            Target.target_goal_home_no,
            Target.target_goal_away_yes,
            Target.target_goal_away_no,
            Target.target_total_over,
            Target.target_total_under,
            Target.target_total_home_over,
            Target.target_total_home_under,
            Target.target_total_away_over,
            Target.target_total_away_under,
            Target.target_total_amount,
            Target.target_total_home_amount,
            Target.target_total_away_amount
        ).join(
            Match, Outcome.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).outerjoin(
            Target, Outcome.match_id == Target.match_id
        ).filter(
            func.date(Match.gameData) == func.curdate()
        ).order_by(
            desc(Outcome.confidence),
            desc(Outcome.probability)
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        
        logger.info(f"Загружено {len(df)} конформных прогнозов на сегодня")
        return df


def get_yesterday_outcomes() -> pd.DataFrame:
    """
    Загружает исходы вчерашних матчей с завершенными результатами.
        
    Returns:
        DataFrame с результатами вчерашних матчей
    """
    with Session_pool() as db_session:
        # Создаем алиасы для таблиц команд
        TeamHome = aliased(Team)
        TeamAway = aliased(Team)
        
        # Вычисляем дату вчера
        yesterday = date.today() - timedelta(days=1)
        
        query = db_session.query(
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
            Outcome.updated_at,
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
            Sport.sportName,
            Target.target_win_draw_loss_home_win,
            Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win,
            Target.target_oz_both_score,
            Target.target_oz_not_both_score,
            Target.target_goal_home_yes,
            Target.target_goal_home_no,
            Target.target_goal_away_yes,
            Target.target_goal_away_no,
            Target.target_total_over,
            Target.target_total_under,
            Target.target_total_home_over,
            Target.target_total_home_under,
            Target.target_total_away_over,
            Target.target_total_away_under,
            Target.target_total_amount,
            Target.target_total_home_amount,
            Target.target_total_away_amount
        ).join(
            Match, Outcome.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).outerjoin(
            Target, Outcome.match_id == Target.match_id
        ).filter(
            and_(
                func.date(Match.gameData) == yesterday,
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.numOfHeadsHome >= 0,
                Match.numOfHeadsAway >= 0
            )
        ).order_by(
            desc(Outcome.confidence),
            desc(Outcome.probability)
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        
        logger.info(f"Загружено {len(df)} исходов вчерашних матчей с завершенными результатами")
        return df


def get_forecasts_for_date(target_date: date) -> pd.DataFrame:
    """
    Загружает прогнозы на указанную дату.
    
    Args:
        target_date: Дата для загрузки прогнозов
        
    Returns:
        DataFrame с прогнозами на указанную дату
    """
    with Session_pool() as session:
        # Создаем алиасы для таблиц команд
        TeamHome = aliased(Team)
        TeamAway = aliased(Team)
        
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
            Outcome.updated_at,
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
            Sport.sportName,
            Target.target_win_draw_loss_home_win,
            Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win,
            Target.target_oz_both_score,
            Target.target_oz_not_both_score,
            Target.target_goal_home_yes,
            Target.target_goal_home_no,
            Target.target_goal_away_yes,
            Target.target_goal_away_no,
            Target.target_total_over,
            Target.target_total_under,
            Target.target_total_home_over,
            Target.target_total_home_under,
            Target.target_total_away_over,
            Target.target_total_away_under,
            Target.target_total_amount,
            Target.target_total_home_amount,
            Target.target_total_away_amount
        ).join(
            Match, Outcome.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).outerjoin(
            Target, Outcome.match_id == Target.match_id
        ).filter(
            func.date(Match.gameData) == target_date
        ).order_by(
            desc(Outcome.confidence),
            desc(Outcome.probability)
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        
        logger.info(f"Загружено {len(df)} прогнозов на {target_date}")
        return df


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


def get_tournament_match_dates(tournament_id: int) -> List[str]:
    """
    Получает список дат матчей в чемпионате, для которых есть прогнозы.
    
    Args:
        tournament_id: ID турнира
    
    Returns:
        Список дат в формате строк
    """
    with Session_pool() as session:
        query = session.query(
            func.date(Match.gameData).label('match_date')
        ).join(
            Outcome, Match.id == Outcome.match_id
        ).filter(
            Match.tournament_id == tournament_id
        ).distinct().order_by(
            func.date(Match.gameData)
        )
        
        result = query.all()
        dates = [row.match_date.strftime('%Y-%m-%d') for row in result if row.match_date]
        
        logger.info(f"Найдено {len(dates)} дат матчей с прогнозами в чемпионате {tournament_id}")
        return dates


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


def get_tournament_ids_with_predictions() -> List[int]:
    """
    Получает список ID турниров, для которых есть прогнозы.
    
    Returns:
        Список ID турниров с прогнозами
    """
    with Session_pool() as session:
        query = session.query(
            distinct(Match.tournament_id)
        ).join(
            Prediction, Match.id == Prediction.match_id
        ).filter(
            Match.tournament_id.isnot(None)
        ).order_by(
            Match.tournament_id
        )
        
        result = query.all()
        tournament_ids = [row[0] for row in result if row[0] is not None]
        
        logger.info(f"Найдено {len(tournament_ids)} турниров с прогнозами")
        return tournament_ids


def get_predictions_for_tournament(session: Session, tournament_id: int):
    """
    Получает прогнозы для указанного турнира.
    
    Args:
        session: Сессия базы данных
        tournament_id: ID турнира
        
    Returns:
        List[Dict]: список словарей с данными прогнозов для турнира
    """
    query = (
        session.query(
            Prediction.id,
            Prediction.match_id,
            Prediction.win_draw_loss_home_win,
            Prediction.win_draw_loss_draw,
            Prediction.win_draw_loss_away_win,
            Prediction.oz_yes,
            Prediction.oz_no,
            Prediction.goal_home_yes,
            Prediction.goal_home_no,
            Prediction.goal_away_yes,
            Prediction.goal_away_no,
            Prediction.total_yes,
            Prediction.total_no,
            Prediction.total_home_yes,
            Prediction.total_home_no,
            Prediction.total_away_yes,
            Prediction.total_away_no,
            Prediction.forecast_total_amount,
            Prediction.forecast_total_home_amount,
            Prediction.forecast_total_away_amount,
            Prediction.created_at,
            Prediction.updated_at
        )
        .join(Match, Prediction.match_id == Match.id)
        .filter(Match.tournament_id == tournament_id)
        .order_by(Match.gameData.desc())
    )

    predictions = query.all()
    logger.info(f"Загружено {len(predictions)} прогнозов для турнира {tournament_id}")
    
    # Преобразуем результаты в список словарей
    prediction_dicts = []
    for pred in predictions:
        pred_dict = {
            'id': pred.id,
            'match_id': pred.match_id,
            'win_draw_loss_home_win': pred.win_draw_loss_home_win,
            'win_draw_loss_draw': pred.win_draw_loss_draw,
            'win_draw_loss_away_win': pred.win_draw_loss_away_win,
            'oz_yes': pred.oz_yes,
            'oz_no': pred.oz_no,
            'goal_home_yes': pred.goal_home_yes,
            'goal_home_no': pred.goal_home_no,
            'goal_away_yes': pred.goal_away_yes,
            'goal_away_no': pred.goal_away_no,
            'total_yes': pred.total_yes,
            'total_no': pred.total_no,
            'total_home_yes': pred.total_home_yes,
            'total_home_no': pred.total_home_no,
            'total_away_yes': pred.total_away_yes,
            'total_away_no': pred.total_away_no,
            'forecast_total_amount': pred.forecast_total_amount,
            'forecast_total_home_amount': pred.forecast_total_home_amount,
            'forecast_total_away_amount': pred.forecast_total_away_amount,
            'created_at': pred.created_at,
            'updated_at': pred.updated_at
        }
        prediction_dicts.append(pred_dict)
    
    return prediction_dicts


def get_training_predictions() -> pd.DataFrame:
    """
    Получает прогнозы для обучения конформного предиктора.
    
    Returns:
        DataFrame с прогнозами для обучения
    """
    with Session_pool() as session:
        # Создаем алиасы для таблиц команд
        TeamHome = aliased(Team)
        TeamAway = aliased(Team)
        
        query = session.query(
            Prediction.id,
            Prediction.match_id,
            Prediction.win_draw_loss_home_win,
            Prediction.win_draw_loss_draw,
            Prediction.win_draw_loss_away_win,
            Prediction.oz_yes,
            Prediction.oz_no,
            Prediction.goal_home_yes,
            Prediction.goal_home_no,
            Prediction.goal_away_yes,
            Prediction.goal_away_no,
            Prediction.total_yes,
            Prediction.total_no,
            Prediction.total_home_yes,
            Prediction.total_home_no,
            Prediction.total_away_yes,
            Prediction.total_away_no,
            Prediction.forecast_total_amount,
            Prediction.forecast_total_home_amount,
            Prediction.forecast_total_away_amount,
            Prediction.created_at,
            Match.gameData,
            Match.tournament_id,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            TeamHome.teamName.label('teamHome_name'),
            TeamAway.teamName.label('teamAway_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Prediction.match_id == Match.id
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
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None)
            )
        ).order_by(
            Match.gameData.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        
        logger.info(f"Загружено {len(df)} прогнозов для обучения")
        return df


def get_training_targets() -> pd.DataFrame:
    """
    Получает целевые переменные для обучения конформного предиктора.
    
    Returns:
        DataFrame с целевыми переменными для обучения
    """
    with Session_pool() as session:
        query = session.query(
            Target.match_id,
            Target.target_win_draw_loss_home_win,
            Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win,
            Target.target_oz_both_score,
            Target.target_oz_not_both_score,
            Target.target_goal_home_yes,
            Target.target_goal_home_no,
            Target.target_goal_away_yes,
            Target.target_goal_away_no,
            Target.target_total_over,
            Target.target_total_under,
            Target.target_total_home_over,
            Target.target_total_home_under,
            Target.target_total_away_over,
            Target.target_total_away_under,
            Target.target_total_amount,
            Target.target_total_home_amount,
            Target.target_total_away_amount
        ).join(
            Match, Target.match_id == Match.id
        ).filter(
            and_(
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None)
            )
        ).order_by(
            Match.gameData.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        
        logger.info(f"Загружено {len(df)} целевых переменных для обучения")
        return df
