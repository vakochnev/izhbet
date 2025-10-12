# izhbet/db/queries/statistics.py
"""
Запросы для работы с таблицей statistics.
"""

import logging
from datetime import date, datetime
from typing import List, Optional
import pandas as pd

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from config import Session_pool
from db.models.statistics import Statistic
from db.models.match import Match
from db.models.team import Team
from db.models.championship import ChampionShip
from db.models.sport import Sport
from db.models.prediction import Prediction
from db.models.outcome import Outcome
from db.models.target import Target

logger = logging.getLogger(__name__)


def get_statistics_for_today() -> pd.DataFrame:
    """
    Получает статистику прогнозов на сегодня.
    
    Returns:
        pd.DataFrame: DataFrame с прогнозами на сегодня
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Statistic.id,
            Statistic.outcome_id,
            Statistic.prediction_id,
            Statistic.match_id,
            Statistic.championship_id,
            Statistic.sport_id,
            Statistic.match_date,
            Statistic.match_round,
            Statistic.match_stage,
            Statistic.forecast_type,
            Statistic.forecast_subtype,
            Statistic.model_name,
            Statistic.model_version,
            Statistic.model_type,
            Statistic.actual_result,
            Statistic.actual_value,
            Statistic.prediction_correct,
            Statistic.prediction_accuracy,
            Statistic.prediction_error,
            Statistic.prediction_residual,
            Statistic.coefficient,
            Statistic.potential_profit,
            Statistic.actual_profit,
            Statistic.created_at,
            Statistic.updated_at,
            Match.gameData,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Statistic.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Statistic.championship_id == ChampionShip.id
        ).outerjoin(
            Sport, Statistic.sport_id == Sport.id
        ).filter(
            func.date(Match.gameData) == func.curdate()
        ).order_by(
            Match.gameData, Statistic.prediction_accuracy.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} прогнозов на сегодня из статистики')
        return df


def get_statistics_for_date(target_date: date) -> pd.DataFrame:
    """
    Получает статистику прогнозов на указанную дату.
    
    Args:
        target_date: Дата для загрузки
        
    Returns:
        pd.DataFrame: DataFrame с прогнозами на указанную дату
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Statistic.id,
            Statistic.outcome_id,
            Statistic.prediction_id,
            Statistic.match_id,
            Statistic.championship_id,
            Statistic.sport_id,
            Statistic.match_date,
            Statistic.match_round,
            Statistic.match_stage,
            Statistic.forecast_type,
            Statistic.forecast_subtype,
            Statistic.model_name,
            Statistic.model_version,
            Statistic.model_type,
            Statistic.actual_result,
            Statistic.actual_value,
            Statistic.prediction_correct,
            Statistic.prediction_accuracy,
            Statistic.prediction_error,
            Statistic.prediction_residual,
            Statistic.coefficient,
            Statistic.potential_profit,
            Statistic.actual_profit,
            Statistic.created_at,
            Statistic.updated_at,
            Match.gameData,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Statistic.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Statistic.championship_id == ChampionShip.id
        ).outerjoin(
            Sport, Statistic.sport_id == Sport.id
        ).filter(
            func.date(Match.gameData) == target_date
        ).order_by(
            Match.gameData, Statistic.prediction_accuracy.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} прогнозов на {target_date} из статистики')
        return df


def get_statistics_for_period(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Получает статистику прогнозов за указанный период.
    
    Args:
        start_date: Начальная дата
        end_date: Конечная дата
        
    Returns:
        pd.DataFrame: DataFrame с прогнозами за период
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Statistic.id,
            Statistic.outcome_id,
            Statistic.prediction_id,
            Statistic.match_id,
            Statistic.championship_id,
            Statistic.sport_id,
            Statistic.match_date,
            Statistic.match_round,
            Statistic.match_stage,
            Statistic.forecast_type,
            Statistic.forecast_subtype,
            Statistic.model_name,
            Statistic.model_version,
            Statistic.model_type,
            Statistic.actual_result,
            Statistic.actual_value,
            Statistic.prediction_correct,
            Statistic.prediction_accuracy,
            Statistic.prediction_error,
            Statistic.prediction_residual,
            Statistic.coefficient,
            Statistic.potential_profit,
            Statistic.actual_profit,
            Statistic.created_at,
            Statistic.updated_at,
            Match.gameData,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Statistic.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Statistic.championship_id == ChampionShip.id
        ).outerjoin(
            Sport, Statistic.sport_id == Sport.id
        ).filter(
            and_(
                func.date(Match.gameData) >= start_date,
                func.date(Match.gameData) <= end_date
            )
        ).order_by(
            Match.gameData, Statistic.prediction_accuracy.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} прогнозов за период {start_date} - {end_date} из статистики')
        return df


def get_statistics_summary() -> dict:
    """
    Получает сводную статистику по всем прогнозам.
    
    Returns:
        dict: Сводная статистика
    """
    with Session_pool() as session:
        # Общая статистика
        total_predictions = session.query(Statistic).count()
        correct_predictions = session.query(Statistic).filter(
            Statistic.prediction_correct == True
        ).count()
        
        # Статистика по типам прогнозов
        type_stats = session.query(
            Statistic.forecast_type,
            func.count(Statistic.id).label('total'),
            func.sum(func.case([(Statistic.prediction_correct == True, 1)], else_=0)).label('correct')
        ).group_by(
            Statistic.forecast_type
        ).all()
        
        # Статистика по моделям
        model_stats = session.query(
            Statistic.model_name,
            func.count(Statistic.id).label('total'),
            func.sum(func.case([(Statistic.prediction_correct == True, 1)], else_=0)).label('correct')
        ).group_by(
            Statistic.model_name
        ).all()
        
        return {
            'total_predictions': total_predictions,
            'correct_predictions': correct_predictions,
            'accuracy': correct_predictions / total_predictions if total_predictions > 0 else 0,
            'by_type': {row.forecast_type: {
                'total': row.total,
                'correct': row.correct,
                'accuracy': row.correct / row.total if row.total > 0 else 0
            } for row in type_stats},
            'by_model': {row.model_name: {
                'total': row.total,
                'correct': row.correct,
                'accuracy': row.correct / row.total if row.total > 0 else 0
            } for row in model_stats}
        }


def get_all_statistics() -> pd.DataFrame:
    """
    Получает всю статистику прогнозов.
    
    Returns:
        pd.DataFrame: DataFrame со всей статистикой
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Statistic.id,
            Statistic.outcome_id,
            Statistic.prediction_id,
            Statistic.match_id,
            Statistic.championship_id,
            Statistic.sport_id,
            Statistic.match_date,
            Statistic.match_round,
            Statistic.match_stage,
            Statistic.forecast_type,
            Statistic.forecast_subtype,
            Statistic.model_name,
            Statistic.model_version,
            Statistic.model_type,
            Statistic.actual_result,
            Statistic.actual_value,
            Statistic.prediction_correct,
            Statistic.prediction_accuracy,
            Statistic.prediction_error,
            Statistic.prediction_residual,
            Statistic.coefficient,
            Statistic.potential_profit,
            Statistic.actual_profit,
            Statistic.created_at,
            Statistic.updated_at,
            Match.gameData,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName,
            # Добавляем поля из таблицы outcomes
            Outcome.probability,
            Outcome.confidence,
            Outcome.uncertainty,
            Outcome.lower_bound,
            Outcome.upper_bound,
            Outcome.outcome,
            Outcome.forecast
        ).join(
            Match, Statistic.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Statistic.championship_id == ChampionShip.id
        ).outerjoin(
            Sport, Statistic.sport_id == Sport.id
        ).outerjoin(
            Outcome, Statistic.outcome_id == Outcome.id
        ).order_by(
            Match.gameData, Statistic.prediction_accuracy.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} записей из всей статистики')
        return df


def get_predictions_for_today() -> pd.DataFrame:
    """
    Получает обычные прогнозы на сегодня.
    
    Returns:
        pd.DataFrame: DataFrame с обычными прогнозами на сегодня
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
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
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Prediction.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).filter(
            func.date(Match.gameData) == func.curdate()
        ).order_by(
            Match.gameData, Prediction.created_at.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} обычных прогнозов на сегодня')
        return df


def get_predictions_for_date(target_date: date) -> pd.DataFrame:
    """
    Получает обычные прогнозы на указанную дату.
    
    Args:
        target_date: Дата для загрузки
        
    Returns:
        pd.DataFrame: DataFrame с обычными прогнозами на указанную дату
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
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
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Prediction.match_id == Match.id
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
            Match.gameData, Prediction.created_at.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} обычных прогнозов на {target_date}')
        return df


def get_all_predictions() -> pd.DataFrame:
    """
    Получает все обычные прогнозы.
    
    Returns:
        pd.DataFrame: DataFrame со всеми обычными прогнозами
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
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
            Match.typeOutcome,
            Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
        ).join(
            Match, Prediction.match_id == Match.id
        ).outerjoin(
            TeamHome, Match.teamHome_id == TeamHome.c.id
        ).outerjoin(
            TeamAway, Match.teamAway_id == TeamAway.c.id
        ).outerjoin(
            ChampionShip, Match.tournament_id == ChampionShip.id
        ).outerjoin(
            Sport, ChampionShip.sport_id == Sport.id
        ).order_by(
            Match.gameData, Prediction.created_at.desc()
        )
        
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} записей из всех обычных прогнозов')
        return df


def get_outcomes_for_today() -> pd.DataFrame:
    """
    Получает итоги матчей на сегодня.
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        query = session.query(
            Outcome.id, Outcome.match_id, Outcome.feature, Outcome.forecast,
            Outcome.outcome, Outcome.probability, Outcome.confidence,
            Outcome.uncertainty, Outcome.lower_bound, Outcome.upper_bound,
            Outcome.created_at, Outcome.updated_at,
            Match.gameData, Match.tournament_id, Match.teamHome_id, Match.teamAway_id,
            Match.numOfHeadsHome, Match.numOfHeadsAway, Match.typeOutcome, Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'), TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName, Sport.sportName,
            Target.target_win_draw_loss_home_win, Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win, Target.target_oz_both_score,
            Target.target_oz_not_both_score, Target.target_goal_home_yes,
            Target.target_goal_home_no, Target.target_goal_away_yes,
            Target.target_goal_away_no, Target.target_total_over,
            Target.target_total_under, Target.target_total_home_over,
            Target.target_total_home_under, Target.target_total_away_over,
            Target.target_total_away_under, Target.target_total_amount,
            Target.target_total_home_amount, Target.target_total_away_amount
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
        ).outerjoin(
            Target, Outcome.match_id == Target.match_id
        ).filter(
            and_(
                func.date(Match.gameData) == func.curdate(),
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.numOfHeadsHome >= 0,
                Match.numOfHeadsAway >= 0
            )
        ).order_by(
            Match.gameData, Outcome.created_at.desc()
        )
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} итогов матчей на сегодня')
        return df


def get_outcomes_for_date(target_date: date) -> pd.DataFrame:
    """
    Получает итоги матчей на указанную дату.
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        query = session.query(
            Outcome.id, Outcome.match_id, Outcome.feature, Outcome.forecast,
            Outcome.outcome, Outcome.probability, Outcome.confidence,
            Outcome.uncertainty, Outcome.lower_bound, Outcome.upper_bound,
            Outcome.created_at, Outcome.updated_at,
            Match.gameData, Match.tournament_id, Match.teamHome_id, Match.teamAway_id,
            Match.numOfHeadsHome, Match.numOfHeadsAway, Match.typeOutcome, Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'), TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName, Sport.sportName,
            Target.target_win_draw_loss_home_win, Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win, Target.target_oz_both_score,
            Target.target_oz_not_both_score, Target.target_goal_home_yes,
            Target.target_goal_home_no, Target.target_goal_away_yes,
            Target.target_goal_away_no, Target.target_total_over,
            Target.target_total_under, Target.target_total_home_over,
            Target.target_total_home_under, Target.target_total_away_over,
            Target.target_total_away_under, Target.target_total_amount,
            Target.target_total_home_amount, Target.target_total_away_amount
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
        ).outerjoin(
            Target, Outcome.match_id == Target.match_id
        ).filter(
            and_(
                func.date(Match.gameData) == target_date,
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.numOfHeadsHome >= 0,
                Match.numOfHeadsAway >= 0
            )
        ).order_by(
            Match.gameData, Outcome.created_at.desc()
        )
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} итогов матчей на {target_date}')
        return df


def get_all_outcomes() -> pd.DataFrame:
    """
    Получает все итоги матчей.
    """
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        query = session.query(
            Outcome.id, Outcome.match_id, Outcome.feature, Outcome.forecast,
            Outcome.outcome, Outcome.probability, Outcome.confidence,
            Outcome.uncertainty, Outcome.lower_bound, Outcome.upper_bound,
            Outcome.created_at, Outcome.updated_at,
            Match.gameData, Match.tournament_id, Match.teamHome_id, Match.teamAway_id,
            Match.numOfHeadsHome, Match.numOfHeadsAway, Match.typeOutcome, Match.gameComment,
            TeamHome.c.teamName.label('team_home_name'), TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName, Sport.sportName,
            Target.target_win_draw_loss_home_win, Target.target_win_draw_loss_draw,
            Target.target_win_draw_loss_away_win, Target.target_oz_both_score,
            Target.target_oz_not_both_score, Target.target_goal_home_yes,
            Target.target_goal_home_no, Target.target_goal_away_yes,
            Target.target_goal_away_no, Target.target_total_over,
            Target.target_total_under, Target.target_total_home_over,
            Target.target_total_home_under, Target.target_total_away_over,
            Target.target_total_away_under, Target.target_total_amount,
            Target.target_total_home_amount, Target.target_total_away_amount
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
        ).outerjoin(
            Target, Outcome.match_id == Target.match_id
        ).filter(
            and_(
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.numOfHeadsHome >= 0,
                Match.numOfHeadsAway >= 0
            )
        ).order_by(
            Match.gameData, Outcome.created_at.desc()
        )
        result = query.all()
        df = pd.DataFrame([row._asdict() for row in result])
        logger.info(f'Загружено {len(df)} записей из всех итогов матчей')
        return df
