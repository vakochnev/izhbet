from typing import Dict, Any, List
import logging
from datetime import datetime, timedelta, date
from sqlalchemy import desc, asc, func

from db.queries.championship import get_championship_season
from db.models import Match
from config import Session_pool, Session
from core.constants import LOAD_PICKLE, DIR_PICKLE


logger = logging.getLogger(__name__)


def get_match_id(db_session, match_id: int) -> Match:
    return db_session.query(Match).filter(
        Match.id == match_id
    ).one_or_none()

def get_match_id_pool(match_id: int) -> Match:
    with Session_pool() as session:
        return session.query(Match).filter(
            Match.id == match_id
        ).one_or_none()

def get_match_tournament_id(
    championship_id: int,
    played_only: bool = False
) -> list[Match]:
    """
    Получает матчи для конкретного чемпионата.
    
    Args:
        championship_id: ID чемпионата
        played_only: Если True, возвращает только прошедшие матчи
    """
    with Session_pool() as session:
        query = session.query(Match).filter(
            Match.tournament_id == championship_id
        )
        
        if played_only:
            query = query.filter(Match.gameData < datetime.utcnow())
            
        return query.order_by(Match.gameData).all()


def get_match_in_season(
    season: list
) -> list[Match]:
    with Session_pool() as session:
        return (
            session.query(Match).filter(
                Match.season_id.in_(season)
            ).order_by(asc(Match.gameData)).all()
        )


def get_match_in(
    match_ids: list[int]
) -> list[Match]:
    with Session_pool() as session:
        return (
            session.query(Match).filter(
                Match.id.in_(match_ids)
            ).order_by(asc(Match.gameData)).all()
        )


def get_match_not_in_season(season: list) -> list[Match]:
    with Session_pool() as session:
        return (
            session.query(Match).filter(
                Match.season_id.notin_(season)
            ).order_by(asc(Match.gameData)).all()
        )


def get_match_season_between() -> list[Match]:
    """
    Переделать надо получать начало сезона и конец
    из таблицы в виде цифр и по ним строить выборку
    """
    day_begin = datetime(
        year=2024, month=1, day=1, hour=0, minute=0, second=0
    )
    day_end = datetime(
        year=2024, month=12, day=31, hour=23, minute=59, second=59
    )
    with Session_pool() as session:
        return (
            session.query(Match).filter(
                Match.gameData.between(day_begin, day_end)
            ).all()
        )


def get_match(match_id: int) -> Match:
    with Session_pool() as session:
        return session.query(Match).filter(
            Match.id == match_id
        ).one()


def get_match_all() -> list[Match]:
    with Session_pool() as session:
        return session.query(Match).all()


def get_match_scalar() -> list[Match]:
    with Session_pool() as session:
        return session.query(func.count(Match.id)).scalar()


def get_match_count(match_id: int) -> int:
    with Session_pool() as session:
        return (
            session.query(Match).filter(
                Match.tournamet_id == match_id
            ).count()
        )


def select_match_yesterday(delta_pred_day):

    now_utc = datetime.utcnow()

    match_day_begin = now_utc.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    match_day_end = (
        match_day_begin -
        #timedelta(days=delta_pred_day) -
        timedelta(microseconds=1)
    )

    match_day_begin = match_day_begin - timedelta(days=delta_pred_day)

    with Session_pool() as session:
        query = (
            session.query(Match).
            filter(Match.gameData.between(
                match_day_begin, match_day_end)
            )
        )

    ids = [x.id for x in query.all()]

    team_home_ids = [x.teamHome_id for x in query.all()]
    team_away_ids = [x.teamAway_id for x in query.all()]
    team_ids = list(set(team_home_ids + team_away_ids))

    return ids, team_ids


def select_match_today(delta_next_day):

    now_utc = datetime.utcnow()

    match_day_begin = now_utc.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    match_day_end = (
        match_day_begin +
        timedelta(days=delta_next_day) -
        timedelta(microseconds=1)
    )

    with Session_pool() as session:
        query = (
            session.query(Match).
            filter(Match.gameData.between(
                match_day_begin, match_day_end)
            )
        )

    match_ids = [x.id for x in query.all()]

    team_home_ids = [x.teamHome_id for x in query.all()]
    team_away_ids = [x.teamAway_id for x in query.all()]
    team_ids = list(set(team_home_ids + team_away_ids))

    return match_ids, team_ids


def get_match_modeling(
    create_model: bool = False,
    current_year: bool = False
):
    """
        create_model: определяет какую часть матчей выбираем,
            True - прошедшие матчи, для создания модели
            False - матчи текущий сезон (прошедшие и не сыгранные)

        current_year: определяем фильтрацию матчей текущего сезона
            (определяем по дате today)
            True - прошедшие матчи
            False - несыгранные матчи
    """
    from core.utils import save_pickle, load_pickle

    if LOAD_PICKLE:
        current_season = load_pickle(f'{DIR_PICKLE}/match_modeling.pickl')
        if current_season is None:
            current_season = get_championship_season()
            save_pickle(f'{DIR_PICKLE}/match_modeling.pickl', current_season)
    else:
        current_season = get_championship_season()

    if create_model:
        logger.info(
            f'Начало отбора матчей для построение модели. '
            'Матчи все кроме текущего сезона. '
        )
        if LOAD_PICKLE:
            matchs = load_pickle(f'{DIR_PICKLE}/match_not_in_season.pickl')
            if matchs is None:
                matchs = get_match_not_in_season(current_season)
                save_pickle(f'{DIR_PICKLE}/match_not_in_season.pickl', matchs)
        else:
            matchs = get_match_not_in_season(current_season)

        logger.info(
            'Отобрано матчей для построение модели. '
            f'Все кроме текущего сезона. Отобрано: {len(matchs)}'
        )
    else:
        logger.info(
            'Начало отбора матчей для построение прогноза. '
            'Матчи только текущий сезон.'
        )
        if LOAD_PICKLE:
            matchs = load_pickle(f'{DIR_PICKLE}/match_in_season.pickl')
            if matchs is None:
                matchs = get_match_in_season(current_season)
                save_pickle(f'{DIR_PICKLE}/match_in_season.pickl', matchs)
        else:
            matchs = get_match_in_season(current_season)

        logger.info(
            'Отобрано матчей для построение прогноза. '
            f'Матчи только текущий сезон: {len(matchs)}'
        )

    return matchs


def get_championship_info(session: Any, match_id: int) -> Dict[str, Any]:
    """
    Получение информации о чемпионате по ID матча.

    Args:
        session: Сессия БД
        match_id: ID матча

    Returns:
        Информация о чемпионате
    """
    try:
        from db.models import Match, Championship, Sport, Country

        match = session.query(
            Match.championship_id,
            Championship.championshipName,
            Sport.sportName,
            Country.countryName
        ).join(Championship, Match.championship_id == Championship.id).join(
            Sport, Championship.sport_id == Sport.id
        ).join(Country, Championship.country_id == Country.id).filter(
            Match.id == match_id
        ).first()

        if match:
            return {
                'championship_id': match.championship_id,
                'championship_name': match.championshipName,
                'sport_name': match.sportName,
                'country_name': match.countryName
            }

    except Exception as e:
        logger.error(f"Ошибка получения информации о чемпионате: {e}")

    return {}


def get_matches_for_date(target_date: date) -> List[Dict]:
    """
    Получает список матчей на указанную дату из таблицы matches.
    
    Args:
        target_date: Дата для выборки матчей
        
    Returns:
        List[Dict]: Список матчей с полной информацией
    """
    from db.models import Team, ChampionShip, Sport
    
    with Session_pool() as session:
        TeamHome = Team.__table__.alias('team_home')
        TeamAway = Team.__table__.alias('team_away')
        
        query = session.query(
            Match.id,
            Match.gameData,
            Match.teamHome_id,
            Match.teamAway_id,
            Match.numOfHeadsHome,
            Match.numOfHeadsAway,
            Match.typeOutcome,
            Match.tournament_id,
            TeamHome.c.teamName.label('team_home_name'),
            TeamAway.c.teamName.label('team_away_name'),
            ChampionShip.championshipName,
            Sport.sportName
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
            Match.gameData
        )
        
        result = query.all()
        matches = [row._asdict() for row in result]
        return matches


def get_statistics_for_match(match_id: int) -> List[Dict]:
    """
    Получает statistics для конкретного матча.
    
    Args:
        match_id: ID матча
        
    Returns:
        List[Dict]: Список statistics
    """
    from db.models.statistics import Statistic
    
    with Session_pool() as session:
        query = session.query(Statistic).filter(Statistic.match_id == match_id)
        result = query.all()
        return [row.to_dict() if hasattr(row, 'to_dict') else row.__dict__ for row in result]