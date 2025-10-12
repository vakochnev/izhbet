import logging

from db.models import (
    ChampionShip, Team, Coef, Match,
)
from config import Session_pool


logger = logging.getLogger(__name__)


def calculation_records_championship() -> int:
    with Session_pool() as session:
        return session.query(ChampionShip).count()


def calculation_records_team(team_id: int) -> int:
    with Session_pool() as session:
        return session.query(Team).filter(Team.id == team_id).count()


def calculation_records_coef() -> int:
    with Session_pool() as session:
        return session.query(Coef).count()


def calculation_records_match(match_id: int) -> int:
    with Session_pool() as session:
        return (
            session.query(Match).filter(
                Match.id == match_id).count()
        )

# def get_info_match(
#         db_session: DBSession,
#         match_id: int
# ):
#     match_info = {}
#     match = get_match_id(db_session, match_id)
#     game_data = dt.datetime.strftime(
#         match.gameData,
#         format=f'%d.%m.%Y %H:%M'
#     )
#     vs = sport.get_sport_id(
#         db_session,
#         sport_id=match.sport_id
#     ).sportName
#     co = country.get_country_id(
#         db_session,
#         country_id=match.country_id).countryName
#     tr = tournament.get_championship_id(
#         db_session,
#         championship_id=match.tournament_id,
#         data=match.gameData.timestamp()
#     )
#     if tr is not None:
#         tr = tr.nameTournament
#     else:
#         tr = f'Название турнира отсутствует id={match.tournament_id}'
#     try:
#         home = team.get_team_id(
#             db_session,
#             team_id=match.teamHome_id
#         ).teamName
#     except AttributeError:
#         home = 'Домашная команда не определена'
#     try:
#         guest = team.get_team_id(
#             db_session,
#             team_id=match.teamAway_id
#         ).teamName
#     except AttributeError:
#         guest = 'Гостевая команда не определена'
#     if not isinstance(match.numOfHeadsHome, int):
#         gol_home = ''
#     else:
#         gol_home = match.numOfHeadsHome
#     if not isinstance(match.numOfHeadsAway, int):
#         gol_guest = ''
#     else:
#         gol_guest = match.numOfHeadsAway
#
#     match_info = {
#         'gameData': game_data, 'sportName': vs, 'countryName': co,
#     }
#
#     return None


# def get_info_tournaments(db_session: DBSession) -> None:
#     match_all = match.get_match_all(db_session)
#     for match_db in match_all:
#         md = dt.datetime.strftime(match_db.gameData, "%d.%m.%Y %H:%M")
#         vs = sport.get_sport_id(
#             db_session,
#             sport_id=match_db.sport_id
#         ).sportName
#         co = country.get_country_id(
#             db_session,
#             country_id=match_db.country_id).countryName
#         tr = tournament.get_championship_id(
#             db_session,
#             championship_id=match_db.tournament_id,
#             data=match_db.gameData.timestamp()
#         )
#
#         if tr is not None:
#             tr = tr.nameTournament
#         else:
#             tr = f'Название турнира отсутствует id={match_db.tournament_id}'
#         try:
#             home = team.get_team_id(
#                 db_session,
#                 team_id=match_db.teamHome_id
#             ).teamName
#         except AttributeError:
#             home = 'Домашная команда не определена'
#         try:
#             guest = team.get_team_id(
#                 db_session,
#                 team_id=match_db.teamAway_id
#             ).teamName
#         except AttributeError:
#             guest = 'Гостевая команда не определена'
#         if not isinstance(match_db.numOfHeadsHome, int):
#             gol_home = ''
#         else:
#             gol_home = match_db.numOfHeadsHome
#         if not isinstance(match_db.numOfHeadsAway, int):
#             gol_guest = ''
#         else:
#             gol_guest = match_db.numOfHeadsAway
#
#         print(f'{md} {vs}={co}-{tr} {home}-{guest} ({gol_home}:{gol_guest})')
#
#         return None


# def checking_data_match(db_session: DBSession) -> None:
#     match_all = match.get_match_season_between(db_session)
#     for match_db in match_all:
#         md = dt.datetime.strftime(
#             match_db.gameData,
#             format="%d.%m.%Y %H:%M"
#         )
#         vs = sport.get_sport_id(
#             db_session,
#             sport_id=match_db.sport_id
#         ).sportName
#         co = country.get_country_id(
#             db_session,
#             country_id=match_db.country_id
#         ).countryName
#         tr = tournament.get_championship_id(
#             db_session,
#             championship_id=match_db.tournament_id,
#             data=match_db.gameData.timestamp()
#         )
#
#         if tr is not None:
#             tr = tr.nameTournament
#         else:
#             tr = f'Название турнира отсутствует id={match_db.tournament_id}'
#         try:
#             home = team.get_team_id(
#                 db_session,
#                 team_id=match_db.teamHome_id
#             ).teamName
#         except AttributeError:
#             home = 'Домашная команда не определена'
#         try:
#             guest = team.get_team_id(
#                 db_session,
#                 team_id=match_db.teamAway_id
#             ).teamName
#         except AttributeError:
#             guest = 'Гостевая команда не определена'
#         if not isinstance(match_db.numOfHeadsHome, int):
#             gol_home = ''
#         else:
#             gol_home = match_db.numOfHeadsHome
#         if not isinstance(match_db.numOfHeadsAway, int):
#             gol_guest = ''
#         else:
#             gol_guest = match_db.numOfHeadsAway
#         go = goal.get_goal_id(db_session, goal_id=match_db.id)
#         if go is None:
#             print(f'Not GOAL => {match_db.id} {md} {vs}={co}-{tr} {home}-{guest} ({gol_home}:{gol_guest})')


# def print_info_match(title: str, games: list) -> None:
#     print(title)
#     for game in games:
#         print(
#             f'{game.gameData} ' # {game.id}.
#             f'{game.sports.sportName} ' # {game.sports.id}.
#             f'{game.championships.championshipName} ' # {game.championships.id}.
#             f'({game.countrys.countryName}) ' # {game.country_id}.
#             f'{game.teamhomes.teamName}-' # {game.teamHome_id}.
#             f'{game.teamaways.teamName}' # {game.teamAway_id}.
#             f'({game.numOfHeadsHome}:{game.numOfHeadsAway})'
#         )
#     return None
