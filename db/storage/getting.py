import logging
import datetime as dt
import time
from sqlalchemy.exc import OperationalError

from config import DBSession, Session_pool
from db.models.sport import Sport
from db.models.country import Country
from db.models.tournament import Tournament
from db.models.championship import ChampionShip
from db.models.match import Match
from db.models.team import Team
from db.models.goal import Goal
from db.models.period import Period
from db.models.coef import Coef
from db.models.table import Table
from db.queries.sport import get_sport_id
from db.queries.country import get_country_id
from db.queries.tournament import get_tournament_id
from db.queries.championship import get_championship_id
from db.queries.match import get_match_id
from db.queries.team import get_team_id
from db.queries.goal import get_goal_id
from db.queries.period import get_period_id
from db.queries.coef import get_coef_id
from db.queries.table import get_table_id
from db.queries.general import (
    calculation_records_team, calculation_records_match
)
from core.logger_message import MEASSGE_LOG


logger = logging.getLogger(__name__)


def retry_on_lock(func):
    """Декоратор для повтора при блокировке БД."""
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except OperationalError as e:
                if '1205' in str(e) and attempt < max_retries - 1:
                    # Lock wait timeout - ждем и повторяем
                    wait_time = (attempt + 1) * 0.5  # 0.5, 1.0, 1.5 секунд
                    logger.warning(f'Блокировка БД, повтор через {wait_time}s (попытка {attempt + 1}/{max_retries})')
                    time.sleep(wait_time)
                else:
                    raise
    return wrapper


def save_sport(self, data):
    try:
        record = (
            get_sport_id(self.db_session, data['id'])
        )
        if record is None:
            record = Sport(id=data['id'])

        record.id = data['id']
        record.sportName = data['name']
        record.isActive = data['isActive']

        self.db_session.add_model(record)
        self.db_session.commit()

        logger.debug(MEASSGE_LOG['saving_db_sport'])

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных в SPORT: {e}'
        )


def save_country(self, data_save):
    try:
        for data in data_save:
                country_record = (
                    get_country_id(self.db_session, data['id'])
                )
                if country_record is None:
                    country_record = Country(id=data['id'])

                country_record.id = data['id']
                country_record.sport_id = data['sId']
                country_record.countryName = data['name']
                country_record.countryCode = data['countryCode']

                self.db_session.add_model(country_record)
                self.db_session.commit()

                logger.debug(MEASSGE_LOG['saving_db_country'])

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных в COUNTRY: {e}'
        )


def save_tournament(self, data_save):
    try:
        for data in data_save:
            tournament_record = (
                get_tournament_id(self.db_session, data['id'])
            )
            if tournament_record is None:
                tournament_record = Tournament(id=data['id'])

            tournament_record.id = data['id']
            tournament_record.sport_id = data['sId']
            tournament_record.championship_id = data['tId']
            tournament_record.nameTournament = data['name']
            tournament_record.shortNameTournament = data['shortName']
            tournament_record.yearTournament = data['year']
            tournament_record.startTournament = data['start']
            tournament_record.endTournament = data['end']

            self.db_session.add_model(tournament_record)
            self.db_session.commit()

            logger.debug(MEASSGE_LOG['saving_db_tournament'])

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных в TOURNAMENT: {e}'
        )


def save_championship(self, data_save):
    try:
        for data in data_save:
            championship_record = (
                get_championship_id(self.db_session, data['id'])
            )
            if championship_record is None:
                championship_record = ChampionShip(id=data['id'])

            championship_record.id = data['id']
            championship_record.sport_id = data['sId']
            championship_record.country_id = data['catId']
            championship_record.curSeason = data['curSeason']
            championship_record.championshipName = data['name']
            championship_record.isTop = data['isTop']
            championship_record.priority = data['priority']
            championship_record.countTeams = (
                calculation_records_team(data['id'])
            )
            championship_record.countMatches = (
                calculation_records_match(data['id'])
            )
            # championship_record.countComand = self._championship[id]['countComand']
            # championship_record.countGame = self._championship[id]['countGame']

            self.db_session.add_model(championship_record)
            self.db_session.commit()

            logger.debug(MEASSGE_LOG['saving_db_championship'])

    except Exception as e:
        logger.critical(
            f'Ошибка при сохранении данных в CHAMPIONSHIP: {e}'
        )


def save_match(self, data_save):
    batch_size = 50  # Размер пакета для коммита
    processed = 0
    
    # Отключаем autoflush для избежания блокировок
    with self.db_session._session.no_autoflush:
        for data in data_save:
            try:
                # Не проверяем существование команд здесь, чтобы избежать autoflush
                # Если команда не существует, получим ошибку FK при коммите
                # Используем merge вместо проверки существования для избежания autoflush
                match_record = Match(id=data['id'])
                match_record.id = data['id']
                match_record.sport_id = data['sId']
                match_record.country_id = data['catId']
                match_record.tournament_id = data['tId']
                # match_record.gameData = datetime(1970, 1, 1) + timedelta(seconds=data_json['time'])
                match_record.gameData = (
                    dt.datetime.fromtimestamp(data['time'])
                )
                match_record.teamHome_id = data['homeId']
                match_record.teamAway_id = data['awayId']
                match_record.tour = data['round']['id'] if 'round' in data else 0
                if 'home' in data['result'].keys():
                    match_record.numOfHeadsHome = data['result']['home']
                    match_record.numOfHeadsAway = data['result']['away']

                if 'winner' in data['result'].keys():
                    match_record.winner = data['result']['winner']

                if data['result']['period'] != 'nt':
                    match_record.typeOutcome = data['result']['period']
                if 'comment' in data.keys():
                    match_record.gameComment = data['comment']
                match_record.isCanceled = data['isCanceled']
                match_record.season_id = data['snId']
                match_record.stages_id = data['stId']

                # merge автоматически обновит если существует, или вставит новую запись
                self.db_session.update_model(match_record)
                processed += 1
                
                # Коммитим пакетами
                if processed % batch_size == 0:
                    try:
                        self.db_session.commit()
                        logger.debug(f'Сохранено {processed} матчей')
                    except Exception as commit_error:
                        logger.warning(
                            f'Ошибка при коммите пакета матчей (около {processed} записей): {commit_error}'
                        )
                        self.db_session.rollback()

            except Exception as e:
                logger.critical(
                    f'Ошибка при подготовке матча ID={data["id"]} '
                    f'(teamHome_id={data["homeId"]}, teamAway_id={data["awayId"]}, '
                    f'tournament_id={data["tId"]}): {e}'
                )
        
        # Финальный коммит для оставшихся записей
        if processed % batch_size != 0:
            try:
                self.db_session.commit()
                logger.debug(f'Финальное сохранение: всего {processed} матчей')
            except Exception as commit_error:
                logger.warning(
                    f'Ошибка при финальном коммите матчей: {commit_error}'
                )
                self.db_session.rollback()


def save_team(self, data_save):
    batch_size = 100  # Размер пакета для коммита
    processed = 0
    
    # Отключаем autoflush для избежания блокировок
    with self.db_session._session.no_autoflush:
        for data in data_save:
            try:
                # Проверяем существование страны
                country_id = data['catId']
                country_exists = get_country_id(self.db_session, country_id)
                
                if country_exists is None:
                    # Пропускаем команды с несуществующими странами
                    # (вероятно расформированные команды/лиги)
                    logger.warning(
                        f'Команда ID={data["id"]} (name={data.get("name", "unknown")}) '
                        f'пропущена: страна с ID={country_id} не найдена в БД. '
                        f'Возможно, это расформированная команда или лига.'
                    )
                    continue
                
                # Используем merge вместо проверки существования для избежания autoflush
                team_record = Team(id=data['id'])
                team_record.sport_id = data['sId']
                team_record.country_id = country_id
                team_record.teamName = data['name']

                # merge автоматически обновит если существует, или вставит новую запись
                self.db_session.update_model(team_record)
                processed += 1
                
                # Коммитим пакетами
                if processed % batch_size == 0:
                    self.db_session.commit()
                    logger.debug(f'Сохранено {processed} команд')

            except Exception as e:
                logger.critical(
                    f'Ошибка при сохранении команды ID={data["id"]} '
                    f'(name={data.get("name", "unknown")}, '
                    f'country_id={data.get("catId", "unknown")}): {e}'
                )
                # Откатываем текущую транзакцию при ошибке
                self.db_session.rollback()
        
        # Финальный коммит для оставшихся записей
        if processed % batch_size != 0:
            self.db_session.commit()
            logger.debug(f'Финальное сохранение: всего {processed} команд')


def save_goal(self, data_save):
    batch_size = 50  # Уменьшили для снижения конкуренции
    processed = 0
    
    # Отключаем autoflush для избежания блокировок
    with self.db_session._session.no_autoflush:
        for data in data_save:
            for data_json in data:
                try:
                    if 'match_id' not in data_json.keys():
                        continue

                    # Проверяем существование в режиме no_autoflush
                    # Ищем по уникальной комбинации: match_id + team_id + seconds
                    record = (
                        self.db_session.query(Goal)
                        .filter(
                            Goal.match_id == data_json['match_id'],
                            Goal.team_id == data_json['team_id'],
                            Goal.seconds == data_json['seconds']
                        )
                        .first()
                    )
                    
                    if record is None:
                        record = Goal()
                        record.match_id = data_json['match_id']
                        record.team_id = data_json['team_id']
                        record.seconds = data_json['seconds']
                    
                    record.scorer = data_json['scorer']

                    self.db_session.add_model(record)
                    processed += 1
                    
                    # Коммитим пакетами с retry
                    if processed % batch_size == 0:
                        for retry in range(3):
                            try:
                                self.db_session.commit()
                                logger.debug(f'Сохранено {processed} голов')
                                # Небольшая задержка между коммитами для снижения конкуренции
                                time.sleep(0.01)
                                break
                            except OperationalError as e:
                                if '1205' in str(e) and retry < 2:
                                    wait = (retry + 1) * 0.3
                                    logger.warning(f'Блокировка при сохранении голов, retry {retry + 1}/3 через {wait}s')
                                    self.db_session.rollback()
                                    time.sleep(wait)
                                else:
                                    raise

                except Exception as e:
                    logger.critical(
                        f'Ошибка при сохранении гола для матча {data_json.get("match_id", "unknown")}: {e}'
                    )
                    # Откатываем текущую транзакцию при ошибке
                    self.db_session.rollback()
        
        # Финальный коммит для оставшихся записей
        if processed % batch_size != 0:
            for retry in range(3):
                try:
                    self.db_session.commit()
                    logger.debug(f'Финальное сохранение: всего {processed} голов')
                    break
                except OperationalError as e:
                    if '1205' in str(e) and retry < 2:
                        wait = (retry + 1) * 0.3
                        logger.warning(f'Блокировка при финальном сохранении голов, retry {retry + 1}/3 через {wait}s')
                        self.db_session.rollback()
                        time.sleep(wait)
                    else:
                        raise


def save_period(self, data_save):
    batch_size = 50  # Уменьшили для снижения конкуренции
    processed = 0
    
    # Отключаем autoflush для избежания блокировок
    with self.db_session._session.no_autoflush:
        for data in data_save:
            for data_json in data:
                try:
                    if 'match_id' not in data_json.keys():
                        continue

                    # Проверяем существование в режиме no_autoflush
                    period_record = (
                        self.db_session.query(Period)
                        .filter(
                            Period.match_id == data_json['match_id'],
                            Period.period == data_json['period']
                        )
                        .first()
                    )
                    
                    if period_record is None:
                        period_record = Period()
                        period_record.match_id = data_json['match_id']
                        period_record.period = data_json['period']

                    period_record.numOfHeadsHome = data_json['home']
                    period_record.numOfHeadsAway = data_json['away']

                    self.db_session.add_model(period_record)
                    processed += 1
                    
                    # Коммитим пакетами с retry
                    if processed % batch_size == 0:
                        for retry in range(3):
                            try:
                                self.db_session.commit()
                                logger.debug(f'Сохранено {processed} периодов')
                                time.sleep(0.01)
                                break
                            except OperationalError as e:
                                if '1205' in str(e) and retry < 2:
                                    wait = (retry + 1) * 0.3
                                    logger.warning(f'Блокировка при сохранении периодов, retry {retry + 1}/3 через {wait}s')
                                    self.db_session.rollback()
                                    time.sleep(wait)
                                else:
                                    raise

                except Exception as e:
                    logger.critical(
                        f'Ошибка при сохранении периода для матча '
                        f'{data_json.get("match_id", "unknown")}, '
                        f'период {data_json.get("period", "unknown")}: {e}'
                    )
                    # Откатываем текущую транзакцию при ошибке
                    self.db_session.rollback()
        
        # Финальный коммит для оставшихся записей
        if processed % batch_size != 0:
            for retry in range(3):
                try:
                    self.db_session.commit()
                    logger.debug(f'Финальное сохранение: всего {processed} периодов')
                    break
                except OperationalError as e:
                    if '1205' in str(e) and retry < 2:
                        wait = (retry + 1) * 0.3
                        logger.warning(f'Блокировка при финальном сохранении периодов, retry {retry + 1}/3 через {wait}s')
                        self.db_session.rollback()
                        time.sleep(wait)
                    else:
                        raise


def save_coef(data_save):
    with Session_pool() as session:
        db = DBSession(session)

        for data in data_save:
            try:
                for index in range(0, len(data)):
                    data_p1x_p2 = data_x112_x2 = data_f = data_tb_tm = []
                    if (data[index][0]['name'] == '1' and
                            data[index][1]['name'] == 'X' and
                            data[index][2]['name'] == '2'):
                        data_p1x_p2 = data[index]
                        index += 1
                    if (data[index][0]['name'] == '1X' and
                            data[index][1]['name'] == '12' and
                            data[index][2]['name'] == 'X2'):
                        data_x112_x2 = data[index]
                        index += 1
                    if (data[index][0]['name'] == 'Бол' and
                            data[index][1]['name'] == 'Мен'):
                        data_tb_tm = data[index]
                        index += 1
                    if (data[index][0]['name'] == 'Ф1к' and
                            data[index][1]['name'] == 'Ф2к'):
                        data_f = data[index]
                        index += 1

                    coef_db = get_coef_id(db, data_p1x_p2[0]['id'])
                    if coef_db is None:
                        c = Coef(id=data_p1x_p2[0]['id'])
                    else:
                        c = coef_db

                    c.match_id = data_p1x_p2[0]['id']
                    c.home_id = data_p1x_p2[0]['coefId']
                    c.home = data_p1x_p2[0]['odds']
                    c.draw_id = data_p1x_p2[1]['coefId']
                    c.draw = data_p1x_p2[1]['odds']
                    c.away_id = data_p1x_p2[2]['coefId']
                    c.away = data_p1x_p2[2]['odds']
                    c.homeDraw_id = data_x112_x2[0]['coefId']
                    c.homeDraw = data_x112_x2[0]['odds']
                    c.homeAway_id = data_x112_x2[1]['coefId']
                    c.homeAway = data_x112_x2[1]['odds']
                    c.drawAway_id = data_x112_x2[2]['coefId']
                    c.drawAway = data_x112_x2[2]['odds']
                    c.foraHome_id = data_f[0]['coefId']
                    c.foraHome = data_f[0]['odds']
                    c.sizeForaHome = data_f[0]['value']
                    c.foraAway_id = data_f[1]['coefId']
                    c.foraAway = data_f[1]['odds']
                    c.sizeForaAway = data_f[1]['value']
                    c.coefUp_id = data_tb_tm[0]['coefId']
                    c.coefUp = data_tb_tm[0]['odds']
                    c.coefUp_size = data_tb_tm[0]['value']
                    c.coefDown_id = data_tb_tm[1]['coefId']
                    c.coefDown = data_tb_tm[1]['odds']
                    c.coefDown_size = data_tb_tm[1]['value']

                    c.golAllYes = .0
                    c.golAllNo = .0

                    db.add_model(c)

                logger.debug(MEASSGE_LOG['saving_db_coef'])

            except Exception as e:
                logger.critical(
                    f'Ошибка при сохранении данных в COEF: {e}'
                )

            finally:
                db.commit_session()


def save_table(data_save):
    with Session_pool() as session:
        db = DBSession(session)

        for data in data_save:
            data_id = data['id']
            data_table = data['table']
            data_match = data['match']
            try:
                for i in range(len(data_id)):

                    table_db = get_table_id(db, data_id['tableId'])
                    if table_db is None:

                        table_db = Table(id=data_id['tableId'])
                        try:
                            table_db.id = data_id['tableId']
                            table_db.match_id = data_match
                            table_db.team_id = data_table[i]['teamId']
                            table_db.round = data_id['round']
                            table_db.chageTotal = data_table[i]['values']['changeTotal']
                            table_db.changeHome = data_table[i]['values']['changeHome']
                            table_db.changeAway = data_table[i]['values']['changeAway']
                            table_db.drawTotal = data_table[i]['values']['drawTotal']
                            table_db.drawHome = data_table[i]['values']['drawHome']
                            table_db.drawAway = data_table[i]['values']['drawAway']
                            table_db.goalDiffTotal = data_table[i]['values']['goalDiffTotal']
                            table_db.goalDiffHome = data_table[i]['values']['goalDiffHome']
                            table_db.goalDiffAway = data_table[i]['values']['goalDiffAway']
                            table_db.goalsAgainstTotal = data_table[i]['values']['goalsAgainstTotal']
                            table_db.goalsAgainstHome = data_table[i]['values']['goalsAgainstHome']
                            table_db.goalsAgainstAway = data_table[i]['values']['goalsAgainstAway']
                            table_db.goalsForTotal = data_table[i]['values']['goalsForTotal']
                            table_db.goalsForHome = data_table[i]['values']['goalsForHome']
                            table_db.goalsForAway = data_table[i]['values']['goalsForAway']
                            table_db.lossTotal = data_table[i]['values']['lossTotal']
                            table_db.lossHome = data_table[i]['values']['lossHome']
                            table_db.lossAway = data_table[i]['values']['lossAway']
                            table_db.total = data_table[i]['values']['total']
                            table_db.home = data_table[i]['values']['home']
                            table_db.away = data_table[i]['values']['away']
                            table_db.pointsTotal = data_table[i]['values']['pointsTotal']
                            table_db.pointsHome = data_table[i]['values']['pointsHome']
                            table_db.pointsAway = data_table[i]['values']['pointsAway']
                            table_db.pos = data_table[i]['values']['pos']
                            table_db.posHome = data_table[i]['values']['posHome']
                            table_db.posAway = data_table[i]['values']['posAway']
                            table_db.sortPositionTotal = data_table[i]['values']['sortPositionTotal']
                            table_db.sortPositionHome = data_table[i]['values']['sortPositionHome']
                            table_db.sortPositionAway = data_table[i]['values']['sortPositionAway']
                            table_db.winTotal = data_table[i]['values']['winTotal']
                            table_db.winHome = data_table[i]['values']['winHome']
                            table_db.winAway = data_table[i]['values']['winAway']
                            table_db.avgGoalsForPerGameTotal = (
                                float(data_table[i]['values']['avgGoalsForPerGameTotal'].
                                      replace(',', '.'))
                            )
                            table_db.avgPointsPerGameTotal = (
                                float(data_table[i]['values']['avgPointsPerGameTotal'].
                                      replace(',', '.'))
                            )
                            table_db.avgGoalsForPerGameHome = (
                                float(data_table[i]['values']['avgGoalsForPerGameHome'].
                                      replace(',', '.'))
                            )
                            table_db.avgPointsPerGameHome = (
                                float(data_table[i]['values']['avgPointsPerGameHome'].
                                      replace(',', '.'))
                            )
                            table_db.avgGoalsForPerGameAway = (
                                float(data_table[i]['values']['avgGoalsForPerGameAway'].
                                      replace(',', '.'))
                            )
                            table_db.avgPointsPerGameAway = (
                                float(data_table[i]['values']['avgPointsPerGameAway'].
                                      replace(',', '.'))
                            )
                        except KeyError as err:
                            logger.error(f'ERROR, not found KEY table. {err}')

                    db.add_model(table_db)

                logger.debug(MEASSGE_LOG['saving_db_goal'])

            except Exception as e:
                logger.critical(
                    f'Ошибка при сохранении данных в TABLE: {e}'
                )

            finally:
                db.commit_session()
