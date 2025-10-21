# izhbet/calculation/tournament.py
"""
Модуль реализации классов для работы с турнирами и обработкой данных.
"""
import os
import logging
import pandas as pd
from multiprocessing import JoinableQueue, cpu_count

from db.queries.match import get_match_modeling
from db.storage.calculation import (
    save_feature, save_standing
)
from .standings import (
    Tournament,
    GeneralTableStrategy,
    HomeGamesTableStrategy,
    AwayGamesTableStrategy,
    StrongOpponentsTableStrategy,
    MediumOpponentsTableStrategy,
    WeakOpponentsTableStrategy,
    HomeGamesStrongOpponentsTableStrategy,
    HomeGamesMediumOpponentsTableStrategy,
    HomeGamesWeakOpponentsTableStrategy,
    AwayGamesStrongOpponentsTableStrategy,
    AwayGamesMediumOpponentsTableStrategy,
    AwayGamesWeakOpponentsTableStrategy,
)
from db.queries.team import get_team_id
from db.queries.country import get_country_id
from db.queries.sport import get_sport_id
from db.queries.tournament import get_tournament_id
from core.constants import TIME_FRAME, MATCH_TYPE
from core.utils import (
    convert_standing, create_feature_attr, create_feature_attr_onehot,
    create_feature_vector, create_feature_vector_new,
    normalize_features, validate_features, analyze_feature_quality
)
from core.consumer import Consumer
from config import get_db_session
from db.base import DBSession


logger = logging.getLogger(__name__)


strategies = [
    GeneralTableStrategy,
    HomeGamesTableStrategy,
    AwayGamesTableStrategy,
    StrongOpponentsTableStrategy,
    MediumOpponentsTableStrategy,
    WeakOpponentsTableStrategy,
    HomeGamesStrongOpponentsTableStrategy,
    HomeGamesMediumOpponentsTableStrategy,
    HomeGamesWeakOpponentsTableStrategy,
    AwayGamesStrongOpponentsTableStrategy,
    AwayGamesMediumOpponentsTableStrategy,
    AwayGamesWeakOpponentsTableStrategy
]


class CalculationDataPipeline:
    """
    Шаблон проектирования — Pipeline.
    Этот шаблон подразумевает разбиение сложного процесса на ряд более
    мелких, независимых шагов, каждый из которых выполняет определенную
    задачу.
    В вашем случае шаги будут следующими:
    1. Извлечение данных: извлечение части данных из источника
       (например, базы данных, файла, API).
    2. Выборка данных: выбор подмножества полученных данных на основе
       определенных критериев.
    3. Обработка данных: обработка выбранных данных (например, очистка,
       преобразование, анализ).
    4. Хранение данных: сохранение обработанных данных в системе
       хранения (например, в базе данных, файле).
       Повторить: вернуться к шагу 1 и получить следующую порцию данных.
    """
    def __init__(
            self,
            data_source,
            data_processor,
            data_storage
    ):
        """
        Турнирная таблица строиться сквозная,
        поэтому надо получить на дату с которого нет расчета.

        Args:
            data_source: Источник данных
            data_processor: Процессор данных
            data_storage: Хранилище данных
        """
        self.tournament = Tournament()
        self.full_time = None
        self.df = pd.DataFrame()
        self.df_summary = pd.DataFrame()
        self.data_source = data_source
        self.data_processor = data_processor
        self.data_storage = data_storage

    def process_data(self, time_frame):
        """
        Координация процесса обработки данных по определенному алгоритму:
        - Получение пакета данных.
        - Выберите подмножество данных.
        - Обработать выбранные данные.
        - Сохраните обработанные данные.

        Args:
            time_frame: Временной диапазон для обработки данных
        """
        full_time = time_frame == 'ALL_TIME' #TIME_FRAME[0]
        self.data_source.retrieve(full_time)

        tournaments = self.data_source.tournaments_id

        tasks = JoinableQueue()

        number_consumers = cpu_count()
        consumers = [
            Consumer(tasks, None)
                for _ in range(number_consumers)
        ]
        for consumer in consumers:
            consumer.start()

        for tournament_id in tournaments:
            tasks.put(
                TournamentConsumer(
                    self.select_data,
                    self.data_processor,
                    self.data_storage,
                    tournament_id
                )
            )

        for _ in range(number_consumers):
            tasks.put(None)

        tasks.join()
        
        # Ждем завершения всех потребителей
        for consumer in consumers:
            consumer.join()
        
        logger.info(f'Обработка {len(tournaments)} турниров завершена')

    def select_data(self, data: object) -> object:
        """
        Метод для реализации свей логики выбора данных.

        Args: data

        Returns:
        """
        pass


class DataSource:
    """
    Базовый класс для реализации логики поиска данных.
    """
    def retrieve(self, full_time):
        """
        Инициализация получения данных

        Args:
            full_time: Получать данные за все время (True)
            или только за текущий сезон
        """
        pass


class DataProcessor:
    """
    Базовый класс для реализации логики обработки данных.
    """

    def process(self, df_match, df_team):
        """
        Обработка данных

        Args:
            df_match: DataFrame с данными о матчах
            df_team: DataFrame с данными о командах
        """
        pass


class DataStorage:
    """
    Базовый класс для реализации логики хранения данных.
    """

    def save(self, tournament_id, standing, vector):

        """
        Сохранение данных

        Args:
            tournament_id: Идентификатор турнира
            standing: Турнирная таблица
            vector: Вектор признаков
        """
        pass


class DatabaseSource(DataSource):
    """
    Определите конкретную реализацию класса источника данных.
    """
    def __init__(self):
        self.df_tournament = pd.DataFrame()
        self.df = pd.DataFrame()
        self.tournaments_id = None
        self.tournaments = None

    def retrieve(self, full_time):
        """
        Получение данных из таблицы.

        Args:
            full_time: Получать данные за все время (True)
            или только за текущий сезон
        """

        matchs = get_match_modeling(full_time)

        logger.debug(
            f'Отобрано для обработки: {len(matchs)}, '
            f'матчей по всем лигам.'
        )
        self.df = pd.DataFrame([match.as_dict() for match in matchs])
        # Не заполняем NaN пустыми строками, это может нарушить логику
        # self.df.fillna(value='', inplace=True)
        self.df = self.df.astype(MATCH_TYPE, errors='ignore')
        self.df_tournament = (
            self.df['tournament_id'].drop_duplicates(
                ignore_index=True
            ).sort_values()
        )
        self.tournaments_id = self.df_tournament.to_list()
        #self.tournaments_id = [785]
        #self.tournaments_id = [714, 268, 1159, 844, 234, 2386] #[785, 826, 828] #[17, 18, 24, 25, 173]
        #self.tournaments_id = [17, 18, 24, 25, 173] #[17, 18, 24, 25, 173]
        #pass


class CreatingStandings(DataProcessor):
    """
    Определите конкретную реализацию класса DataProcessor
    """
    def __init__(self) -> None:
        self.db_session = None

    def set_db_session(self, db_session: DBSession):
        self.db_session = db_session

    def process(self, df_match, df_team):
        """
        Расчет рейтингов и построение турнирной таблицы.

        GeneralTableStrategy - стратегия расчета общей ТТ.
        HomeGamesTableStrategy - стратегия расчета ТТ по домашним матчем.
        AwayGamesTableStrategy - стратегия расчета ТТ по выездным матчем.

        StrongOpponentsTableStrategy - стратегия расчета ТТ с сильным соперником.
        MediumOpponentsTableStrategy - стратегия расчета ТТ со средним соперником.
        WeakOpponentsTableStrategy - стратегия расчета ТТ со слабым соперником.

        HomeStrongGamesTableStrategy - стратегия расчета ТТ с сильным соперником, домашние матчи.
        HomeMediumGamesTableStrategy - стратегия расчета ТТ со средним соперником, домашние матчи.
        HomeWeakGamesTableStrategy - стратегия расчета ТТ со слабым соперником, домашние матчи.

        AwayStrongGamesTableStrategy - стратегия расчета ТТ с сильным соперником, выездные матчи.
        AwayMediumGamesTableStrategy - стратегия расчета ТТ со средним соперником, выездные матчи.
        AwayWeakGamesTableStrategy - стратегия расчета ТТ со слабым соперником, выездные матчи.
        """
        standings = {}
        standing_save = {}
        features = {}
        #snapshots = []  # накопление срезов состояния команды на дату матча

        tournament = Tournament()

        for team in df_team:
            tournament.add_team(get_team_id(self.db_session, team))

        logger.info(f"Начало обработки {len(df_match)} матчей")
        for index, row in df_match.iterrows():

            logger.debug(
                f'Добавлен матч: {row["id"]} дата: '
                f'{row["gameData"]} спорт: {row["sport_id"]} '
                f'турнир: {row["tournament_id"]} '
                f'{row["teamHome_id"]}-{row["teamAway_id"]} '
                f'({row["numOfHeadsHome"]}:{row["numOfHeadsAway"]}) для '
                f'расчета ТТ'
            )

            match_over = False
            if (
                pd.notna(row['numOfHeadsHome']) and 
                pd.notna(row['numOfHeadsAway']) and
                row['numOfHeadsHome'] != '' and
                row['numOfHeadsAway'] != ''
            ):
                match_over = True

            if match_over:
                # Создать вектор нужно для каждого матча, даже у
                # непрошедшего иначе прогноза по нему не построишь,
                # для прогноза нужен вектор
                # А фичи сохраняем только по прошедшим матчам,
                # у не прошедших их нет.
                tournament.add_match(
                    row['id'],
                    get_sport_id(self.db_session, row['sport_id']),
                    get_country_id(self.db_session, row['country_id']),
                    get_tournament_id(self.db_session, row['tournament_id']),
                    row['gameData'],
                    get_team_id(self.db_session, row['teamHome_id']),
                    get_team_id(self.db_session, row['teamAway_id']),
                    row['numOfHeadsHome'],
                    row['numOfHeadsAway'],
                    row['typeOutcome'],
                    row['season_id'],
                    row['stages_id'],
                )
                for strategy in strategies:
                    strategy_instance = strategy()
                    strategy_instance.filter_matches(tournament.teams)
                    tournament.calculate_ratings(strategy_instance)
                    standings[strategy.__name__.lower()] = (
                        strategy_instance.get_standings()
                    )

            # Из словаря с данными по каждой СТРАТЕГИЙ, создаем
            # одну запись для сохранения в таблице
            standing_home = convert_standing(
                standings,
                row['teamHome_id']
            )
            standing_away = convert_standing(
                standings,
                row['teamAway_id']
            )
            standing_save[f'{row["id"]}_{row["teamHome_id"]}'] = (
                standing_home
            )
            standing_save[f'{row["id"]}_{row["teamAway_id"]}'] = (
                standing_away
            )
            # Из данных ТТ по соперникам создаем относительный вектор параметров ТТ
            # standing_home и standing_away - это параметры рассчитанные по ТТ
            # а feature_vector - это расширеная значения фичей (home, away, diff, ratio)
            # ВАЖНО: передаем текущий match_id, чтобы targets создавались для правильного матча
            feature_vector = create_feature_vector_new(
                standing_home,
                standing_away,
                row['id']  # Передаем ID текущего обрабатываемого матча
            )
            
            # Применяем нормализацию для улучшения качества моделей
            #feature_vector = normalize_features(feature_vector)
            
            # Валидируем фичи для выявления проблемных данных
            # validation = validate_features(feature_vector)
            # if not validation['is_valid']:
            #     logger.warning(f"Проблемы с фичами для матча {row['id']}: {validation['errors']}")
            # if validation['warnings']:
            #     logger.debug(f"Предупреждения по фичам для матча {row['id']}: {validation['warnings']}")
            
            # Сохраняем features для ВСЕХ матчей (прошедших и будущих)
            # Это необходимо для прогнозирования будущих матчей
            if match_over:
                # Для прошедших матчей: размечаем данные на основе результата
                # Присваиваем в зависимости от результата матча значения 
                # категориальным переменным (One-Hot encoding)
                # Targets создаются автоматически внутри create_feature_attr_onehot
                for name_vector in feature_vector:
                    feature_vector[name_vector] = create_feature_attr_onehot(
                        feature_vector[name_vector],
                        row['sport_id'],
                        row['numOfHeadsHome'],
                        row['numOfHeadsAway'],
                        row['typeOutcome']
                    )
            else:
                # Для будущих матчей: target поля остаются пустыми
                # Это позволяет использовать features для прогнозирования,
                # но без разметки результата (которого еще нет)
                logger.debug(f'Создаем features для будущего матча {row["id"]} без разметки результата')
            
            # Сохраняем features для всех матчей
            features[row['id']] = feature_vector

            # Сохраняем snapshot для домашней и гостевой команд на дату матча
            # try:
            #     def _standing_to_dict(st_obj, side):
            #         d = {k: v for k, v in st_obj.__dict__.items() if not k.startswith('_')}
            #         d['side'] = side
            #         return d
            #     # snapshots.append(_standing_to_dict(standing_home, 'home'))
            #     # snapshots.append(_standing_to_dict(standing_away, 'away'))
            # except Exception as _e:
            #     logger.debug(f"Не удалось сформировать snapshot для матча {row['id']}: {_e}")

        tournament.clean()

        # Анализ качества созданных фичей
        # if features:
        #     quality_analysis = analyze_feature_quality(features)
        #     logger.info(f"  Всего матчей: {quality_analysis['total_matches']}")
        #     # logger.info(f"  Матчей с проблемами: {quality_analysis['matches_with_issues']}")
        #
        #     if quality_analysis['common_issues']:
        #         logger.warning(f"  Частые проблемы: {quality_analysis['common_issues']}")
            
            # # Логируем статистику по наиболее проблемным фичам
            # for feature_name, stats in quality_analysis['feature_distribution'].items():
            #     if stats['zeros'] > stats['count'] * 0.8:  # Более 80% нулей
            #         logger.warning(f"  Фича {feature_name}: {stats['zeros']}/{stats['count']} нулевых значений")
            #     if stats['negative'] > stats['count'] * 0.1:  # Более 10% отрицательных
            #         logger.warning(f"  Фича {feature_name}: {stats['negative']}/{stats['count']} отрицательных значений")

        return standing_save, features #, snapshots


class FileStorage(DataStorage):
    """
    Конкретная реализация класса хранения данных.
    """
    def __init__(self) -> None:
        self.db_session = None

    def set_db_session(self, db_session: DBSession):
        self.db_session = db_session

    #def save(self, tournament_id, standings, features, snapshots=None):
    def save(self, tournament_id, standings, features):
        """
        Сохранение данных

        Args:
            tournament_id: Идентификатор турнира
            standings: Турнирная таблица
            features: Вектор признаков
        """
        save_standing(self, tournament_id, standings)
        save_feature(self, tournament_id, features)
        # Экспортируем snapshots в файл (append)
        # try:
        #     if snapshots:
        #         out_dir = 'results/standings_snapshots'
        #         os.makedirs(out_dir, exist_ok=True)
        #         out_path = os.path.join(out_dir, f'{tournament_id}.csv')
        #         df = pd.DataFrame(snapshots)
        #         header = not os.path.exists(out_path)
        #         df.to_csv(out_path, mode='a', header=header, index=False)
        # except Exception as e:
        #     logger.error(f'Ошибка сохранения snapshots для турнира {tournament_id}: {e}')


class TournamentConsumer:

    def __init__(
            self,
            select_data,
            data_processor,
            data_storage,
            tournament_id: int
    ) -> None:
        self.select_data = select_data
        self.data_processor = data_processor
        self.data_storage = data_storage
        self.tournament_id = tournament_id

    def process(self):
        """Обрабатывает один турнир в отдельном процессе."""
        try:
            with (get_db_session() as db_session):

                self.data_storage.set_db_session(db_session)
                self.data_processor.set_db_session(db_session)

                df_team, df_match = self.select_data(self.tournament_id)

                standings, feature = self.data_processor.process(
                    df_match,
                    df_team
                )
                # Совместимость: поддержка возврата (standings, features) и (standings, features, snapshots)
                # if isinstance(result, tuple) and len(result) == 3:
                #     standings, feature, snapshots = result
                #     self.data_storage.save(
                #         self.tournament_id,
                #         standings,
                #         feature,
                #         snapshots
                #     )
                # else:
                #     standings, feature = result
                #     self.data_storage.save(
                #         self.tournament_id,
                #         standings,
                #         feature,
                #         None
                #     )
                self.data_storage.save(
                    self.tournament_id,
                    standings,
                    feature
                )

        except Exception as e:
            logger.error(
                f'Ошибка при обработке турнира '
                f'{self.tournament_id}: {e}'
            )
