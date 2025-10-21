"""
Модуль для загрузки и обработки данных о спортивных событиях.

Содержит классы для обработки различных типов данных с использованием шаблонного метода
и многопроцессорной обработки для улучшения производительности.
"""
import logging
import abc
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional, TypeVar, Generic
from multiprocessing import JoinableQueue, cpu_count

from core.constants import OPERATIONS
from db.queries.tournament import (
    get_tournament_id, get_tournament_all, get_season_tournament
)
from db.queries.championship import (
    get_championship_all, get_championship_season
)
from core.constants import (
    URL_COUNTRYS, URL_TOURNAMENTS, URL_MATCHES, SPR_SPORTS
)
from .datahandler import DataHandlerFactory
from core.consumer import Consumer
from db.base import DBSession
from config import get_db_session

logger = logging.getLogger(__name__)
T = TypeVar('T')


class DataProcessingTemplate(ABC):
    """Абстрактный базовый класс шаблона обработки данных."""

    def __init__(self):
        pass

    def process(self) -> None:
        """Шаблонный метод для обработки данных."""
        self.initialize()
        self.fetch_data()
        self.process_data()
        self.save_data()
        self.finalize()

    def initialize(self) -> None:
        """Инициализация сессии БД."""

    def fetch_data(self) -> None:
        """Загрузка данных."""
        raise NotImplementedError(
            'Метод fetch_data должен быть переопределен.'
        )

    def process_data(self) -> None:
        """Обработка данных."""
        raise NotImplementedError(
            'Метод process_data должен быть переопределен.'
        )

    def save_data(self) -> None:
        """Сохранение данных."""
        raise NotImplementedError(
            'Метод save_data должен быть переопределен.'
        )

    def finalize(self) -> None:
        """Завершающие действия - закрытие сессии."""
        pass

class SportDataProcessing(DataProcessingTemplate):
    """Обработка данных видов спорта."""

    def __init__(self, url: str):
        super().__init__()
        self.url = url
        self.data = None
        self.sports = None
        self.countries = None
        self.championships = None
        self.sport_handler = None
        self.championship_handler = None
        self.country_handler = None
        self.db_session = None

    def initialize(self) -> None:
        """Инициализация обработчиков."""
        self.sport_handler = (
            DataHandlerFactory.create_handler(
                'sport',
                self.db_session
            )
        )
        self.championship_handler = (
            DataHandlerFactory.create_handler(
                'championship',
                self.db_session
            )
        )
        self.country_handler = (
            DataHandlerFactory.create_handler(
                'country',
                self.db_session
            )
        )

    def set_db_session(self, db_session: DBSession):
        self.db_session = db_session

    def fetch_data(self) -> None:
        """Загрузка данных о виде спорта."""
        self.data = self.sport_handler.fetch_data(self.url)

    def process_data(self) -> None:
        """Обработка данных о виде спорта."""
        if self.data:
            self.sports = (
                self.sport_handler.preparing_data(self.data)
            )
            self.countries = (
                self.country_handler.preparing_data(self.data)
            )
            self.championships = (
                self.championship_handler.preparing_data(self.data)
            )

    def save_data(self) -> None:
        """Сохранение данных о виде спорта."""
        if self.sports:
            self.sport_handler.save_data(self.sports)
        if self.countries:
            self.country_handler.save_data(self.countries)
        if self.championships:
            self.championship_handler.save_data(self.championships)


class TournamentDataProcessing(DataProcessingTemplate):
    """Обработка данных о турнирах."""

    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url
        self.data = None
        self.tournaments = None
        self.tournament_handler = None
        self.db_session = None

    def initialize(self) -> None:
        """Инициализация обработчика турниров."""
        self.tournament_handler = (
            DataHandlerFactory.create_handler(
                'tournament',
                self.db_session
            )
        )

    def set_db_session(self, db_session: DBSession):
        self.db_session = db_session

    def fetch_data(self) -> None:
        """Загрузка данных о турнирах."""
        self.data = self.tournament_handler.fetch_data(self.url)

    def process_data(self) -> None:
        """Загрузка данных о турнирах."""
        if self.data:
            self.tournaments = (
                self.tournament_handler.preparing_data(self.data)
            )

    def save_data(self) -> None:
        """Сохранение данных о турнирах."""
        if self.tournaments:
            self.tournament_handler.save_data(self.tournaments)


class MatchDataProcessing(DataProcessingTemplate):
    """Обработка данных о матчах."""

    def __init__(self, tournament_id: int) -> None:
        super().__init__()
        self.tournament_id = tournament_id
        self.data = None
        self.matches = None
        self.teams = None
        self.goals = None
        self.periods = None
        self.url = None
        self.match_handler = None
        self.team_handler = None
        self.goal_handler = None
        self.period_handler = None
        self.db_session = None

    def set_db_session(self, db_session: DBSession):
        self.db_session = db_session

    def initialize(self) -> None:
        """Инициализация обработчиков данных матчей."""
        self.match_handler = (
            DataHandlerFactory.create_handler(
                'match',
                self.db_session
            )
        )
        self.team_handler = (
            DataHandlerFactory.create_handler(
                'team',
                self.db_session
            )
        )
        self.goal_handler = (
            DataHandlerFactory.create_handler(
                'goal',
                self.db_session
            )
        )
        self.period_handler = (
            DataHandlerFactory.create_handler(
                'period',
                self.db_session
            )
        )

    def fetch_data(self) -> None:
        """Загрузка данных о матчах."""
        self.url = URL_MATCHES % self.tournament_id
        self.data = self.match_handler.fetch_data(self.url)

        tour = get_tournament_id(self.db_session, self.tournament_id)

        if tour is not None:
            try:
                sport_name = tour.sports.sportName
                country_name = tour.championships[0].countrys.countryName
                season_cur = tour.yearTournament
                championship_name = tour.championships[0].championshipName
                tournament_name = tour.nameTournament
            except AttributeError as err:
                logger.error(f'Ошибка атрибутов: {err}')
            finally:
                logger.info(
                   f'Загрузка: спорт={sport_name}, страна={country_name}, '
                   f'сезон={season_cur}, чемпионат={championship_name}, '
                   f'лига={tournament_name}'
                )


    def process_data(self) -> None:
        """Обработка данных о матчах."""
        if self.data:
            self.matches = (
                self.match_handler.preparing_data(self.data)
            )
            self.teams = (
                self.team_handler.preparing_data(self.data)
            )
            self.goals = (
                self.goal_handler.preparing_data(self.data)
            )
            self.periods = (
                self.period_handler.preparing_data(self.data)
            )

    def save_data(self) -> None:
        """Сохранение данных о матчах."""
        # Сначала сохраняем команды, так как матчи ссылаются на них
        if self.teams:
            self.team_handler.save_data(self.teams)
        # Затем сохраняем матчи
        if self.matches:
            self.match_handler.save_data(self.matches)
        # Голы и периоды зависят от матчей
        if self.goals:
            self.goal_handler.save_data(self.goals)
        if self.periods:
            self.period_handler.save_data(self.periods)


class GetSportRadar:
    """Основной класс для получения и обработки данных с SportRadar."""

    def __init__(self, action: bool) -> None:
        self.action = action
        self._sports = []
        self._countries = []
        self._championships = []
        self._tournaments = []

    def init_getting_processing(self) -> None:
        """
        Инициализирует процесс получения и обработки данных.
        Использует многопроцессорную обработку для матчей.
        """

        if self.action:
            with get_db_session() as db_session:

                for vid_sporta in SPR_SPORTS:
                    sports_url = URL_COUNTRYS % vid_sporta

                    sport_processor = SportDataProcessing(sports_url)
                    sport_processor.set_db_session(db_session)
                    sport_processor.process()

                    self._championships = get_championship_all()

                for champion_ship in self._championships:
                    url_tournament = URL_TOURNAMENTS % champion_ship.id

                    tournament_processor = (
                        TournamentDataProcessing(url_tournament)
                    )
                    tournament_processor.set_db_session(db_session)
                    tournament_processor.process()
                    self._tournaments = get_tournament_all()
        else:
            self._championships = get_championship_all()
            ch_season = get_championship_season()
            self._tournaments = get_season_tournament(ch_season)

        tournament_ids = [t.id for t in self._tournaments]

        # Новая версия
        tasks = JoinableQueue()

        number_consumers = 10 #cpu_count()
        consumers = [
            Consumer(tasks, None)
                for _ in range(number_consumers)
        ]
        for consumer in consumers:
            consumer.start()

        for tournament_id in tournament_ids:
            tasks.put(TournamentConsumer(tournament_id))

        for _ in range(number_consumers):
            tasks.put(None)

        tasks.join()
        
        # Ждем завершения всех потребителей
        for consumer in consumers:
            consumer.join()
        
        logger.info(f'Обработка {len(tournament_ids)} турниров завершена')


class TournamentConsumer:
    def __init__(self, tournament_id: int) -> None:
        self.tournament_id = tournament_id

    def process(self) -> None:
        """Обрабатывает один турнир в отдельном процессе."""
        try:
            with get_db_session() as db_session:
                processor = MatchDataProcessing(self.tournament_id)
                processor.set_db_session(db_session)
                processor.process()

        except Exception as e:
            logger.error(
                f'Ошибка при обработке турнира {self.tournament_id}: {e}'
            )


class Download:
    """Класс для управления процессом загрузки данных."""

    def __init__(self, action: str) -> None:
        self.action = action == OPERATIONS[0]

    def download_sportradar(self) -> None:
        """Запускает процесс загрузки данных с SportRadar."""
        sport_radar = GetSportRadar(self.action)
        sport_radar.init_getting_processing()
