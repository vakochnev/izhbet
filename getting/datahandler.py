"""
Модуль для обработки данных о спортивных событиях.

Содержит фабрику обработчиков данных и классы для обработки различных типов данных:
- Виды спорта
- Страны
- Турниры
- Чемпионаты
- Матчи
- Команды
- Голы
- Периоды
- Коэффициенты
- Таблицы
"""

import requests
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Type

from db.queries import (sport, country, championship)
from db.storage.getting import (
    save_coef, save_team, save_match, save_country, save_tournament,
    save_goal, save_sport, save_table, save_period, save_championship,
)
from db.base import DBSession

logger = logging.getLogger(__name__)


class DataHandler(ABC):
    """Абстрактный базовый класс обработчика данных."""

    @staticmethod
    def fetch_data(api_url: str) -> Optional[Dict[str, Any]]:
        """
        Получает данные из API.

        Args:
            api_url: URL для запроса к API

        Returns:
            Словарь с данными в формате JSON или None в случае ошибки
        """
        headers = {
            'Content-Type': 'text/html',
            'User-Agent': 'Mozilla / 5.0(X11; Linux x86_64; rv: 122.0)'
                          ' Gecko / 20100101 Firefox / 122.0'
        }
        try:
            response = requests.get(
                api_url,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f'Ошибка при запросе к API: {e}')
            return None

        if response.status_code == 200:
            logger.debug(
                f'Загрузка страницы из {api_url}, '
                f'код ответа: {response.status_code}.'
            )
            return response.json()
        else:
            logger.error(
                f'Ошибка загрузка страницы из {api_url}, '
                f'код ответа: {response.status_code}.'
            )
            return None

    @abstractmethod
    def save_data(self, data: Any) -> None:
        """Абстрактный метод для сохранения данных."""
        raise NotImplementedError(
            'Метод save_data должен быть переопределен.'
        )

    @staticmethod
    @abstractmethod
    def preparing_data(data: Any) -> Any:
        """Абстрактный метод для подготовки данных перед сохранением."""
        raise NotImplementedError(
            'Метод preparing_data должен быть переопределен.'
        )


class SportHandler(DataHandler):
    """Обработчик данных видов спорта."""
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: Dict[str, Any]) -> None:
        """Сохраняет данные о виде спорта."""
        save_sport(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Подготавливает данные о виде спорта."""
        for sport_data in data.get('sports', []):
            if sport_data['id'] in sport.SPR_SPORTS:
                return sport_data
        return None


class CountryHandler(DataHandler):
    """ Обработчик данных стран. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data):
        """Сохраняет данные о странах."""
        save_country(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Подготавливает данные о странах."""
        return [
            category for category in data.get('categories', [])
            if country.is_country_top(category['id'])
        ]


class TournamentHandler(DataHandler):
    """ Обработчик данных турниров. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        save_tournament(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Сохраняет данные о турнирах."""
        return data #.get('tournaments', [])


class ChampionshipHandler(DataHandler):
    """ Обработчик данных чемпионатов. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Сохраняет данные о чемпионатах."""
        save_championship(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Подготавливает данные о чемпионатах."""
        return [
            tournament for tournament in data.get('tournaments', [])
            if championship.is_chmpionship_top(tournament['id'])
        ]


class MatchHandler(DataHandler):
    """ Обработчик данных матчей. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Сохраняет данные о матчах."""
        save_match(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Подготавливает данные о матчах."""
        return data.get('matches', [])


class TeamHandler(DataHandler):
    """ Обработчик данных команд. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Сохраняет данные о командах."""
        save_team(self, data)

    @staticmethod
    def preparing_data(data:List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Подготавливает данные о командах."""
        return data.get('teams', [])


class GoalHandler(DataHandler):
    """Обработчик данных голов."""
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Сохраняет данные о голах."""
        save_goal(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Подготавливает данные о голах."""
        data_prepare = []
        for i in range(len(data['matches'])):
            if len(data['matches'][i]['result']) != 1:
                if 'goals' not in data['matches'][i].keys():
                    continue
                goal_temp = []
                k = 0
                for j in range(len(data['matches'][i]['goals'])):
                    goal_temp.append(data['matches'][i]['goals'][j])
                    goal_temp[k]['match_id'] = data['matches'][i]['id']
                    if goal_temp[k]['team'] == 'home':
                        goal_temp[k]['team_id'] = (
                            data['matches'][i]['homeId']
                        )
                    else:
                        goal_temp[k]['team_id'] = (
                            data['matches'][i]['awayId']
                        )
                    k += 1
                data_prepare.append(goal_temp)
        return data_prepare


class PeriodHandler(DataHandler):
    """ Обработчик данных периодов. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Сохраняет данные о периодах."""
        save_period(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Подготавливает данные о периодах."""
        data_prepare = []
        for i in range(len(data['matches'])):
            if len(data['matches'][i]['result']) != 1:
                if type(data['matches'][i]) is str:
                    continue
                if 'periods' not in data['matches'][i].keys():
                    continue
                period_temp = []
                k = 0
                for j in range(len(data['matches'][i]['periods'])):
                    period_temp.append(
                        data['matches'][i]['periods'][j]
                    )
                    period_temp[k]['match_id'] = (
                        data['matches'][i]['id']
                    )
                    k += 1
                data_prepare.append(period_temp)
        return data_prepare


class CoefHandler(DataHandler):
    """ Обработчик данных коэффициентов. """
    def __init__(self, db_session):
        super().__init__()
        self.db_session = db_session

    def save_data(self, data):
        """Сохраняет данные о коэффициентах."""
        save_coef(self, data)

    @staticmethod
    def preparing_data(
        data: List[Dict[str, Any]],
        coefs_id: int
    ) -> None:
        """Подготавливает данные о коэффициентах."""
        # data_prepare = []
        # for i in range(len(data['main'])):
        #     data['main'][i]['coefs'][0]['id'] = coefs_id
        #     data_prepare.append(data['main'][i]['coefs'])
        # return data_prepare
        for coef in data.get('main', []):
            coef['coefs'][0]['id'] = coefs_id
        return [coef['coefs'] for coef in data.get('main', [])]


class TableHandler(DataHandler):
    """ Обработчик данных коэффициентов. """
    def __init__(self, session_local=None) -> None:
        super().__init__()
        self.session_local = session_local

    def save_data(self, data: Dict[str, Any]) -> None:
        """Обработчик данных таблиц."""
        save_table(self, data)

    @staticmethod
    def preparing_data(data: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
        """Подготавливает данные таблиц."""
        return data['id'], data.get('rows', [])


class DataHandlerFactory:
    """
    Фабрика для создания обработчиков данных.
    Создает экземпляр обработчика данных по типу.
    Args:
        handler_type: Тип обработчика (sport, team, tournament и т.д.)
    Returns:
        Экземпляр соответствующего обработчика данных
    Raises:
        ValueError: Если передан неизвестный тип обработчика
    """
    @staticmethod
    def create_handler(handler_type: str, db_session=None) -> DataHandler:

        if handler_type == 'sport':
            return SportHandler(db_session)
        if handler_type == 'country':
            return CountryHandler(db_session)
        if handler_type == 'tournament':
            return TournamentHandler(db_session)
        if handler_type == 'championship':
            return ChampionshipHandler(db_session)
        if handler_type == 'match':
            return MatchHandler(db_session)
        elif handler_type == 'team':
            return TeamHandler(db_session)
        elif handler_type == 'goal':
            return GoalHandler(db_session)
        elif handler_type == 'period':
            return PeriodHandler(db_session)
        else:
            raise ValueError(f"Неизвестный тип обработчика: {handler_type}")
