# izhbet/processing/datasource.py
"""
Модуль источников данных.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Any
import pandas as pd
import logging

from db.queries.match import get_match_modeling
from db.base import DBSession
from core.constants import MATCH_TYPE
from db.queries.feature import get_match_in_feature_all

logger = logging.getLogger(__name__)


class DataSource(ABC):
    """Абстрактный класс источника данных."""

    @abstractmethod
    def retrieve(self, create_model: bool) -> None:
        """Получение данных из источника."""
        pass

    @abstractmethod
    def set_db_session(self, db_session: DBSession) -> None:
        """Установка сессии базы данных."""
        pass


class DatabaseSource(DataSource):
    """Реализация источника данных из базы данных."""

    def __init__(self) -> None:
        self.tournaments_id: List[int] = []
        self.df = pd.DataFrame()
        self.db_session: DBSession = None

    def set_db_session(self, db_session: DBSession) -> None:
        self.db_session = db_session

    def retrieve(self, create_model: bool) -> None:
        """
        Получение данных из базы данных.

        Args:
            create_model: Флаг создания модели (True - все время, False - текущий сезон)
        """
        matches_all = get_match_modeling(create_model)

        if not matches_all:
            logger.warning('Не найдены матчи для обработки')
            return

        logger.info(f'Отобрано для обработки: {len(matches_all)} матчей')

        self.df = pd.DataFrame([match.as_dict() for match in matches_all])
        self.df.fillna(value='', inplace=True)
        self.df = self.df.astype(MATCH_TYPE, errors='ignore')

        # Извлечение уникальных tournament_id
        df_tournament = self.df['tournament_id'].drop_duplicates().sort_values()
        self.tournaments_id = df_tournament.tolist()
        #self.tournaments_id = [17, 18, 24, 25, 173]

        logger.info(f'Найдено {len(self.tournaments_id)} турниров для обработки')

    def select_data(self, tournament_id: int) -> pd.DataFrame:
        """
        Выбор данных для конкретного турнира.

        Args:
            tournament_id: ID турнира

        Returns:
            DataFrame с данными матчей турнира (только те, для которых существуют фичи)
        """
        if self.df.empty:
            logger.warning('Нет данных для выборки')
            return pd.DataFrame()

        df_match_tournament = self.df[
            self.df['tournament_id'] == tournament_id
            ].copy()

        # Фильтрация по наличию фич в БД
        match_ids = df_match_tournament['id'].astype(int).tolist()
        feature_rows = get_match_in_feature_all(self.db_session, match_ids)
        feature_match_ids = {row.match_id for row in feature_rows}

        if not feature_match_ids:
            logger.warning(
                f'В турнире {tournament_id} нет матчей с рассчитанными фичами. '
                f'Пропускаем турнир.'
            )
            return pd.DataFrame()

        df_match_tournament = df_match_tournament[
            df_match_tournament['id'].isin(feature_match_ids)
        ]

        logger.info(
            f'Обработка турнира {tournament_id}: '
            f'отобрано {df_match_tournament.shape[0]} матчей'
        )

        return df_match_tournament