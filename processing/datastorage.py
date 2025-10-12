# izhbet/processing/datastorage.py
"""
Модуль хранилищ данных.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from db.storage.processing import save_prediction
from db.base import DBSession

logger = logging.getLogger(__name__)


class DataStorage(ABC):
    """Абстрактный класс хранилища данных."""

    @abstractmethod
    def save(
            self,
            db_session: DBSession,
            predictions: Dict[str, Any]
    ) -> None:
        """Сохранение данных."""
        pass

    @abstractmethod
    def set_db_session(self, db_session: DBSession) -> None:
        """Установка сессии базы данных."""
        pass


class FileStorage(DataStorage):
    """Реализация хранилища данных."""

    def __init__(self) -> None:
        self.db_session: DBSession = None

    def set_db_session(self, db_session: DBSession) -> None:
        self.db_session = db_session

    def save(
            self,
            db_session: DBSession,
            predictions: Dict[str, Any]
    ) -> None:
        """Сохранение предсказаний."""
        if predictions:
            logger.info('Сохранение прогнозов')
            save_prediction(db_session, predictions)
