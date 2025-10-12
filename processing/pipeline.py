# izhbet/processing/pipeline.py
"""
Реализация паттерна Pipeline для обработки данных.
"""

import logging
from abc import ABC, abstractmethod
from multiprocessing import JoinableQueue, Queue
from typing import Any, Dict, List, Optional
import pandas as pd

from core.constants import ACTION_MODEL
from core.consumer import Consumer
from .datasource import DataSource
from .balancing_config import DataProcessor
from .datastorage import DataStorage

logger = logging.getLogger(__name__)


class PipelineComponent(ABC):
    """Абстрактный базовый класс для компонентов pipeline."""

    @abstractmethod
    def set_db_session(self, db_session: Any) -> None:
        """Установка сессии базы данных."""
        pass


class EmbeddingCalculationPipeline:
    """
    Конвейер расчета эмбеддингов для спортивных событий.
    Реализует паттерн Pipeline с шагами: извлечение -> выборка -> обработка -> сохранение.
    """

    def __init__(
        self,
        data_source: DataSource,
        data_processor: DataProcessor,
        data_storage: DataStorage
    ) -> None:
        """
        Инициализация конвейера.

        Args:
            data_source: Источник данных
            data_processor: Обработчик данных
            data_storage: Хранилище данных
        """
        self.data_source = data_source
        self.data_processor = data_processor
        self.data_storage = data_storage

    def process_data(self, action: str) -> List[int]:
        """
        Координация процесса обработки данных.

        Args:
            action: Действие (CREATE_MODEL или CREATE_PROGNOZ)
            
        Returns:
            List[int]: Список ID обработанных турниров
        """
        is_create_model = action == ACTION_MODEL[0]
        self.data_source.retrieve(is_create_model)

        if not self.data_source.tournaments_id:
            logger.warning('Нет турниров для обработки')
            return []

        self._process_tournaments_parallel(action)
        
        # Возвращаем список ID турниров для дальнейшего использования
        return self.data_source.tournaments_id.copy()

    def _process_tournaments_parallel(self, action: str) -> None:
        """Многопроцессорная обработка турниров."""
        tasks = JoinableQueue()
        results = Queue()

        # Создание потребителей
        number_consumers = min(10, len(self.data_source.tournaments_id))
        consumers = [
            Consumer(tasks, results) for _ in range(number_consumers)
        ]

        for consumer in consumers:
            consumer.start()

        # Добавление задач в очередь
        for tournament_id in self.data_source.tournaments_id:
            tournament_task = TournamentTask(
                action=action,
                data_source=self.data_source,
                data_processor=self.data_processor,
                data_storage=self.data_storage,
                tournament_id=tournament_id
            )
            tasks.put(tournament_task)

        # Сигналы завершения для потребителей
        for _ in range(number_consumers):
            tasks.put(None)

        tasks.join()
        self._process_results(results, len(self.data_source.tournaments_id))

    def _process_results(self, results: Queue, total_tasks: int) -> None:
        """Обработка результатов выполнения задач."""
        for _ in range(total_tasks):
            result = results.get()
            if result is not None:
                logger.error(f'Ошибка при обработке: {result}')


class TournamentTask:
    """Задача обработки одного турнира."""

    def __init__(
        self,
        action: str,
        data_source: DataSource,
        data_processor: DataProcessor,
        data_storage: DataStorage,
        tournament_id: int
    ) -> None:
        self.action = action
        self.data_source = data_source
        self.data_processor = data_processor
        self.data_storage = data_storage
        self.tournament_id = tournament_id

    def process(self) -> None:
        """Обработка одного турнира."""
        try:
            from config import get_db_session

            with get_db_session() as db_session:
                self._setup_components(db_session)

                # Выборка данных турнира
                df_match = self.data_source.select_data(self.tournament_id)

                if df_match.empty:
                    logger.warning(
                        f'Нет данных для турнира {self.tournament_id}'
                    )
                    return

                # Передаем информацию о чемпионате в обработчик
                if hasattr(self.data_processor, 'set_championship_info'):
                    championship_info = self._get_championship_info(df_match, db_session)
                    self.data_processor.set_championship_info(championship_info)

                # Обработка данных
                predictions = self.data_processor.process(df_match)

                # Сохранение результатов (только для прогнозирования)
                if predictions and not self._is_create_model_action():
                    self.data_storage.save(db_session, predictions)

        except Exception as e:
            logger.error(
                f'Ошибка при обработке турнира '
                f'{self.tournament_id}: {e}'
            )
            raise

    def _get_championship_info(
            self,
            df_match: pd.DataFrame,
            db_session: Any
    ) -> Dict[str, Any]:
        """Получение информации о чемпионате."""
        try:
            from db.queries.match import get_championship_info

            if not df_match.empty:
                match_id = df_match.iloc[0]['id']
                championship_info = get_championship_info(db_session, match_id)
                return championship_info

        except Exception as e:
            logger.warning(
                f"Не удалось получить информацию "
                f"о чемпионате: {e}"
            )

        return {}

    def _setup_components(self, db_session: Any) -> None:
        """Настройка компонентов с сессией БД."""
        self.data_source.set_db_session(db_session)
        self.data_processor.set_db_session(db_session)
        self.data_storage.set_db_session(db_session)

    def _is_create_model_action(self) -> bool:
        """Проверка, является ли действие созданием модели."""
        from core.constants import ACTION_MODEL
        return self.action == ACTION_MODEL[0]