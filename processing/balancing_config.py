# izhbet/processing/balancing_config.py
"""
Модуль обработчиков данных с поддержкой мониторинга чемпионатов.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
import logging

from core.utils import create_feature_config, prepare_features
from db.queries.feature import get_feature_match_ids
from db.queries.match import get_match_id
from db.queries.target import get_target_match_ids
from db.base import DBSession
from processing.prediction_keras import (
    make_prediction_keras, train_and_save_keras
)
import os

logger = logging.getLogger(__name__)


class DataProcessor(ABC):
    """Абстрактный класс обработчика данных."""

    @abstractmethod
    def process(self, df_match: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Обработка данных."""
        pass

    @abstractmethod
    def set_db_session(self, db_session: Any) -> None:
        """Установка сессии базы данных."""
        pass


class ProcessFeatures(DataProcessor):
    """Обработчик признаков для матчей с поддержкой мониторинга."""

    def __init__(self, action_model: str) -> None:
        self.create_model = action_model == 'CREATE_MODEL'
        self.db_session: DBSession = None
        self.current_championship_id = None
        self.current_championship_name = None
        self.tournament_id = None

    def set_db_session(self, db_session: DBSession) -> None:
        self.db_session = db_session

    def process(self, df_match: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """
        Обработка признаков матча с сохранением метрик обучения.
        """
        if df_match.empty:
            logger.warning('Пустой DataFrame для обработки')
            return None

        match_ids = df_match['id'].tolist()
        match_tournaments = get_feature_match_ids(self.db_session, match_ids)
        target_tournaments = get_target_match_ids(self.db_session, match_ids)

        info_match = get_match_id(self.db_session, match_ids[0])
        models_dir = self._get_models_dir(info_match)

        # Сохраняем информацию о чемпионате для мониторинга
        self.current_championship_id = info_match.tournament_id
        self.tournament_id = info_match.tournament_id
        self.current_championship_name = (
            f"{info_match.sports.sportName} - "
            f"{info_match.countrys.countryName} - "
            f"{info_match.championships.championshipName}"
        )

        df = pd.DataFrame([x.as_dict() for x in match_tournaments])
        df_target = pd.DataFrame([x.as_dict() for x in target_tournaments])

        logger.info(f"Получено {len(df)} записей features и {len(df_target)} записей targets для турнира {self.tournament_id}")

        if df.empty:
            self._log_empty_data_warning('фичи', info_match)
            return None
        
        # Для режима прогнозирования target данные не обязательны
        if self.create_model:
            # При создании модели target данные обязательны
            if df_target.empty:
                self._log_empty_data_warning('целевые переменные', info_match)
                return None

            # Проверяем соответствие количества записей
            if len(df) != len(df_target):
                logger.warning(f"Несоответствие количества записей: features={len(df)}, targets={len(df_target)}")
                
                # Получаем match_id из features
                feature_match_ids = set(df['match_id'].tolist()) if 'match_id' in df.columns else set()
                target_match_ids = set(df_target['match_id'].tolist()) if 'match_id' in df_target.columns else set()
                
                # Находим общие match_id
                common_match_ids = feature_match_ids.intersection(target_match_ids)
                logger.info(f"Общие match_id: {len(common_match_ids)} из {len(feature_match_ids)} features и {len(target_match_ids)} targets")
                
                if common_match_ids:
                    # Фильтруем данные по общим match_id
                    df = df[df['match_id'].isin(common_match_ids)]
                    df_target = df_target[df_target['match_id'].isin(common_match_ids)]
                    logger.info(f"Отфильтровано до {len(df)} записей по общим match_id")
                else:
                    logger.error("Нет общих match_id между features и targets!")
                    return None
        else:
            # Для прогнозирования target данные не нужны
            logger.info(f"Режим прогнозирования: используем {len(df)} записей features без target данных")

        return self._process_features(df, df_target, models_dir)

    def _get_models_dir(self, info_match: Any) -> str:
        """Получение пути для сохранения моделей."""
        return os.path.join(
            './models',
            info_match.sports.sportName.replace(' ', '_'),
            info_match.countrys.countryName.replace(' ', '_'),
            info_match.championships.championshipName.replace(' ', '_')
        )

    def _process_features(
            self,
            df: pd.DataFrame,
            df_target: pd.DataFrame,
            models_dir: str
    ) -> Dict[str, Any]:
        """Обработка признаков и создание/применение модели."""
        df_feature = prepare_features(df)
        
        # Создаем правильную конфигурацию фичей на основе объединенных данных из get_feature_match_ids
        # Исключаем служебные поля и поля, которые не являются фичами
        feature_columns = [col for col in df_feature.columns 
                          if col not in ['match_id', 'id', 'created_at', 'updated_at'] 
                          and not col.startswith('target_')]
        
        logger.info(f"Создание feature_config для {len(feature_columns)} фичей из объединенных данных")
        logger.debug(f"Первые 10 колонок фичей: {feature_columns[:10]}")
        
        feature_config = create_feature_config(feature_columns)

        if self.create_model:
            logger.info('Создание модели')
            championship_info = {
                'championship_id': self.current_championship_id,
                'championship_name': self.current_championship_name,
            }
            training_result = train_and_save_keras(
                models_dir,
                df_feature,
                df_target,
                feature_config,
                championship_info
            )

            return training_result
        else:
            logger.info('Прогнозирование результатов')
            # Используем обработанные фичи для прогнозирования
            # В режиме прогнозирования df_target может быть пустым
            return make_prediction_keras(models_dir, df_feature, feature_config)

    @staticmethod
    def _log_empty_data_warning(message: str, info_match: Any) -> None:
        """Логирование предупреждения о пустых данных."""
        logger.warning(
            f'Нет данных ({message}) для прогнозов: '
            f'{info_match.sports.sportName} '
            f'{info_match.countrys.countryName} '
            f'{info_match.championships.championshipName}'
        )
