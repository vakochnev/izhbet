# izhbet/processing/keras_manager.py
"""
Менеджер моделей Keras.
"""

import os
import logging
from typing import Any, Dict, Optional
import pandas as pd
import joblib
import numpy as np
from keras import models

from core.types import FeatureConfig, ModelData
from core.utils import get_scalers

logger = logging.getLogger(__name__)


class KerasModelManager:
    """Менеджер моделей Keras с поддержкой загрузки/сохранения."""

    def __init__(
            self,
            models_dir: str,
            feature_config: Dict[str, FeatureConfig]
    ) -> None:
        """
        Инициализация менеджера моделей.

        Args:
            models_dir: Директория с моделями
            feature_config: Конфигурация признаков
        """
        self.feature_config = feature_config
        self.loaded_models = self.load_models(models_dir)

    @staticmethod
    def save_models(
            save_dir: str,
            models_data: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Сохранение моделей и связанных артефактов.

        Args:
            save_dir: Директория для сохранения
            models_data: Словарь с моделями и артефактами
        """
        os.makedirs(save_dir, exist_ok=True)

        for model_name, model_data in models_data.items():
            try:
                # Сохранение модели
                model_path = os.path.join(save_dir, f'{model_name}_model.keras')
                model_data['model'].save(model_path)

                # Сохранение скалера (исправлено: доступ через processed_data)
                scaler_path = os.path.join(save_dir, f'{model_name}_scaler.joblib')
                joblib.dump(model_data['processed_data'].scaler, scaler_path)  # Исправлено

                # Сохранение label encoder для классификации (исправлено)
                if (hasattr(model_data['processed_data'], 'label_encoder') and
                        model_data['processed_data'].label_encoder is not None):
                    label_path = os.path.join(save_dir, f'{model_name}_label_encoder.joblib')
                    joblib.dump(model_data['processed_data'].label_encoder, label_path)  # Исправлено

            except Exception as e:
                logger.error(f'Ошибка сохранения модели {model_name}: {e}')
                continue

        logger.info('Все модели и артефакты успешно сохранены.')

    def load_models(
            self,
            models_dir: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Загрузка сохраненных моделей и артефактов.

        Args:
            models_dir: Директория с моделями

        Returns:
            Словарь загруженных моделей
        """
        loaded_models = {}

        for model_name in self.feature_config.keys():
            try:
                # Сначала ищем обычную модель, затем лучшую
                model_path = os.path.join(models_dir, f'{model_name}_model.keras')
                if not os.path.exists(model_path):
                    # Если обычной модели нет, ищем лучшую
                    best_model_path = os.path.join(models_dir, f'{model_name}_best_model.keras')
                    if os.path.exists(best_model_path):
                        model_path = best_model_path
                        logger.debug(f'Используется лучшая модель: {model_path}')
                    else:
                        logger.debug(f'Файл модели не найден: {model_path} или {best_model_path}')
                        continue

                # Загрузка модели и артефактов
                model = models.load_model(model_path)
                scaler = self._load_scaler(models_dir, model_name)
                label_encoder = self._load_label_encoder(models_dir, model_name)

                loaded_models[model_name] = {
                    'model': model,
                    'scaler': scaler,
                    'label_encoder': label_encoder
                }

            except Exception as e:
                logger.error(f'Ошибка загрузки модели {model_name}: {e}')
                continue

        return loaded_models

    @staticmethod
    def _load_scaler(models_dir: str, model_name: str) -> Any:
        """Загрузка скалера."""
        scaler_path = os.path.join(models_dir, f'{model_name}_scaler.joblib')
        if os.path.exists(scaler_path):
            return joblib.load(scaler_path)
        return get_scalers('standard')

    @staticmethod
    def _load_label_encoder(models_dir: str, model_name: str) -> Optional[Any]:
        """Загрузка label encoder."""
        label_path = os.path.join(models_dir, f'{model_name}_label_encoder.joblib')
        if os.path.exists(label_path):
            return joblib.load(label_path)
        return None

    def predict(
            self,
            model_name: str,
            input_features: np.ndarray
    ) -> Dict[str, Any]:
        """
        Предсказание с помощью конкретной модели.

        Args:
            model_name: Название модели
            input_features: Входные признаки

        Returns:
            Словарь с результатами предсказания
        """
        try:
            if model_name not in self.loaded_models:
                raise ValueError(f'Модель {model_name} не найдена')

            model_data = self.loaded_models[model_name]
            prepared_features = self._prepare_input_features(input_features)
            scaled_features = model_data['scaler'].transform(prepared_features)

            prediction = model_data['model'].predict(scaled_features, verbose=0)
            return self._process_prediction(prediction, model_data)

        except Exception as e:
            logger.error(f'Ошибка предсказания моделью {model_name}: {e}')
            return {'error': str(e)}

    @staticmethod
    def _prepare_input_features(input_features: np.ndarray) -> np.ndarray:
        """Подготовка входных признаков."""
        if not isinstance(input_features, np.ndarray):
            input_features = np.array(input_features)

        if input_features.ndim == 1:
            input_features = input_features.reshape(1, -1)

        return input_features

    @staticmethod
    def _process_prediction(
            prediction: np.ndarray,
            model_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Обработка результата предсказания."""
        result = {'probabilities': prediction[0].tolist()}

        if model_data['label_encoder'] is not None:
            # Классификация
            class_index = np.argmax(prediction, axis=1)[0]
            original_label = model_data['label_encoder'].inverse_transform([class_index])[0]
            result['prediction'] = original_label
        else:
            # Регрессия
            result['prediction'] = float(prediction[0][0])

        return result

    def batch_predict(
            self,
            df_feature: pd.DataFrame,
            feature_config: Dict[str, FeatureConfig]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Пакетное предсказание для нескольких моделей.

        Args:
            df_feature: DataFrame с признаками
            feature_config: Конфигурация признаков

        Returns:
            Словарь с результатами предсказаний
        """
        batch_results = {}

        for model_name, config in feature_config.items():
            try:
                features = df_feature[config.features].values
                batch_results[model_name] = self.predict(model_name, features)
            except Exception as e:
                logger.error(f'Ошибка пакетного предсказания {model_name}: {e}')
                batch_results[model_name] = {'error': str(e)}

        return batch_results