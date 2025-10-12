# izhbet/processing/keras_preprocessor.py
"""
Предобработка данных для моделей Keras.
"""

import logging
from typing import Dict, Any, Tuple, Optional, List
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler, RobustScaler
from sklearn.impute import SimpleImputer

from core.types import ModelData, FeatureConfig
from core.utils import (
    get_scalers, prepare_features_and_targets,
    validate_targets
)

logger = logging.getLogger(__name__)


class OverfittingMonitor:
    """Монитор для обнаружения переобучения."""

    @staticmethod
    def check_overfitting(
            history: Dict[str, List[float]],
            patience: int = 3
    ) -> bool:
        """
        Проверка на переобучение по истории обучения.
        """
        if 'loss' not in history or 'val_loss' not in history:
            return False

        train_loss = history['loss']
        val_loss = history['val_loss']

        if len(train_loss) < patience or len(val_loss) < patience:
            return False

        last_epochs_train = train_loss[-patience:]
        last_epochs_val = val_loss[-patience:]

        # Если validation loss растет, а train уменьшается - переобучение
        if np.mean(np.diff(last_epochs_val)) > 0 and np.mean(np.diff(last_epochs_train)) < 0:
            return True

        # Если разница между train и validation слишком большая
        if np.mean(train_loss) * 1.5 < np.mean(val_loss):
            return True

        return False


class DataPreprocessor:
    """Класс для предобработки данных для нейронных сетей."""

    def __init__(
        self,
        features: pd.DataFrame,
        target: pd.DataFrame,
        feature_config: Dict[str, FeatureConfig]
    ) -> None:
        self.original_features = features
        self.original_target = target
        self.feature_config = feature_config

        # Очищаем и валидируем данные
        self.features, self.target = prepare_features_and_targets(
            features, target, feature_config
        )

    def preprocess_data(self) -> Dict[str, ModelData]:
        """
        Предобработка данных с поддержкой множественных целевых переменных.
        """
        processed_data = {}

        for model_name, config in self.feature_config.items():
            try:
                # # Получаем данные для конкретной модели
                X_raw = self.features[config.features].values
                y_raw = self.target[config.target] #.values

                # Продолжаем стандартную предобработку
                X, scaler = self._normalize_features(config, model_name)
                y, label_encoder, num_classes = self._prepare_target(y_raw, config, model_name)
                X_train, X_test, y_train, y_test = self._split_data(X, y, config.task_type, model_name)

                processed_data[model_name] = ModelData(
                    X_train=X_train.astype(np.float32),
                    X_test=X_test.astype(np.float32),
                    y_train=y_train,
                    y_test=y_test,
                    scaler=scaler,
                    label_encoder=label_encoder,
                    task_type=config.task_type,
                    num_classes=num_classes
                )

            except Exception as e:
                logger.error(f'Ошибка предобработки для модели {model_name}: {e}')
                continue

        return processed_data


    def _process_single_model(
            self,
            config: FeatureConfig,
            model_name: str
    ) -> Optional[ModelData]:
        """Обработка данных для одной модели."""
        try:
            # Валидируем таргет для конкретной модели
            target_data = self._validate_model_target(config.target, model_name)
            if target_data is None:
                return None

            # Получаем признаки
            X = self.features[config.features].values
            y = target_data.values

            # Метод _normalize_features принимает только 2 аргумента: config и model_name
            X_normalized, scaler = self._normalize_features(config, model_name)
            if X_normalized is None:
                return None

            # Подготовка целевой переменной
            y_processed, label_encoder, num_classes = self._prepare_target(y, config, model_name)
            if y_processed is None:
                return None

            # Разделение на train/test
            X_train, X_test, y_train, y_test = self._split_data(
                X_normalized, y_processed, config.task_type, model_name
            )

            return ModelData(
                X_train=X_train.astype(np.float32),
                X_test=X_test.astype(np.float32),
                y_train=y_train,
                y_test=y_test,
                scaler=scaler,
                label_encoder=label_encoder,
                task_type=config.task_type,
                num_classes=num_classes
            )

        except Exception as e:
            logger.error(f'Ошибка обработки модели {model_name}: {e}')
            return None

    def _validate_model_target(
            self,
            target_col: str,
            model_name: str
    ) -> Optional[pd.Series]:
        """Валидация таргета для конкретной модели."""
        if target_col not in self.target.columns:
            logger.error(
                f"Таргет {target_col} не найден для модели "
                f"{model_name}"
            )
            return None

        try:
            return validate_targets(self.target[target_col], model_name)
        except ValueError:
            logger.critical(
                f"Пропущенные значения в таргете {target_col} "
                f"для модели {model_name}"
            )
            return None


    def _normalize_features(
            self,
            config: FeatureConfig,
            model_name: str
    ) -> Tuple[Optional[np.ndarray], Any]:
        """Нормализация признаков."""
        try:
            # Проверяем, что все фичи существуют
            missing_features = [f for f in config.features if f not in self.features.columns]
            if missing_features:
                logger.error(
                    f"Отсутствующие фичи для модели {model_name}:"
                    f" {missing_features}"
                )
                return None, None

            features_data = self.features[config.features].values

            # Дополнительная проверка на пропуски
            if np.any(np.isnan(features_data)):
                nan_count = np.isnan(features_data).sum()
                logger.error(
                    f"Обнаружено {nan_count} NaN значений в фичах "
                    f"модели {model_name} после очистки"
                )
                # Заполняем оставшиеся NaN
                features_data = np.nan_to_num(features_data, nan=0.0)

            # Проверяем, что данные не пустые
            if features_data.size == 0:
                logger.error(
                    f"Пустые данные признаков для модели {model_name}"
                )
                return None, None

            scaler = get_scalers(config.normalization_method)
            normalized_data = scaler.fit_transform(features_data)
            logger.info(
                f"Модель {model_name}: нормализовано "
                f"{normalized_data.shape[1]} признаков"
            )
            return normalized_data, scaler

        except Exception as e:
            logger.error(
                f'Ошибка нормализации признаков для модели'
                f' {model_name}: {e}'
            )
            return None, None

    def _prepare_target(
            self,
            target_data: pd.Series,
            config: FeatureConfig,
            model_name: str
    ) -> Tuple[Optional[np.ndarray], Any, int]:
        """Подготовка целевой переменной."""
        try:
            # Финализируем валидацию
            target_values = target_data.values

            # Проверяем на NaN после валидации
            if np.any(np.isnan(target_values)):
                logger.critical(
                    f"КРИТИЧЕСКАЯ ОШИБКА: NaN в таргете после "
                    f"валидации для модели {model_name}"
                )
                return None, None, 0

            if config.task_type == 'classification':
                # Проверяем, является ли это One-Hot encoded данными
                if self._is_onehot_encoded(target_data, model_name):
                    # Данные уже в One-Hot формате - это бинарная классификация
                    y = target_values.astype(np.float32)
                    # Для бинарной классификации используем 1 класс (sigmoid)
                    num_classes = 1
                    logger.info(f"Модель {model_name}: используем One-Hot encoded данные, бинарная классификация")
                    return y, None, num_classes
                else:
                    # Обычная классификация - используем LabelEncoder
                    label_encoder = LabelEncoder()
                    y = label_encoder.fit_transform(target_values)
                    num_classes = len(np.unique(y))

                    # Проверяем, что есть хотя бы 2 класса
                    if num_classes < 2:
                        logger.warning(
                            f"Модель {model_name}: только 1 класс "
                            f"в таргете"
                        )
                        return None, None, 0

                    return y, label_encoder, num_classes
            else:
                y = target_values.astype(np.float32)
                return y, None, 1

        except Exception as e:
            logger.error(
                f'Ошибка подготовки целевой переменной '
                f'для модели {model_name}: {e}'
            )
            return None, None, 0

    def _is_onehot_encoded(self, target_data: pd.Series, model_name: str) -> bool:
        """Проверяет, являются ли данные One-Hot encoded."""
        try:
            # Проверяем, содержит ли название модели индикаторы One-Hot encoding
            onehot_indicators = [
                'win_draw_loss_home_win', 'win_draw_loss_draw', 'win_draw_loss_away_win',
                'oz_both_score', 'oz_not_both_score',
                'goal_home_scores', 'goal_home_no_score',
                'goal_away_scores', 'goal_away_no_score',
                'total_over', 'total_under',
                'total_home_over', 'total_home_under',
                'total_away_over', 'total_away_under'
            ]
            
            # Проверяем по названию модели
            for indicator in onehot_indicators:
                if indicator in model_name:
                    return True
            
            # Проверяем по данным - если это DataFrame с несколькими колонками
            if hasattr(target_data, 'shape') and len(target_data.shape) > 1:
                return True
                
            # Проверяем по значениям - если все значения 0 или 1 (бинарная классификация)
            unique_values = np.unique(target_data.dropna())
            if len(unique_values) <= 2 and all(v in [0, 1] for v in unique_values):
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"Ошибка проверки One-Hot encoding для {model_name}: {e}")
            return False

    def _split_data(
        self,
        X: np.ndarray,
        y: np.ndarray,
        task_type: str,
        model_name: str
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Разделение данных на train/test."""
        try:
            # Проверяем, что данные не пустые
            if X.size == 0 or y.size == 0:
                raise ValueError("Пустые данные для разделения")

            # Для классификации проверяем, можно ли использовать стратификацию
            if task_type == 'classification':
                unique_classes, class_counts = np.unique(y, return_counts=True)

                logger.info(
                    f"Модель {model_name}: классы {unique_classes}, "
                    f"counts {class_counts}"
                )

                # Проверяем, что все классы имеют хотя бы 2 samples
                can_stratify = np.all(class_counts >= 2) and len(unique_classes) > 1

                if can_stratify:
                    X_train, X_test, y_train, y_test = train_test_split(
                        X, y, test_size=0.2, random_state=42, stratify=y
                    )
                    logger.debug(
                        f"Модель {model_name}: использовано "
                        f"стратифицированное разделение"
                    )
                else:
                    # Используем обычное разделение без стратификации
                    X_train, X_test, y_train, y_test = train_test_split(
                        X, y, test_size=0.2, random_state=42
                    )
                    logger.warning(
                        f"Модель {model_name}: невозможно стратифицировать. "
                        f"Классы: {unique_classes}, counts: {class_counts}. "
                        f"Использовано обычное разделение."
                    )
            else:
                # Для регрессии используем обычное разделение
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.2, random_state=42
                )

            return X_train, X_test, y_train, y_test

        except Exception as e:
            logger.error(
                f'Ошибка разделения данных для модели '
                f'{model_name}: {e}'
            )
            raise