"""
Конфигурация проекта с настройками мониторинга.
"""

import os
from typing import Dict, Any


class Config:
    """Конфигурация приложения."""

    # Настройки нейронной сети
    BATCH_SIZE = 32
    EPOCHS = 100
    VALIDATION_SPLIT = 0.2
    RANDOM_STATE = 42

    # Настройки обработки
    MAX_CONSUMERS = 5
    CHUNK_SIZE = 1000

    # Настройки мониторинга
    MONITORING_ENABLED = True
    MIN_SAMPLES_FOR_TRAINING = 50
    TRAINING_HISTORY_DAYS = 90

    # Настройки регуляризации по умолчанию
    DEFAULT_L1_REG = 0.001
    DEFAULT_L2_REG = 0.001
    DEFAULT_DROPOUT_RATE = 0.3
    DEFAULT_LEARNING_RATE = 0.001


    @classmethod
    def get_pickle_path(cls, category: str, filename: str) -> str:
        """Получить путь для pickle файла."""
        return os.path.join('./pickle', category, filename)

    @classmethod
    def get_model_config(cls, model_type: str) -> Dict[str, Any]:
        """Получить конфигурацию модели по типу."""
        configs = {
            'classification': {
                'l1_reg': cls.DEFAULT_L1_REG,
                'l2_reg': cls.DEFAULT_L2_REG,
                'dropout_rate': cls.DEFAULT_DROPOUT_RATE,
                'learning_rate': cls.DEFAULT_LEARNING_RATE,
                'patience': 20,
                'min_delta': 0.0005
            },
            'regression': {
                'l1_reg': cls.DEFAULT_L1_REG * 0.5,
                'l2_reg': cls.DEFAULT_L2_REG * 0.5,
                'dropout_rate': cls.DEFAULT_DROPOUT_RATE * 0.7,
                'learning_rate': cls.DEFAULT_LEARNING_RATE,
                'patience': 15,
                'min_delta': 0.001
            }
        }
        return configs.get(model_type, configs['classification'])


# Конфигурация по умолчанию
DEFAULT_CONFIG = {
    'processing': {
        'batch_size': 32,
        'max_workers': 4,
        'timeout': 3600
    },
    'model': {
        'epochs': 100,
        'learning_rate': 0.001,
        'dropout_rate': 0.2
    },
    'monitoring': {
        'enabled': True,
        'save_interval_minutes': 60,
        'retention_days': 90
    }
}