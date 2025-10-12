"""
Типы данных для проекта.
"""

from typing import Dict, List, Any, Union, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np


@dataclass
class FeatureConfig:
    """Конфигурация признаков для модели."""
    features: List[str]
    target: str
    task_type: str
    normalization_method: str


@dataclass
class ModelData:
    """Данные модели для обучения/предсказания."""
    X_train: np.ndarray
    X_test: np.ndarray
    y_train: np.ndarray
    y_test: np.ndarray
    scaler: Any
    label_encoder: Optional[Any] = None
    task_type: str = "classification"
    num_classes: int = 1


@dataclass
class PredictionResult:
    """Результат предсказания."""
    match_id: int
    predictions: Dict[str, Any]
    team_home_id: int
    team_away_id: int
    features: Dict[str, Any]


@dataclass
class TournamentTask:
    """Задача обработки турнира."""
    tournament_id: int
    action: str
    data: Optional[pd.DataFrame] = None