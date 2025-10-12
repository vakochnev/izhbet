
# izhbet/db/storage/monitoring.py

"""
Совместимость: перенесено в db.storage.metric
Оставлено для обратной совместимости и плавной миграции импортов.
"""

import logging
from typing import Any, Dict

from db.storage.metric import save_model_training_metrics  # re-export

logger = logging.getLogger(__name__)

__all__ = [
    'save_model_training_metrics',
]
