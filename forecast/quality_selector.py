"""
Селектор качества прогнозов: решает, пропускать ли outcome в statistics.

Правила основаны на конфиге `forecast.quality_config`.
"""

from typing import Optional

from forecast.quality_config import QUALITY_THRESHOLDS


def is_quality_outcome(forecast_type: str, probability: Optional[float], confidence: Optional[float]) -> bool:
    """
    Проверяет, удовлетворяет ли outcome порогам качества для публикации/сохранения в statistics.
    """
    cfg = QUALITY_THRESHOLDS.get(forecast_type)
    if not cfg:
        # Если конфиг отсутствует или None — считаем, что данный тип пока не публикуем
        return False

    try:
        prob = float(probability) if probability is not None else 0.0
    except Exception:
        prob = 0.0

    try:
        conf = float(confidence) if confidence is not None else 0.0
    except Exception:
        conf = 0.0

    return prob >= cfg['min_probability'] and conf >= cfg['min_confidence']


