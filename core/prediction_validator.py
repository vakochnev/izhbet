"""
Модуль для проверки правильности прогнозов на основе таблицы targets.

Этот модуль предоставляет единую точку для определения правильности прогнозов,
используя уже рассчитанные значения из таблицы targets.
Это устраняет дублирование логики в разных модулях проекта.
"""

import logging
from typing import Optional
from db.models.target import Target

logger = logging.getLogger(__name__)


def is_prediction_correct_from_target(
    feature: int,
    outcome: str,
    target: Target
) -> bool:
    """
    Определяет правильность прогноза на основе target.
    
    Args:
        feature: Код feature (1-10)
        outcome: Прогноз (п1, х, п2, тб, тм, ит1б, ит1м и т.д.)
        target: Объект Target с рассчитанными значениями
        
    Returns:
        bool: True если прогноз правильный, False иначе
    """
    if not target:
        return False
    
    outcome_lower = str(outcome).lower().strip()
    
    # Feature 1: WIN_DRAW_LOSS
    if feature == 1:
        if outcome_lower == 'п1':
            return target.target_win_draw_loss_home_win == 1
        elif outcome_lower == 'х':
            return target.target_win_draw_loss_draw == 1
        elif outcome_lower == 'п2':
            return target.target_win_draw_loss_away_win == 1
    
    # Feature 2: OZ (Обе забьют)
    elif feature == 2:
        if 'да' in outcome_lower:
            return target.target_oz_both_score == 1
        elif 'нет' in outcome_lower:
            return target.target_oz_not_both_score == 1
    
    # Feature 3: GOAL_HOME
    elif feature == 3:
        if 'да' in outcome_lower:
            return target.target_goal_home_yes == 1
        elif 'нет' in outcome_lower:
            return target.target_goal_home_no == 1
    
    # Feature 4: GOAL_AWAY
    elif feature == 4:
        if 'да' in outcome_lower:
            return target.target_goal_away_yes == 1
        elif 'нет' in outcome_lower:
            return target.target_goal_away_no == 1
    
    # Feature 5: TOTAL
    elif feature == 5:
        if outcome_lower in ['тб', 'больше']:
            return target.target_total_over == 1
        elif outcome_lower in ['тм', 'меньше']:
            return target.target_total_under == 1
    
    # Feature 6: TOTAL_HOME
    elif feature == 6:
        if outcome_lower == 'ит1б':
            return target.target_total_home_over == 1
        elif outcome_lower == 'ит1м':
            return target.target_total_home_under == 1
    
    # Feature 7: TOTAL_AWAY
    elif feature == 7:
        if outcome_lower == 'ит2б':
            return target.target_total_away_over == 1
        elif outcome_lower == 'ит2м':
            return target.target_total_away_under == 1
    
    # Feature 8: TOTAL_AMOUNT (регрессия, но проверяется как классификация)
    elif feature == 8:
        if outcome_lower in ['тм', 'меньше']:
            return target.target_total_under == 1
        elif outcome_lower in ['тб', 'больше']:
            return target.target_total_over == 1
    
    # Feature 9: TOTAL_HOME_AMOUNT
    elif feature == 9:
        if outcome_lower == 'ит1м':
            return target.target_total_home_under == 1
        elif outcome_lower == 'ит1б':
            return target.target_total_home_over == 1
    
    # Feature 10: TOTAL_AWAY_AMOUNT
    elif feature == 10:
        if outcome_lower == 'ит2м':
            return target.target_total_away_under == 1
        elif outcome_lower == 'ит2б':
            return target.target_total_away_over == 1
    
    logger.warning(f"Не удалось определить правильность прогноза: feature={feature}, outcome={outcome}")
    return False


def get_prediction_status_from_target(
    feature: int,
    outcome: str,
    target: Optional[Target]
) -> str:
    """
    Возвращает статус прогноза (✅/❌/⏳) на основе target.
    
    Args:
        feature: Код feature (1-10)
        outcome: Прогноз
        target: Объект Target или None (если матч не состоялся)
        
    Returns:
        str: '✅' - правильный, '❌' - неправильный, '⏳' - ожидание
    """
    # Если target нет - матч еще не состоялся
    if target is None:
        return '⏳'
    
    # Проверяем правильность прогноза
    is_correct = is_prediction_correct_from_target(feature, outcome, target)
    return '✅' if is_correct else '❌'


def get_targets_batch(match_ids: list[int]) -> dict[int, Target]:
    """
    Получает targets для списка матчей одним запросом (оптимизация).
    
    Args:
        match_ids: Список ID матчей
        
    Returns:
        dict[int, Target]: Словарь {match_id: Target}
    """
    from db.queries.target import get_target_match_ids
    from config import Session_pool
    
    with Session_pool() as session:
        targets = get_target_match_ids(session, match_ids)
        return {t.match_id: t for t in targets}

