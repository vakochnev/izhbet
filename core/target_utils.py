"""
Утилиты для работы с целевыми переменными (targets).
"""

import logging
from typing import Optional, Dict, Any
from db.models.target import Target
from db.storage.target_storage import TargetStorage

logger = logging.getLogger(__name__)


def create_target_from_match_result(match_id: int, goal_home: Optional[int], goal_away: Optional[int]) -> Optional[Target]:
    """
    Создает целевую переменную на основе результата матча.
    
    Args:
        match_id: ID матча
        goal_home: Количество голов домашней команды
        goal_away: Количество голов гостевой команды
        
    Returns:
        Target: Объект целевой переменной или None при ошибке
    """
    try:
        # Конвертируем в int, если возможно
        goal_home_int = int(goal_home) if goal_home is not None else None
        goal_away_int = int(goal_away) if goal_away is not None else None
        
        # Проверяем корректность данных
        # if goal_home_int is not None and goal_away_int is not None:
        #     if goal_home_int < 0 or goal_away_int < 0:
        #         logger.warning(f"Отрицательные голы: home={goal_home_int}, away={goal_away_int}")
        #         return None
        
        # Создаем словарь с данными целевой переменной
        target_data = {
            'match_id': match_id,
            'target_win_draw_loss_home_win': None,
            'target_win_draw_loss_draw': None,
            'target_win_draw_loss_away_win': None,
            'target_oz_both_score': None,
            'target_oz_not_both_score': None,
            'target_goal_home_yes': None,
            'target_goal_home_no': None,
            'target_goal_away_yes': None,
            'target_goal_away_no': None,
            'target_total_over': None,
            'target_total_under': None,
            'target_total_home_over': None,
            'target_total_home_under': None,
            'target_total_away_over': None,
            'target_total_away_under': None,
            'target_total_amount': None,
            'target_total_home_amount': None,
            'target_total_away_amount': None
        }
        
        # Win/Draw/Loss One-Hot encoding
        if goal_home_int is not None and goal_away_int is not None:
            if goal_home_int > goal_away_int:
                # Победа домашней команды
                target_data['target_win_draw_loss_home_win'] = 1
                target_data['target_win_draw_loss_draw'] = 0
                target_data['target_win_draw_loss_away_win'] = 0
            elif goal_home_int < goal_away_int:
                # Победа гостевой команды
                target_data['target_win_draw_loss_home_win'] = 0
                target_data['target_win_draw_loss_draw'] = 0
                target_data['target_win_draw_loss_away_win'] = 1
            else:  # goal_home_int == goal_away_int
                # Ничья
                target_data['target_win_draw_loss_home_win'] = 0
                target_data['target_win_draw_loss_draw'] = 1
                target_data['target_win_draw_loss_away_win'] = 0

        # Обе забьют One-Hot encoding
        if goal_home_int is not None and goal_away_int is not None:
            if goal_home_int > 0 and goal_away_int > 0:
                # Обе команды забили
                target_data['target_oz_both_score'] = 1
                target_data['target_oz_not_both_score'] = 0
            else:
                # Не обе забили
                target_data['target_oz_both_score'] = 0
                target_data['target_oz_not_both_score'] = 1

        # Забьет ли домашняя команда One-Hot encoding
        if goal_home_int is not None:
            if goal_home_int > 0:
                # Домашняя забила
                target_data['target_goal_home_yes'] = 1
                target_data['target_goal_home_no'] = 0
            else:
                # Домашняя не забила
                target_data['target_goal_home_yes'] = 0
                target_data['target_goal_home_no'] = 1

        # Забьет ли гостевая команда One-Hot encoding
        if goal_away_int is not None:
            if goal_away_int > 0:
                # Гостевая забила
                target_data['target_goal_away_yes'] = 1
                target_data['target_goal_away_no'] = 0
            else:
                # Гостевая не забила
                target_data['target_goal_away_yes'] = 0
                target_data['target_goal_away_no'] = 1

        # Тотал больше/меньше 2.5 One-Hot encoding
        if goal_home_int is not None and goal_away_int is not None:
            total_goals = goal_home_int + goal_away_int
            if total_goals > 2.5:
                target_data['target_total_over'] = 1
                target_data['target_total_under'] = 0
            else:
                target_data['target_total_over'] = 0
                target_data['target_total_under'] = 1

        # Индивидуальный тотал домашней команды больше/меньше 1.5 One-Hot encoding
        if goal_home_int is not None:
            if goal_home_int > 1.5:
                target_data['target_total_home_over'] = 1
                target_data['target_total_home_under'] = 0
            else:
                target_data['target_total_home_over'] = 0
                target_data['target_total_home_under'] = 1

        # Индивидуальный тотал гостевой команды больше/меньше 1.5 One-Hot encoding
        if goal_away_int is not None:
            if goal_away_int > 1.5:
                target_data['target_total_away_over'] = 1
                target_data['target_total_away_under'] = 0
            else:
                target_data['target_total_away_over'] = 0
                target_data['target_total_away_under'] = 1

        # Регрессионные переменные
        if goal_home_int is not None and goal_away_int is not None:
            target_data['target_total_amount'] = goal_home_int + goal_away_int
        else:
            target_data['target_total_amount'] = None

        if goal_home_int is not None:
            target_data['target_total_home_amount'] = goal_home_int
        else:
            target_data['target_total_home_amount'] = None

        if goal_away_int is not None:
            target_data['target_total_away_amount'] = goal_away_int
        else:
            target_data['target_total_away_amount'] = None

        # Сохраняем в БД
        logger.debug(f"Попытка сохранения target для матча {match_id} (голы: {goal_home_int}:{goal_away_int})")
        target_storage = TargetStorage()
        result = target_storage.save_target(target_data)
        
        if result:
            logger.debug(f"Target успешно сохранен для матча {match_id}, ID={result.get('id') if isinstance(result, dict) else 'N/A'}")
            return result
        else:
            logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось сохранить target для матча {match_id} - save_target вернул None")
            return None
        
    except Exception as e:
        logger.critical(f"Критическая ошибка при создании целевой переменной для матча {match_id}: {e}", exc_info=True)
        return None


def create_targets_for_features(features_data: list) -> int:
    """
    Создает целевые переменные для списка features.
    
    Args:
        features_data: Список словарей с данными features
        
    Returns:
        int: Количество созданных целевых переменных
    """
    created_count = 0
    target_storage = TargetStorage()
    
    for feature_data in features_data:
        match_id = feature_data.get('match_id')
        goal_home = feature_data.get('goal_home')
        goal_away = feature_data.get('goal_away')
        
        if match_id and (goal_home is not None or goal_away is not None):
            target = create_target_from_match_result(match_id, goal_home, goal_away)
            if target:
                created_count += 1
    
    logger.info(f"Создано {created_count} целевых переменных из {len(features_data)} features")
    return created_count


def get_target_data_for_training(match_ids: list) -> Dict[int, Dict[str, Any]]:
    """
    Получает данные целевых переменных для обучения.
    
    Args:
        match_ids: Список ID матчей
        
    Returns:
        Dict[int, Dict[str, Any]]: Словарь с данными целевых переменных по match_id
    """
    target_storage = TargetStorage()
    targets = target_storage.get_targets_by_match_ids(match_ids)
    
    result = {}
    for target in targets:
        result[target.match_id] = target.as_dict()
    
    return result
