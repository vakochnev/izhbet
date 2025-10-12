# izhbet/forecast/fjrecast.py
"""
Утилиты подготовки и проверки прогнозов для модулей публикации.

Содержит чистые функции форматирования и валидации прогнозов, вынесенные
из громоздких классов публикации для упрощения читаемости и переиспользования.
"""

from datetime import datetime
from typing import Dict
import pandas as pd


class ForecastFormatter:
    """Класс для форматирования и валидации прогнозов."""
    
    def format_forecast_type(self, forecast_type: str) -> str:
        """Форматирует тип прогноза для отображения."""
        return format_forecast_type(forecast_type)
    
    def format_outcome(self, outcome: str, forecast_type: str) -> str:
        """Форматирует исход для красивого отображения."""
        return format_outcome(outcome, forecast_type)
    
    def get_match_result(self, home_goals: int, away_goals: int) -> str:
        """Определяет результат матча на основе счета."""
        return get_match_result(home_goals, away_goals)
    
    def is_forecast_correct(self, forecast_data: dict, match: pd.Series) -> bool:
        """Определяет, был ли прогноз правильным."""
        return is_forecast_correct(forecast_data, match)


def format_forecast_type(forecast_type: str) -> str:
    """Форматирует тип прогноза для отображения."""
    type_mapping = {
        'win_draw_loss': 'WIN_DRAW_LOSS',
        'oz': 'OZ',
        'goal_home': 'GOAL_HOME',
        'goal_away': 'GOAL_AWAY',
        'total': 'TOTAL',
        'total_home': 'TOTAL_HOME',
        'total_away': 'TOTAL_AWAY',
        'total_amount': 'TOTAL_AMOUNT',
        'total_home_amount': 'TOTAL_HOME_AMOUNT',
        'total_away_amount': 'TOTAL_AWAY_AMOUNT'
    }
    return type_mapping.get(forecast_type, forecast_type.upper())


def format_outcome(outcome: str, forecast_type: str) -> str:
    """Форматирует исход для красивого отображения."""
    outcome_mapping = {
        'п1': 'П1 (Победа хозяев)',
        'х': 'Х (Ничья)',
        'п2': 'П2 (Победа гостей)',
        'да': 'ДА',
        'нет': 'НЕТ',
        'больше': 'БОЛЬШЕ',
        'меньше': 'МЕНЬШЕ'
    }
    return outcome_mapping.get(str(outcome).lower(), str(outcome))


def get_match_result(home_goals: int, away_goals: int) -> str:
    if home_goals is None or away_goals is None:
        return 'Неизвестно'
    if home_goals > away_goals:
        return 'П1'
    if home_goals < away_goals:
        return 'П2'
    return 'Н'


def check_match_outcome_correct_from_targets(predicted_outcome: str, match: pd.Series) -> bool:
    home_goals = match.get('numOfHeadsHome', None)
    away_goals = match.get('numOfHeadsAway', None)
    if pd.isna(home_goals) or pd.isna(away_goals):
        return False
    if home_goals > away_goals:
        actual_outcome = '1'
    elif home_goals < away_goals:
        actual_outcome = '2'
    else:
        actual_outcome = 'X'
    po = str(predicted_outcome).lower()
    if po == 'п1':
        return actual_outcome == '1'
    if po == 'п2':
        return actual_outcome == '2'
    if po == 'х':
        return actual_outcome == 'X'
    return False


def check_both_teams_score_correct_from_targets(predicted_outcome: str, match: pd.Series) -> bool:
    home_goals = match.get('numOfHeadsHome', None)
    away_goals = match.get('numOfHeadsAway', None)
    if pd.isna(home_goals) or pd.isna(away_goals):
        return False
    both_scored = home_goals > 0 and away_goals > 0
    po = str(predicted_outcome).lower()
    if 'да' in po or 'обезабьют' in po or 'обе забьют' in po:
        return both_scored
    if 'нет' in po or 'незабьют' in po or 'не забьют' in po:
        return not both_scored
    return False


def check_team_goals_correct_from_targets(predicted_outcome: str, forecast_type: str, match: pd.Series) -> bool:
    home_goals = match.get('numOfHeadsHome', None)
    away_goals = match.get('numOfHeadsAway', None)
    if pd.isna(home_goals) or pd.isna(away_goals):
        return False
    if forecast_type == 'goal_home':
        actual_scored = home_goals > 0
    elif forecast_type == 'goal_away':
        actual_scored = away_goals > 0
    else:
        return False
    po = str(predicted_outcome).lower()
    if 'да' in po or 'забьет' in po:
        return actual_scored
    if 'нет' in po or 'не забьет' in po:
        return not actual_scored
    return False


# Старые функции check_total_correct_from_targets и check_amount_correct_from_targets удалены.
# Теперь используется единая логика через core/prediction_validator.py и таблицу targets.

def build_nn_value_suffix(forecast_type: str, forecast_data: Dict) -> str:
    if forecast_type in ['total_amount', 'total_home_amount', 'total_away_amount']:
        try:
            nn_value = forecast_data.get('forecast', None)
            if nn_value is not None:
                return f" | NN={float(nn_value):.2f}"
        except Exception:
            return ""
    return ""


def is_forecast_correct(forecast_data: dict, match: pd.Series) -> bool:
    """
    Определяет, был ли прогноз правильным используя target из БД.
    
    Args:
        forecast_data: Словарь с данными прогноза (forecast_type, outcome)
        match: Series с данными матча (id, numOfHeadsHome, numOfHeadsAway)
        
    Returns:
        bool: True если прогноз правильный, False иначе
    """
    from core.prediction_validator import is_prediction_correct_from_target
    from db.queries.target import get_target_by_match_id
    
    forecast_type = forecast_data.get('forecast_type', '')
    outcome = forecast_data.get('outcome', '')
    match_id = match.get('id', match.get('match_id', 0))
    
    if not forecast_type or not outcome or not match_id:
        return False
    
    # Маппинг forecast_type -> feature
    forecast_type_to_feature = {
        'win_draw_loss': 1,
        'oz': 2,
        'goal_home': 3,
        'goal_away': 4,
        'total': 5,
        'total_home': 6,
        'total_away': 7,
        'total_amount': 8,
        'total_home_amount': 9,
        'total_away_amount': 10
    }
    
    feature = forecast_type_to_feature.get(forecast_type)
    if not feature:
        return False
    
    # Получаем target и проверяем правильность
    target = get_target_by_match_id(match_id)
    if not target:
        return False
    
    return is_prediction_correct_from_target(feature, outcome, target)


