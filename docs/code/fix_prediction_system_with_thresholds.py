#!/usr/bin/env python3
"""
Скрипт для исправления логики обновления статистики с учётом динамических порогов.
Обновляет prediction_correct, actual_result, actual_value в statistics_optimized.
"""

import logging
from sqlalchemy.orm import Session
from sqlalchemy import case, func
from config import Session_pool
from db.models.statistics_optimized import StatisticsOptimized
from db.models.match import Match
from core.constants import SIZE_TOTAL, SIZE_ITOTAL, SPR_SPORTS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_dynamic_thresholds(sport_id: int) -> tuple:
    """Возвращает пороги (total, itotal) для вида спорта."""
    try:
        sport_key = SPR_SPORTS.get(sport_id)
        if not sport_key:
            return 2.5, 1.5
        total = SIZE_TOTAL.get(sport_key, 2.5)
        itotal = SIZE_ITOTAL.get(sport_key, 1.5)
        return total, itotal
    except Exception:
        return 2.5, 1.5


def calculate_actual_result(match: Match, forecast_type: str) -> str:
    """Вычисляет фактический результат матча для заданного типа прогноза."""
    if not match.numOfHeadsHome or not match.numOfHeadsAway:
        return "неизвестно"
    
    home_goals = match.numOfHeadsHome
    away_goals = match.numOfHeadsAway
    total_goals = home_goals + away_goals
    
    if forecast_type == 'win_draw_loss':
        if home_goals > away_goals:
            return "п1"
        elif home_goals < away_goals:
            return "п2"
        else:
            return "н"
    
    elif forecast_type == 'oz':
        return "обе забьют - да" if home_goals > 0 and away_goals > 0 else "обе забьют - нет"
    
    elif forecast_type == 'goal_home':
        return "гол хозяев - да" if home_goals > 0 else "гол хозяев - нет"
    
    elif forecast_type == 'goal_away':
        return "гол гостей - да" if away_goals > 0 else "гол гостей - нет"
    
    elif forecast_type == 'total':
        # Используем динамический порог для total
        sport_key = SPR_SPORTS.get(match.sport_id)
        threshold = SIZE_TOTAL.get(sport_key, 2.5) if sport_key else 2.5
        return f"тотал {threshold} - больше" if total_goals > threshold else f"тотал {threshold} - меньше"
    
    elif forecast_type == 'total_home':
        # Используем динамический порог для total_home
        sport_key = SPR_SPORTS.get(match.sport_id)
        threshold = SIZE_ITOTAL.get(sport_key, 1.5) if sport_key else 1.5
        return f"тотал хозяев {threshold} - больше" if home_goals > threshold else f"тотал хозяев {threshold} - меньше"
    
    elif forecast_type == 'total_away':
        # Используем динамический порог для total_away
        sport_key = SPR_SPORTS.get(match.sport_id)
        threshold = SIZE_ITOTAL.get(sport_key, 1.5) if sport_key else 1.5
        return f"тотал гостей {threshold} - больше" if away_goals > threshold else f"тотал гостей {threshold} - меньше"
    
    elif forecast_type in ['total_amount', 'total_home_amount', 'total_away_amount']:
        # Для регрессионных типов возвращаем числовое значение
        if forecast_type == 'total_amount':
            return str(total_goals)
        elif forecast_type == 'total_home_amount':
            return str(home_goals)
        elif forecast_type == 'total_away_amount':
            return str(away_goals)
    
    return "неизвестно"


def calculate_actual_value(match: Match, forecast_type: str) -> float:
    """Вычисляет фактическое числовое значение для регрессионных прогнозов."""
    if not match.numOfHeadsHome or not match.numOfHeadsAway:
        return None
    
    home_goals = match.numOfHeadsHome
    away_goals = match.numOfHeadsAway
    total_goals = home_goals + away_goals
    
    if forecast_type == 'total_amount':
        return float(total_goals)
    elif forecast_type == 'total_home_amount':
        return float(home_goals)
    elif forecast_type == 'total_away_amount':
        return float(away_goals)
    
    return None


def is_prediction_correct(stat: StatisticsOptimized, match: Match, forecast_type: str, db_session: Session) -> bool:
    """Определяет, правильный ли прогноз, с учётом динамических порогов."""
    if not match.numOfHeadsHome or not match.numOfHeadsAway:
        return False
    
    home_goals = match.numOfHeadsHome
    away_goals = match.numOfHeadsAway
    total_goals = home_goals + away_goals
    
    # Для регрессионных типов сравниваем числовые значения
    if forecast_type in ['total_amount', 'total_home_amount', 'total_away_amount']:
        # Получаем прогноз из связанной таблицы outcomes
        from db.models.outcome import Outcome
        outcome = db_session.query(Outcome).filter(Outcome.id == stat.outcome_id).first()
        if not outcome or outcome.forecast is None:
            return False
        
        forecast_value = float(outcome.forecast)
        
        # Получаем динамические пороги
        total_thresh, itotal_thresh = get_dynamic_thresholds(match.sport_id)
        
        if forecast_type == 'total_amount':
            predicted_over = forecast_value >= total_thresh
            actual_over = total_goals > total_thresh
            return predicted_over == actual_over
        
        elif forecast_type == 'total_home_amount':
            predicted_over = forecast_value >= itotal_thresh
            actual_over = home_goals > itotal_thresh
            return predicted_over == actual_over
        
        elif forecast_type == 'total_away_amount':
            predicted_over = forecast_value >= itotal_thresh
            actual_over = away_goals > itotal_thresh
            return predicted_over == actual_over
    
    # Для классификационных типов сравниваем текстовые результаты
    else:
        actual_result = calculate_actual_result(match, forecast_type)
        return stat.actual_result == actual_result
    
    return False


def fix_prediction_system():
    """Исправляет логику обновления статистики с учётом динамических порогов."""
    print("\n🔧 ИСПРАВЛЕНИЕ СИСТЕМЫ ПРОГНОЗИРОВАНИЯ С ДИНАМИЧЕСКИМИ ПОРОГАМИ")
    print("=" * 70)
    
    with Session_pool() as db_session:
        # Получаем все записи статистики
        stats = db_session.query(StatisticsOptimized).all()
        total_records = len(stats)
        
        print(f"Обрабатываем {total_records:,} записей...")
        
        updated_count = 0
        correct_count = 0
        
        for i, stat in enumerate(stats):
            if i % 1000 == 0:
                print(f"Обработано {i:,}/{total_records:,} записей...")
            
            # Получаем матч
            match = db_session.query(Match).filter(Match.id == stat.match_id).first()
            if not match:
                continue
            
            # Обновляем actual_result и actual_value
            new_actual_result = calculate_actual_result(match, stat.forecast_type)
            new_actual_value = calculate_actual_value(match, stat.forecast_type)
            
            stat.actual_result = new_actual_result
            stat.actual_value = new_actual_value
            
            # Пересчитываем prediction_correct с учётом динамических порогов
            is_correct = is_prediction_correct(stat, match, stat.forecast_type, db_session)
            stat.prediction_correct = is_correct
            
            if is_correct:
                correct_count += 1
            
            updated_count += 1
        
        # Сохраняем изменения
        db_session.commit()
        
        print(f"\n✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО!")
        print(f"Обновлено записей: {updated_count:,}")
        print(f"Правильных прогнозов: {correct_count:,}")
        print(f"Общая точность: {correct_count / updated_count * 100:.2f}%" if updated_count > 0 else "Общая точность: 0.00%")


if __name__ == "__main__":
    fix_prediction_system()
