#!/usr/bin/env python3
"""
Тестовый скрипт для миграции данных в statistics_optimized.
"""

import logging
import sys
from datetime import datetime
from typing import Dict, Any, List, Tuple

from config import Session_pool
from db.models.outcome import Outcome
from db.models.prediction import Prediction
from db.models.match import Match
from db.models.championship import ChampionShip
from db.models.sport import Sport
from sqlalchemy import text

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_migration():
    """Тестирует миграцию на небольшом наборе данных."""
    print("🧪 ТЕСТИРОВАНИЕ МИГРАЦИИ")
    print("=" * 40)
    
    # Маппинг типов прогнозов
    feature_mapping = {
        1: ('win_draw_loss', 'classification'),
        2: ('oz', 'classification'),
        3: ('goal_home', 'classification'),
        4: ('goal_away', 'classification'),
        5: ('total', 'classification'),
        6: ('total_home', 'classification'),
        7: ('total_away', 'classification'),
        8: ('total_amount', 'regression'),
        9: ('total_home_amount', 'regression'),
        10: ('total_away_amount', 'regression')
    }
    
    with Session_pool() as db_session:
        # 1. Проверяем текущее состояние
        print("\\n📊 Проверяем текущее состояние...")
        
        total_outcomes = db_session.query(Outcome).count()
        total_predictions = db_session.query(Prediction).count()
        
        result = db_session.execute(text("SELECT COUNT(*) FROM statistics_optimized")).fetchone()
        total_statistics = result[0] if result else 0
        
        print(f"   Outcomes: {total_outcomes}")
        print(f"   Predictions: {total_predictions}")
        print(f"   Statistics: {total_statistics}")
        
        # 2. Тестируем миграцию outcomes
        print("\\n🔄 Тестируем миграцию outcomes...")
        
        # Получаем первые 5 outcomes с связанными данными
        test_outcomes = (
            db_session.query(Outcome)
            .join(Match, Outcome.match_id == Match.id)
            .join(ChampionShip, Match.tournament_id == ChampionShip.id)
            .join(Sport, ChampionShip.sport_id == Sport.id)
            .limit(5)
            .all()
        )
        
        print(f"   Найдено {len(test_outcomes)} outcomes для тестирования")
        
        migrated_count = 0
        for outcome in test_outcomes:
            try:
                # Проверяем, не мигрирован ли уже
                existing = db_session.execute(
                    text("SELECT id FROM statistics_optimized WHERE outcome_id = :outcome_id"),
                    {"outcome_id": outcome.id}
                ).fetchone()
                
                if existing:
                    print(f"   ⏭️ Outcome {outcome.id} уже мигрирован")
                    continue
                
                # Получаем связанные данные
                match = db_session.query(Match).filter(Match.id == outcome.match_id).first()
                championship = db_session.query(ChampionShip).filter(ChampionShip.id == match.tournament_id).first()
                sport = db_session.query(Sport).filter(Sport.id == championship.sport_id).first()
                
                # Определяем тип прогноза
                forecast_type, model_type = feature_mapping.get(outcome.feature, ('unknown', 'unknown'))
                forecast_subtype = outcome.outcome or 'unknown'
                
                # Вычисляем результат матча
                actual_result, actual_value = calculate_actual_result(match)
                
                # Создаем запись через SQL
                insert_sql = text("""
                    INSERT INTO statistics_optimized (
                        outcome_id, prediction_id, match_id, championship_id, sport_id,
                        match_date, match_round, match_stage, forecast_type, forecast_subtype,
                        model_name, model_version, model_type, actual_result, actual_value,
                        prediction_correct, prediction_accuracy, prediction_error, prediction_residual,
                        coefficient, potential_profit, actual_profit
                    ) VALUES (
                        :outcome_id, :prediction_id, :match_id, :championship_id, :sport_id,
                        :match_date, :match_round, :match_stage, :forecast_type, :forecast_subtype,
                        :model_name, :model_version, :model_type, :actual_result, :actual_value,
                        :prediction_correct, :prediction_accuracy, :prediction_error, :prediction_residual,
                        :coefficient, :potential_profit, :actual_profit
                    )
                """)
                
                db_session.execute(insert_sql, {
                    'outcome_id': outcome.id,
                    'prediction_id': None,
                    'match_id': outcome.match_id,
                    'championship_id': match.tournament_id,
                    'sport_id': championship.sport_id,
                    'match_date': match.gameData.date() if match.gameData else datetime.now().date(),
                    'match_round': getattr(match, 'tour', None),
                    'match_stage': getattr(match, 'stage', None),
                    'forecast_type': forecast_type,
                    'forecast_subtype': forecast_subtype,
                    'model_name': 'conformal_predictor',
                    'model_version': '1.0',
                    'model_type': model_type,
                    'actual_result': actual_result,
                    'actual_value': actual_value,
                    'prediction_correct': None,
                    'prediction_accuracy': None,
                    'prediction_error': None,
                    'prediction_residual': None,
                    'coefficient': None,
                    'potential_profit': None,
                    'actual_profit': None
                })
                
                migrated_count += 1
                print(f"   ✅ Outcome {outcome.id} -> Statistics")
                
            except Exception as e:
                print(f"   ❌ Outcome {outcome.id} - ошибка: {e}")
        
        # Коммитим изменения
        db_session.commit()
        print(f"\\n✅ Успешно мигрировано {migrated_count} записей")
        
        # 3. Проверяем результат
        print("\\n📊 Проверяем результат...")
        
        result = db_session.execute(text("SELECT COUNT(*) FROM statistics_optimized")).fetchone()
        new_total_statistics = result[0] if result else 0
        
        print(f"   Статистика до: {total_statistics}")
        print(f"   Статистика после: {new_total_statistics}")
        print(f"   Прирост: {new_total_statistics - total_statistics}")
        
        # 4. Проверяем качество данных
        print("\\n🔍 Проверяем качество данных...")
        
        # Проверяем связи
        result = db_session.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(outcome_id) as with_outcomes,
                COUNT(prediction_id) as with_predictions
            FROM statistics_optimized
        """)).fetchone()
        
        print(f"   Всего записей: {result[0]}")
        print(f"   С outcomes: {result[1]}")
        print(f"   С predictions: {result[2]}")
        
        # Проверяем типы прогнозов
        result = db_session.execute(text("""
            SELECT forecast_type, COUNT(*) as count
            FROM statistics_optimized
            GROUP BY forecast_type
            ORDER BY count DESC
        """)).fetchall()
        
        print("\\n   Типы прогнозов:")
        for row in result:
            print(f"     {row[0]}: {row[1]}")
        
        print("\\n🎉 Тестирование завершено успешно!")


def calculate_actual_result(match: Match) -> Tuple[str, float]:
    """Вычисляет фактический результат матча."""
    if not match.numOfHeadsHome or not match.numOfHeadsAway:
        return None, None
    
    goal_home = int(match.numOfHeadsHome)
    goal_away = int(match.numOfHeadsAway)
    
    # Определяем результат матча
    if goal_home > goal_away:
        actual_result = 'home_win'
    elif goal_home < goal_away:
        actual_result = 'away_win'
    else:
        actual_result = 'draw'
    
    # Для регрессии (total_amount) возвращаем сумму голов
    actual_value = float(goal_home + goal_away)
    
    return actual_result, actual_value


if __name__ == "__main__":
    try:
        test_migration()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        print(f"\\n❌ Критическая ошибка: {e}")
        sys.exit(1)
