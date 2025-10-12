#!/usr/bin/env python3
"""
Тестовый скрипт для запуска conformal_predictor и проверки интеграции с statistics_optimized.
"""

import logging
import sys
from datetime import datetime

from processing.conformal_predictor import ConformalPredictor
from config import Session_pool
from sqlalchemy import text

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_conformal_predictor():
    """Тестирует conformal_predictor и проверяет интеграцию с statistics_optimized."""
    print("🧪 ТЕСТИРОВАНИЕ CONFORMAL_PREDICTOR")
    print("=" * 50)
    
    # Проверяем текущее состояние
    with Session_pool() as session:
        result = session.execute(text('SELECT COUNT(*) FROM statistics_optimized')).fetchone()
        current_statistics = result[0] if result else 0
        
        result = session.execute(text('SELECT COUNT(*) FROM outcomes')).fetchone()
        current_outcomes = result[0] if result else 0
        
        print(f"📊 Состояние до тестирования:")
        print(f"   statistics_optimized: {current_statistics} записей")
        print(f"   outcomes: {current_outcomes} записей")
    
    try:
        # Создаем экземпляр ConformalPredictor
        print("\\n🔄 Создание ConformalPredictor...")
        predictor = ConformalPredictor()
        print("✅ ConformalPredictor создан успешно")
        
        # Получаем список чемпионатов
        print("\\n📋 Получение списка чемпионатов...")
        tournament_ids = predictor.get_tournament_ids()
        print(f"   Найдено {len(tournament_ids)} чемпионатов")
        
        if not tournament_ids:
            print("❌ Нет чемпионатов для обработки")
            return
        
        # Берем только первые 3 чемпионата для тестирования
        test_tournament_ids = tournament_ids[:3]
        print(f"   Тестируем на {len(test_tournament_ids)} чемпионатах: {test_tournament_ids}")
        
        # Запускаем обработку
        print("\\n🚀 Запуск обработки...")
        result = predictor.create_conformal_predictions(test_tournament_ids)
        
        if result['success']:
            print(f"✅ Обработка завершена успешно:")
            print(f"   Всего чемпионатов: {result['total_tournaments']}")
            print(f"   Успешно обработано: {result['successful']}")
            print(f"   Ошибок: {result['failed']}")
            
            # Проверяем результаты
            print("\\n📊 Результаты обработки:")
            for i, res in enumerate(result['results']):
                print(f"   Чемпионат {test_tournament_ids[i]}: {res}")
        else:
            print(f"❌ Ошибка обработки: {result.get('error', 'Неизвестная ошибка')}")
            return
        
        # Проверяем изменения в базе данных
        print("\\n🔍 Проверка изменений в базе данных...")
        with Session_pool() as session:
            result = session.execute(text('SELECT COUNT(*) FROM statistics_optimized')).fetchone()
            new_statistics = result[0] if result else 0
            
            result = session.execute(text('SELECT COUNT(*) FROM outcomes')).fetchone()
            new_outcomes = result[0] if result else 0
            
            print(f"📊 Состояние после тестирования:")
            print(f"   statistics_optimized: {new_statistics} записей (+{new_statistics - current_statistics})")
            print(f"   outcomes: {new_outcomes} записей (+{new_outcomes - current_outcomes})")
            
            # Проверяем последние записи в statistics_optimized
            if new_statistics > current_statistics:
                result = session.execute(text('''
                    SELECT id, outcome_id, match_id, forecast_type, forecast_subtype, 
                           model_name, actual_result, created_at
                    FROM statistics_optimized 
                    ORDER BY id DESC 
                    LIMIT 5
                ''')).fetchall()
                
                print(f"\\n📋 Последние записи в statistics_optimized:")
                for row in result:
                    print(f"   ID: {row[0]}, Outcome: {row[1]}, Match: {row[2]}")
                    print(f"   Тип: {row[3]}, Подтип: {row[4]}, Модель: {row[5]}")
                    print(f"   Результат: {row[6]}, Создано: {row[7]}")
                    print("   ---")
            else:
                print("⚠️ Новые записи в statistics_optimized не найдены")
        
        print("\\n✅ Тестирование завершено!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_conformal_predictor()
