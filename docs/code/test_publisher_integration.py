#!/usr/bin/env python3
"""
Тестовый скрипт для проверки интеграции publisher.py с созданием прогнозов и воронкой.
"""

import logging
import sys
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_publisher_all_time():
    """Тестирует publisher.py ALL_TIME с созданием новых прогнозов."""
    print("🧪 ТЕСТИРОВАНИЕ PUBLISHER.PY ALL_TIME")
    print("=" * 50)
    
    try:
        # Импортируем необходимые модули
        from publisher.app import PublisherApp
        
        # Создаем приложение
        app = PublisherApp()
        
        # Запускаем режим ALL_TIME для 2024 года
        print("\\n🚀 Запуск publisher.py ALL_TIME 2024...")
        app.run_with_params('ALL_TIME', '2024')
        
        print("\\n✅ Тест publisher.py ALL_TIME завершен успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования publisher.py ALL_TIME: {e}")
        import traceback
        traceback.print_exc()

def test_publisher_funnel():
    """Тестирует publisher.py FUNNEL для анализа воронки."""
    print("\\n🧪 ТЕСТИРОВАНИЕ PUBLISHER.PY FUNNEL")
    print("=" * 50)
    
    try:
        # Импортируем необходимые модули
        from publisher.app import PublisherApp
        
        # Создаем приложение
        app = PublisherApp()
        
        # Запускаем режим FUNNEL для 2024 года
        print("\\n🚀 Запуск publisher.py FUNNEL 2024...")
        app.run_with_params('FUNNEL', '2024')
        
        print("\\n✅ Тест publisher.py FUNNEL завершен успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования publisher.py FUNNEL: {e}")
        import traceback
        traceback.print_exc()

def test_funnel_statistics():
    """Тестирует получение статистики воронки."""
    print("\\n🧪 ТЕСТИРОВАНИЕ СТАТИСТИКИ ВОРОНКИ")
    print("=" * 50)
    
    try:
        from publisher.service import PublisherService
        
        # Создаем сервис
        service = PublisherService()
        
        # Получаем статистику за последние 30 дней
        print("\\n📊 Получение статистики воронки...")
        stats = service.get_funnel_statistics()
        
        if stats:
            print("✅ Статистика получена успешно:")
            print(f"  Период: {stats.get('period', {})}")
            print(f"  Сводка: {stats.get('summary', {})}")
            print(f"  По типам: {len(stats.get('by_type', []))} типов")
            print(f"  По моделям: {len(stats.get('by_model', []))} моделей")
            print(f"  Ежедневно: {len(stats.get('daily', []))} дней")
        else:
            print("⚠️ Статистика не получена")
        
        print("\\n✅ Тест статистики воронки завершен!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования статистики воронки: {e}")
        import traceback
        traceback.print_exc()

def check_database_changes():
    """Проверяет изменения в базе данных."""
    print("\\n🔍 ПРОВЕРКА ИЗМЕНЕНИЙ В БАЗЕ ДАННЫХ")
    print("=" * 50)
    
    try:
        from config import Session_pool
        from sqlalchemy import text
        
        with Session_pool() as session:
            # Проверяем количество записей в statistics_optimized
            result = session.execute(text('SELECT COUNT(*) FROM statistics_optimized')).fetchone()
            total_statistics = result[0] if result else 0
            
            result = session.execute(text('SELECT COUNT(*) FROM outcomes')).fetchone()
            total_outcomes = result[0] if result else 0
            
            print(f"📊 Текущее состояние базы данных:")
            print(f"   statistics_optimized: {total_statistics} записей")
            print(f"   outcomes: {total_outcomes} записей")
            
            # Проверяем последние записи
            result = session.execute(text('''
                SELECT id, forecast_type, model_name, created_at
                FROM statistics_optimized 
                ORDER BY id DESC 
                LIMIT 5
            ''')).fetchall()
            
            print(f"\\n📋 Последние 5 записей в statistics_optimized:")
            for row in result:
                print(f"   ID: {row[0]}, Тип: {row[1]}, Модель: {row[2]}, Создано: {row[3]}")
        
        print("\\n✅ Проверка базы данных завершена!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки базы данных: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ PUBLISHER.PY")
    print("=" * 60)
    
    # Проверяем изменения в базе данных до тестов
    check_database_changes()
    
    # Тестируем статистику воронки
    test_funnel_statistics()
    
    # Тестируем publisher.py FUNNEL
    test_publisher_funnel()
    
    # Тестируем publisher.py ALL_TIME
    test_publisher_all_time()
    
    # Проверяем изменения в базе данных после тестов
    check_database_changes()
    
    print("\\n🎉 ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ!")
