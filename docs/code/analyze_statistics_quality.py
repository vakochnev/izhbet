#!/usr/bin/env python3
"""
Анализ качества прогнозов в таблице statistics_optimized
"""

import logging
from sqlalchemy import func, text, case
from config import Session_pool
from db.models.statistics import Statistic
from db.models.outcome import Outcome
from db.models.prediction import Prediction
from db.models.match import Match
from db.models.championship import ChampionShip
from db.models.sport import Sport

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_prediction_quality():
    """Анализирует качество прогнозов в statistics_optimized"""
    
    print("🔍 АНАЛИЗ КАЧЕСТВА ПРОГНОЗОВ")
    print("=" * 60)
    
    with Session_pool() as db_session:
        # 1. Общая статистика
        print("\n📊 ОБЩАЯ СТАТИСТИКА")
        print("-" * 40)
        
        total_records = db_session.query(Statistic).count()
        print(f"Всего записей в statistics_optimized: {total_records:,}")
        
        # Записи с outcomes
        with_outcomes = db_session.query(Statistic).filter(
            Statistic.outcome_id.isnot(None)
        ).count()
        print(f"Записей с outcomes: {with_outcomes:,} ({with_outcomes/total_records*100:.1f}%)")
        
        # Записи с predictions
        with_predictions = db_session.query(StatisticsOptimized).filter(
            StatisticsOptimized.prediction_id.isnot(None)
        ).count()
        print(f"Записей с predictions: {with_predictions:,} ({with_predictions/total_records*100:.1f}%)")
        
        # 2. Анализ точности прогнозов
        print("\n🎯 АНАЛИЗ ТОЧНОСТИ ПРОГНОЗОВ")
        print("-" * 40)
        
        # Общая точность
        correct_predictions = db_session.query(StatisticsOptimized).filter(
            StatisticsOptimized.prediction_correct == True
        ).count()
        
        total_with_correctness = db_session.query(StatisticsOptimized).filter(
            StatisticsOptimized.prediction_correct.isnot(None)
        ).count()
        
        if total_with_correctness > 0:
            accuracy = correct_predictions / total_with_correctness * 100
            print(f"Общая точность: {accuracy:.2f}% ({correct_predictions:,}/{total_with_correctness:,})")
        else:
            print("Нет данных о правильности прогнозов")
        
        # 3. Анализ по типам прогнозов
        print("\n📈 АНАЛИЗ ПО ТИПАМ ПРОГНОЗОВ")
        print("-" * 40)
        
        type_stats = db_session.query(
            StatisticsOptimized.forecast_type,
            func.count(StatisticsOptimized.id).label('total'),
            func.sum(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('correct'),
            func.avg(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('accuracy')
        ).group_by(StatisticsOptimized.forecast_type).all()
        
        for stat in type_stats:
            accuracy_pct = (stat.accuracy * 100) if stat.accuracy else 0
            print(f"{stat.forecast_type:20} | {stat.total:6,} прогнозов | {stat.correct:6,} правильных | {accuracy_pct:6.2f}%")
        
        # 4. Анализ по моделям
        print("\n🤖 АНАЛИЗ ПО МОДЕЛЯМ")
        print("-" * 40)
        
        model_stats = db_session.query(
            StatisticsOptimized.model_name,
            func.count(StatisticsOptimized.id).label('total'),
            func.sum(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('correct'),
            func.avg(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('accuracy')
        ).group_by(StatisticsOptimized.model_name).all()
        
        for stat in model_stats:
            accuracy_pct = (stat.accuracy * 100) if stat.accuracy else 0
            print(f"{stat.model_name:20} | {stat.total:6,} прогнозов | {stat.correct:6,} правильных | {accuracy_pct:6.2f}%")
        
        # 5. Анализ по чемпионатам
        print("\n🏆 АНАЛИЗ ПО ЧЕМПИОНАТАМ")
        print("-" * 40)
        
        champ_stats = db_session.query(
            ChampionShip.championshipName,
            func.count(StatisticsOptimized.id).label('total'),
            func.sum(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('correct'),
            func.avg(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('accuracy')
        ).join(StatisticsOptimized, StatisticsOptimized.championship_id == ChampionShip.id).group_by(ChampionShip.championshipName).all()
        
        for stat in champ_stats:
            accuracy_pct = (stat.accuracy * 100) if stat.accuracy else 0
            print(f"{stat.championshipName:30} | {stat.total:6,} прогнозов | {stat.correct:6,} правильных | {accuracy_pct:6.2f}%")
        
        # 6. Временной анализ
        print("\n📅 ВРЕМЕННОЙ АНАЛИЗ")
        print("-" * 40)
        
        # Последние 30 дней
        recent_stats = db_session.query(
            func.count(StatisticsOptimized.id).label('total'),
            func.sum(case((StatisticsOptimized.prediction_correct == True, 1), else_=0)).label('correct')
        ).filter(
            StatisticsOptimized.created_at >= text("CURDATE() - INTERVAL 30 DAY")
        ).first()
        
        if recent_stats and recent_stats.total > 0:
            recent_accuracy = recent_stats.correct / recent_stats.total * 100
            print(f"Последние 30 дней: {recent_stats.total:,} прогнозов, {recent_stats.correct:,} правильных ({recent_accuracy:.2f}%)")
        
        # 7. Анализ коэффициентов
        print("\n💰 АНАЛИЗ КОЭФФИЦИЕНТОВ")
        print("-" * 40)
        
        coeff_stats = db_session.query(
            func.count(StatisticsOptimized.id).label('total'),
            func.avg(StatisticsOptimized.coefficient).label('avg_coeff'),
            func.min(StatisticsOptimized.coefficient).label('min_coeff'),
            func.max(StatisticsOptimized.coefficient).label('max_coeff')
        ).filter(StatisticsOptimized.coefficient.isnot(None)).first()
        
        if coeff_stats and coeff_stats.total > 0:
            print(f"Прогнозов с коэффициентами: {coeff_stats.total:,}")
            print(f"Средний коэффициент: {coeff_stats.avg_coeff:.2f}")
            print(f"Минимальный коэффициент: {coeff_stats.min_coeff:.2f}")
            print(f"Максимальный коэффициент: {coeff_stats.max_coeff:.2f}")
        else:
            print("Нет данных о коэффициентах")
        
        # 8. Анализ прибыльности
        print("\n💵 АНАЛИЗ ПРИБЫЛЬНОСТИ")
        print("-" * 40)
        
        profit_stats = db_session.query(
            func.count(StatisticsOptimized.id).label('total'),
            func.sum(StatisticsOptimized.potential_profit).label('total_potential'),
            func.sum(StatisticsOptimized.actual_profit).label('total_actual'),
            func.avg(StatisticsOptimized.potential_profit).label('avg_potential'),
            func.avg(StatisticsOptimized.actual_profit).label('avg_actual')
        ).filter(StatisticsOptimized.potential_profit.isnot(None)).first()
        
        if profit_stats and profit_stats.total > 0:
            print(f"Прогнозов с данными о прибыли: {profit_stats.total:,}")
            print(f"Общая потенциальная прибыль: {profit_stats.total_potential:.2f}")
            print(f"Общая фактическая прибыль: {profit_stats.total_actual:.2f}")
            print(f"Средняя потенциальная прибыль: {profit_stats.avg_potential:.2f}")
            print(f"Средняя фактическая прибыль: {profit_stats.avg_actual:.2f}")
        else:
            print("Нет данных о прибыльности")
        
        # 9. Проблемные области
        print("\n⚠️  ПРОБЛЕМНЫЕ ОБЛАСТИ")
        print("-" * 40)
        
        # Типы с низкой точностью
        low_accuracy_types = [stat for stat in type_stats if stat.accuracy and stat.accuracy < 0.3]
        if low_accuracy_types:
            print("Типы прогнозов с низкой точностью (<30%):")
            for stat in low_accuracy_types:
                accuracy_pct = stat.accuracy * 100
                print(f"  - {stat.forecast_type}: {accuracy_pct:.1f}% ({stat.correct}/{stat.total})")
        
        # Модели с низкой точностью
        low_accuracy_models = [stat for stat in model_stats if stat.accuracy and stat.accuracy < 0.3]
        if low_accuracy_models:
            print("\nМодели с низкой точностью (<30%):")
            for stat in low_accuracy_models:
                accuracy_pct = stat.accuracy * 100
                print(f"  - {stat.model_name}: {accuracy_pct:.1f}% ({stat.correct}/{stat.total})")
        
        # 10. Рекомендации
        print("\n💡 РЕКОМЕНДАЦИИ")
        print("-" * 40)
        
        if total_with_correctness > 0:
            if accuracy < 50:
                print("🔴 КРИТИЧЕСКАЯ ПРОБЛЕМА: Общая точность ниже 50%")
                print("   - Требуется пересмотр алгоритмов прогнозирования")
                print("   - Проверьте качество входных данных")
                print("   - Рассмотрите возможность переобучения моделей")
            elif accuracy < 70:
                print("🟡 ВНИМАНИЕ: Точность ниже 70%")
                print("   - Рекомендуется улучшение моделей")
                print("   - Проанализируйте проблемные типы прогнозов")
            else:
                print("🟢 ХОРОШО: Точность выше 70%")
                print("   - Система работает удовлетворительно")
                print("   - Продолжайте мониторинг качества")
        
        # Рекомендации по типам
        if low_accuracy_types:
            print(f"\n📊 Рекомендации по типам прогнозов:")
            for stat in low_accuracy_types:
                print(f"   - {stat.forecast_type}: требует пересмотра алгоритма")
        
        print("\n" + "=" * 60)
        print("АНАЛИЗ ЗАВЕРШЕН")

if __name__ == "__main__":
    analyze_prediction_quality()
