#!/usr/bin/env python3
"""
Скрипт для анализа статистики прогнозов.
Показывает успешность прогнозов по лигам и типам прогнозов.
"""

import sys
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy import create_engine, text
from config import settings
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def analyze_by_league_and_type(
    sport: Optional[str] = None,
    championship: Optional[str] = None,
    min_predictions: int = 10
) -> pd.DataFrame:
    """
    Анализирует статистику прогнозов по лигам и типам прогнозов.
    
    Args:
        sport: Фильтр по виду спорта (None = все)
        championship: Фильтр по чемпионату (None = все)
        min_predictions: Минимальное количество прогнозов для включения в отчет
        
    Returns:
        DataFrame с результатами
    """
    engine = create_engine(settings.DATABASE_URL_mysql)
    
    where_clauses = ["st.prediction_correct IS NOT NULL"]
    if sport:
        where_clauses.append(f"s.sportName = '{sport}'")
    if championship:
        where_clauses.append(f"ch.championshipName = '{championship}'")
    
    where_sql = " AND ".join(where_clauses)
    
    query = f'''
    SELECT 
        s.sportName as sport,
        c.countryName as country,
        ch.championshipName as championship,
        st.forecast_type,
        COUNT(*) as total_predictions,
        SUM(CASE WHEN st.prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
        SUM(CASE WHEN st.prediction_correct = 0 THEN 1 ELSE 0 END) as incorrect_predictions,
        ROUND(SUM(CASE WHEN st.prediction_correct = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percent,
        MIN(st.created_at) as first_prediction,
        MAX(st.created_at) as last_prediction
    FROM statistics st
    JOIN sports s ON st.sport_id = s.id
    JOIN championships ch ON st.championship_id = ch.id
    JOIN countrys c ON ch.country_id = c.id
    WHERE {where_sql}
    GROUP BY s.sportName, c.countryName, ch.championshipName, st.forecast_type
    HAVING total_predictions >= {min_predictions}
    ORDER BY s.sportName, c.countryName, ch.championshipName, st.forecast_type
    '''
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    return df


def analyze_by_league_summary(min_predictions: int = 10) -> pd.DataFrame:
    """
    Сводная статистика по лигам (все типы прогнозов вместе).
    
    Args:
        min_predictions: Минимальное количество прогнозов
        
    Returns:
        DataFrame с результатами
    """
    engine = create_engine(settings.DATABASE_URL_mysql)
    
    query = f'''
    SELECT 
        s.sportName as sport,
        c.countryName as country,
        ch.championshipName as championship,
        COUNT(*) as total_predictions,
        SUM(CASE WHEN st.prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
        SUM(CASE WHEN st.prediction_correct = 0 THEN 1 ELSE 0 END) as incorrect_predictions,
        ROUND(SUM(CASE WHEN st.prediction_correct = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percent,
        COUNT(DISTINCT st.forecast_type) as forecast_types_count,
        MIN(st.created_at) as first_prediction,
        MAX(st.created_at) as last_prediction
    FROM statistics st
    JOIN sports s ON st.sport_id = s.id
    JOIN championships ch ON st.championship_id = ch.id
    JOIN countrys c ON ch.country_id = c.id
    WHERE st.prediction_correct IS NOT NULL
    GROUP BY s.sportName, c.countryName, ch.championshipName
    HAVING total_predictions >= {min_predictions}
    ORDER BY accuracy_percent DESC, total_predictions DESC
    '''
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    return df


def analyze_by_forecast_type(min_predictions: int = 10) -> pd.DataFrame:
    """
    Сводная статистика по типам прогнозов (все лиги вместе).
    
    Args:
        min_predictions: Минимальное количество прогнозов
        
    Returns:
        DataFrame с результатами
    """
    engine = create_engine(settings.DATABASE_URL_mysql)
    
    query = f'''
    SELECT 
        st.forecast_type,
        COUNT(*) as total_predictions,
        SUM(CASE WHEN st.prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
        SUM(CASE WHEN st.prediction_correct = 0 THEN 1 ELSE 0 END) as incorrect_predictions,
        ROUND(SUM(CASE WHEN st.prediction_correct = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percent,
        COUNT(DISTINCT st.championship_id) as leagues_count
    FROM statistics st
    WHERE st.prediction_correct IS NOT NULL
    GROUP BY st.forecast_type
    HAVING total_predictions >= {min_predictions}
    ORDER BY accuracy_percent DESC, total_predictions DESC
    '''
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    return df


def print_detailed_report(df: pd.DataFrame, title: str):
    """Выводит детальный отчет."""
    print("\n" + "=" * 120)
    print(f"{title}")
    print("=" * 120)
    
    if df.empty:
        print("Нет данных для отображения")
        return
    
    for _, row in df.iterrows():
        accuracy = row['accuracy_percent']
        
        # Определяем эмодзи по точности
        if accuracy >= 70:
            emoji = "🟢"
        elif accuracy >= 50:
            emoji = "🟡"
        else:
            emoji = "🔴"
        
        print(f"\n{emoji} {row['sport']} | {row['country']} | {row['championship']}")
        print(f"   Тип прогноза: {row['forecast_type']}")
        print(f"   Всего прогнозов: {row['total_predictions']}")
        print(f"   ✅ Правильных: {row['correct_predictions']} ({accuracy:.2f}%)")
        print(f"   ❌ Неправильных: {row['incorrect_predictions']}")
        print(f"   📅 Период: {row['first_prediction']} - {row['last_prediction']}")


def print_summary_report(df: pd.DataFrame, title: str):
    """Выводит сводный отчет."""
    print("\n" + "=" * 120)
    print(f"{title}")
    print("=" * 120)
    
    if df.empty:
        print("Нет данных для отображения")
        return
    
    print(f"\n{'Лига':<60} | {'Прогнозов':<10} | {'Правильных':<12} | {'Точность':<10}")
    print("-" * 120)
    
    for _, row in df.iterrows():
        accuracy = row['accuracy_percent']
        
        # Определяем эмодзи по точности
        if accuracy >= 70:
            emoji = "🟢"
        elif accuracy >= 50:
            emoji = "🟡"
        else:
            emoji = "🔴"
        
        league_name = f"{row['sport']} - {row['country']} - {row['championship']}"
        if len(league_name) > 57:
            league_name = league_name[:54] + "..."
        
        print(f"{emoji} {league_name:<57} | {row['total_predictions']:<10} | {row['correct_predictions']:<12} | {accuracy:.2f}%")


def print_forecast_type_report(df: pd.DataFrame):
    """Выводит отчет по типам прогнозов."""
    print("\n" + "=" * 100)
    print("СТАТИСТИКА ПО ТИПАМ ПРОГНОЗОВ")
    print("=" * 100)
    
    if df.empty:
        print("Нет данных для отображения")
        return
    
    print(f"\n{'Тип прогноза':<20} | {'Прогнозов':<10} | {'Правильных':<12} | {'Точность':<10} | {'Лиг':<6}")
    print("-" * 100)
    
    for _, row in df.iterrows():
        accuracy = row['accuracy_percent']
        
        # Определяем эмодзи по точности
        if accuracy >= 70:
            emoji = "🟢"
        elif accuracy >= 50:
            emoji = "🟡"
        else:
            emoji = "🔴"
        
        print(f"{emoji} {row['forecast_type']:<18} | {row['total_predictions']:<10} | {row['correct_predictions']:<12} | {accuracy:.2f}%      | {row['leagues_count']:<6}")


def save_to_csv(df: pd.DataFrame, filename: str, sort_by_accuracy: bool = True):
    """
    Сохраняет результаты в CSV.
    
    Args:
        df: DataFrame для сохранения
        filename: Имя файла
        sort_by_accuracy: Сортировать по убыванию точности (по умолчанию True)
    """
    if sort_by_accuracy and 'accuracy_percent' in df.columns:
        df = df.sort_values('accuracy_percent', ascending=False)
    
    df.to_csv(filename, index=False, encoding='utf-8')
    logger.info(f"Результаты сохранены в файл: {filename}")


def main():
    """Основная функция."""
    print("\n" + "=" * 120)
    print("АНАЛИЗ СТАТИСТИКИ ПРОГНОЗОВ")
    print(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 120)
    
    # Параметры
    min_predictions = 10
    
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help', 'help']:
            print("""
Использование: python analyze_predictions.py [MIN_PREDICTIONS]

Параметры:
  MIN_PREDICTIONS  Минимальное количество прогнозов для включения в отчет (по умолчанию: 10)

Примеры:
  python analyze_predictions.py           # Минимум 10 прогнозов
  python analyze_predictions.py 50        # Минимум 50 прогнозов
  python analyze_predictions.py 100       # Минимум 100 прогнозов

Отчеты:
  1. Сводная статистика по лигам
  2. Детальная статистика по лигам и типам прогнозов
  3. Статистика по типам прогнозов
            """)
            return
        
        try:
            min_predictions = int(sys.argv[1])
            logger.info(f"Минимальное количество прогнозов: {min_predictions}")
        except ValueError:
            logger.error(f"Неверный параметр: {sys.argv[1]}. Используется значение по умолчанию: {min_predictions}")
    
    # 1. Сводная статистика по лигам
    logger.info("Получение сводной статистики по лигам...")
    df_summary = analyze_by_league_summary(min_predictions)
    print_summary_report(df_summary, "СВОДНАЯ СТАТИСТИКА ПО ЛИГАМ")
    save_to_csv(df_summary, f'results/prediction_stats_by_league_{datetime.now().strftime("%Y%m%d")}.csv')
    
    # 2. Статистика по типам прогнозов
    logger.info("Получение статистики по типам прогнозов...")
    df_types = analyze_by_forecast_type(min_predictions)
    print_forecast_type_report(df_types)
    save_to_csv(df_types, f'results/prediction_stats_by_type_{datetime.now().strftime("%Y%m%d")}.csv')
    
    # 3. Детальная статистика
    logger.info("Получение детальной статистики...")
    df_detailed = analyze_by_league_and_type(min_predictions=min_predictions)
    # Сортируем детальный отчет по убыванию точности
    df_detailed_sorted = df_detailed.sort_values('accuracy_percent', ascending=False)
    print_detailed_report(df_detailed_sorted, "ДЕТАЛЬНАЯ СТАТИСТИКА ПО ЛИГАМ И ТИПАМ ПРОГНОЗОВ (отсортировано по точности)")
    save_to_csv(df_detailed, f'results/prediction_stats_detailed_{datetime.now().strftime("%Y%m%d")}.csv')
    
    # Общая статистика
    print("\n" + "=" * 120)
    print("ОБЩАЯ СТАТИСТИКА")
    print("=" * 120)
    print(f"Всего лиг проанализировано: {len(df_summary)}")
    print(f"Всего прогнозов: {df_summary['total_predictions'].sum()}")
    print(f"Правильных прогнозов: {df_summary['correct_predictions'].sum()}")
    print(f"Общая точность: {(df_summary['correct_predictions'].sum() / df_summary['total_predictions'].sum() * 100):.2f}%")
    print("=" * 120)
    
    logger.info("Анализ завершен успешно")


if __name__ == '__main__':
    main()

