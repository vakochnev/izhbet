#!/usr/bin/env python3
"""
Анализ текущих фичей и предложения по добавлению новых.
Анализирует эффективность существующих фичей и предлагает улучшения.
"""

import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from config import Session_pool
from db.models import Feature, Match, Outcome
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


class FeatureAnalysis:
    """Анализ текущих фичей и предложения по улучшению."""
    
    def __init__(self, output_dir: str = 'results/feature_analysis'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def analyze_current_features(self) -> Dict[str, Any]:
        """Анализирует текущие фичи."""
        logger.info('🔍 Анализ текущих фичей')
        
        with Session_pool() as db:
            # Получаем статистику по фичам
            feature_stats = db.execute(text("""
                SELECT 
                    prefix,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT match_id) as unique_matches,
                    AVG(CASE WHEN target_win_draw_loss IS NOT NULL THEN 1 ELSE 0 END) as wdl_coverage,
                    AVG(CASE WHEN target_oz IS NOT NULL THEN 1 ELSE 0 END) as oz_coverage,
                    AVG(CASE WHEN target_total_amount IS NOT NULL THEN 1 ELSE 0 END) as total_coverage
                FROM features 
                GROUP BY prefix
                ORDER BY prefix
            """)).fetchall()
            
            # Получаем информацию о колонках
            columns_info = db.execute(text("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'features' 
                AND TABLE_SCHEMA = DATABASE()
                ORDER BY ORDINAL_POSITION
            """)).fetchall()
            
            # Анализируем покрытие фичей
            feature_coverage = self._analyze_feature_coverage(db)
            
            # Анализируем корреляции
            correlations = self._analyze_correlations(db)
            
            results = {
                'feature_stats': [dict(row._mapping) for row in feature_stats],
                'columns_info': [dict(row._mapping) for row in columns_info],
                'feature_coverage': feature_coverage,
                'correlations': correlations,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            return results
    
    def _analyze_feature_coverage(self, db) -> Dict[str, Any]:
        """Анализирует покрытие фичей."""
        logger.info('📊 Анализ покрытия фичей')
        
        # Получаем статистику по типам фичей
        coverage_stats = db.execute(text("""
            SELECT 
                prefix,
                COUNT(*) as total_features,
                SUM(CASE WHEN COLUMN_NAME LIKE 'general_%' THEN 1 ELSE 0 END) as general_features,
                SUM(CASE WHEN COLUMN_NAME LIKE 'home_%' THEN 1 ELSE 0 END) as home_features,
                SUM(CASE WHEN COLUMN_NAME LIKE 'away_%' THEN 1 ELSE 0 END) as away_features,
                SUM(CASE WHEN COLUMN_NAME LIKE 'strong_%' THEN 1 ELSE 0 END) as strong_features,
                SUM(CASE WHEN COLUMN_NAME LIKE 'medium_%' THEN 1 ELSE 0 END) as medium_features,
                SUM(CASE WHEN COLUMN_NAME LIKE 'weak_%' THEN 1 ELSE 0 END) as weak_features
            FROM (
                SELECT DISTINCT prefix, COLUMN_NAME
                FROM features f
                JOIN INFORMATION_SCHEMA.COLUMNS c ON c.TABLE_NAME = 'features'
                WHERE c.COLUMN_NAME NOT IN ('id', 'match_id', 'prefix', 'created_at', 'updated_at')
                AND c.COLUMN_NAME NOT LIKE 'target_%'
            ) feature_types
            GROUP BY prefix
        """)).fetchall()
        
        return [dict(row._mapping) for row in coverage_stats]
    
    def _analyze_correlations(self, db) -> Dict[str, Any]:
        """Анализирует корреляции между фичами и таргетами."""
        logger.info('🔗 Анализ корреляций')
        
        # Получаем выборку данных для анализа корреляций
        sample_data = db.execute(text("""
            SELECT 
                f.match_id,
                f.prefix,
                f.target_win_draw_loss,
                f.target_oz,
                f.target_total_amount,
                m.numOfHeadsHome,
                m.numOfHeadsAway,
                m.sport_id,
                m.tournament_id
            FROM features f
            JOIN matchs m ON f.match_id = m.id
            WHERE f.prefix = 'home'
            AND f.target_win_draw_loss IS NOT NULL
            AND f.target_oz IS NOT NULL
            AND f.target_total_amount IS NOT NULL
            AND m.numOfHeadsHome IS NOT NULL
            AND m.numOfHeadsAway IS NOT NULL
            LIMIT 1000
        """)).fetchall()
        
        if not sample_data:
            return {'error': 'Недостаточно данных для анализа корреляций'}
        
        # Преобразуем в DataFrame
        df = pd.DataFrame([dict(row._mapping) for row in sample_data])
        
        # Вычисляем корреляции
        correlations = {}
        
        # Корреляция с результатами матчей
        df['total_goals'] = df['numOfHeadsHome'] + df['numOfHeadsAway']
        df['home_win'] = (df['numOfHeadsHome'] > df['numOfHeadsAway']).astype(int)
        df['draw'] = (df['numOfHeadsHome'] == df['numOfHeadsAway']).astype(int)
        df['away_win'] = (df['numOfHeadsHome'] < df['numOfHeadsAway']).astype(int)
        df['both_score'] = ((df['numOfHeadsHome'] > 0) & (df['numOfHeadsAway'] > 0)).astype(int)
        
        # Корреляции с таргетами
        target_columns = ['target_win_draw_loss', 'target_oz', 'target_total_amount']
        result_columns = ['home_win', 'draw', 'away_win', 'both_score', 'total_goals']
        
        for target in target_columns:
            if target in df.columns:
                correlations[target] = {}
                for result in result_columns:
                    if result in df.columns:
                        corr = df[target].corr(df[result])
                        correlations[target][result] = corr if not pd.isna(corr) else 0.0
        
        return correlations
    
    def suggest_new_features(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Предлагает новые фичи на основе анализа."""
        logger.info('💡 Генерация предложений по новым фичам')
        
        suggestions = []
        
        # Анализируем текущие результаты
        correlations = analysis_results.get('correlations', {})
        feature_coverage = analysis_results.get('feature_coverage', [])
        
        # 1. Фичи на основе формы команд
        suggestions.append({
            'category': 'Форма команд',
            'features': [
                {
                    'name': 'home_form_last_5',
                    'description': 'Форма домашней команды за последние 5 матчей (очки)',
                    'calculation': 'Сумма очков за последние 5 матчей',
                    'priority': 'HIGH',
                    'reason': 'Форма команды сильно влияет на результат'
                },
                {
                    'name': 'away_form_last_5',
                    'description': 'Форма гостевой команды за последние 5 матчей (очки)',
                    'calculation': 'Сумма очков за последние 5 матчей',
                    'priority': 'HIGH',
                    'reason': 'Форма команды сильно влияет на результат'
                },
                {
                    'name': 'form_difference',
                    'description': 'Разница в форме команд',
                    'calculation': 'home_form_last_5 - away_form_last_5',
                    'priority': 'MEDIUM',
                    'reason': 'Относительная форма команд'
                }
            ]
        })
        
        # 2. Фичи на основе календаря
        suggestions.append({
            'category': 'Календарные фичи',
            'features': [
                {
                    'name': 'days_since_last_match_home',
                    'description': 'Дней с последнего матча домашней команды',
                    'calculation': 'Разница в днях между текущим и предыдущим матчем',
                    'priority': 'MEDIUM',
                    'reason': 'Влияет на физическую готовность команды'
                },
                {
                    'name': 'days_since_last_match_away',
                    'description': 'Дней с последнего матча гостевой команды',
                    'calculation': 'Разница в днях между текущим и предыдущим матчем',
                    'priority': 'MEDIUM',
                    'reason': 'Влияет на физическую готовность команды'
                },
                {
                    'name': 'is_weekend_match',
                    'description': 'Матч в выходные дни',
                    'calculation': '1 если суббота/воскресенье, 0 иначе',
                    'priority': 'LOW',
                    'reason': 'Влияет на посещаемость и атмосферу'
                }
            ]
        })
        
        # 3. Фичи на основе турнирной таблицы
        suggestions.append({
            'category': 'Турнирная позиция',
            'features': [
                {
                    'name': 'home_position',
                    'description': 'Позиция домашней команды в турнирной таблице',
                    'calculation': 'Текущая позиция в чемпионате',
                    'priority': 'HIGH',
                    'reason': 'Прямо отражает силу команды'
                },
                {
                    'name': 'away_position',
                    'description': 'Позиция гостевой команды в турнирной таблице',
                    'calculation': 'Текущая позиция в чемпионате',
                    'priority': 'HIGH',
                    'reason': 'Прямо отражает силу команды'
                },
                {
                    'name': 'position_difference',
                    'description': 'Разница в позициях команд',
                    'calculation': 'abs(home_position - away_position)',
                    'priority': 'MEDIUM',
                    'reason': 'Показывает разницу в классе команд'
                }
            ]
        })
        
        # 4. Фичи на основе статистики голов
        suggestions.append({
            'category': 'Статистика голов',
            'features': [
                {
                    'name': 'home_goals_per_match',
                    'description': 'Среднее количество голов домашней команды за матч',
                    'calculation': 'Общее количество голов / количество матчей',
                    'priority': 'HIGH',
                    'reason': 'Прямая корреляция с результатом'
                },
                {
                    'name': 'away_goals_per_match',
                    'description': 'Среднее количество голов гостевой команды за матч',
                    'calculation': 'Общее количество голов / количество матчей',
                    'priority': 'HIGH',
                    'reason': 'Прямая корреляция с результатом'
                },
                {
                    'name': 'home_conceded_per_match',
                    'description': 'Среднее количество пропущенных голов домашней команды',
                    'calculation': 'Общее количество пропущенных голов / количество матчей',
                    'priority': 'HIGH',
                    'reason': 'Показывает слабость защиты'
                },
                {
                    'name': 'away_conceded_per_match',
                    'description': 'Среднее количество пропущенных голов гостевой команды',
                    'calculation': 'Общее количество пропущенных голов / количество матчей',
                    'priority': 'HIGH',
                    'reason': 'Показывает слабость защиты'
                }
            ]
        })
        
        # 5. Фичи на основе погодных условий
        suggestions.append({
            'category': 'Погодные условия',
            'features': [
                {
                    'name': 'temperature',
                    'description': 'Температура воздуха во время матча',
                    'calculation': 'Температура в градусах Цельсия',
                    'priority': 'LOW',
                    'reason': 'Влияет на физическую активность игроков'
                },
                {
                    'name': 'humidity',
                    'description': 'Влажность воздуха',
                    'calculation': 'Влажность в процентах',
                    'priority': 'LOW',
                    'reason': 'Влияет на физическую активность игроков'
                },
                {
                    'name': 'wind_speed',
                    'description': 'Скорость ветра',
                    'calculation': 'Скорость ветра в м/с',
                    'priority': 'LOW',
                    'reason': 'Влияет на точность ударов'
                }
            ]
        })
        
        # 6. Фичи на основе состава команд
        suggestions.append({
            'category': 'Состав команд',
            'features': [
                {
                    'name': 'home_key_player_injured',
                    'description': 'Ключевые игроки домашней команды травмированы',
                    'calculation': 'Количество травмированных ключевых игроков',
                    'priority': 'MEDIUM',
                    'reason': 'Влияет на силу команды'
                },
                {
                    'name': 'away_key_player_injured',
                    'description': 'Ключевые игроки гостевой команды травмированы',
                    'calculation': 'Количество травмированных ключевых игроков',
                    'priority': 'MEDIUM',
                    'reason': 'Влияет на силу команды'
                },
                {
                    'name': 'home_suspensions',
                    'description': 'Дисквалификации в домашней команде',
                    'calculation': 'Количество дисквалифицированных игроков',
                    'priority': 'MEDIUM',
                    'reason': 'Влияет на состав команды'
                },
                {
                    'name': 'away_suspensions',
                    'description': 'Дисквалификации в гостевой команде',
                    'calculation': 'Количество дисквалифицированных игроков',
                    'priority': 'MEDIUM',
                    'reason': 'Влияет на состав команды'
                }
            ]
        })
        
        return suggestions
    
    def generate_feature_analysis_report(self, analysis_results: Dict[str, Any], suggestions: List[Dict[str, Any]]) -> str:
        """Генерирует отчет по анализу фичей."""
        report = []
        report.append("=" * 80)
        report.append("АНАЛИЗ ФИЧЕЙ И ПРЕДЛОЖЕНИЯ ПО УЛУЧШЕНИЮ")
        report.append("=" * 80)
        report.append(f"Время анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Текущие фичи
        report.append("📊 ТЕКУЩИЕ ФИЧИ:")
        report.append("-" * 40)
        
        feature_stats = analysis_results.get('feature_stats', [])
        for stat in feature_stats:
            report.append(f"Префикс {stat['prefix']}:")
            report.append(f"  Записей: {stat['total_records']:,}")
            report.append(f"  Уникальных матчей: {stat['unique_matches']:,}")
            report.append(f"  Покрытие WDL: {stat['wdl_coverage']:.2%}")
            report.append(f"  Покрытие OZ: {stat['oz_coverage']:.2%}")
            report.append(f"  Покрытие Total: {stat['total_coverage']:.2%}")
            report.append("")
        
        # Корреляции
        correlations = analysis_results.get('correlations', {})
        if correlations and 'error' not in correlations:
            report.append("🔗 КОРРЕЛЯЦИИ С РЕЗУЛЬТАТАМИ:")
            report.append("-" * 40)
            
            for target, corrs in correlations.items():
                report.append(f"{target}:")
                for result, corr in corrs.items():
                    report.append(f"  {result}: {corr:.3f}")
                report.append("")
        
        # Предложения по новым фичам
        report.append("💡 ПРЕДЛОЖЕНИЯ ПО НОВЫМ ФИЧАМ:")
        report.append("-" * 40)
        
        for category in suggestions:
            report.append(f"\n📁 {category['category']}:")
            
            for feature in category['features']:
                priority_emoji = {
                    'HIGH': '🔴',
                    'MEDIUM': '🟡',
                    'LOW': '🟢'
                }
                
                emoji = priority_emoji.get(feature['priority'], '❓')
                report.append(f"  {emoji} {feature['name']}")
                report.append(f"     Описание: {feature['description']}")
                report.append(f"     Расчет: {feature['calculation']}")
                report.append(f"     Приоритет: {feature['priority']}")
                report.append(f"     Обоснование: {feature['reason']}")
                report.append("")
        
        # Рекомендации по приоритетам
        report.append("🎯 РЕКОМЕНДАЦИИ ПО ПРИОРИТЕТАМ:")
        report.append("-" * 40)
        
        high_priority_features = []
        medium_priority_features = []
        low_priority_features = []
        
        for category in suggestions:
            for feature in category['features']:
                if feature['priority'] == 'HIGH':
                    high_priority_features.append(f"{category['category']}: {feature['name']}")
                elif feature['priority'] == 'MEDIUM':
                    medium_priority_features.append(f"{category['category']}: {feature['name']}")
                else:
                    low_priority_features.append(f"{category['category']}: {feature['name']}")
        
        report.append("🔴 ВЫСОКИЙ ПРИОРИТЕТ (реализовать в первую очередь):")
        for feature in high_priority_features:
            report.append(f"  - {feature}")
        
        report.append("\n🟡 СРЕДНИЙ ПРИОРИТЕТ (реализовать во вторую очередь):")
        for feature in medium_priority_features:
            report.append(f"  - {feature}")
        
        report.append("\n🟢 НИЗКИЙ ПРИОРИТЕТ (реализовать в последнюю очередь):")
        for feature in low_priority_features:
            report.append(f"  - {feature}")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_analysis_results(self, analysis_results: Dict[str, Any], suggestions: List[Dict[str, Any]]):
        """Сохраняет результаты анализа."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Сохраняем JSON данные
        json_file = os.path.join(self.output_dir, f'feature_analysis_{timestamp}.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                'analysis_results': analysis_results,
                'suggestions': suggestions
            }, f, ensure_ascii=False, indent=2, default=str)
        
        # Генерируем и сохраняем отчет
        report = self.generate_feature_analysis_report(analysis_results, suggestions)
        report_file = os.path.join(self.output_dir, f'feature_analysis_report_{timestamp}.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f'Результаты анализа сохранены: {self.output_dir}')
        return report_file


def main():
    """Основная функция анализа фичей."""
    analyzer = FeatureAnalysis()
    
    # Анализируем текущие фичи
    logger.info('🔍 Начинаем анализ фичей')
    analysis_results = analyzer.analyze_current_features()
    
    # Генерируем предложения
    suggestions = analyzer.suggest_new_features(analysis_results)
    
    # Сохраняем результаты
    report_file = analyzer.save_analysis_results(analysis_results, suggestions)
    
    logger.info(f'📄 Отчет сохранен: {report_file}')
    
    # Выводим краткий отчет
    print("\n" + analyzer.generate_feature_analysis_report(analysis_results, suggestions))


if __name__ == '__main__':
    main()
