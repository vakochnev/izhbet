#!/usr/bin/env python3
"""
Скрипт для проверки наличия всех моделей по турнирам.
"""

import os
import sys
from typing import Dict, List, Set, Tuple
from sqlalchemy import create_engine, text
from config import settings

# Ожидаемые типы моделей (из feature_config)
EXPECTED_MODELS = [
    'win_draw_loss_home_win',
    'win_draw_loss_draw', 
    'win_draw_loss_away_win',
    'oz_both_score',
    'oz_not_both_score',
    'goal_home_scores',
    'goal_home_no_score',
    'goal_away_scores',
    'goal_away_no_score',
    'total_over',
    'total_under',
    'total_home_over',
    'total_home_under',
    'total_away_over',
    'total_away_under',
    'total_amount',
    'total_home_amount',
    'total_away_amount'
]


def get_tournaments_with_models() -> Dict[int, Dict[str, any]]:
    """Получить список турниров, для которых должны быть модели."""
    engine = create_engine(settings.DATABASE_URL_mysql)
    
    query = """
    SELECT DISTINCT 
        m.tournament_id,
        s.sportName,
        c.countryName,
        ch.championshipName,
        COUNT(DISTINCT m.id) as match_count
    FROM matchs m
    JOIN sports s ON m.sport_id = s.id
    JOIN countrys c ON m.country_id = c.id
    JOIN championships ch ON m.tournament_id = ch.id
    WHERE m.isCanceled = 0
    GROUP BY m.tournament_id, s.sportName, c.countryName, ch.championshipName
    HAVING match_count >= 100
    ORDER BY s.sportName, c.countryName, ch.championshipName
    """
    
    tournaments = {}
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            tournaments[row.tournament_id] = {
                'sport': row.sportName,
                'country': row.countryName,
                'championship': row.championshipName,
                'match_count': row.match_count
            }
    
    return tournaments


def check_models_for_tournament(tournament_info: Dict) -> Tuple[List[str], List[str], int, int]:
    """Проверить наличие моделей для турнира."""
    sport = tournament_info['sport'].replace(' ', '_')
    country = tournament_info['country'].replace(' ', '_')
    championship = tournament_info['championship'].replace(' ', '_')
    
    models_dir = f'./models/{sport}/{country}/{championship}'
    
    existing_models = []
    missing_models = []
    total_files = 0
    
    if os.path.exists(models_dir):
        # Получаем список файлов моделей
        files = os.listdir(models_dir)
        total_files = len([f for f in files if f.endswith(('.keras', '.joblib'))])
        
        existing_model_names = set()
        
        for file in files:
            if file.endswith('_model.keras') or file.endswith('_best_model.keras'):
                # Извлекаем имя модели
                model_name = file.replace('_best_model.keras', '').replace('_model.keras', '')
                existing_model_names.add(model_name)
        
        for model_name in EXPECTED_MODELS:
            if model_name in existing_model_names:
                existing_models.append(model_name)
            else:
                missing_models.append(model_name)
    else:
        missing_models = EXPECTED_MODELS.copy()
    
    # Ожидаем 54 файла: 18 моделей × 3 файла (model.keras, scaler.joblib, label_encoder.joblib)
    expected_files = len(EXPECTED_MODELS) * 3
    
    return existing_models, missing_models, total_files, expected_files


def main():
    """Основная функция проверки."""
    print("=" * 80)
    print("ПРОВЕРКА НАЛИЧИЯ МОДЕЛЕЙ ПО ТУРНИРАМ")
    print("=" * 80)
    print()
    
    tournaments = get_tournaments_with_models()
    print(f"Найдено турниров с достаточным количеством матчей (≥100): {len(tournaments)}")
    print()
    
    incomplete_tournaments = []
    complete_tournaments = []
    
    for tournament_id, info in tournaments.items():
        existing, missing, total_files, expected_files = check_models_for_tournament(info)
        
        if missing or total_files < expected_files:
            incomplete_tournaments.append({
                'id': tournament_id,
                'info': info,
                'existing': existing,
                'missing': missing,
                'total_files': total_files,
                'expected_files': expected_files
            })
        else:
            complete_tournaments.append({
                'id': tournament_id,
                'info': info,
                'total_files': total_files
            })
    
    print(f"✅ Турниров с полным набором моделей: {len(complete_tournaments)}")
    print(f"❌ Турниров с неполным набором моделей: {len(incomplete_tournaments)}")
    print()
    
    if complete_tournaments:
        print("=" * 80)
        print("ТУРНИРЫ С ПОЛНЫМ НАБОРОМ МОДЕЛЕЙ:")
        print("=" * 80)
        for t in complete_tournaments:
            files_info = f"({t['total_files']} файлов)" if 'total_files' in t else ""
            print(f"  ID: {t['id']:4d} | {t['info']['sport']:12s} | {t['info']['country']:20s} | {t['info']['championship']} {files_info}")
        print()
    
    if incomplete_tournaments:
        print("=" * 80)
        print("ТУРНИРЫ С НЕПОЛНЫМ НАБОРОМ МОДЕЛЕЙ:")
        print("=" * 80)
        
        for t in incomplete_tournaments:
            info = t['info']
            print(f"\n🔴 ID: {t['id']:4d} | {info['sport']:12s} | {info['country']:20s} | {info['championship']}")
            print(f"   Матчей: {info['match_count']}")
            print(f"   Файлов: {t['total_files']}/{t['expected_files']} (ожидается 54)")
            print(f"   Моделей создано: {len(t['existing'])}/{len(EXPECTED_MODELS)}")
            
            if t['existing']:
                print(f"   ✅ Есть ({len(t['existing'])}): {', '.join(sorted(t['existing']))}")
            
            if t['missing']:
                print(f"   ❌ Отсутствуют ({len(t['missing'])}): {', '.join(sorted(t['missing']))}")
        
        print()
        print("=" * 80)
        print("TOURNAMENT IDs ДЛЯ ПОВТОРНОГО ЗАПУСКА:")
        print("=" * 80)
        tournament_ids = [str(t['id']) for t in incomplete_tournaments]
        print(','.join(tournament_ids))
        print()
        
        # Сохраняем в файл
        with open('incomplete_tournaments.txt', 'w') as f:
            for t in incomplete_tournaments:
                f.write(f"{t['id']}\n")
        print("📝 Список ID сохранен в файл: incomplete_tournaments.txt")
        print()


if __name__ == '__main__':
    main()

