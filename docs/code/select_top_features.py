#!/usr/bin/env python3
import logging
import os
import pandas as pd
import glob
from typing import Dict, List, Set

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_importance_data() -> Dict[str, pd.DataFrame]:
    """Загружает данные о важности фичей из CSV файлов."""
    out_dir = os.path.join('results', 'quality', 'feature_importance')
    
    targets = ['win_draw_loss', 'oz', 'total_amount', 'total_home_amount', 'total_away_amount']
    importance_data = {}
    
    for target in targets:
        csv_pattern = os.path.join(out_dir, f'{target}_*.csv')
        files = glob.glob(csv_pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            df = pd.read_csv(latest_file)
            df = df.sort_values('MI', ascending=False)
            importance_data[target] = df
            print(f"✅ {target}: {len(df)} фичей загружено")
        else:
            print(f"❌ {target}: файл не найден")
    
    return importance_data


def select_top_features(importance_data: Dict[str, pd.DataFrame], 
                       top_n: int = 500, 
                       mi_threshold: float = 0.001) -> Dict[str, Set[str]]:
    """Выбирает топ-N фичей для каждой цели."""
    top_features = {}
    
    for target, df in importance_data.items():
        # Фильтруем по порогу MI
        filtered_df = df[df['MI'] >= mi_threshold]
        
        # Берем топ-N
        top_df = filtered_df.head(top_n)
        
        # Собираем названия фичей
        feature_names = set(top_df['feature'].tolist())
        top_features[target] = feature_names
        
        print(f"📊 {target}: {len(feature_names)} фичей (MI >= {mi_threshold})")
    
    return top_features


def find_common_features(top_features: Dict[str, Set[str]]) -> Set[str]:
    """Находит фичи, важные для всех целей."""
    if not top_features:
        return set()
    
    # Пересечение всех множеств
    common = set.intersection(*top_features.values())
    print(f"\n🔗 Общие важные фичи: {len(common)}")
    
    return common


def find_target_specific_features(top_features: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """Находит фичи, специфичные для каждой цели."""
    specific_features = {}
    
    for target, features in top_features.items():
        # Фичи этой цели, которые НЕ важны для других
        other_features = set()
        for other_target, other_feat in top_features.items():
            if other_target != target:
                other_features.update(other_feat)
        
        specific = features - other_features
        specific_features[target] = specific
        print(f"🎯 {target} специфичные: {len(specific)} фичей")
    
    return specific_features


def export_selected_features(top_features: Dict[str, Set[str]], 
                           common_features: Set[str],
                           specific_features: Dict[str, Set[str]]) -> str:
    """Экспортирует отобранные фичи в файл."""
    out_dir = os.path.join('results', 'quality', 'feature_selection')
    os.makedirs(out_dir, exist_ok=True)
    
    out_path = os.path.join(out_dir, 'selected_features.txt')
    
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write("ОТОБРАННЫЕ ФИЧИ ДЛЯ УЛУЧШЕНИЯ ПРОГНОЗИРОВАНИЯ\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"ОБЩИЕ ВАЖНЫЕ ФИЧИ ({len(common_features)}):\n")
        f.write("-" * 40 + "\n")
        for feat in sorted(common_features):
            f.write(f"  {feat}\n")
        f.write("\n")
        
        for target, features in top_features.items():
            f.write(f"ТОП ФИЧИ ДЛЯ {target.upper()} ({len(features)}):\n")
            f.write("-" * 40 + "\n")
            for feat in sorted(features):
                f.write(f"  {feat}\n")
            f.write("\n")
        
        f.write("СПЕЦИФИЧНЫЕ ФИЧИ ПО ЦЕЛЯМ:\n")
        f.write("-" * 40 + "\n")
        for target, features in specific_features.items():
            f.write(f"{target.upper()} ({len(features)}):\n")
            for feat in sorted(features):
                f.write(f"  {feat}\n")
            f.write("\n")
    
    return out_path


def main():
    print("\n🔍 ОТБОР ВАЖНЫХ ФИЧЕЙ")
    print("=" * 50)
    
    # Загружаем данные о важности
    importance_data = load_importance_data()
    if not importance_data:
        print("❌ Нет данных о важности фичей")
        return
    
    # Выбираем топ фичи
    top_features = select_top_features(importance_data, top_n=500, mi_threshold=0.001)
    
    # Находим общие и специфичные фичи
    common_features = find_common_features(top_features)
    specific_features = find_target_specific_features(top_features)
    
    # Экспортируем результаты
    out_path = export_selected_features(top_features, common_features, specific_features)
    print(f"\n📄 Результаты сохранены: {out_path}")
    
    # Статистика
    print(f"\n📊 СТАТИСТИКА:")
    print(f"Общих важных фичей: {len(common_features)}")
    total_unique = len(set.union(*top_features.values()))
    print(f"Всего уникальных важных фичей: {total_unique}")
    
    for target, features in top_features.items():
        print(f"{target}: {len(features)} фичей")


if __name__ == '__main__':
    main()
