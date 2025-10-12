#!/usr/bin/env python3
import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, classification_report, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import joblib
import glob

from config import Session_pool
from db.models import Feature, Match
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def load_top_features() -> Dict[str, List[str]]:
    """Загружает отобранные важные фичи."""
    out_dir = os.path.join('results', 'quality', 'feature_importance')
    
    targets = ['win_draw_loss', 'oz', 'total_amount', 'total_home_amount', 'total_away_amount']
    top_features = {}
    
    for target in targets:
        csv_pattern = os.path.join(out_dir, f'{target}_*.csv')
        files = glob.glob(csv_pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            df = pd.read_csv(latest_file)
            # Берем топ-300 фичей по MI
            top_300 = df.head(300)['feature'].tolist()
            top_features[target] = top_300
            print(f"✅ {target}: {len(top_300)} топ фичей")
        else:
            print(f"❌ {target}: файл не найден")
    
    return top_features


def load_training_data_with_features(selected_features: List[str], limit_matches: int = 8000) -> Tuple[pd.DataFrame, Dict[str, pd.Series]]:
    """Загружает данные для обучения с отобранными фичами."""
    with Session_pool() as db:
        match_ids = (
            db.query(Feature.match_id)
            .group_by(Feature.match_id)
            .limit(limit_matches)
            .all()
        )
        match_ids = [r[0] for r in match_ids]
        
        if not match_ids:
            return pd.DataFrame(), {}
        
        rows = (
            db.query(Feature)
            .filter(Feature.match_id.in_(match_ids))
            .all()
        )
    
    # Собираем данные
    records = {}
    for r in rows:
        d = r.as_dict()
        mid = d['match_id']
        pref = d['prefix']
        rec = records.setdefault(mid, {'match_id': mid})
        
        for k, v in d.items():
            if k in META or k in DROP_FIELD_EMBEDDING or k.startswith('_'):
                continue
            
            # Таргеты
            if pref == 'home':
                if k == 'target_total_amount':
                    rec['target_total_amount'] = v
                if k == 'target_win_draw_loss_home_win' and v is not None and int(v) == 1:
                    rec['target_wdl'] = 0
                if k == 'target_win_draw_loss_draw' and v is not None and int(v) == 1:
                    rec['target_wdl'] = 1
                if k == 'target_win_draw_loss_away_win' and v is not None and int(v) == 1:
                    rec['target_wdl'] = 2
                if k == 'target_oz_both_score' and v is not None and int(v) == 1:
                    rec['target_oz'] = 1
                if k == 'target_oz_not_both_score' and v is not None and int(v) == 1:
                    rec['target_oz'] = 0
            
            if k in TARGET_FIELDS:
                continue
            
            rec[f'{pref}_{k}'] = v
    
    df = pd.DataFrame.from_dict(records, orient='index')
    
    # Фильтруем только отобранные фичи
    available_features = [f for f in selected_features if f in df.columns]
    X = df[available_features].copy()
    
    # Приводим к числовым типам
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors='coerce')
    
    X = X.fillna(0.0).astype(float)
    
    # Подготавливаем таргеты
    targets = {}
    if 'target_wdl' in df.columns:
        targets['wdl'] = df['target_wdl'].dropna().astype(int)
    if 'target_oz' in df.columns:
        targets['oz'] = df['target_oz'].dropna().astype(int)
    if 'target_total_amount' in df.columns:
        targets['total_amount'] = pd.to_numeric(df['target_total_amount'], errors='coerce').dropna()
    
    return X, targets


def train_ensemble_model(X: pd.DataFrame, y: pd.Series, target_name: str, model_type: str = 'classification') -> Dict[str, Any]:
    """Обучает ансамблевую модель."""
    print(f"\n🚀 Обучение ансамбля для {target_name}")
    print("-" * 40)
    
    # Разделяем данные
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y if model_type == 'classification' else None)
    
    # Нормализация
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    models = {}
    predictions = {}
    
    if model_type == 'classification':
        # Классификация
        models['rf'] = RandomForestClassifier(n_estimators=200, max_depth=20, min_samples_split=5, random_state=42)
        models['gb'] = GradientBoostingClassifier(n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42)
        
        # Обучаем модели
        for name, model in models.items():
            model.fit(X_train_scaled, y_train)
            pred = model.predict(X_test_scaled)
            predictions[name] = pred
            
            # Кросс-валидация
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='accuracy')
            print(f"{name}: CV accuracy = {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        # Ансамбль (простое голосование)
        ensemble_pred = np.round(np.mean([pred.astype(int) for pred in predictions.values()], axis=0)).astype(int)
        
        # Результаты
        ensemble_accuracy = accuracy_score(y_test, ensemble_pred)
        print(f"\nАнсамбль: точность = {ensemble_accuracy:.4f}")
        
        # Детальный отчет
        print("\nДетальный отчет:")
        print(classification_report(y_test, ensemble_pred))
        
        return {
            'models': models,
            'scaler': scaler,
            'accuracy': ensemble_accuracy,
            'predictions': ensemble_pred,
            'feature_names': X.columns.tolist()
        }
    
    else:
        # Регрессия
        models['rf'] = RandomForestRegressor(n_estimators=200, max_depth=20, min_samples_split=5, random_state=42)
        models['gb'] = GradientBoostingRegressor(n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42)
        
        # Обучаем модели
        for name, model in models.items():
            model.fit(X_train_scaled, y_train)
            pred = model.predict(X_test_scaled)
            predictions[name] = pred
            
            # Кросс-валидация
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='neg_mean_squared_error')
            rmse = np.sqrt(-cv_scores.mean())
            print(f"{name}: CV RMSE = {rmse:.4f}")
        
        # Ансамбль (среднее)
        ensemble_pred = np.mean(list(predictions.values()), axis=0)
        
        # Результаты
        ensemble_rmse = np.sqrt(mean_squared_error(y_test, ensemble_pred))
        ensemble_r2 = r2_score(y_test, ensemble_pred)
        
        print(f"\nАнсамбль: RMSE = {ensemble_rmse:.4f}, R² = {ensemble_r2:.4f}")
        
        return {
            'models': models,
            'scaler': scaler,
            'rmse': ensemble_rmse,
            'r2': ensemble_r2,
            'predictions': ensemble_pred,
            'feature_names': X.columns.tolist()
        }


def save_improved_models(results: Dict[str, Dict[str, Any]]) -> str:
    """Сохраняет улучшенные модели."""
    out_dir = os.path.join('results', 'quality', 'improved_models')
    os.makedirs(out_dir, exist_ok=True)
    
    for target_name, result in results.items():
        # Сохраняем модели
        for model_name, model in result['models'].items():
            model_path = os.path.join(out_dir, f'{target_name}_{model_name}_model.joblib')
            joblib.dump(model, model_path)
        
        # Сохраняем скалер
        scaler_path = os.path.join(out_dir, f'{target_name}_scaler.joblib')
        joblib.dump(result['scaler'], scaler_path)
        
        # Сохраняем метаданные
        metadata = {
            'feature_names': result['feature_names'],
            'performance': {k: v for k, v in result.items() if k not in ['models', 'scaler', 'predictions']}
        }
        
        metadata_path = os.path.join(out_dir, f'{target_name}_metadata.joblib')
        joblib.dump(metadata, metadata_path)
    
    return out_dir


def main():
    print("\n🚀 ОБУЧЕНИЕ УЛУЧШЕННЫХ МОДЕЛЕЙ")
    print("=" * 50)
    
    # Загружаем отобранные фичи
    top_features = load_top_features()
    
    if not top_features:
        print("❌ Нет данных о важных фичах")
        return
    
    # Объединяем все важные фичи
    all_important_features = set()
    for features in top_features.values():
        all_important_features.update(features)
    
    print(f"📊 Всего уникальных важных фичей: {len(all_important_features)}")
    
    # Загружаем данные с отобранными фичами
    X, targets = load_training_data_with_features(list(all_important_features))
    
    if X.empty or not targets:
        print("❌ Нет данных для обучения")
        return
    
    print(f"📊 Загружено: {X.shape[0]} объектов, {X.shape[1]} фичей")
    
    results = {}
    
    # Обучаем модели для каждой цели
    for target_name, y in targets.items():
        if target_name in ['wdl', 'oz']:
            # Классификация
            results[target_name] = train_ensemble_model(X, y, target_name, 'classification')
        else:
            # Регрессия
            results[target_name] = train_ensemble_model(X, y, target_name, 'regression')
    
    # Сохраняем результаты
    out_dir = save_improved_models(results)
    print(f"\n💾 Улучшенные модели сохранены в: {out_dir}")
    
    # Сводка
    print(f"\n📊 СВОДКА РЕЗУЛЬТАТОВ:")
    print("-" * 30)
    for target_name, result in results.items():
        if target_name in ['wdl', 'oz']:
            print(f"{target_name}: точность = {result['accuracy']:.4f}")
        else:
            print(f"{target_name}: RMSE = {result['rmse']:.4f}, R² = {result['r2']:.4f}")


if __name__ == '__main__':
    main()
