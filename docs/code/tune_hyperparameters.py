#!/usr/bin/env python3
import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_squared_error, classification_report
import joblib

from config import Session_pool
from db.models import Feature, Match
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def load_training_data(limit_matches: int = 5000) -> Tuple[pd.DataFrame, Dict[str, pd.Series]]:
    """Загружает данные для обучения."""
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
    
    # Подготавливаем фичи
    feature_cols = [c for c in df.columns if c not in ('match_id', 'target_wdl', 'target_oz', 'target_total_amount')]
    X = df[feature_cols].copy()
    
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


def tune_classification_model(X: pd.DataFrame, y: pd.Series, target_name: str) -> Dict[str, Any]:
    """Настраивает гиперпараметры для классификации."""
    print(f"\n🔧 Настройка модели для {target_name}")
    print("-" * 40)
    
    # Параметры для настройки
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'max_features': ['sqrt', 'log2', None]
    }
    
    # Создаем модель
    model = RandomForestClassifier(random_state=42, n_jobs=-1)
    
    # Временные ряды для валидации
    tscv = TimeSeriesSplit(n_splits=3)
    
    # Grid Search
    grid_search = GridSearchCV(
        model, param_grid, 
        cv=tscv, 
        scoring='accuracy',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    
    # Результаты
    best_params = grid_search.best_params_
    best_score = grid_search.best_score_
    
    print(f"Лучшие параметры: {best_params}")
    print(f"Лучший score: {best_score:.4f}")
    
    # Тестируем на последних данных
    X_train, X_test = X.iloc[:-500], X.iloc[-500:]
    y_train, y_test = y.iloc[:-500], y.iloc[-500:]
    
    best_model = RandomForestClassifier(**best_params, random_state=42)
    best_model.fit(X_train, y_train)
    
    y_pred = best_model.predict(X_test)
    test_accuracy = accuracy_score(y_test, y_pred)
    
    print(f"Точность на тесте: {test_accuracy:.4f}")
    
    return {
        'model': best_model,
        'params': best_params,
        'cv_score': best_score,
        'test_score': test_accuracy,
        'feature_importance': dict(zip(X.columns, best_model.feature_importances_))
    }


def tune_regression_model(X: pd.DataFrame, y: pd.Series, target_name: str) -> Dict[str, Any]:
    """Настраивает гиперпараметры для регрессии."""
    print(f"\n🔧 Настройка модели для {target_name}")
    print("-" * 40)
    
    # Параметры для настройки
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'max_features': ['sqrt', 'log2', None]
    }
    
    # Создаем модель
    model = RandomForestRegressor(random_state=42, n_jobs=-1)
    
    # Временные ряды для валидации
    tscv = TimeSeriesSplit(n_splits=3)
    
    # Grid Search
    grid_search = GridSearchCV(
        model, param_grid, 
        cv=tscv, 
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    
    # Результаты
    best_params = grid_search.best_params_
    best_score = -grid_search.best_score_  # Отрицательный MSE
    
    print(f"Лучшие параметры: {best_params}")
    print(f"Лучший RMSE: {np.sqrt(best_score):.4f}")
    
    # Тестируем на последних данных
    X_train, X_test = X.iloc[:-500], X.iloc[-500:]
    y_train, y_test = y.iloc[:-500], y.iloc[-500:]
    
    best_model = RandomForestRegressor(**best_params, random_state=42)
    best_model.fit(X_train, y_train)
    
    y_pred = best_model.predict(X_test)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    print(f"RMSE на тесте: {test_rmse:.4f}")
    
    return {
        'model': best_model,
        'params': best_params,
        'cv_score': best_score,
        'test_score': test_rmse,
        'feature_importance': dict(zip(X.columns, best_model.feature_importances_))
    }


def save_models(results: Dict[str, Dict[str, Any]]) -> str:
    """Сохраняет настроенные модели."""
    out_dir = os.path.join('results', 'quality', 'tuned_models')
    os.makedirs(out_dir, exist_ok=True)
    
    for target_name, result in results.items():
        model_path = os.path.join(out_dir, f'{target_name}_model.joblib')
        joblib.dump(result['model'], model_path)
        
        # Сохраняем метаданные
        metadata = {
            'params': result['params'],
            'cv_score': result['cv_score'],
            'test_score': result['test_score'],
            'feature_importance': result['feature_importance']
        }
        
        metadata_path = os.path.join(out_dir, f'{target_name}_metadata.joblib')
        joblib.dump(metadata, metadata_path)
    
    return out_dir


def main():
    print("\n🎛️ НАСТРОЙКА ГИПЕРПАРАМЕТРОВ")
    print("=" * 50)
    
    # Загружаем данные
    X, targets = load_training_data()
    
    if X.empty or not targets:
        print("❌ Нет данных для обучения")
        return
    
    print(f"📊 Загружено: {X.shape[0]} объектов, {X.shape[1]} фичей")
    
    results = {}
    
    # Настраиваем модели для каждой цели
    for target_name, y in targets.items():
        if target_name in ['wdl', 'oz']:
            # Классификация
            results[target_name] = tune_classification_model(X, y, target_name)
        else:
            # Регрессия
            results[target_name] = tune_regression_model(X, y, target_name)
    
    # Сохраняем результаты
    out_dir = save_models(results)
    print(f"\n💾 Модели сохранены в: {out_dir}")
    
    # Сводка
    print(f"\n📊 СВОДКА РЕЗУЛЬТАТОВ:")
    print("-" * 30)
    for target_name, result in results.items():
        if target_name in ['wdl', 'oz']:
            print(f"{target_name}: точность = {result['test_score']:.4f}")
        else:
            print(f"{target_name}: RMSE = {result['test_score']:.4f}")


if __name__ == '__main__':
    main()
