#!/usr/bin/env python3
import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import joblib
import glob

from config import Session_pool
from db.models import Feature, Match
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


class ImprovedPredictor:
    """Улучшенный предиктор с отобранными фичами и ансамблевыми моделями."""
    
    def __init__(self, models_dir: str = 'results/quality/improved_models'):
        self.models_dir = models_dir
        self.models = {}
        self.scalers = {}
        self.feature_names = {}
        self.load_models()
    
    def load_models(self):
        """Загружает обученные модели."""
        print("🔄 Загрузка улучшенных моделей...")
        
        targets = ['wdl', 'oz', 'total_amount']
        
        for target in targets:
            try:
                # Загружаем модели
                rf_path = os.path.join(self.models_dir, f'{target}_rf_model.joblib')
                gb_path = os.path.join(self.models_dir, f'{target}_gb_model.joblib')
                scaler_path = os.path.join(self.models_dir, f'{target}_scaler.joblib')
                metadata_path = os.path.join(self.models_dir, f'{target}_metadata.joblib')
                
                if all(os.path.exists(p) for p in [rf_path, gb_path, scaler_path, metadata_path]):
                    self.models[target] = {
                        'rf': joblib.load(rf_path),
                        'gb': joblib.load(gb_path)
                    }
                    self.scalers[target] = joblib.load(scaler_path)
                    metadata = joblib.load(metadata_path)
                    self.feature_names[target] = metadata['feature_names']
                    print(f"✅ {target}: модели загружены")
                else:
                    print(f"❌ {target}: не все файлы найдены")
                    
            except Exception as e:
                print(f"❌ {target}: ошибка загрузки - {e}")
    
    def prepare_features(self, match_id: int) -> Dict[str, pd.DataFrame]:
        """Подготавливает фичи для матча."""
        with Session_pool() as db:
            rows = (
                db.query(Feature)
                .filter(Feature.match_id == match_id)
                .all()
            )
        
        if not rows:
            return {}
        
        # Собираем данные
        record = {'match_id': match_id}
        for r in rows:
            d = r.as_dict()
            pref = d['prefix']
            
            for k, v in d.items():
                if k in META or k in DROP_FIELD_EMBEDDING or k.startswith('_'):
                    continue
                if k in TARGET_FIELDS:
                    continue
                record[f'{pref}_{k}'] = v
        
        # Создаем DataFrame
        df = pd.DataFrame([record])
        
        # Подготавливаем фичи для каждой цели
        prepared_features = {}
        for target, feature_list in self.feature_names.items():
            available_features = [f for f in feature_list if f in df.columns]
            if available_features:
                X = df[available_features].copy()
                # Приводим к числовым типам
                for col in X.columns:
                    X[col] = pd.to_numeric(X[col], errors='coerce')
                X = X.fillna(0.0).astype(float)
                prepared_features[target] = X
            else:
                print(f"⚠️ {target}: нет доступных фичей")
        
        return prepared_features
    
    def predict_match(self, match_id: int) -> Dict[str, Any]:
        """Делает прогноз для матча."""
        print(f"\n🎯 Прогноз для матча {match_id}")
        print("-" * 30)
        
        # Подготавливаем фичи
        features = self.prepare_features(match_id)
        
        if not features:
            return {'error': 'Нет данных для матча'}
        
        predictions = {}
        
        for target, X in features.items():
            if target not in self.models:
                continue
            
            try:
                # Нормализуем фичи
                X_scaled = self.scalers[target].transform(X)
                
                # Получаем прогнозы от обеих моделей
                rf_pred = self.models[target]['rf'].predict(X_scaled)[0]
                gb_pred = self.models[target]['gb'].predict(X_scaled)[0]
                
                # Ансамбль
                if target in ['wdl', 'oz']:
                    # Классификация - голосование
                    ensemble_pred = int(np.round((rf_pred + gb_pred) / 2))
                else:
                    # Регрессия - среднее
                    ensemble_pred = (rf_pred + gb_pred) / 2
                
                predictions[target] = {
                    'rf_prediction': rf_pred,
                    'gb_prediction': gb_pred,
                    'ensemble_prediction': ensemble_pred
                }
                
                print(f"{target}: RF={rf_pred}, GB={gb_pred}, Ансамбль={ensemble_pred}")
                
            except Exception as e:
                print(f"❌ {target}: ошибка предсказания - {e}")
                predictions[target] = {'error': str(e)}
        
        return predictions
    
    def batch_predict(self, match_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Делает прогнозы для списка матчей."""
        results = {}
        
        for match_id in match_ids:
            try:
                results[match_id] = self.predict_match(match_id)
            except Exception as e:
                results[match_id] = {'error': str(e)}
        
        return results


def test_improved_predictor():
    """Тестирует улучшенный предиктор."""
    print("\n🧪 ТЕСТИРОВАНИЕ УЛУЧШЕННОГО ПРЕДИКТОРА")
    print("=" * 50)
    
    # Создаем предиктор
    predictor = ImprovedPredictor()
    
    if not predictor.models:
        print("❌ Модели не загружены")
        return
    
    # Получаем несколько матчей для тестирования
    with Session_pool() as db:
        match_ids = (
            db.query(Feature.match_id)
            .group_by(Feature.match_id)
            .limit(5)
            .all()
        )
        test_match_ids = [r[0] for r in match_ids]
    
    print(f"📊 Тестируем на {len(test_match_ids)} матчах")
    
    # Делаем прогнозы
    results = predictor.batch_predict(test_match_ids)
    
    # Выводим результаты
    for match_id, predictions in results.items():
        print(f"\nМатч {match_id}:")
        if 'error' in predictions:
            print(f"  ❌ Ошибка: {predictions['error']}")
        else:
            for target, pred_data in predictions.items():
                if 'error' not in pred_data:
                    print(f"  {target}: {pred_data['ensemble_prediction']}")


def compare_with_actual_results():
    """Сравнивает прогнозы с фактическими результатами."""
    print("\n📊 СРАВНЕНИЕ С ФАКТИЧЕСКИМИ РЕЗУЛЬТАТАМИ")
    print("=" * 50)
    
    predictor = ImprovedPredictor()
    
    if not predictor.models:
        print("❌ Модели не загружены")
        return
    
    # Получаем матчи с результатами
    with Session_pool() as db:
        query = db.query(Feature, Match).join(Match, Feature.match_id == Match.id).filter(
            Feature.prefix == 'home',
            Match.numOfHeadsHome.isnot(None),
            Match.numOfHeadsAway.isnot(None)
        ).limit(10)
        
        results = query.all()
    
    if not results:
        print("❌ Нет матчей с результатами")
        return
    
    correct_predictions = 0
    total_predictions = 0
    
    for feature, match in results:
        match_id = match.id
        home_goals = match.numOfHeadsHome
        away_goals = match.numOfHeadsAway
        
        print(f"\nМатч {match_id}: {home_goals}:{away_goals}")
        
        # Получаем прогноз
        predictions = predictor.predict_match(match_id)
        
        if 'error' not in predictions:
            # Проверяем WDL
            if 'wdl' in predictions:
                wdl_pred = predictions['wdl']['ensemble_prediction']
                wdl_actual = 0 if home_goals > away_goals else (1 if home_goals == away_goals else 2)
                
                wdl_correct = wdl_pred == wdl_actual
                print(f"  WDL: прогноз={wdl_pred}, факт={wdl_actual}, {'✅' if wdl_correct else '❌'}")
                
                if wdl_correct:
                    correct_predictions += 1
                total_predictions += 1
            
            # Проверяем OZ
            if 'oz' in predictions:
                oz_pred = predictions['oz']['ensemble_prediction']
                oz_actual = 1 if home_goals > 0 and away_goals > 0 else 0
                
                oz_correct = oz_pred == oz_actual
                print(f"  OZ: прогноз={oz_pred}, факт={oz_actual}, {'✅' if oz_correct else '❌'}")
                
                if oz_correct:
                    correct_predictions += 1
                total_predictions += 1
    
    if total_predictions > 0:
        accuracy = correct_predictions / total_predictions
        print(f"\n📊 Общая точность: {accuracy:.2%} ({correct_predictions}/{total_predictions})")


def main():
    test_improved_predictor()
    compare_with_actual_results()


if __name__ == '__main__':
    main()
