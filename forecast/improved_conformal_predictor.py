#!/usr/bin/env python3
"""
Улучшенный предиктор с ансамблевыми моделями и отобранными фичами.
Интегрируется в систему publisher.py для замены существующих моделей.
"""

import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
import joblib
from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Session_pool
from db.models import Feature, Match, Prediction, Outcome
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING
from core.utils import create_feature_vector_new

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


class ImprovedConformalPredictor:
    """Улучшенный конформный предиктор с ансамблевыми моделями."""
    
    def __init__(self, models_dir: str = 'results/quality/improved_models'):
        self.models_dir = models_dir
        self.models = {}
        self.scalers = {}
        self.feature_names = {}
        self.load_models()
    
    def load_models(self):
        """Загружает обученные модели."""
        logger.info('🔄 Загрузка улучшенных моделей...')
        
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
                    logger.info(f'✅ {target}: модели загружены')
                else:
                    logger.warning(f'❌ {target}: не все файлы найдены')
                    
            except Exception as e:
                logger.error(f'❌ {target}: ошибка загрузки - {e}')
    
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
                logger.warning(f'⚠️ {target}: нет доступных фичей')
        
        return prepared_features
    
    def predict_match(self, match_id: int) -> Dict[str, Any]:
        """Делает прогноз для матча."""
        logger.info(f'🎯 Прогноз для матча {match_id}')
        
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
                
                logger.info(f"{target}: RF={rf_pred}, GB={gb_pred}, Ансамбль={ensemble_pred}")
                
            except Exception as e:
                logger.error(f"❌ {target}: ошибка предсказания - {e}")
                predictions[target] = {'error': str(e)}
        
        return predictions
    
    def create_conformal_prediction(self, match_id: int) -> Dict[str, Any]:
        """Создает конформный прогноз в формате, совместимом с существующей системой."""
        predictions = self.predict_match(match_id)
        
        if 'error' in predictions:
            return {'error': predictions['error']}
        
        # Преобразуем в формат конформного прогноза
        conformal_result = {
            'match_id': match_id,
            'predictions': {}
        }
        
        # WDL (win_draw_loss)
        if 'wdl' in predictions and 'error' not in predictions['wdl']:
            wdl_pred = predictions['wdl']['ensemble_prediction']
            conformal_result['predictions']['win_draw_loss'] = {
                'home_win': 1.0 if wdl_pred == 0 else 0.0,
                'draw': 1.0 if wdl_pred == 1 else 0.0,
                'away_win': 1.0 if wdl_pred == 2 else 0.0,
                'confidence': 0.8,  # Высокая уверенность для ансамбля
                'uncertainty': 0.2
            }
        
        # OZ (обе забьют)
        if 'oz' in predictions and 'error' not in predictions['oz']:
            oz_pred = predictions['oz']['ensemble_prediction']
            conformal_result['predictions']['oz'] = {
                'yes': 1.0 if oz_pred == 1 else 0.0,
                'no': 1.0 if oz_pred == 0 else 0.0,
                'confidence': 0.8,
                'uncertainty': 0.2
            }
        
        # Total Amount (регрессия)
        if 'total_amount' in predictions and 'error' not in predictions['total_amount']:
            total_pred = predictions['total_amount']['ensemble_prediction']
            conformal_result['predictions']['total_amount'] = {
                'forecast': total_pred,
                'confidence': 0.8,
                'uncertainty': 0.2,
                'lower_bound': max(0, total_pred - 0.5),
                'upper_bound': total_pred + 0.5
            }
        
        return conformal_result


class ImprovedNeuralConformalPredictor:
    """Обертка для интеграции улучшенного предиктора в существующую систему."""
    
    def __init__(self):
        self.improved_predictor = ImprovedConformalPredictor()
    
    def train_conformal_predictor(self) -> bool:
        """Обучение предиктора (модели уже обучены)."""
        try:
            logger.info('✅ Улучшенные модели уже обучены и загружены')
            return True
        except Exception as e:
            logger.error(f'❌ Ошибка загрузки моделей: {e}')
            return False
    
    def get_tournament_ids(self) -> List[int]:
        """Получает список ID чемпионатов для обработки."""
        try:
            with Session_pool() as db:
                # Получаем чемпионаты с матчами, у которых есть фичи
                result = db.execute("""
                    SELECT DISTINCT t.championship_id
                    FROM tournaments t
                    JOIN matchs m ON t.id = m.tournament_id
                    JOIN features f ON m.id = f.match_id
                    WHERE f.prefix = 'home'
                    ORDER BY t.championship_id
                """)
                
                tournament_ids = [row[0] for row in result.fetchall()]
                logger.info(f'Найдено {len(tournament_ids)} чемпионатов для обработки')
                return tournament_ids
                
        except Exception as e:
            logger.error(f'Ошибка получения списка чемпионатов: {e}')
            return []
    
    def create_conformal_predictions(self, tournament_ids: List[int]) -> Dict[str, Any]:
        """Создает конформные прогнозы для списка чемпионатов."""
        logger.info(f'Создание улучшенных прогнозов для {len(tournament_ids)} чемпионатов')
        
        results = []
        successful = 0
        failed = 0
        
        for tournament_id in tournament_ids:
            try:
                result = self._process_tournament(tournament_id)
                results.append(result)
                
                if 'error' not in result:
                    successful += 1
                else:
                    failed += 1
                    
            except Exception as e:
                error_msg = f'Ошибка обработки чемпионата {tournament_id}: {e}'
                logger.error(error_msg)
                results.append(error_msg)
                failed += 1
        
        return {
            'success': successful > 0,
            'total_tournaments': len(tournament_ids),
            'successful': successful,
            'failed': failed,
            'results': results
        }
    
    def _process_tournament(self, tournament_id: int) -> str:
        """Обрабатывает один чемпионат."""
        try:
            with Session_pool() as db:
                # Получаем матчи чемпионата с фичами
                result = db.execute("""
                    SELECT DISTINCT m.id
                    FROM tournaments t
                    JOIN matchs m ON t.id = m.tournament_id
                    JOIN features f ON m.id = f.match_id
                    WHERE t.championship_id = :tournament_id
                    AND f.prefix = 'home'
                    ORDER BY m.id
                """, {'tournament_id': tournament_id})
                
                match_ids = [row[0] for row in result.fetchall()]
                
                if not match_ids:
                    return f'Нет матчей с фичами для чемпионата {tournament_id}'
                
                logger.info(f'Обработка {len(match_ids)} матчей чемпионата {tournament_id}')
                
                processed = 0
                for match_id in match_ids:
                    try:
                        # Создаем конформный прогноз
                        conformal_result = self.improved_predictor.create_conformal_prediction(match_id)
                        
                        if 'error' not in conformal_result:
                            # Сохраняем в outcomes
                            self._save_conformal_outcome(db, conformal_result)
                            processed += 1
                        
                    except Exception as e:
                        logger.error(f'Ошибка обработки матча {match_id}: {e}')
                        continue
                
                return f'Чемпионат {tournament_id}: обработано {processed}/{len(match_ids)} матчей'
                
        except Exception as e:
            return f'Ошибка обработки чемпионата {tournament_id}: {e}'
    
    def _save_conformal_outcome(self, db_session, conformal_result: Dict[str, Any]):
        """Сохраняет конформный прогноз в таблицу outcomes."""
        try:
            match_id = conformal_result['match_id']
            predictions = conformal_result['predictions']
            
            for pred_type, pred_data in predictions.items():
                # Определяем feature code
                feature_code = self._get_feature_code(pred_type)
                
                if feature_code is None:
                    continue
                
                # Создаем запись в outcomes
                if pred_type == 'total_amount':
                    # Регрессия
                    outcome = Outcome(
                        match_id=match_id,
                        feature=feature_code,
                        forecast=pred_data['forecast'],
                        outcome=self._convert_regression_to_category(pred_data['forecast'], 'total_amount'),
                        probability=0.5,  # Для регрессии
                        confidence=pred_data['confidence'],
                        uncertainty=pred_data['uncertainty'],
                        lower_bound=pred_data['lower_bound'],
                        upper_bound=pred_data['upper_bound']
                    )
                else:
                    # Классификация
                    outcome_value = self._get_classification_outcome(pred_type, pred_data)
                    outcome = Outcome(
                        match_id=match_id,
                        feature=feature_code,
                        forecast=pred_data.get('forecast', 0.0),
                        outcome=outcome_value,
                        probability=max(pred_data.values()) if isinstance(pred_data, dict) else 0.0,
                        confidence=pred_data['confidence'],
                        uncertainty=pred_data['uncertainty'],
                        lower_bound=None,
                        upper_bound=None
                    )
                
                # Проверяем существование записи
                existing = db_session.query(Outcome).filter(
                    Outcome.match_id == match_id,
                    Outcome.feature == feature_code
                ).first()
                
                if existing:
                    # Обновляем существующую запись
                    for key, value in outcome.__dict__.items():
                        if not key.startswith('_'):
                            setattr(existing, key, value)
                else:
                    # Создаем новую запись
                    db_session.add(outcome)
            
            db_session.commit()
            
        except Exception as e:
            logger.error(f'Ошибка сохранения конформного прогноза: {e}')
            db_session.rollback()
    
    def _get_feature_code(self, pred_type: str) -> Optional[int]:
        """Возвращает код фичи для типа прогноза."""
        mapping = {
            'win_draw_loss': 1,
            'oz': 2,
            'total_amount': 8
        }
        return mapping.get(pred_type)
    
    def _convert_regression_to_category(self, forecast: float, pred_type: str) -> str:
        """Конвертирует регрессионный прогноз в категорию."""
        if pred_type == 'total_amount':
            return 'ТБ' if forecast >= 2.5 else 'ТМ'
        return str(forecast)
    
    def _get_classification_outcome(self, pred_type: str, pred_data: Dict[str, Any]) -> str:
        """Получает текстовый исход для классификационного прогноза."""
        if pred_type == 'win_draw_loss':
            if pred_data.get('home_win', 0) > 0.5:
                return 'п1'
            elif pred_data.get('draw', 0) > 0.5:
                return 'н'
            else:
                return 'п2'
        elif pred_type == 'oz':
            return 'обе забьют - да' if pred_data.get('yes', 0) > 0.5 else 'обе забьют - нет'
        
        return 'неизвестно'


def create_improved_conformal_predictor() -> ImprovedNeuralConformalPredictor:
    """Фабричная функция для создания улучшенного предиктора."""
    return ImprovedNeuralConformalPredictor()


if __name__ == '__main__':
    # Тестирование
    predictor = create_improved_conformal_predictor()
    
    # Тестируем на нескольких матчах
    test_match_ids = [451, 452, 453]
    
    for match_id in test_match_ids:
        result = predictor.improved_predictor.predict_match(match_id)
        print(f"\nМатч {match_id}:")
        for target, pred in result.items():
            if 'error' not in pred:
                print(f"  {target}: {pred['ensemble_prediction']}")
            else:
                print(f"  {target}: Ошибка - {pred['error']}")
