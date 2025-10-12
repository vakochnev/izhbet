#!/usr/bin/env python3
"""
Тестирование улучшенных моделей на исторических данных.
Сравнивает точность старых и новых моделей.
"""

import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
import joblib
from datetime import datetime, timedelta
from tqdm import tqdm

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Session_pool
from db.models import Feature, Match, Outcome
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


class HistoricalTester:
    """Тестер для сравнения старых и новых моделей на исторических данных."""
    
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
                
            except Exception as e:
                logger.error(f"❌ {target}: ошибка предсказания - {e}")
                predictions[target] = {'error': str(e)}
        
        return predictions
    
    def get_historical_matches(self, limit: int = 1000) -> List[Tuple[int, Match]]:
        """Получает исторические матчи с результатами."""
        with Session_pool() as db:
            query = db.query(Match).filter(
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.gameData < datetime.now() - timedelta(days=1)  # Только завершенные матчи
            ).order_by(Match.gameData.desc()).limit(limit)
            
            matches = query.all()
            
            # Фильтруем матчи с фичами
            matches_with_features = []
            for match in matches:
                features_count = db.query(Feature).filter(Feature.match_id == match.id).count()
                if features_count > 0:
                    matches_with_features.append((match.id, match))
            
            logger.info(f'Найдено {len(matches_with_features)} матчей с фичами и результатами')
            return matches_with_features
    
    def test_historical_accuracy(self, limit: int = 1000) -> Dict[str, Any]:
        """Тестирует точность на исторических данных."""
        logger.info(f'\n🧪 ТЕСТИРОВАНИЕ НА ИСТОРИЧЕСКИХ ДАННЫХ')
        logger.info('=' * 60)
        
        # Получаем исторические матчи
        historical_matches = self.get_historical_matches(limit)
        
        if not historical_matches:
            return {'error': 'Нет исторических матчей для тестирования'}
        
        # Результаты тестирования
        results = {
            'total_matches': len(historical_matches),
            'wdl': {'correct': 0, 'total': 0, 'predictions': []},
            'oz': {'correct': 0, 'total': 0, 'predictions': []},
            'total_amount': {'correct': 0, 'total': 0, 'predictions': []}
        }
        
        logger.info(f'Тестируем на {len(historical_matches)} матчах...')
        
        for match_id, match in tqdm(historical_matches, desc="Обработка матчей"):
            try:
                # Получаем прогноз
                predictions = self.predict_match(match_id)
                
                if 'error' in predictions:
                    continue
                
                home_goals = match.numOfHeadsHome
                away_goals = match.numOfHeadsAway
                total_goals = home_goals + away_goals
                
                # Тестируем WDL
                if 'wdl' in predictions and 'error' not in predictions['wdl']:
                    wdl_pred = predictions['wdl']['ensemble_prediction']
                    wdl_actual = 0 if home_goals > away_goals else (1 if home_goals == away_goals else 2)
                    
                    is_correct = wdl_pred == wdl_actual
                    results['wdl']['total'] += 1
                    if is_correct:
                        results['wdl']['correct'] += 1
                    
                    results['wdl']['predictions'].append({
                        'match_id': match_id,
                        'predicted': wdl_pred,
                        'actual': wdl_actual,
                        'correct': is_correct,
                        'score': f'{home_goals}:{away_goals}'
                    })
                
                # Тестируем OZ
                if 'oz' in predictions and 'error' not in predictions['oz']:
                    oz_pred = predictions['oz']['ensemble_prediction']
                    oz_actual = 1 if home_goals > 0 and away_goals > 0 else 0
                    
                    is_correct = oz_pred == oz_actual
                    results['oz']['total'] += 1
                    if is_correct:
                        results['oz']['correct'] += 1
                    
                    results['oz']['predictions'].append({
                        'match_id': match_id,
                        'predicted': oz_pred,
                        'actual': oz_actual,
                        'correct': is_correct,
                        'score': f'{home_goals}:{away_goals}'
                    })
                
                # Тестируем Total Amount
                if 'total_amount' in predictions and 'error' not in predictions['total_amount']:
                    total_pred = predictions['total_amount']['ensemble_prediction']
                    
                    # Сравниваем с порогом 2.5
                    pred_over = total_pred >= 2.5
                    actual_over = total_goals > 2.5
                    
                    is_correct = pred_over == actual_over
                    results['total_amount']['total'] += 1
                    if is_correct:
                        results['total_amount']['correct'] += 1
                    
                    results['total_amount']['predictions'].append({
                        'match_id': match_id,
                        'predicted': total_pred,
                        'actual': total_goals,
                        'predicted_over': pred_over,
                        'actual_over': actual_over,
                        'correct': is_correct,
                        'score': f'{home_goals}:{away_goals}'
                    })
                
            except Exception as e:
                logger.error(f'Ошибка обработки матча {match_id}: {e}')
                continue
        
        # Вычисляем точность
        for target in ['wdl', 'oz', 'total_amount']:
            if results[target]['total'] > 0:
                accuracy = results[target]['correct'] / results[target]['total']
                results[target]['accuracy'] = accuracy
            else:
                results[target]['accuracy'] = 0.0
        
        return results
    
    def compare_with_existing_predictions(self, limit: int = 1000) -> Dict[str, Any]:
        """Сравнивает с существующими прогнозами в outcomes."""
        logger.info(f'\n📊 СРАВНЕНИЕ С СУЩЕСТВУЮЩИМИ ПРОГНОЗАМИ')
        logger.info('=' * 60)
        
        with Session_pool() as db:
            # Получаем существующие прогнозы с результатами
            query = db.query(Outcome, Match).join(Match, Outcome.match_id == Match.id).filter(
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.gameData < datetime.now() - timedelta(days=1)
            ).limit(limit)
            
            existing_predictions = query.all()
            
            if not existing_predictions:
                return {'error': 'Нет существующих прогнозов для сравнения'}
            
            logger.info(f'Найдено {len(existing_predictions)} существующих прогнозов')
            
            comparison_results = {
                'total_predictions': len(existing_predictions),
                'by_feature': {}
            }
            
            for outcome, match in existing_predictions:
                feature_code = outcome.feature
                
                if feature_code not in comparison_results['by_feature']:
                    comparison_results['by_feature'][feature_code] = {
                        'correct': 0,
                        'total': 0,
                        'predictions': []
                    }
                
                # Определяем правильность прогноза
                is_correct = self._evaluate_existing_prediction(outcome, match)
                
                comparison_results['by_feature'][feature_code]['total'] += 1
                if is_correct:
                    comparison_results['by_feature'][feature_code]['correct'] += 1
                
                comparison_results['by_feature'][feature_code]['predictions'].append({
                    'match_id': match.id,
                    'outcome': outcome.outcome,
                    'forecast': outcome.forecast,
                    'correct': is_correct,
                    'score': f'{match.numOfHeadsHome}:{match.numOfHeadsAway}'
                })
            
            # Вычисляем точность
            for feature_code, data in comparison_results['by_feature'].items():
                if data['total'] > 0:
                    data['accuracy'] = data['correct'] / data['total']
                else:
                    data['accuracy'] = 0.0
            
            return comparison_results
    
    def _evaluate_existing_prediction(self, outcome: Outcome, match: Match) -> bool:
        """Оценивает правильность существующего прогноза."""
        home_goals = match.numOfHeadsHome
        away_goals = match.numOfHeadsAway
        total_goals = home_goals + away_goals
        
        feature_code = outcome.feature
        
        if feature_code == 1:  # win_draw_loss
            actual = 'п1' if home_goals > away_goals else ('н' if home_goals == away_goals else 'п2')
            return outcome.outcome == actual
        
        elif feature_code == 2:  # oz
            actual = 'обе забьют - да' if home_goals > 0 and away_goals > 0 else 'обе забьют - нет'
            return outcome.outcome == actual
        
        elif feature_code == 8:  # total_amount
            pred_over = outcome.outcome == 'ТБ'
            actual_over = total_goals > 2.5
            return pred_over == actual_over
        
        return False
    
    def generate_report(self, results: Dict[str, Any], comparison_results: Dict[str, Any]) -> str:
        """Генерирует отчет о тестировании."""
        report = []
        report.append("=" * 80)
        report.append("ОТЧЕТ О ТЕСТИРОВАНИИ УЛУЧШЕННЫХ МОДЕЛЕЙ")
        report.append("=" * 80)
        report.append("")
        
        # Результаты новых моделей
        report.append("📊 РЕЗУЛЬТАТЫ НОВЫХ МОДЕЛЕЙ:")
        report.append("-" * 40)
        
        if 'error' not in results:
            report.append(f"Всего протестировано матчей: {results['total_matches']}")
            report.append("")
            
            for target in ['wdl', 'oz', 'total_amount']:
                data = results[target]
                if data['total'] > 0:
                    accuracy = data['accuracy'] * 100
                    report.append(f"{target.upper()}:")
                    report.append(f"  Точность: {accuracy:.2f}% ({data['correct']}/{data['total']})")
                    report.append("")
        else:
            report.append(f"Ошибка: {results['error']}")
        
        # Сравнение с существующими прогнозами
        report.append("📈 СРАВНЕНИЕ С СУЩЕСТВУЮЩИМИ ПРОГНОЗАМИ:")
        report.append("-" * 40)
        
        if 'error' not in comparison_results:
            report.append(f"Всего существующих прогнозов: {comparison_results['total_predictions']}")
            report.append("")
            
            feature_names = {1: 'WDL', 2: 'OZ', 8: 'Total Amount'}
            
            for feature_code, data in comparison_results['by_feature'].items():
                if data['total'] > 0:
                    accuracy = data['accuracy'] * 100
                    feature_name = feature_names.get(feature_code, f'Feature {feature_code}')
                    report.append(f"{feature_name}:")
                    report.append(f"  Точность: {accuracy:.2f}% ({data['correct']}/{data['total']})")
                    report.append("")
        else:
            report.append(f"Ошибка: {comparison_results['error']}")
        
        # Рекомендации
        report.append("💡 РЕКОМЕНДАЦИИ:")
        report.append("-" * 40)
        
        if 'error' not in results and 'error' not in comparison_results:
            # Сравниваем точность
            new_wdl_acc = results['wdl']['accuracy'] if results['wdl']['total'] > 0 else 0
            new_oz_acc = results['oz']['accuracy'] if results['oz']['total'] > 0 else 0
            new_total_acc = results['total_amount']['accuracy'] if results['total_amount']['total'] > 0 else 0
            
            old_wdl_acc = comparison_results['by_feature'].get(1, {}).get('accuracy', 0)
            old_oz_acc = comparison_results['by_feature'].get(2, {}).get('accuracy', 0)
            old_total_acc = comparison_results['by_feature'].get(8, {}).get('accuracy', 0)
            
            improvements = []
            
            if new_wdl_acc > old_wdl_acc:
                improvement = (new_wdl_acc - old_wdl_acc) * 100
                improvements.append(f"WDL: +{improvement:.2f}%")
            
            if new_oz_acc > old_oz_acc:
                improvement = (new_oz_acc - old_oz_acc) * 100
                improvements.append(f"OZ: +{improvement:.2f}%")
            
            if new_total_acc > old_total_acc:
                improvement = (new_total_acc - old_total_acc) * 100
                improvements.append(f"Total Amount: +{improvement:.2f}%")
            
            if improvements:
                report.append("✅ Улучшения:")
                for imp in improvements:
                    report.append(f"  - {imp}")
            else:
                report.append("⚠️ Улучшений не обнаружено")
            
            # Общая оценка
            avg_new_acc = (new_wdl_acc + new_oz_acc + new_total_acc) / 3
            avg_old_acc = (old_wdl_acc + old_oz_acc + old_total_acc) / 3
            
            if avg_new_acc > avg_old_acc:
                overall_improvement = (avg_new_acc - avg_old_acc) * 100
                report.append(f"\n🎯 Общее улучшение: +{overall_improvement:.2f}%")
                report.append("✅ Рекомендуется использовать новые модели")
            else:
                report.append("\n⚠️ Новые модели не показали улучшения")
                report.append("🔍 Требуется дополнительный анализ")
        
        report.append("")
        report.append("=" * 80)
        report.append(f"Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 80)
        
        return "\n".join(report)


def main():
    """Основная функция тестирования."""
    tester = HistoricalTester()
    
    # Тестируем новые модели
    logger.info("Запуск тестирования улучшенных моделей...")
    results = tester.test_historical_accuracy(limit=500)
    
    # Сравниваем с существующими прогнозами
    comparison_results = tester.compare_with_existing_predictions(limit=500)
    
    # Генерируем отчет
    report = tester.generate_report(results, comparison_results)
    
    # Сохраняем отчет
    report_file = f'results/quality/historical_test_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    logger.info(f"\n📄 Отчет сохранен: {report_file}")
    print("\n" + report)


if __name__ == '__main__':
    main()
