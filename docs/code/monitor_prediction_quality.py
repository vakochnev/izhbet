#!/usr/bin/env python3
"""
Система мониторинга качества прогнозов.
Отслеживает точность прогнозов в реальном времени и генерирует отчеты.
"""

import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json
from pathlib import Path

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Session_pool
from db.models import Feature, Match, Outcome
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class PredictionQualityMonitor:
    """Монитор качества прогнозов в реальном времени."""
    
    def __init__(self, output_dir: str = 'results/monitoring'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Настройки мониторинга
        self.monitoring_config = {
            'update_interval_hours': 6,  # Обновление каждые 6 часов
            'alert_threshold_accuracy': 0.3,  # Алерт при точности < 30%
            'min_predictions_for_analysis': 10,  # Минимум прогнозов для анализа
            'retention_days': 30  # Хранение данных за 30 дней
        }
    
    def get_recent_predictions(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Получает недавние прогнозы с результатами."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with Session_pool() as db:
            query = db.query(Outcome, Match).join(Match, Outcome.match_id == Match.id).filter(
                Match.numOfHeadsHome.isnot(None),
                Match.numOfHeadsAway.isnot(None),
                Match.gameData >= cutoff_time
            ).order_by(Match.gameData.desc())
            
            results = query.all()
            
            predictions = []
            for outcome, match in results:
                prediction_data = {
                    'match_id': match.id,
                    'feature_code': outcome.feature,
                    'predicted_outcome': outcome.outcome,
                    'predicted_forecast': outcome.forecast,
                    'confidence': outcome.confidence,
                    'uncertainty': outcome.uncertainty,
                    'match_date': match.gameData,
                    'home_goals': match.numOfHeadsHome,
                    'away_goals': match.numOfHeadsAway,
                    'sport_id': match.sport_id,
                    'tournament_id': match.tournament_id
                }
                predictions.append(prediction_data)
            
            logger.info(f'Найдено {len(predictions)} прогнозов за последние {hours} часов')
            return predictions
    
    def evaluate_prediction_accuracy(self, predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Оценивает точность прогнозов."""
        if not predictions:
            return {'error': 'Нет прогнозов для анализа'}
        
        results = {
            'total_predictions': len(predictions),
            'by_feature': {},
            'by_model': {},
            'by_tournament': {},
            'overall_accuracy': 0.0,
            'confidence_stats': {
                'avg_confidence': 0.0,
                'high_confidence_accuracy': 0.0,
                'low_confidence_accuracy': 0.0
            }
        }
        
        # Группируем по типам прогнозов
        feature_groups = {}
        for pred in predictions:
            feature_code = pred['feature_code']
            if feature_code not in feature_groups:
                feature_groups[feature_code] = []
            feature_groups[feature_code].append(pred)
        
        # Анализируем каждый тип прогноза
        total_correct = 0
        total_predictions = len(predictions)
        confidence_sum = 0.0
        high_conf_correct = 0
        high_conf_total = 0
        low_conf_correct = 0
        low_conf_total = 0
        
        for feature_code, preds in feature_groups.items():
            correct = 0
            for pred in preds:
                is_correct = self._evaluate_single_prediction(pred)
                if is_correct:
                    correct += 1
                    total_correct += 1
                
                # Статистика по уверенности
                confidence = pred.get('confidence', 0.0)
                confidence_sum += confidence
                
                if confidence >= 0.7:  # Высокая уверенность
                    high_conf_total += 1
                    if is_correct:
                        high_conf_correct += 1
                else:  # Низкая уверенность
                    low_conf_total += 1
                    if is_correct:
                        low_conf_correct += 1
            
            accuracy = correct / len(preds) if preds else 0.0
            
            feature_name = self._get_feature_name(feature_code)
            results['by_feature'][feature_name] = {
                'total': len(preds),
                'correct': correct,
                'accuracy': accuracy,
                'feature_code': feature_code
            }
        
        # Общая точность
        results['overall_accuracy'] = total_correct / total_predictions if total_predictions > 0 else 0.0
        
        # Статистика по уверенности
        results['confidence_stats']['avg_confidence'] = confidence_sum / total_predictions if total_predictions > 0 else 0.0
        results['confidence_stats']['high_confidence_accuracy'] = high_conf_correct / high_conf_total if high_conf_total > 0 else 0.0
        results['confidence_stats']['low_confidence_accuracy'] = low_conf_correct / low_conf_total if low_conf_total > 0 else 0.0
        
        return results
    
    def _evaluate_single_prediction(self, prediction: Dict[str, Any]) -> bool:
        """Оценивает правильность одного прогноза."""
        feature_code = prediction['feature_code']
        predicted_outcome = prediction['predicted_outcome']
        home_goals = prediction['home_goals']
        away_goals = prediction['away_goals']
        total_goals = home_goals + away_goals
        
        if feature_code == 1:  # win_draw_loss
            actual = 'п1' if home_goals > away_goals else ('н' if home_goals == away_goals else 'п2')
            return predicted_outcome == actual
        
        elif feature_code == 2:  # oz
            actual = 'обе забьют - да' if home_goals > 0 and away_goals > 0 else 'обе забьют - нет'
            return predicted_outcome == actual
        
        elif feature_code == 8:  # total_amount
            pred_over = predicted_outcome == 'ТБ'
            actual_over = total_goals > 2.5
            return pred_over == actual_over
        
        return False
    
    def _get_feature_name(self, feature_code: int) -> str:
        """Возвращает название типа прогноза."""
        feature_names = {
            1: 'WDL',
            2: 'OZ',
            3: 'Goal Home',
            4: 'Goal Away',
            5: 'Total',
            6: 'Total Home',
            7: 'Total Away',
            8: 'Total Amount',
            9: 'Total Home Amount',
            10: 'Total Away Amount'
        }
        return feature_names.get(feature_code, f'Feature {feature_code}')
    
    def check_quality_alerts(self, accuracy_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Проверяет качество и генерирует алерты."""
        alerts = []
        threshold = self.monitoring_config['alert_threshold_accuracy']
        
        # Проверяем общую точность
        overall_accuracy = accuracy_results.get('overall_accuracy', 0.0)
        if overall_accuracy < threshold:
            alerts.append({
                'type': 'LOW_OVERALL_ACCURACY',
                'severity': 'HIGH',
                'message': f'Общая точность прогнозов критически низкая: {overall_accuracy:.2%}',
                'value': overall_accuracy,
                'threshold': threshold
            })
        
        # Проверяем точность по типам прогнозов
        for feature_name, data in accuracy_results.get('by_feature', {}).items():
            if data['total'] >= self.monitoring_config['min_predictions_for_analysis']:
                accuracy = data['accuracy']
                if accuracy < threshold:
                    alerts.append({
                        'type': 'LOW_FEATURE_ACCURACY',
                        'severity': 'MEDIUM',
                        'message': f'Низкая точность для {feature_name}: {accuracy:.2%}',
                        'feature': feature_name,
                        'value': accuracy,
                        'threshold': threshold
                    })
        
        # Проверяем статистику по уверенности
        conf_stats = accuracy_results.get('confidence_stats', {})
        high_conf_acc = conf_stats.get('high_confidence_accuracy', 0.0)
        if high_conf_acc < 0.5:  # Высокоуверенные прогнозы должны быть точными
            alerts.append({
                'type': 'LOW_HIGH_CONFIDENCE_ACCURACY',
                'severity': 'HIGH',
                'message': f'Низкая точность высокоуверенных прогнозов: {high_conf_acc:.2%}',
                'value': high_conf_acc,
                'threshold': 0.5
            })
        
        return alerts
    
    def generate_monitoring_report(self, accuracy_results: Dict[str, Any], alerts: List[Dict[str, Any]]) -> str:
        """Генерирует отчет мониторинга."""
        report = []
        report.append("=" * 80)
        report.append("ОТЧЕТ МОНИТОРИНГА КАЧЕСТВА ПРОГНОЗОВ")
        report.append("=" * 80)
        report.append(f"Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Общая статистика
        report.append("📊 ОБЩАЯ СТАТИСТИКА:")
        report.append("-" * 40)
        report.append(f"Всего прогнозов: {accuracy_results.get('total_predictions', 0)}")
        report.append(f"Общая точность: {accuracy_results.get('overall_accuracy', 0.0):.2%}")
        
        conf_stats = accuracy_results.get('confidence_stats', {})
        report.append(f"Средняя уверенность: {conf_stats.get('avg_confidence', 0.0):.2%}")
        report.append(f"Точность высокоуверенных: {conf_stats.get('high_confidence_accuracy', 0.0):.2%}")
        report.append(f"Точность низкоуверенных: {conf_stats.get('low_confidence_accuracy', 0.0):.2%}")
        report.append("")
        
        # Детализация по типам прогнозов
        report.append("📈 ДЕТАЛИЗАЦИЯ ПО ТИПАМ ПРОГНОЗОВ:")
        report.append("-" * 40)
        
        by_feature = accuracy_results.get('by_feature', {})
        for feature_name, data in by_feature.items():
            if data['total'] > 0:
                report.append(f"{feature_name}:")
                report.append(f"  Прогнозов: {data['total']}")
                report.append(f"  Правильных: {data['correct']}")
                report.append(f"  Точность: {data['accuracy']:.2%}")
                report.append("")
        
        # Алерты
        if alerts:
            report.append("🚨 АЛЕРТЫ:")
            report.append("-" * 40)
            
            high_severity = [a for a in alerts if a['severity'] == 'HIGH']
            medium_severity = [a for a in alerts if a['severity'] == 'MEDIUM']
            
            if high_severity:
                report.append("🔴 КРИТИЧЕСКИЕ:")
                for alert in high_severity:
                    report.append(f"  - {alert['message']}")
                report.append("")
            
            if medium_severity:
                report.append("🟡 ПРЕДУПРЕЖДЕНИЯ:")
                for alert in medium_severity:
                    report.append(f"  - {alert['message']}")
                report.append("")
        else:
            report.append("✅ АЛЕРТОВ НЕТ - СИСТЕМА РАБОТАЕТ НОРМАЛЬНО")
            report.append("")
        
        # Рекомендации
        report.append("💡 РЕКОМЕНДАЦИИ:")
        report.append("-" * 40)
        
        overall_accuracy = accuracy_results.get('overall_accuracy', 0.0)
        
        if overall_accuracy < 0.3:
            report.append("🔴 КРИТИЧЕСКАЯ СИТУАЦИЯ:")
            report.append("  - Требуется немедленный пересмотр моделей")
            report.append("  - Проверьте качество входных данных")
            report.append("  - Рассмотрите возможность отката к предыдущей версии")
        elif overall_accuracy < 0.5:
            report.append("🟡 ТРЕБУЕТСЯ ВНИМАНИЕ:")
            report.append("  - Рекомендуется улучшение моделей")
            report.append("  - Проанализируйте проблемные типы прогнозов")
            report.append("  - Рассмотрите дополнительное обучение")
        elif overall_accuracy < 0.7:
            report.append("🟢 УДОВЛЕТВОРИТЕЛЬНО:")
            report.append("  - Система работает в пределах нормы")
            report.append("  - Есть потенциал для улучшения")
            report.append("  - Продолжайте мониторинг")
        else:
            report.append("🟢 ОТЛИЧНО:")
            report.append("  - Система показывает высокое качество")
            report.append("  - Продолжайте текущую стратегию")
            report.append("  - Рассмотрите оптимизацию производительности")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_monitoring_data(self, accuracy_results: Dict[str, Any], alerts: List[Dict[str, Any]]):
        """Сохраняет данные мониторинга."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Сохраняем результаты точности
        accuracy_file = os.path.join(self.output_dir, f'accuracy_{timestamp}.json')
        with open(accuracy_file, 'w', encoding='utf-8') as f:
            json.dump(accuracy_results, f, ensure_ascii=False, indent=2, default=str)
        
        # Сохраняем алерты
        alerts_file = os.path.join(self.output_dir, f'alerts_{timestamp}.json')
        with open(alerts_file, 'w', encoding='utf-8') as f:
            json.dump(alerts, f, ensure_ascii=False, indent=2, default=str)
        
        # Сохраняем текстовый отчет
        report = self.generate_monitoring_report(accuracy_results, alerts)
        report_file = os.path.join(self.output_dir, f'report_{timestamp}.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f'Данные мониторинга сохранены: {self.output_dir}')
        return report_file
    
    def run_monitoring_cycle(self, hours: int = 24) -> str:
        """Запускает полный цикл мониторинга."""
        logger.info(f'🔄 Запуск цикла мониторинга за последние {hours} часов')
        
        # Получаем недавние прогнозы
        predictions = self.get_recent_predictions(hours)
        
        if not predictions:
            logger.warning('Нет прогнозов для анализа')
            return None
        
        # Оцениваем точность
        accuracy_results = self.evaluate_prediction_accuracy(predictions)
        
        # Проверяем алерты
        alerts = self.check_quality_alerts(accuracy_results)
        
        # Сохраняем данные
        report_file = self.save_monitoring_data(accuracy_results, alerts)
        
        # Выводим краткий отчет
        overall_accuracy = accuracy_results.get('overall_accuracy', 0.0)
        total_predictions = accuracy_results.get('total_predictions', 0)
        
        logger.info(f'📊 Мониторинг завершен:')
        logger.info(f'  Прогнозов: {total_predictions}')
        logger.info(f'  Точность: {overall_accuracy:.2%}')
        logger.info(f'  Алертов: {len(alerts)}')
        
        if alerts:
            logger.warning('🚨 Обнаружены алерты:')
            for alert in alerts:
                logger.warning(f'  - {alert["message"]}')
        
        return report_file


def main():
    """Основная функция мониторинга."""
    monitor = PredictionQualityMonitor()
    
    # Запускаем мониторинг за последние 7 дней
    report_file = monitor.run_monitoring_cycle(hours=24*7)
    
    if report_file:
        logger.info(f'📄 Отчет сохранен: {report_file}')
        
        # Выводим краткий отчет в консоль
        with open(report_file, 'r', encoding='utf-8') as f:
            report_content = f.read()
            print("\n" + report_content)
    else:
        logger.info('Мониторинг не выполнен - нет данных для анализа')


if __name__ == '__main__':
    main()
