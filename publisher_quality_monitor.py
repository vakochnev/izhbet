#!/usr/bin/env python3
"""
Интеграция мониторинга качества в publisher.py.
Добавляет автоматическую проверку качества после генерации прогнозов.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Добавляем путь к publisher для monitor_prediction_quality
publisher_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'publisher')
if publisher_path not in sys.path:
    sys.path.insert(0, publisher_path)

from monitor_prediction_quality import PredictionQualityMonitor

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class PublisherQualityMonitor:
    """Монитор качества для интеграции в publisher.py."""
    
    def __init__(self):
        self.monitor = PredictionQualityMonitor()
    
    def check_quality_after_prediction(self, hours: int = 24) -> Dict[str, Any]:
        """Проверяет качество после генерации прогнозов."""
        logger.info('🔍 Проверка качества прогнозов после генерации')
        
        try:
            # Получаем недавние прогнозы
            predictions = self.monitor.get_recent_predictions(hours)
            
            if not predictions:
                logger.warning('Нет прогнозов для проверки качества')
                return {'status': 'no_data', 'message': 'Нет прогнозов для анализа'}
            
            # Оцениваем точность
            accuracy_results = self.monitor.evaluate_prediction_accuracy(predictions)
            
            # Проверяем алерты
            alerts = self.monitor.check_quality_alerts(accuracy_results)
            
            # Определяем статус
            overall_accuracy = accuracy_results.get('overall_accuracy', 0.0)
            
            if overall_accuracy < 0.3:
                status = 'critical'
                message = f'Критически низкая точность: {overall_accuracy:.2%}'
            elif overall_accuracy < 0.5:
                status = 'warning'
                message = f'Низкая точность: {overall_accuracy:.2%}'
            elif overall_accuracy < 0.7:
                status = 'acceptable'
                message = f'Удовлетворительная точность: {overall_accuracy:.2%}'
            else:
                status = 'good'
                message = f'Хорошая точность: {overall_accuracy:.2%}'
            
            result = {
                'status': status,
                'message': message,
                'overall_accuracy': overall_accuracy,
                'total_predictions': accuracy_results.get('total_predictions', 0),
                'alerts_count': len(alerts),
                'alerts': alerts,
                'by_feature': accuracy_results.get('by_feature', {})
            }
            
            # Логируем результат
            logger.info(f'📊 Качество прогнозов: {message}')
            logger.info(f'  Всего прогнозов: {result["total_predictions"]}')
            logger.info(f'  Алертов: {result["alerts_count"]}')
            
            if alerts:
                logger.warning('🚨 Обнаружены алерты:')
                for alert in alerts[:3]:  # Показываем первые 3 алерта
                    logger.warning(f'  - {alert["message"]}')
            
            return result
            
        except Exception as e:
            logger.error(f'❌ Ошибка проверки качества: {e}')
            return {'status': 'error', 'message': str(e)}
    
    def generate_quality_summary(self, quality_result: Dict[str, Any]) -> str:
        """Генерирует краткую сводку по качеству."""
        if quality_result['status'] == 'no_data':
            return "📊 Качество: Нет данных для анализа"
        
        if quality_result['status'] == 'error':
            return f"❌ Качество: Ошибка - {quality_result['message']}"
        
        status_emoji = {
            'critical': '🔴',
            'warning': '🟡',
            'acceptable': '🟢',
            'good': '🟢'
        }
        
        emoji = status_emoji.get(quality_result['status'], '❓')
        accuracy = quality_result['overall_accuracy']
        total = quality_result['total_predictions']
        alerts = quality_result['alerts_count']
        
        summary = f"{emoji} Качество: {quality_result['message']} ({total} прогнозов, {alerts} алертов)"
        
        return summary


def check_prediction_quality(hours: int = 24) -> Dict[str, Any]:
    """Функция для вызова из publisher.py."""
    monitor = PublisherQualityMonitor()
    return monitor.check_quality_after_prediction(hours)


def get_quality_summary(hours: int = 24) -> str:
    """Возвращает краткую сводку по качеству для publisher.py."""
    monitor = PublisherQualityMonitor()
    quality_result = monitor.check_quality_after_prediction(hours)
    return monitor.generate_quality_summary(quality_result)


if __name__ == '__main__':
    # Тестирование
    print("🧪 Тестирование мониторинга качества для publisher.py")
    
    # Проверяем качество
    quality_result = check_prediction_quality(hours=24*7)  # За последнюю неделю
    
    # Выводим сводку
    summary = get_quality_summary(hours=24*7)
    print(f"\n{summary}")
    
    # Детальная информация
    if quality_result['status'] != 'no_data' and quality_result['status'] != 'error':
        print(f"\n📊 Детали:")
        print(f"  Статус: {quality_result['status']}")
        print(f"  Точность: {quality_result['overall_accuracy']:.2%}")
        print(f"  Прогнозов: {quality_result['total_predictions']}")
        print(f"  Алертов: {quality_result['alerts_count']}")
        
        if quality_result['alerts']:
            print(f"\n🚨 Алерты:")
            for alert in quality_result['alerts'][:3]:
                print(f"  - {alert['message']}")
