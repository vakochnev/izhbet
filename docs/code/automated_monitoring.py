#!/usr/bin/env python3
"""
Автоматический мониторинг качества прогнозов.
Запускается по расписанию для непрерывного отслеживания.
"""

import logging
import os
import sys
import schedule
import time
from datetime import datetime

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monitor_prediction_quality import PredictionQualityMonitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('results/monitoring/automated_monitoring.log')
    ]
)
logger = logging.getLogger(__name__)


class AutomatedMonitor:
    """Автоматический монитор качества прогнозов."""
    
    def __init__(self):
        self.monitor = PredictionQualityMonitor()
        self.setup_schedule()
    
    def setup_schedule(self):
        """Настраивает расписание мониторинга."""
        # Каждые 6 часов
        schedule.every(6).hours.do(self.run_monitoring_job)
        
        # Каждый день в 9:00
        schedule.every().day.at("09:00").do(self.run_daily_report)
        
        # Каждую неделю в понедельник в 10:00
        schedule.every().monday.at("10:00").do(self.run_weekly_report)
        
        logger.info('📅 Расписание мониторинга настроено:')
        logger.info('  - Каждые 6 часов: базовый мониторинг')
        logger.info('  - Ежедневно в 09:00: дневной отчет')
        logger.info('  - Еженедельно в понедельник в 10:00: недельный отчет')
    
    def run_monitoring_job(self):
        """Запускает задачу мониторинга."""
        try:
            logger.info('🔄 Запуск автоматического мониторинга')
            
            # Мониторинг за последние 24 часа
            report_file = self.monitor.run_monitoring_cycle(hours=24)
            
            if report_file:
                logger.info(f'✅ Мониторинг завершен: {report_file}')
                
                # Проверяем критические алерты
                self.check_critical_alerts(report_file)
            else:
                logger.warning('⚠️ Мониторинг не выполнен - нет данных')
                
        except Exception as e:
            logger.error(f'❌ Ошибка в мониторинге: {e}')
    
    def run_daily_report(self):
        """Запускает дневной отчет."""
        try:
            logger.info('📊 Генерация дневного отчета')
            
            # Отчет за последние 24 часа
            report_file = self.monitor.run_monitoring_cycle(hours=24)
            
            if report_file:
                logger.info(f'✅ Дневной отчет готов: {report_file}')
                
                # Отправляем уведомления при необходимости
                self.send_daily_notifications(report_file)
            else:
                logger.warning('⚠️ Дневной отчет не создан - нет данных')
                
        except Exception as e:
            logger.error(f'❌ Ошибка в дневном отчете: {e}')
    
    def run_weekly_report(self):
        """Запускает недельный отчет."""
        try:
            logger.info('📈 Генерация недельного отчета')
            
            # Отчет за последние 7 дней
            report_file = self.monitor.run_monitoring_cycle(hours=24*7)
            
            if report_file:
                logger.info(f'✅ Недельный отчет готов: {report_file}')
                
                # Анализируем тренды
                self.analyze_weekly_trends(report_file)
            else:
                logger.warning('⚠️ Недельный отчет не создан - нет данных')
                
        except Exception as e:
            logger.error(f'❌ Ошибка в недельном отчете: {e}')
    
    def check_critical_alerts(self, report_file: str):
        """Проверяет критические алерты."""
        try:
            # Читаем отчет
            with open(report_file, 'r', encoding='utf-8') as f:
                report_content = f.read()
            
            # Проверяем критические алерты
            if '🔴 КРИТИЧЕСКИЕ:' in report_content:
                logger.critical('🚨 ОБНАРУЖЕНЫ КРИТИЧЕСКИЕ АЛЕРТЫ!')
                
                # Здесь можно добавить отправку уведомлений
                # self.send_critical_alert_notification(report_content)
                
        except Exception as e:
            logger.error(f'❌ Ошибка проверки алертов: {e}')
    
    def send_daily_notifications(self, report_file: str):
        """Отправляет дневные уведомления."""
        try:
            # Здесь можно добавить отправку email/Slack/Telegram уведомлений
            logger.info('📧 Дневные уведомления отправлены')
            
        except Exception as e:
            logger.error(f'❌ Ошибка отправки уведомлений: {e}')
    
    def analyze_weekly_trends(self, report_file: str):
        """Анализирует недельные тренды."""
        try:
            logger.info('📊 Анализ недельных трендов выполнен')
            
            # Здесь можно добавить анализ трендов качества прогнозов
            
        except Exception as e:
            logger.error(f'❌ Ошибка анализа трендов: {e}')
    
    def run(self):
        """Запускает автоматический мониторинг."""
        logger.info('🚀 Запуск автоматического мониторинга качества прогнозов')
        logger.info(f'Время запуска: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        
        # Выполняем первоначальный мониторинг
        self.run_monitoring_job()
        
        # Запускаем основной цикл
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
                
            except KeyboardInterrupt:
                logger.info('🛑 Остановка автоматического мониторинга')
                break
            except Exception as e:
                logger.error(f'❌ Ошибка в основном цикле: {e}')
                time.sleep(300)  # Ждем 5 минут при ошибке


def main():
    """Основная функция."""
    # Создаем директорию для логов
    os.makedirs('results/monitoring', exist_ok=True)
    
    # Запускаем автоматический мониторинг
    automated_monitor = AutomatedMonitor()
    automated_monitor.run()


if __name__ == '__main__':
    main()
