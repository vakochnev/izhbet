# izhbet/run_pipeline.py
"""
Единый интерфейс для запуска всей цепочки обработки данных.
Координирует работу модулей processing → forecast → publisher.
"""

import argparse
import logging
import sys
from datetime import datetime

from core.integration_service import IntegrationService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(name)s->%(funcName)s():%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def main():
    """Основная функция для запуска пайплайна."""
    parser = argparse.ArgumentParser(
        description='Единый интерфейс для запуска всей цепочки обработки данных',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python run_pipeline.py today          # Полная цепочка для сегодня
  python run_pipeline.py all_time       # Полная цепочка для всего времени
  python run_pipeline.py processing     # Только этап processing
  python run_pipeline.py forecast       # Только этап forecast
  python run_pipeline.py publisher      # Только этап publisher
  python run_pipeline.py status         # Статус всех компонентов
        """
    )
    
    parser.add_argument(
        'mode',
        choices=['today', 'all_time', 'processing', 'forecast', 'publisher', 'status'],
        help='Режим работы пайплайна'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Подробный вывод'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Создаем интеграционный сервис
        integration_service = IntegrationService()
        
        # Выводим заголовок
        print_header(args.mode)
        
        # Выполняем соответствующий режим
        if args.mode == 'today':
            run_today_mode(integration_service)
        elif args.mode == 'all_time':
            run_all_time_mode(integration_service)
        elif args.mode == 'processing':
            run_processing_only(integration_service)
        elif args.mode == 'forecast':
            run_forecast_only(integration_service)
        elif args.mode == 'publisher':
            run_publisher_only(integration_service)
        elif args.mode == 'status':
            show_status(integration_service)
        
        # Выводим сообщение о завершении
        print_completion()
        
    except KeyboardInterrupt:
        logger.info('Выполнение прервано пользователем')
        sys.exit(0)
    except Exception as e:
        logger.error(f'Критическая ошибка: {e}')
        sys.exit(1)


def print_header(mode: str) -> None:
    """Выводит заголовок выполнения."""
    print("=" * 80)
    print("🚀 ЗАПУСК ИНТЕГРИРОВАННОГО ПАЙПЛАЙНА ОБРАБОТКИ ДАННЫХ")
    print("=" * 80)
    print(f"📋 Режим работы: {mode.upper()}")
    print(f"⏰ Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()


def print_completion() -> None:
    """Выводит сообщение о завершении."""
    print()
    print("=" * 80)
    print("✅ ПАЙПЛАЙН ОБРАБОТКИ ДАННЫХ ЗАВЕРШЕН УСПЕШНО")
    print(f"⏰ Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


def run_today_mode(integration_service: IntegrationService) -> None:
    """Запускает режим TODAY - полная цепочка для сегодня."""
    logger.info('Запуск режима TODAY - полная цепочка для сегодня')
    
    results = integration_service.run_full_pipeline('TODAY')
    
    if results['success']:
        logger.info('Режим TODAY выполнен успешно')
        print_results_summary(results)
    else:
        logger.error('Ошибка в режиме TODAY')
        print_error_summary(results)
        sys.exit(1)


def run_all_time_mode(integration_service: IntegrationService) -> None:
    """Запускает режим ALL_TIME - полная цепочка для всего времени."""
    logger.info('Запуск режима ALL_TIME - полная цепочка для всего времени')
    
    results = integration_service.run_full_pipeline('ALL_TIME')
    
    if results['success']:
        logger.info('Режим ALL_TIME выполнен успешно')
        print_results_summary(results)
    else:
        logger.error('Ошибка в режиме ALL_TIME')
        print_error_summary(results)
        sys.exit(1)


def run_processing_only(integration_service: IntegrationService) -> None:
    """Запускает только этап processing."""
    logger.info('Запуск только этапа processing')
    
    results = integration_service.run_processing_only()
    
    if results['success']:
        logger.info('Этап processing выполнен успешно')
        print_stage_results('PROCESSING', results)
    else:
        logger.error('Ошибка на этапе processing')
        print_stage_error('PROCESSING', results)
        sys.exit(1)


def run_forecast_only(integration_service: IntegrationService) -> None:
    """Запускает только этап forecast."""
    logger.info('Запуск только этапа forecast')
    
    results = integration_service.run_forecast_only()
    
    if results['success']:
        logger.info('Этап forecast выполнен успешно')
        print_stage_results('FORECAST', results)
    else:
        logger.error('Ошибка на этапе forecast')
        print_stage_error('FORECAST', results)
        sys.exit(1)


def run_publisher_only(integration_service: IntegrationService) -> None:
    """Запускает только этап publisher."""
    logger.info('Запуск только этапа publisher')
    
    results = integration_service.run_publisher_only()
    
    if results['success']:
        logger.info('Этап publisher выполнен успешно')
        print_stage_results('PUBLISHER', results)
    else:
        logger.error('Ошибка на этапе publisher')
        print_stage_error('PUBLISHER', results)
        sys.exit(1)


def show_status(integration_service: IntegrationService) -> None:
    """Показывает статус всех компонентов."""
    logger.info('Получение статуса всех компонентов')
    
    status = integration_service.get_pipeline_status()
    
    print("📊 СТАТУС КОМПОНЕНТОВ ПАЙПЛАЙНА")
    print("=" * 50)
    
    for component, info in status['components'].items():
        print(f"🔧 {component.upper()}:")
        print(f"   Статус: {info['status']}")
        print(f"   Путь: {info['path']}")
        if 'error' in info:
            print(f"   Ошибка: {info['error']}")
        print()
    
    print(f"⏰ Время проверки: {status['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")


def print_results_summary(results: dict) -> None:
    """Выводит сводку результатов выполнения."""
    print("📊 СВОДКА РЕЗУЛЬТАТОВ")
    print("=" * 50)
    print(f"✅ Общий результат: УСПЕШНО")
    print(f"⏱️  Время выполнения: {results.get('duration', 0):.2f} секунд")
    print()
    
    # Результаты по этапам
    stages = ['processing', 'forecast', 'publisher']
    for stage in stages:
        if stage in results and results[stage]:
            stage_result = results[stage]
            status = "✅ УСПЕШНО" if stage_result['success'] else "❌ ОШИБКА"
            print(f"🔧 {stage.upper()}: {status}")
            if 'command' in stage_result:
                print(f"   Команда: {stage_result['command']}")
        else:
            print(f"🔧 {stage.upper()}: ⏭️  ПРОПУЩЕН")
        print()


def print_error_summary(results: dict) -> None:
    """Выводит сводку ошибок."""
    print("❌ СВОДКА ОШИБОК")
    print("=" * 50)
    
    if results['errors']:
        for error in results['errors']:
            print(f"• {error}")
        print()
    
    # Ошибки по этапам
    stages = ['processing', 'forecast', 'publisher']
    for stage in stages:
        if stage in results and results[stage] and not results[stage]['success']:
            stage_result = results[stage]
            print(f"🔧 {stage.upper()}:")
            if 'error' in stage_result:
                print(f"   Ошибка: {stage_result['error']}")
            if 'stderr' in stage_result:
                print(f"   Детали: {stage_result['stderr'][:200]}...")
            print()


def print_stage_results(stage_name: str, results: dict) -> None:
    """Выводит результаты выполнения этапа."""
    print(f"📊 РЕЗУЛЬТАТЫ ЭТАПА {stage_name}")
    print("=" * 50)
    print(f"✅ Результат: УСПЕШНО")
    if 'command' in results:
        print(f"🔧 Команда: {results['command']}")
    print()


def print_stage_error(stage_name: str, results: dict) -> None:
    """Выводит ошибки выполнения этапа."""
    print(f"❌ ОШИБКИ ЭТАПА {stage_name}")
    print("=" * 50)
    
    if 'error' in results:
        print(f"Ошибка: {results['error']}")
    if 'stderr' in results:
        print(f"Детали: {results['stderr'][:200]}...")
    print()


if __name__ == '__main__':
    main()
