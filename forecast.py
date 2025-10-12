import argparse
import logging
from datetime import datetime, timedelta

from forecast.conformal_publication import ConformalForecastGenerator
from forecast.conformal_processor import ConformalProcessor
from core.constants import FORECAST


logger = logging.getLogger(__name__)


def run_today() -> int:
    generator = ConformalForecastGenerator()
    reports = generator.generate_forecasts()
    if reports and isinstance(reports, dict):
        print(reports.get('today', ''))
        print()
        print(reports.get('yesterday', ''))
    return 0


def run_all_time() -> int:
    """Обработка всех исторических данных с формированием quality-outcomes"""
    logger.info('Запуск обработки всех исторических данных')
    
    try:
        # 1. Создаем интегрированный процессор конформного прогнозирования
        processor = ConformalProcessor(confidence_level=0.95)
        
        if not processor:
            logger.error('Не удалось создать процессор конформного прогнозирования')
            return 1
        
        # 2. Обрабатываем конформные прогнозы для всех турниров
        logger.info('Обработка конформных прогнозов для всех турниров...')
        success = processor.process_season_conformal_forecasts()
        
        if not success:
            logger.error('Ошибка при обработке конформных прогнозов')
            return 1
        
        # 3. Генерируем отчеты по качественным прогнозам
        logger.info('Генерация отчетов по качественным прогнозам...')
        generator = ConformalForecastGenerator()
        yesterday = datetime.now() - timedelta(days=1)
        generator.generate_quality_outcomes_report(yesterday)
        
        logger.info('Обработка всех исторических данных завершена успешно')
        return 0
        
    except Exception as e:
        logger.error(f'Критическая ошибка при обработке всех исторических данных: {e}')
        return 1


def main() -> int:
    # _setup_logging()

    parser = argparse.ArgumentParser(
        prog='forecast',
        description='Утилита запуска сценариев модуля прогнозов'
    )
    subparsers = parser.add_subparsers(
        dest='command',
        required=True
    )

    subparsers.add_parser(
        name='today',
        help='Сгенерировать прогнозы на сегодня и итоги вчера'
    )
    subparsers.add_parser(
        name='all_time',
        help='Обработать все исторические данные и сформировать quality-outcomes'
    )

    args = parser.parse_args()

    try:
        if args.command == 'today':
            return run_today()
        if args.command == 'all_time':
            return run_all_time()
    except Exception as exc:
        logger.exception("Ошибка выполнения команды: %s", exc)
        return 1

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
