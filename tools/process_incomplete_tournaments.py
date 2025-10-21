#!/usr/bin/env python3
"""
Скрипт для обработки турниров с неполным набором моделей.
"""

import sys
import logging
from typing import List

from core.constants import ACTION_MODEL
from processing.pipeline import EmbeddingCalculationPipeline
from processing.datasource import DatabaseSource
from processing.balancing_config import ProcessFeatures
from processing.datastorage import FileStorage

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(filename)s->%(funcName)s():%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/incomplete_tournaments.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def process_specific_tournaments(tournament_ids: List[int]) -> None:
    """
    Обработка конкретных турниров.
    
    Args:
        tournament_ids: Список ID турниров для обработки
    """
    try:
        logger.info(f'Начало обработки {len(tournament_ids)} турниров: {tournament_ids}')
        
        # Создание компонентов pipeline
        data_source = DatabaseSource()
        data_processor = ProcessFeatures(ACTION_MODEL[0])  # CREATE_MODEL
        data_storage = FileStorage()
        
        # Переопределяем список турниров
        data_source.tournaments_id = tournament_ids
        
        # Создание и запуск конвейера
        pipeline = EmbeddingCalculationPipeline(
            data_source=data_source,
            data_processor=data_processor,
            data_storage=data_storage
        )
        
        logger.info(f'Запуск обработки {len(tournament_ids)} турниров...')
        pipeline._process_tournaments_parallel(ACTION_MODEL[0])
        logger.info('Обработка турниров завершена успешно')
        
    except Exception as e:
        logger.error(f'Ошибка при обработке турниров: {e}')
        raise


def main():
    """Основная функция."""
    # Читаем ID турниров из файла
    try:
        with open('incomplete_tournaments.txt', 'r') as f:
            tournament_ids = [int(line.strip()) for line in f if line.strip()]
        
        if not tournament_ids:
            logger.warning('Файл incomplete_tournaments.txt пуст')
            return
        
        logger.info(f'Загружено {len(tournament_ids)} турниров для обработки')
        
        # Обрабатываем турниры
        process_specific_tournaments(tournament_ids)
        
        logger.info('Все турниры обработаны. Запустите check_models.py для проверки.')
        
    except FileNotFoundError:
        logger.error('Файл incomplete_tournaments.txt не найден. Сначала запустите check_models.py')
        sys.exit(1)
    except Exception as e:
        logger.error(f'Критическая ошибка: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()

