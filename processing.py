# izhbet/processing.py
"""
Основной модуль обработки данных для построения моделей и выполнения прогнозов.
"""

import logging
import sys
import os
from sys import argv
from typing import List, Optional

from core.constants import ACTION_MODEL
from processing.pipeline import EmbeddingCalculationPipeline
from processing.datasource import DatabaseSource
from processing.balancing_config import ProcessFeatures
from processing.datastorage import FileStorage
# Конформное прогнозирование перенесено в модуль forecast
from subprocess import run, CalledProcessError

logger = logging.getLogger(__name__)


def main() -> None:
    """Основная функция модуля обработки данных."""
    try:
        logger.info('Запуск модуля обработки данных')

        action = _get_action_from_args()
        _process_data(action)

        logger.info('Модуль обработки данных завершил работу успешно.')
        
    except KeyboardInterrupt:
        logger.info('Обработка прервана пользователем')
        sys.exit(0)
    except Exception as e:
        logger.error(f'Критическая ошибка в main(): {e}')
        sys.exit(1)


def _get_action_from_args() -> str:
    """
    Получение действия из аргументов командной строки.

    Returns:
        Действие для выполнения
    """
    if len(argv) < 2:
        action = ACTION_MODEL[1]  # CREATE_PROGNOZ по умолчанию
        logger.info(
            f'Аргумент не указан, используется действие '
            f'по умолчанию: {action}'
        )
    elif argv[1] in ['-h', '--help']:
        _show_help()
        sys.exit(0)
    else:
        action = argv[1]
        if action not in ACTION_MODEL:
            logger.warning(
                f'Неизвестное действие "{action}", используется '
                f'по умолчанию: {ACTION_MODEL[1]}'
            )
            action = ACTION_MODEL[1]

    logger.info(f'Выбранное действие: {action}')
    return action


def _show_help() -> None:
    """Отображение справки по использованию."""
    print("""
Модуль обработки данных для построения моделей и выполнения прогнозов.

Использование:
    python processing.py [ACTION]

Аргументы:
    ACTION    Действие для выполнения (по умолчанию: CREATE_PROGNOZ)
              Доступные действия: {actions}

Примеры:
    python processing.py CREATE_MODEL     # Создание модели
    python processing.py CREATE_PROGNOZ   # Выполнение прогнозов
    python processing.py                  # Использование действия по умолчанию
    python processing.py --help           # Показать эту справку
    """.format(actions=', '.join(ACTION_MODEL)))


def _process_data(action: str) -> None:
    """
    Обработка данных с помощью pipeline.

    Args:
        action: Действие для выполнения
    """
    try:
        logger.info(
            f'Инициализация компонентов pipeline '
            f'для действия: {action}'
        )
        
        # Создание компонентов pipeline
        data_source = DatabaseSource()
        data_processor = ProcessFeatures(action)
        data_storage = FileStorage()

        # Создание и запуск конвейера
        pipeline = EmbeddingCalculationPipeline(
            data_source=data_source,
            data_processor=data_processor,
            data_storage=data_storage
        )

        logger.info('Запуск обработки данных...')
        tournament_ids = pipeline.process_data(action)
        logger.info('Обработка данных завершена успешно')

    except Exception as e:
        logger.error(f'Ошибка при обработке данных: {e}')
        raise


if __name__ == '__main__':
    main()