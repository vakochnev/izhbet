"""
Основной модуль для запуска процесса загрузки и обработки спортивной статистики.

Модуль получает аргумент командной строки для определения типа операции
и запускает соответствующий процесс загрузки данных.
"""
import logging
from sys import argv

from core.constants import OPERATIONS
from getting.download import Download


logger = logging.getLogger(__name__)


def main() -> None:
    """
    Основная функция для запуска процесса загрузки данных.

    Получает тип операции из аргументов командной строки или использует значение по умолчанию.
    Запускает процесс загрузки данных через класс Download.
    """
    try:
        operation = argv[1]
    except IndexError:
        operation = OPERATIONS[1]
    else:
        if operation not in OPERATIONS:
            operation = OPERATIONS[1]

    logger.info(
        f'Запущен скрипт, getting.py c параметром: {operation}'
    )
    download_data = Download(operation)
    download_data.download_sportradar()


if __name__ == '__main__':
    main()
