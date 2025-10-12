import os
import logging
from abc import ABC, abstractmethod
from datetime import datetime


logger = logging.getLogger(__name__)


class Publisher(ABC):
    """
    Публикация (Наблюдатель)
    """
    @abstractmethod
    def publish(self, message: str):
        pass


class TelegramPublisher(Publisher):
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    def publish(self, message: str):
        logger.info(f'Telegram publication to {self.chat_id}: {message[:50]}...')


class VkPublisher(Publisher):
    def __init__(self, token: str, group_id: int):
        self.token = token
        self.group_id = group_id

    def publish(self, message: str):
        logger.info(f'VK publication to group {self.group_id}: {message[:50]}...')


class DailyForecastPublisher(Publisher):
    def __init__(self, file: str):
        self.file = file

    def publish(self, message):
        # Получаем текущую дату и время
        now = datetime.now()

        # Формируем путь к директории в формате ГОД/МЕСЯЦ/ДЕНЬ
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')

        # Создаем полный путь к директории
        dir_path = os.path.join(self.file, year, month)

        # Создаем директории, если их нет
        os.makedirs(dir_path, exist_ok=True)

        # Формируем имя файла с временной меткой
        file_name = f'{year}-{month}-{day}_прогноз.txt'

        # Полный путь к файлу
        file_path = os.path.join(dir_path, file_name)

        # Записываем строку в файл
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(message['yesterday'])
            file.write(message['today'])

        logger.info(
            f'Файл сохранен (итоги вчера, прогноз сегодня): '
            f'{file_path}'
        )


class FinalReportPublisher(Publisher):
    def __init__(self, file: str):
        self.file = file

    def publish(self, message: str):
        # Получаем текущую дату и время
        now = datetime.now()

        # Формируем путь к директории в формате ГОД/МЕСЯЦ/ДЕНЬ
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')

        # Создаем полный путь к директории
        dir_path = os.path.join(self.file, year, month)

        # Создаем директории, если их нет
        os.makedirs(dir_path, exist_ok=True)

        # Формируем имя файла с временной меткой
        file_name = f'{year}-{month}-{day}_итоги.txt'

        # Полный путь к файлу
        file_path = os.path.join(dir_path, file_name)

        # Записываем строку в файл
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(message['summary'])

        logger.info(f'Файл сохранен (итоги за день): {file_path}')
