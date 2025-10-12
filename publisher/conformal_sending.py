# izhbet/publisher/conformal_sending.py
"""
Специальные публикаторы для конформных прогнозов с правильным именованием файлов.
"""

import os
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ConformalPublisher(ABC):
    """Базовый класс для публикаторов конформных прогнозов"""
    
    @abstractmethod
    def publish(self, message: dict):
        pass


class ConformalDailyPublisher(ConformalPublisher):
    """
    Публикатор для ежедневных конформных прогнозов.
    Создает два файла:
    - {вчера}_итоги.txt - для итогов вчерашних матчей
    - {сегодня}_прогноз.txt - для прогнозов на сегодня
    """
    
    def __init__(self, file: str):
        self.file = file

    def publish(self, message: dict):
        """
        Публикует конформные прогнозы в файлы.
        
        Args:
            message: Поддерживаются форматы:
                - {'yesterday': str, 'today': str} для режима TODAY
                - {'date': 'YYYY-MM-DD', 'tournament_id': int, 'forecasts': str} для режима ALL_TIME
        """
        # Вариант 1: режим TODAY (вчера+сегодня)
        if 'yesterday' in message or 'today' in message or 'today_quality' in message or 'today_regular' in message:
            from db.storage.publisher import save_conformal_report
            
            today = datetime.now()
            yesterday = today - timedelta(days=1)

            if message.get('yesterday'):
                save_conformal_report(message['yesterday'], 'outcomes', yesterday, self.file)
                logger.info(f'Файл итогов сохранен: outcomes/{yesterday.strftime("%Y/%m")}/{yesterday.strftime("%Y-%m-%d")}_итоги.txt')

            if message.get('today'):
                save_conformal_report(message['today'], 'forecasts', today, self.file)
                logger.info(f'Файл прогнозов сохранен: forecasts/{today.strftime("%Y/%m")}/{today.strftime("%Y-%m-%d")}_прогноз.txt')
            
            if message.get('today_quality'):
                save_conformal_report(message['today_quality'], 'quality', today, self.file)
                logger.info(f'Файл качественных прогнозов сохранен: forecasts/{today.strftime("%Y/%m")}/{today.strftime("%Y-%m-%d")}_quality.txt')
            
            if message.get('today_regular'):
                save_conformal_report(message['today_regular'], 'regular', today, self.file)
                logger.info(f'Файл обычных прогнозов сохранен: forecasts/{today.strftime("%Y/%m")}/{today.strftime("%Y-%m-%d")}_regular.txt')

            return

        # Вариант 2: режим ALL_TIME (на конкретную дату) - прогнозы
        if {'date', 'forecasts'}.issubset(message.keys()):
            try:
                date_obj = datetime.strptime(message['date'], '%Y-%m-%d')
            except Exception:
                logger.error(f"Неверный формат даты в сообщении: {message.get('date')}")
                return

            # forecasts
            from db.storage.publisher import save_conformal_report
            save_conformal_report(message['forecasts'], 'forecasts', date_obj, self.file)
            logger.info(f"Файл прогноза за {message['date']} сохранен: forecasts/{date_obj.strftime('%Y/%m')}/{date_obj.strftime('%Y-%m-%d')}_прогноз.txt")

            return

        # Вариант 2c: режим ежедневных качественных отчетов
        if {'date', 'daily_quality', 'report_type'}.issubset(message.keys()):
            try:
                date_obj = datetime.strptime(message['date'], '%Y-%m-%d')
            except Exception:
                logger.error(f"Неверный формат даты в сообщении: {message.get('date')}")
                return

            # Сохраняем качественный отчет за день
            from db.storage.publisher import save_conformal_report
            save_conformal_report(message['daily_quality'], 'quality', date_obj, self.file)
            logger.info(f"Качественный отчет за {message['date']} сохранен: forecast/{date_obj.strftime('%Y/%m')}/{date_obj.strftime('%Y-%m-%d')}_quality.txt")

            return

        # Вариант 2d: режим ежедневных обычных отчетов
        if {'date', 'daily_regular', 'report_type'}.issubset(message.keys()):
            try:
                date_obj = datetime.strptime(message['date'], '%Y-%m-%d')
            except Exception:
                logger.error(f"Неверный формат даты в сообщении: {message.get('date')}")
                return

            # Сохраняем обычный отчет за день
            from db.storage.publisher import save_conformal_report
            save_conformal_report(message['daily_regular'], 'regular', date_obj, self.file)
            logger.info(f"Обычный отчет за {message['date']} сохранен: forecast/{date_obj.strftime('%Y/%m')}/{date_obj.strftime('%Y-%m-%d')}_regular.txt")

            return

        # Вариант 2e: режим ежедневных отчетов с итогами
        if {'date', 'daily_outcome', 'report_type'}.issubset(message.keys()):
            try:
                date_obj = datetime.strptime(message['date'], '%Y-%m-%d')
            except Exception:
                logger.error(f"Неверный формат даты в сообщении: {message.get('date')}")
                return

            # Сохраняем отчет с итогами за день
            from db.storage.publisher import save_conformal_report
            save_conformal_report(message['daily_outcome'], 'outcome', date_obj, self.file)
            logger.info(f"Отчет с итогами за {message['date']} сохранен: outcome/{date_obj.strftime('%Y/%m')}/{date_obj.strftime('%Y-%m-%d')}_outcome.txt")

            return

        # Вариант 2f: режим ежедневных качественных итогов
        if {'date', 'daily_quality_outcome', 'report_type'}.issubset(message.keys()):
            try:
                date_obj = datetime.strptime(message['date'], '%Y-%m-%d')
            except Exception:
                logger.error(f"Неверный формат даты в сообщении: {message.get('date')}")
                return

            # Сохраняем отчет с качественными итогами за день
            from db.storage.publisher import save_conformal_report
            save_conformal_report(message['daily_quality_outcome'], 'quality_outcome', date_obj, self.file)
            logger.info(f"Отчет с качественными итогами за {message['date']} сохранен: outcome/{date_obj.strftime('%Y/%m')}/{date_obj.strftime('%Y-%m-%d')}_quality_outcome.txt")

            return

        # Вариант 2g: режим ежедневных обычных итогов
        if {'date', 'daily_regular_outcome', 'report_type'}.issubset(message.keys()):
            try:
                date_obj = datetime.strptime(message['date'], '%Y-%m-%d')
            except Exception:
                logger.error(f"Неверный формат даты в сообщении: {message.get('date')}")
                return

            # Сохраняем отчет с обычными итогами за день
            from db.storage.publisher import save_conformal_report
            save_conformal_report(message['daily_regular_outcome'], 'regular_outcome', date_obj, self.file)
            logger.info(f"Отчет с обычными итогами за {message['date']} сохранен: outcome/{date_obj.strftime('%Y/%m')}/{date_obj.strftime('%Y-%m-%d')}_regular_outcome.txt")

            return

        # Вариант 3: режим TODAY/YESTERDAY с указанием типа папки
        if 'folder_type' in message:
            from db.storage.publisher import save_conformal_report
            
            today = datetime.now()
            folder_type = message['folder_type']
            
            # Определяем содержимое для сохранения
            content = None
            if 'today' in message:
                content = message['today']
            elif 'yesterday' in message:
                content = message['yesterday']
            
            if content:
                save_conformal_report(content, folder_type, today, self.file)
                logger.info(f'Файл {folder_type} сохранен: {folder_type}/{today.strftime("%Y/%m")}/{today.strftime("%Y-%m-%d")}_{"прогноз" if folder_type == "forecasts" else "итоги"}.txt')
            
            return

        # Вариант 4: режим ALL_TIME (статистика за весь период) - устаревший
        if 'all_time' in message:
            from db.storage.publisher import save_conformal_report
            
            today = datetime.now()
            
            # Сохраняем отчет по статистике в forecasts (по умолчанию)
            save_conformal_report(message['all_time'], 'forecasts', today, self.file)
            logger.info(f'Файл статистики сохранен: forecasts/{today.strftime("%Y/%m")}/{today.strftime("%Y-%m-%d")}_прогноз.txt')
            
            return

        logger.warning('Сообщение для публикации не распознано: отсутствуют ожидаемые ключи')


class ConformalSummaryPublisher(ConformalPublisher):
    """
    Публикатор для итогового отчета конформных прогнозов.
    Создает файл {вчера}_итоги.txt с полным отчетом.
    """
    
    def __init__(self, file: str):
        self.file = file

    def publish(self, message: dict):
        """
        Публикует итоговый отчет конформных прогнозов.
        
        Args:
            message: Словарь с ключом 'summary'
        """
        # Получаем дату вчера для именования файла
        yesterday = datetime.now() - timedelta(days=1)
        
        # Формируем путь к директории
        dir_path = os.path.join(
            self.file, 
            yesterday.strftime('%Y'), 
            yesterday.strftime('%m')
        )
        
        # Создаем директорию
        os.makedirs(dir_path, exist_ok=True)
        
        # Формируем имя файла
        file_name = f'{yesterday.strftime("%Y-%m-%d")}_итоги.txt'
        file_path = os.path.join(dir_path, file_name)
        
        # Записываем отчет
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(message['summary'])
        
        logger.info(f'Итоговый файл сохранен: {file_path}')


class ConformalForecastPublisher(ConformalPublisher):
    """
    Публикатор для прогнозов конформных прогнозов.
    Создает файл {сегодня}_прогноз.txt.
    """
    
    def __init__(self, file: str):
        self.file = file

    def publish(self, message: dict):
        """
        Публикует прогнозы конформных прогнозов.
        
        Args:
            message: Словарь с ключом 'today'
        """
        # Получаем текущую дату
        today = datetime.now()
        
        # Формируем путь к директории
        dir_path = os.path.join(
            self.file, 
            today.strftime('%Y'), 
            today.strftime('%m')
        )
        
        # Создаем директорию
        os.makedirs(dir_path, exist_ok=True)
        
        # Формируем имя файла
        file_name = f'{today.strftime("%Y-%m-%d")}_прогноз.txt'
        file_path = os.path.join(dir_path, file_name)
        
        # Записываем прогнозы
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(message['today'])
        
        logger.info(f'Файл прогнозов сохранен: {file_path}')
