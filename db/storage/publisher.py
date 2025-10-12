"""
Модуль для сохранения данных, связанных с публикацией прогнозов.
Содержит все операции сохранения для модуля publisher.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def save_conformal_report(content: str, report_type: str, date: datetime, output_dir: str = "results") -> str:
    """
    Сохраняет отчет конформных прогнозов в файл.
    
    Args:
        content: Содержимое отчета
        report_type: Тип отчета ('forecasts', 'outcomes', 'quality', 'regular')
        date: Дата отчета
        output_dir: Базовая директория для сохранения
        
    Returns:
        Путь к сохраненному файлу
    """
    try:
        # Определяем папку назначения
        if report_type in ['quality', 'regular']:
            # Качественные и обычные прогнозы идут в папку forecast
            folder = 'forecast'
        elif report_type in ['outcome', 'quality_outcome', 'regular_outcome']:
            # Итоги идут в папку outcome
            folder = 'outcome'
        else:
            # Остальные типы остаются как есть
            folder = report_type
        
        # Создаем поддиректорию по году и месяцу
        year_month_dir = Path(output_dir) / folder / date.strftime('%Y') / date.strftime('%m')
        year_month_dir.mkdir(parents=True, exist_ok=True)
        
        # Определяем имя файла в зависимости от типа отчета
        if report_type == 'forecasts':
            filename = f"{date.strftime('%Y-%m-%d')}_прогноз.txt"
        elif report_type == 'outcomes':
            filename = f"{date.strftime('%Y-%m-%d')}_итоги.txt"
        elif report_type == 'quality':
            filename = f"{date.strftime('%Y-%m-%d')}_quality.txt"
        elif report_type == 'regular':
            filename = f"{date.strftime('%Y-%m-%d')}_regular.txt"
        elif report_type == 'outcome':
            filename = f"{date.strftime('%Y-%m-%d')}_outcome.txt"
        elif report_type == 'quality_outcome':
            filename = f"{date.strftime('%Y-%m-%d')}_quality.txt"
        elif report_type == 'regular_outcome':
            filename = f"{date.strftime('%Y-%m-%d')}_regular.txt"
        else:
            filename = f"{date.strftime('%Y-%m-%d')}_{report_type}.txt"
        
        file_path = year_month_dir / filename
        
        # Сохраняем файл
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Отчет {report_type} сохранен: {file_path}")
        return str(file_path)
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении отчета {report_type}: {e}")
        raise


def save_quality_outcomes_report(content: str, date: datetime, output_dir: str = "results") -> str:
    """
    Сохраняет отчет с итогами качественных прогнозов в папку outcomes.
    
    Args:
        content: Содержимое отчета
        date: Дата отчета
        output_dir: Базовая директория для сохранения
        
    Returns:
        Путь к сохраненному файлу
    """
    try:
        # Создаем поддиректорию по году и месяцу в папке outcomes
        year_month_dir = Path(output_dir) / 'outcomes' / date.strftime('%Y') / date.strftime('%m')
        year_month_dir.mkdir(parents=True, exist_ok=True)
        
        # Имя файла
        filename = f"{date.strftime('%Y-%m-%d')}_качественные_итоги.txt"
        file_path = year_month_dir / filename
        
        # Сохраняем файл
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Отчет с итогами качественных прогнозов сохранен: {file_path}")
        return str(file_path)
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении отчета с итогами качественных прогнозов: {e}")
        raise


def save_quality_forecast_report(content: str, date: datetime, output_dir: str = "results") -> str:
    """
    Сохраняет отчет с качественными прогнозами в папку quality.
    
    Args:
        content: Содержимое отчета
        date: Дата отчета
        output_dir: Базовая директория для сохранения
        
    Returns:
        Путь к сохраненному файлу
    """
    try:
        # Создаем поддиректорию по году и месяцу в папке quality
        year_month_dir = Path(output_dir) / 'quality' / date.strftime('%Y') / date.strftime('%m')
        year_month_dir.mkdir(parents=True, exist_ok=True)
        
        # Имя файла
        filename = f"{date.strftime('%Y-%m-%d')}_качественные_прогнозы.txt"
        file_path = year_month_dir / filename
        
        # Сохраняем файл
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Отчет с качественными прогнозами сохранен: {file_path}")
        return str(file_path)
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении отчета с качественными прогнозами: {e}")
        raise


def create_report_directories(output_dir: str = "results") -> None:
    """
    Создает необходимые директории для отчетов.
    
    Args:
        output_dir: Базовая директория для отчетов
    """
    try:
        base_dir = Path(output_dir)
        
        # Создаем основные директории
        directories = ['forecasts', 'outcomes', 'quality']
        
        for directory in directories:
            dir_path = base_dir / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Директория создана/проверена: {dir_path}")
        
        logger.info(f"Директории для отчетов созданы в: {base_dir}")
        
    except Exception as e:
        logger.error(f"Ошибка при создании директорий для отчетов: {e}")
        raise


def ensure_report_directory_exists(report_type: str, date: datetime, output_dir: str = "results") -> Path:
    """
    Обеспечивает существование директории для отчета.
    
    Args:
        report_type: Тип отчета ('forecasts', 'outcomes', 'quality')
        date: Дата отчета
        output_dir: Базовая директория для сохранения
        
    Returns:
        Путь к директории отчета
    """
    try:
        year_month_dir = Path(output_dir) / report_type / date.strftime('%Y') / date.strftime('%m')
        year_month_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Директория для отчета {report_type} готова: {year_month_dir}")
        return year_month_dir
        
    except Exception as e:
        logger.error(f"Ошибка при создании директории для отчета {report_type}: {e}")
        raise
