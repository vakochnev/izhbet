# izhbet/core/integration_service.py
"""
Интеграционный сервис для координации работы всех модулей.
Обеспечивает единый интерфейс для запуска всей цепочки processing → forecast → publisher.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
import subprocess
import sys
import os

logger = logging.getLogger(__name__)


class IntegrationService:
    """
    Интеграционный сервис для координации работы всех модулей.
    
    Отвечает за:
    - Запуск модуля processing (создание моделей и базовых прогнозов)
    - Запуск модуля forecast (конформное прогнозирование и отбор качественных прогнозов)
    - Запуск модуля publisher (публикация из таблицы statistics)
    """
    
    def __init__(self):
        """Инициализация интеграционного сервиса."""
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logger.info('Интеграционный сервис инициализирован')
    
    def run_full_pipeline(self, mode: str = 'TODAY') -> Dict[str, Any]:
        """
        Запускает полную цепочку обработки данных.
        
        Args:
            mode: Режим работы ('TODAY' или 'ALL_TIME')
            
        Returns:
            Dict[str, Any]: Результаты выполнения каждого этапа
        """
        logger.info(f'Запуск полной цепочки обработки данных в режиме {mode}')
        
        results = {
            'mode': mode,
            'start_time': datetime.now(),
            'processing': None,
            'forecast': None,
            'publisher': None,
            'success': False,
            'errors': []
        }
        
        try:
            # Этап 1: Processing - создание моделей и базовых прогнозов
            logger.info('=== ЭТАП 1: PROCESSING ===')
            results['processing'] = self._run_processing_stage(mode)
            
            if not results['processing']['success']:
                results['errors'].append('Ошибка на этапе processing')
                return results
            
            # Этап 2: Forecast - конформное прогнозирование и отбор качественных прогнозов
            logger.info('=== ЭТАП 2: FORECAST ===')
            results['forecast'] = self._run_forecast_stage(mode)
            
            if not results['forecast']['success']:
                results['errors'].append('Ошибка на этапе forecast')
                return results
            
            # Этап 3: Publisher - публикация из таблицы statistics
            logger.info('=== ЭТАП 3: PUBLISHER ===')
            results['publisher'] = self._run_publisher_stage(mode)
            
            if not results['publisher']['success']:
                results['errors'].append('Ошибка на этапе publisher')
                return results
            
            # Все этапы выполнены успешно
            results['success'] = True
            results['end_time'] = datetime.now()
            results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f'Полная цепочка обработки данных завершена успешно за {results["duration"]:.2f} секунд')
            
        except Exception as e:
            logger.error(f'Критическая ошибка в полной цепочке обработки данных: {e}')
            results['errors'].append(f'Критическая ошибка: {e}')
            results['end_time'] = datetime.now()
        
        return results
    
    def _run_processing_stage(self, mode: str) -> Dict[str, Any]:
        """
        Запускает этап processing.
        
        Args:
            mode: Режим работы
            
        Returns:
            Dict[str, Any]: Результаты выполнения
        """
        logger.info('Запуск этапа processing')
        
        try:
            # Определяем команду для processing
            if mode == 'TODAY':
                command = ['python3.12', 'processing.py', 'CREATE_PROGNOZ']
            elif mode == 'ALL_TIME':
                command = ['python3.12', 'processing.py', 'CREATE_MODEL']
            else:
                raise ValueError(f'Неподдерживаемый режим для processing: {mode}')
            
            # Запускаем команду
            result = subprocess.run(
                command,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=3600  # 1 час таймаут
            )
            
            if result.returncode == 0:
                logger.info('Этап processing выполнен успешно')
                return {
                    'success': True,
                    'command': ' '.join(command),
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f'Ошибка на этапе processing: {result.stderr}')
                return {
                    'success': False,
                    'command': ' '.join(command),
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'returncode': result.returncode
                }
                
        except subprocess.TimeoutExpired:
            logger.error('Таймаут на этапе processing')
            return {
                'success': False,
                'error': 'Таймаут выполнения',
                'command': ' '.join(command)
            }
        except Exception as e:
            logger.error(f'Критическая ошибка на этапе processing: {e}')
            return {
                'success': False,
                'error': str(e),
                'command': ' '.join(command)
            }
    
    def _run_forecast_stage(self, mode: str) -> Dict[str, Any]:
        """
        Запускает этап forecast.
        
        Args:
            mode: Режим работы
            
        Returns:
            Dict[str, Any]: Результаты выполнения
        """
        logger.info('Запуск этапа forecast')
        
        try:
            # Определяем команду для forecast
            if mode == 'TODAY':
                command = ['python3.12', 'forecast.py', 'today']
            elif mode == 'ALL_TIME':
                command = ['python3.12', 'forecast.py', 'all_time']
            else:
                raise ValueError(f'Неподдерживаемый режим для forecast: {mode}')
            
            # Запускаем команду
            result = subprocess.run(
                command,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=1800  # 30 минут таймаут
            )
            
            if result.returncode == 0:
                logger.info('Этап forecast выполнен успешно')
                return {
                    'success': True,
                    'command': ' '.join(command),
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f'Ошибка на этапе forecast: {result.stderr}')
                return {
                    'success': False,
                    'command': ' '.join(command),
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'returncode': result.returncode
                }
                
        except subprocess.TimeoutExpired:
            logger.error('Таймаут на этапе forecast')
            return {
                'success': False,
                'error': 'Таймаут выполнения',
                'command': ' '.join(command)
            }
        except Exception as e:
            logger.error(f'Критическая ошибка на этапе forecast: {e}')
            return {
                'success': False,
                'error': str(e),
                'command': ' '.join(command)
            }
    
    def _run_publisher_stage(self, mode: str) -> Dict[str, Any]:
        """
        Запускает этап publisher.
        
        Args:
            mode: Режим работы
            
        Returns:
            Dict[str, Any]: Результаты выполнения
        """
        logger.info('Запуск этапа publisher')
        
        try:
            # Определяем команду для publisher
            if mode == 'TODAY':
                command = ['python3.12', 'publisher.py', 'TODAY']
            elif mode == 'ALL_TIME':
                command = ['python3.12', 'publisher.py', 'ALL_TIME']
            else:
                raise ValueError(f'Неподдерживаемый режим для publisher: {mode}')
            
            # Запускаем команду
            result = subprocess.run(
                command,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 минут таймаут
            )
            
            if result.returncode == 0:
                logger.info('Этап publisher выполнен успешно')
                return {
                    'success': True,
                    'command': ' '.join(command),
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f'Ошибка на этапе publisher: {result.stderr}')
                return {
                    'success': False,
                    'command': ' '.join(command),
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'returncode': result.returncode
                }
                
        except subprocess.TimeoutExpired:
            logger.error('Таймаут на этапе publisher')
            return {
                'success': False,
                'error': 'Таймаут выполнения',
                'command': ' '.join(command)
            }
        except Exception as e:
            logger.error(f'Критическая ошибка на этапе publisher: {e}')
            return {
                'success': False,
                'error': str(e),
                'command': ' '.join(command)
            }
    
    def run_processing_only(self) -> Dict[str, Any]:
        """Запускает только этап processing."""
        logger.info('Запуск только этапа processing')
        return self._run_processing_stage('ALL_TIME')
    
    def run_forecast_only(self) -> Dict[str, Any]:
        """Запускает только этап forecast."""
        logger.info('Запуск только этапа forecast')
        return self._run_forecast_stage('TODAY')
    
    def run_publisher_only(self) -> Dict[str, Any]:
        """Запускает только этап publisher."""
        logger.info('Запуск только этапа publisher')
        return self._run_publisher_stage('TODAY')
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Получает статус всех компонентов пайплайна.
        
        Returns:
            Dict[str, Any]: Статус компонентов
        """
        logger.info('Получение статуса пайплайна')
        
        status = {
            'timestamp': datetime.now(),
            'components': {
                'processing': self._check_component_status('processing.py'),
                'forecast': self._check_component_status('forecast.py'),
                'publisher': self._check_component_status('publisher.py')
            }
        }
        
        return status
    
    def _check_component_status(self, component: str) -> Dict[str, Any]:
        """
        Проверяет статус компонента.
        
        Args:
            component: Имя компонента
            
        Returns:
            Dict[str, Any]: Статус компонента
        """
        try:
            component_path = os.path.join(self.project_root, component)
            
            if os.path.exists(component_path):
                return {
                    'exists': True,
                    'path': component_path,
                    'status': 'available'
                }
            else:
                return {
                    'exists': False,
                    'path': component_path,
                    'status': 'missing'
                }
        except Exception as e:
            return {
                'exists': False,
                'error': str(e),
                'status': 'error'
            }


def create_integration_service() -> IntegrationService:
    """
    Создает и настраивает интеграционный сервис.
    
    Returns:
        IntegrationService: Настроенный сервис
    """
    return IntegrationService()
