# """
# Модуль для управления многопроцессорной обработкой.
# Содержит класс Multiprocessor для запуска задач в отдельных процессах.
# """
#
import logging
from multiprocessing import Process, Queue
from typing import Callable, Any, List

logger = logging.getLogger(__name__)


class Consumer(Process):
    """
    Класс для управления параллельным выполнением задач в отдельных процессах.
    Использует multiprocessing.
    Process для управления процессами.
    """
    def __init__(self, task_queue, result_queue):
        """
        Инициализирует новый экземпляр класса Consumer.
        Args:
        """
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):

        while True:

            next_task = self.task_queue.get()

            if next_task is None:
                # Сигнал завершения
                logger.debug(
                    f'Процесс: {self.name} pid={self.pid} завершен.'
                )
                self.task_queue.task_done()
                break

            try:
                logger.info(
                    f'{self.name} обработка турнира: '
                    f'{next_task.tournament_id}'
                )
                # Вызываем метод process() задачи
                next_task.process()
            except Exception as e:
                logger.error(f'Ошибка в процессе {self.name}: {e}')
            finally:
                # Гарантируем вызов task_done() даже при исключении
                self.task_queue.task_done()

