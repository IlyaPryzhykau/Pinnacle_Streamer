import asyncio
import logging
from typing import Any

from app.constants.settings import WRITE_INTERVAL
from app.writer_pinnacle import write_to_storage as write_pinnacle
from app.writer_analyzer import write_analyzer_to_storage as write_analyzer


logger = logging.getLogger(__name__)


class Aggregator:
    """
    Класс для агрегации входящих сообщений с сокетов и периодической отправки в БД.
    """

    def __init__(self, flush_interval: int = WRITE_INTERVAL):
        """
        Инициализация агрегатора.

        :param flush_interval: интервал сброса буфера в секундах
        """
        self.buffer: list[dict[str, Any]] = []
        self.lock = asyncio.Lock()
        self.flush_interval = flush_interval

    async def add(self, message: dict[str, Any]):
        """
        Добавляет сообщение в буфер.

        :param message: словарь с данными от сокета
        """
        async with self.lock:
            self.buffer.append(message)

    async def run_flush_loop(self):
        """
        Запускает вечный цикл сброса буфера.
        """
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush()

    async def flush(self):
        """
        Отправляет накопленные сообщения в соответствующие обработчики и очищает буфер.
        """
        async with self.lock:
            if not self.buffer:
                logger.info('📭 Буфер пуст, пропускаем запись')
                return

            # logger.info(f'🔄 Отправляем {len(self.buffer)} сообщений в обработку')

            # для проверки данных которые нам прилетают
            for i, message in enumerate(self.buffer[:4]):
                logger.info(f'🔹 Сообщение #{i+1}:\n{message}\n')

            # Фильтрация сообщений от разных источников
            pinnacle_msgs = [msg for msg in self.buffer if
                             msg.get('Source') == 'Pinnacle']
            analyzer_msgs = [
                msg for msg in self.buffer
                if isinstance(msg, dict)
                and 'first' in msg and 'second' in msg and 'outcome' in msg
            ]

            if pinnacle_msgs:
                logger.info(
                    f'📦 Отправляем {len(pinnacle_msgs)} сообщений от Pinnacle')
                await write_pinnacle(pinnacle_msgs)

            if analyzer_msgs:
                logger.info(
                    f'🧠 Отправляем {len(analyzer_msgs)} сообщений от Analyzer')
                await write_analyzer(analyzer_msgs)

            self.buffer.clear()
