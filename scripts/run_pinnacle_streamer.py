"""
Модуль запуска приложения.

Запускает все асинхронные задачи:
- Подключение к WebSocket-источникам (Pinnacle, Analyzer)
- Сбор устаревших матчей и экспорт в CSV
- Архивация и загрузка архивов на облако
"""

import asyncio
import logging

from app.archiver import run_archiver_loop
from app.collector_analyzer import run_analyzer_collector_loop
from app.collector_pinnacle import run_pinnacle_collector_loop
from app.uploader_to_mega import run_mega_uploader_loop
from app.websocket_client import run_ws_client
from app.utils import setup_logging


async def main():
    """
    Основная точка входа в приложение.

    Запускает параллельно:
    - WebSocket-клиент для получения live-данных,
    - сбор устаревших матчей Pinnacle и анализатора,
    - архиватор CSV-файлов,
    - загрузчик архивов на Mega.
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info('🚀 Стартуем: WebSocket-клиент + сбор устаревших данных')

    await asyncio.gather(
        run_ws_client(),
        run_pinnacle_collector_loop(),
        run_analyzer_collector_loop(),
        run_archiver_loop(),
        run_mega_uploader_loop()
    )


if __name__ == '__main__':
    asyncio.run(main())
