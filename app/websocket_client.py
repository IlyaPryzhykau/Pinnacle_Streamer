import asyncio
import json
import logging
from typing import Any

import websockets
from websockets.legacy.client import WebSocketClientProtocol

from app.aggregator import Aggregator
from app.config import settings
from app.constants.settings import WRITE_INTERVAL

logger = logging.getLogger(__name__)


class WebSocketClient:
    """
    Клиент для подключения к WebSocket-источнику и получения live-данных.

    :param ws_url: URL WebSocket-соединения
    :param filter_name: Имя фильтра для отправки
    :param source_name: Название источника (например, 'Pinnacle' или 'Analyzer')
    """
    def __init__(self, ws_url: str, filter_name: str, source_name: str) -> None:
        self.ws_url: str = ws_url
        self.source_name: str = source_name
        self.filter: dict[str, Any] = {
            'bookmakers': [
                {
                    'live': {
                        'filter': True,
                        'sports': settings.sports,
                    },
                    'prematch': {
                        'filter': False,
                        'sports': [],
                    },
                    'name': filter_name,
                }
            ]
        }

    async def connect(self, aggregator: Aggregator) -> None:
        """
        Устанавливает WebSocket-соединение, отправляет фильтр и начинает слушать сообщения.
        """
        logger.info(f'[{self.source_name}] Подключение к {self.ws_url}')
        async with websockets.connect(self.ws_url, ping_interval=None) as ws:
            await self.send_filter(ws)
            await self.listen(ws, aggregator)

    async def send_filter(self, ws: WebSocketClientProtocol) -> None:
        """
        Отправляет фильтр (список видов спорта и настройки) на WebSocket-сервер.
        """
        await ws.send(json.dumps(self.filter))
        logger.info(f'[{self.source_name}] Фильтр отправлен')

    async def listen(self, ws: WebSocketClientProtocol, aggregator: Aggregator) -> None:
        """
        Получает входящие сообщения из WebSocket-потока, разбирает JSON и отправляет их в агрегатор.
        Поддерживает как список сообщений, так и отдельные словари.
        """
        logger.info(f'[{self.source_name}] Ожидание входящих сообщений')
        async for message in ws:
            try:
                data = json.loads(message)

                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            await aggregator.add(item)
                        else:
                            logger.warning(
                                f'[{self.source_name}] Элемент в списке не dict: {type(item)}')

                elif isinstance(data, dict):
                    await aggregator.add(data)

                else:
                    logger.warning(
                        f'[{self.source_name}] Неподдерживаемый формат данных: {type(data)}')

            except json.JSONDecodeError:
                logger.warning(
                    f'[{self.source_name}] Ошибка декодирования JSON: {message!r}')


async def run_ws_client() -> None:
    """
    Запускает два клиента WebSocket — для Pinnacle и Analyzer — и их циклы сброса буфера.
    """
    aggregator_pinnacle = Aggregator(flush_interval=WRITE_INTERVAL)
    aggregator_analyzer = Aggregator(flush_interval=WRITE_INTERVAL)

    client_pinnacle = WebSocketClient(
        settings.ws_pinnacle_url, settings.filter_name, source_name='Pinnacle'
    )
    client_analyzer = WebSocketClient(
        settings.ws_analyzer_url, settings.filter_name, source_name='Analyzer'
    )

    await asyncio.gather(
        client_pinnacle.connect(aggregator_pinnacle),
        aggregator_pinnacle.run_flush_loop(),
        client_analyzer.connect(aggregator_analyzer),
        aggregator_analyzer.run_flush_loop(),
    )
