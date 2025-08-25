import asyncio
from typing import Dict, Optional

import aio_pika
from aio_pika.abc import AbstractConnection, AbstractChannel


RABBIT_MQ_URL = "amqp://guest:guest@localhost:5672"


class RabbitMQConnection:
    def __init__(self, rabbitmq_url: str) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.__channels: Dict[str, AbstractChannel] = {}
        self.__connection: Optional[AbstractConnection] = None

    async def connect(self):
        loop = asyncio.get_running_loop()
        self.__connection = await aio_pika.connect_robust(self.rabbitmq_url, loop=loop)  # type: ignore

    async def close(self):
        if not self.__connection:
            raise Exception("RabbitMQ not connected.")

        await self.__connection.close()

    async def get_channel(self, key: str = "default"):
        if not self.__connection:
            raise Exception("RabbitMQ not connected.")

        channel = self.__channels.get(key)

        if channel is None:
            channel = await self.__connection.channel()
            self.__channels[key] = channel  # type: ignore

        return channel


rabbitmq_connection = RabbitMQConnection(RABBIT_MQ_URL)
