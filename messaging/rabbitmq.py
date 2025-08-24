import asyncio
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractRobustConnection


RABBIT_MQ_URL = "amqp://guest:guest@localhost:5672"


class RabbitMQConnection:
    def __init__(self, rabbitmq_url: str) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.__connection: Optional[AbstractRobustConnection] = None

    async def connect(self):
        loop = asyncio.get_running_loop()
        self.__connection = await aio_pika.connect_robust(self.rabbitmq_url, loop=loop)

    async def close(self):
        if not self.__connection:
            raise Exception("RabbitMQ not connected.")

        await self.__connection.close()

    async def get_channel(self):
        if not self.__connection:
            raise Exception("RabbitMQ not connected.")

        return self.__connection.channel()


rabbitmq_connection = RabbitMQConnection(RABBIT_MQ_URL)
