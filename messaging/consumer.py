from abc import ABC, abstractmethod
from typing import Optional
from aio_pika.abc import AbstractRobustChannel

from messaging.rabbitmq import rabbitmq_connection


class Consumer(ABC):
    queue_name: str

    @abstractmethod
    async def listen(self):
        pass


class RabbitMQConsumer(Consumer):
    routing_key: str
    exchange_name: Optional[str]

    @abstractmethod
    async def listen(self):
        pass

    async def __get_exchange(self, channel: AbstractRobustChannel):
        if not self.exchange_name:
            return channel.default_exchange

        return await channel.declare_exchange(self.exchange_name, durable=True)  # type: ignore

    async def _get_queue(self):
        channel = await rabbitmq_connection.get_channel()

        exchange = await self.__get_exchange(channel)  # type: ignore
        queue = await channel.declare_queue(self.queue_name, durable=True)  # type: ignore

        await queue.bind(exchange, routing_key=self.routing_key)  # type: ignore

        return queue
