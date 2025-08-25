from abc import ABC, abstractmethod
from typing import Optional

from aio_pika import Message

from aio_pika.abc import AbstractRobustChannel
from messaging.rabbitmq import rabbitmq_connection


class Producer[T](ABC):
    queue_name: str

    @abstractmethod
    async def send(self, message: T):
        pass


class RabbitMQProducer[T](Producer[T]):
    routing_key: str
    exchange_name: Optional[str]

    async def send(self, message: T):
        channel = await rabbitmq_connection.get_channel()

        exchange = await self.__get_exchange(channel)  # type: ignore
        full_message = self._create_message(message)

        await exchange.publish(full_message, routing_key=self.routing_key)

    @abstractmethod
    def _create_message(self, message: T) -> Message:
        pass

    async def __get_exchange(self, channel: AbstractRobustChannel):
        if not self.exchange_name:
            return channel.default_exchange

        return await channel.declare_exchange(self.exchange_name, durable=True)  # type: ignore
