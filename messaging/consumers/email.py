import logging
import asyncio
from dataclasses import dataclass
from typing import Optional

from db import mongo_db_connection
from pymongo.asynchronous.collection import AsyncCollection

from aio_pika import IncomingMessage
from messaging.consumer import RabbitMQConsumer
from models import EmailMessageDocument, SendEmailDTO


LOGGER = logging.getLogger("uvicorn")


@dataclass
class EmailConsumer(RabbitMQConsumer):
    queue_name: str = "email.queue"
    routing_key: str = ""
    exchange_name: Optional[str] = "email.exchange"

    async def listen(self):
        queue = await self._get_queue()
        await queue.consume(self.__callback)  # type: ignore

    async def __callback(self, message: IncomingMessage):
        db = mongo_db_connection["messages-db"]
        collection: AsyncCollection[EmailMessageDocument] = db["email-message"]

        LOGGER.info("Processing Sending Email...")
        await asyncio.sleep(5)

        async with message.process():  # garante ack automático, rollback em caso de erro
            message_dto = SendEmailDTO.model_validate_json(message.body.decode())

            LOGGER.info(
                f"Sending message from {message_dto.sender_email} to {message_dto.destination_email}"
            )
            LOGGER.info(f'Message: "{message_dto.message}"')

            email_message_document: EmailMessageDocument = EmailMessageDocument(
                **message_dto.model_dump()
            )
            LOGGER.info(f"Inserting document {email_message_document}")
            await collection.insert_one(email_message_document)
