import asyncio
import logging
from aio_pika import Message
from fastapi import FastAPI
from contextlib import asynccontextmanager
from db import mongo_db_connection
from messaging.consumers.email import EmailConsumer
from messaging.rabbitmq import rabbitmq_connection
from models import SendEmailDTO, SendEmailPartialDTO


LOGGER = logging.getLogger("uvicorn")
QUEUE = "email.queue"

destination_emails = ["davi@email.com", "fernanda@email.com", "carlos@email.com"]


async def start_consumers():
    # AMPQ Setup
    LOGGER.info("Starting RabbitMQ consumers")
    LOGGER.info("RabbitMQ consumers startup complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await mongo_db_connection.aconnect()
    await rabbitmq_connection.connect()

    # email_consumer = EmailConsumer()
    # await email_consumer.listen()

    yield

    await mongo_db_connection.aclose()
    await rabbitmq_connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/send-email")
async def send_email_async(message_dto: SendEmailPartialDTO):
    channel = await rabbitmq_connection.get_channel()
    email_exchange = await channel.get_exchange("email.exchange")

    async with asyncio.TaskGroup() as tasks:
        for destination in destination_emails:
            tasks.create_task(
                email_exchange.publish(
                    message=Message(
                        SendEmailDTO(
                            sender_email=message_dto.sender_email,
                            destination_email=destination,
                            message=message_dto.message,
                        )
                        .model_dump_json()
                        .encode("utf-8"),
                        content_type="application/json",
                    ),
                    routing_key="",
                )
            )

    return {"message": "Email enviado com sucesso."}
