import asyncio
import logging
from fastapi import Depends, FastAPI
from contextlib import asynccontextmanager
from db import mongo_db_connection
from messaging.consumers.email import EmailConsumer
from messaging.producers.email import EmailProducer
from messaging.rabbitmq import rabbitmq_connection
from models import SendEmailDTO, SendEmailPartialDTO


LOGGER = logging.getLogger("uvicorn")
QUEUE = "email.queue"

destination_emails = ["davi@email.com", "fernanda@email.com", "carlos@email.com"]


async def start_consumers():
    # AMPQ Setup
    LOGGER.info("Starting RabbitMQ consumers")

    email_consumer = EmailConsumer()

    await email_consumer.listen()

    LOGGER.info("RabbitMQ consumers startup complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await mongo_db_connection.aconnect()
    await rabbitmq_connection.connect()

    await start_consumers()

    yield

    await mongo_db_connection.aclose()
    await rabbitmq_connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/send-email")
async def send_email_async(
    message_dto: SendEmailPartialDTO,
    email_producer: EmailProducer = Depends(EmailProducer),
):
    async with asyncio.TaskGroup() as tasks:
        for destination in destination_emails:
            send_email_dto = SendEmailDTO(
                destination_email=destination, **message_dto.model_dump()
            )

            tasks.create_task(email_producer.send(send_email_dto))

    return {"message": "Emails enviado com sucesso."}
