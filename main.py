import asyncio
import logging
from typing import NotRequired, TypedDict
import aio_pika
from bson import ObjectId
from fastapi import FastAPI, Request
from pydantic import BaseModel, EmailStr
from contextlib import asynccontextmanager
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection


LOGGER = logging.getLogger("uvicorn")
QUEUE = "email.queue"
RABBIT_URL = "amqp://guest:guest@localhost:5672"

mongo_db_connection = AsyncMongoClient("localhost", 27017)
destination_emails = ["davi@email.com", "fernanda@email.com", "carlos@email.com"]


class SendEmailPartialDTO(BaseModel):
    sender_email: EmailStr
    message: str


class SendEmailDTO(SendEmailPartialDTO):
    destination_email: str


class EmailMessageDocument(TypedDict):
    _id: NotRequired[ObjectId]
    sender_email: str
    destination_email: str
    message: str


async def send_email(message: aio_pika.IncomingMessage):
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

        email_message_document: EmailMessageDocument = message_dto.model_dump()
        LOGGER.info(f"Inserting document {email_message_document}")
        await collection.insert_one(email_message_document)


async def start_consumer():
    # AMPQ Setup
    LOGGER.info("Starting RabbitMQ consumers")
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()

    # This setting set how many 
    # messages are gonna be processed by time. 
    # await channel.set_qos(prefetch_count=1)

    # Exchanges
    exchange = await channel.declare_exchange("email.exchange", durable=True)

    # Queues
    email_queue = await channel.declare_queue(QUEUE, durable=True)

    await email_queue.bind(exchange, routing_key="")
    await email_queue.consume(send_email)

    LOGGER.info("RabbitMQ consumers startup complete")
    return connection, channel


@asynccontextmanager
async def lifespan(app: FastAPI):
    await mongo_db_connection.aconnect()
    connection, channel = await start_consumer()

    app.state.amqp = {
        "conn": connection,
        "channel": channel,
    }

    yield

    await connection.close()
    await mongo_db_connection.aclose()


app = FastAPI(lifespan=lifespan)


@app.post("/send-email")
async def send_email_async(request: Request, message_dto: SendEmailPartialDTO):
    channel: aio_pika.channel.Channel = request.app.state.amqp.get("channel")
    email_exchange = await channel.get_exchange("email.exchange")

    tasks = []
    for destination in destination_emails:
        tasks.append(
            email_exchange.publish(
                message=aio_pika.Message(
                    SendEmailDTO(
                        destination_email=destination,
                        sender_email=message_dto.sender_email,
                        message=message_dto.message,
                    )
                    .model_dump_json()
                    .encode(),
                    content_type="application/json",
                ),
                routing_key="",
            )
        )

    await asyncio.gather(*tasks)
    return {"message": "Email enviado com sucesso."}
