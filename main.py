import asyncio
import logging
import aio_pika
from fastapi import FastAPI, Request
from pydantic import BaseModel, EmailStr
from contextlib import asynccontextmanager


LOGGER = logging.getLogger("uvicorn")
QUEUE = "email.queue"
RABBIT_URL = "amqp://guest:guest@localhost:5672"


class SendEmailDTO(BaseModel):
    to: EmailStr
    message: str


async def send_email(message: aio_pika.IncomingMessage):
    LOGGER.info("Processing Sending Email...")
    await asyncio.sleep(5)

    async with message.process():  # garante ack automático, rollback em caso de erro
        message_dto = SendEmailDTO.model_validate_json(message.body.decode())

        LOGGER.info(f"Sending message to {message_dto.to}")
        LOGGER.info(f'Message: "{message_dto.message}"')

        # aqui você pode chamar outros serviços, salvar no DB, etc.
        await asyncio.sleep(1)  # simulação de I/O assíncrono


async def start_consumer(loop: asyncio.AbstractEventLoop):
    # AMPQ Setup
    LOGGER.info("Starting RabbitMQ consumers")
    connection = await aio_pika.connect_robust(RABBIT_URL, loop=loop)
    channel = await connection.channel()

    # Queues
    email_queue = await channel.declare_queue(QUEUE, durable=True)
    await email_queue.consume(send_email)

    LOGGER.info("RabbitMQ consumers startup complete")
    return connection, channel


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    connection, channel = await start_consumer(loop)

    app.state.amqp = {
        "conn": connection,
        "channel": channel,
    }

    yield
    await connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/send-email")
async def send_email_async(request: Request, message_dto: SendEmailDTO):
    channel: aio_pika.channel.Channel = request.app.state.amqp.get("channel")
    email_exchange = await channel.get_exchange("email.exchange")

    await email_exchange.publish(
        message=aio_pika.Message(
            message_dto.model_dump_json().encode(),
            content_type="application/json",
        ),
        routing_key="",
    )

    return {"message": "Email enviado com sucesso."}
