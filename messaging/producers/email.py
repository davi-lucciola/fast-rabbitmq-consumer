from typing import Optional
from aio_pika import Message
from messaging.producer import RabbitMQProducer
from models import SendEmailDTO


class EmailProducer(RabbitMQProducer[SendEmailDTO]):
    queue_name: str = "email.queue"
    routing_key: str = ""
    exchange_name: Optional[str] = "email.exchange"

    def _create_message(self, message: SendEmailDTO) -> Message:
        body = message.model_dump_json().encode()
        return Message(body, content_type="application/json")
