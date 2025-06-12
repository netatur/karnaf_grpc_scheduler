import json
import logging

from aio_pika import connect_robust, Message, DeliveryMode

logging.basicConfig(level=logging.INFO)
RETRY_EXCHANGE = "my_retry_exchange"


class RabbitMQProducer:
    def __init__(self, queue_name: str, host: str = "localhost"):
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None

    async def connect(self):
        """Establish an asynchronous connection and channel."""
        self.connection = await connect_robust(f"amqp://{self.host}/")
        self.channel = await self.connection.channel()
        await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": RETRY_EXCHANGE,
            },
        )
        logging.info(f"Connected to RabbitMQ at {self.host}, queue: {self.queue_name}")

    async def publish(self, message: dict):
        """Publish a message to the queue in JSON format."""
        if not self.channel:
            await self.connect()

        message_body = json.dumps(message).encode()

        msg = Message(body=message_body, delivery_mode=DeliveryMode.PERSISTENT)

        await self.channel.default_exchange.publish(msg, routing_key=self.queue_name)
        logging.info(f"Published message: {message}")

    async def close(self):
        """Close connection gracefully."""
        if self.connection:
            await self.connection.close()
            logging.info("Connection closed.")
