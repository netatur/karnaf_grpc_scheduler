import json
import logging

from aio_pika import connect_robust, Message, DeliveryMode, ExchangeType

from common.config import MAX_RETRIES

logging.basicConfig(level=logging.INFO)


class RabbitMQProducer:
    def __init__(self, queue_name: str, dlx_queue_name: str, dlx_exchange_name: str, delayed_exchange_name: str, delayed_routing_key: str):
        self.queue_name = queue_name
        self.dlx_queue_name = dlx_queue_name
        self.dlx_exchange_name = dlx_exchange_name
        self.delayed_exchange_name = delayed_exchange_name
        self.delayed_routing_key = delayed_routing_key
        self.max_retries = MAX_RETRIES
        self.host = "localhost"
        self.connection = None
        self.channel = None

    async def connect(self):
        """Establish an asynchronous connection and channel."""
        self.connection = await connect_robust(f"amqp://{self.host}/")
        self.channel = await self.connection.channel()
        # Declare the delayed exchange
        self.exchange = await self.channel.declare_exchange(
            self.delayed_exchange_name,
            type=ExchangeType.X_DELAYED_MESSAGE,
            durable=True,
            arguments={"x-delayed-type": "direct"},
        )

        # Declare the queue and bind it to the delayed exchange
        queue = await self.channel.declare_queue(self.queue_name, durable=True, arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": self.max_retries,
            "x-dead-letter-exchange": self.dlx_exchange_name,
            "x-dead-letter-routing-key": self.dlx_queue_name,
        }, )
        await queue.bind(self.exchange, routing_key=self.delayed_routing_key)
        logging.info(f"Connected to RabbitMQ at {self.host}, queue: {self.queue_name}")

    async def publish(self, message: dict, delay_ms: int = 0):
        """Publish a message to the queue in JSON format."""
        if not self.channel:
            await self.connect()

        message_body = json.dumps(message).encode()

        msg = Message(
            body=message_body,
            delivery_mode=DeliveryMode.PERSISTENT,
            headers={"x-delay": delay_ms}
        )
        await self.channel.default_exchange.publish(msg, routing_key=self.queue_name)
        logging.info(f"Published message: {message}")

    async def close(self):
        """Close connection gracefully."""
        if self.connection:
            await self.connection.close()
            logging.info("Connection closed.")
