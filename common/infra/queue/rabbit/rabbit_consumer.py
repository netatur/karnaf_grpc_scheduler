import json
import logging
from typing import Callable, Awaitable

from aio_pika import (
    connect_robust,
    IncomingMessage,
    Message,
    DeliveryMode,
)

from common.config import RETRY_DELAY_MS, MAX_RETRIES

logging.basicConfig(level=logging.INFO)


class RabbitMQConsumer:
    def __init__(self, queue_name: str) -> None:
        self.queue_name = queue_name
        self.dlq_name = f"{queue_name}_dlq"
        self.retry_exchange_name = f"{queue_name}_retry_exchange"
        self.dlq_exchange_name = f"{queue_name}_dlq_exchange"
        self.retry_delay_ms = RETRY_DELAY_MS
        self.max_retries = MAX_RETRIES
        self.connection = None
        self.channel = None
        self.queue = None
        self.retry_exchange = None
        self.dlx_exchange = None

    async def connect(self):
        self.connection = await connect_robust("amqp://localhost/")
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        # Declare retry exchange (delayed message plugin required)
        self.retry_exchange = await self.channel.declare_exchange(
            self.retry_exchange_name,
            type="x-delayed-message",
            durable=True,
            arguments={"x-delayed-type": "direct"},
        )

        # Declare DLX (dead-letter exchange)
        self.dlx_exchange = await self.channel.declare_exchange(
            self.dlq_exchange_name, type="direct", durable=True
        )

        # Declare main queue
        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": self.dlq_exchange_name,
                "x-dead-letter-routing-key": self.dlq_name,
                "x-delivery-limit": self.max_retries,
            },
        )

        # Bind main queue to retry exchange so delayed messages route back here
        await self.queue.bind(self.retry_exchange, routing_key=self.queue_name)

        # Declare DLQ
        self.dlq_queue = await self.channel.declare_queue(
            self.dlq_name,
            durable=True,
        )

        await self.dlq_queue.bind(self.dlx_exchange, routing_key=self.dlq_name)

    async def consume(self, handler: Callable[[dict], Awaitable[None]]):
        async def on_message(message: IncomingMessage):
            try:
                payload = json.loads(message.body.decode())
                logging.info(f"Received message: {payload}")

                await handler(payload)
                await message.ack()

            except Exception:
                retry_count = message.headers.get("x-retry-count", 0) + 1

                if retry_count >= self.max_retries:
                    logging.warning("Max retries reached, sending to DLQ")
                    await message.reject(requeue=False)
                else:
                    logging.warning(
                        f"Retrying message (attempt {retry_count}) after delay"
                    )
                    await self.retry_exchange.publish(
                        Message(
                            body=message.body,
                            delivery_mode=DeliveryMode.PERSISTENT,
                            headers={
                                **message.headers,
                                "x-retry-count": retry_count,
                                "x-delay": self.retry_delay_ms,
                            },
                        ),
                        routing_key=self.queue_name,
                    )
                    await message.ack()

        if not self.queue:
            await self.connect()

        await self.queue.consume(on_message, no_ack=False)
        logging.info("Consumer is listening...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            logging.info("Connection closed.")
