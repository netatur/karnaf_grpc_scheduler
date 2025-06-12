import json
import logging
from typing import Callable, Awaitable

from aio_pika import (
    connect_robust,
    IncomingMessage,
    Message,
    DeliveryMode,
    ExchangeType,
)

from common.config import RETRY_DELAY_MS, MAX_RETRIES

logging.basicConfig(level=logging.INFO)


class RabbitMQConsumer:
    def __init__(self, queue_name: str, dlq_name: str, retry_name: str, dlq_exchange_name: str,
                 retry_exchange_name: str) -> None:
        self.queue_name = queue_name
        self.dlq_name = dlq_name
        self.retry_name = retry_name
        self.dlq_exchange_name = dlq_exchange_name
        self.retry_exchange_name = retry_exchange_name
        self.retry_delay_ms = RETRY_DELAY_MS
        self.max_retries = MAX_RETRIES
        self.connection = None
        self.channel = None
        self.queue = None

    async def connect(self):
        self.connection = await connect_robust("amqp://localhost/")
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
        # Declare exchanges
        self.retry_exchange = await self.channel.declare_exchange(
            self.retry_exchange_name, ExchangeType.DIRECT, durable=True
        )
        self.dlx_exchange = await self.channel.declare_exchange(
            self.dlq_exchange_name, ExchangeType.DIRECT, durable=True
        )

        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.retry_exchange_name,
            },
        )
        self.retry_queue = await self.channel.declare_queue(
            self.retry_name,
            durable=True,
            arguments={
                "x-message-ttl": self.retry_delay_ms,
                "x-dead-letter-exchange": "",  # default exchange routes back to main queue
                "x-dead-letter-routing-key": self.queue_name,
            },
        )

        self.dlq_queue = await self.channel.declare_queue(
            self.dlq_name,
            durable=True,
        )

        await self.retry_queue.bind(self.retry_exchange_name, routing_key=self.retry_name)

        await self.dlq_queue.bind(self.dlx_exchange, routing_key=self.dlq_name)

    async def consume(self, handler: Callable[[dict], Awaitable[None]]):
        async def on_message(message: IncomingMessage):
            try:
                payload = json.loads(message.body.decode())
                print(f"Received message: {payload}")

                # Run the handler
                await handler(payload)

                await message.ack()

            except Exception as e:
                retry_count = (message.headers or {}).get("x-retries", 0)
                logging.warning(f"Processing failed: {e} | Retry count: {retry_count}")

                if retry_count < self.max_retries:
                    # Republish to retry queue with incremented retry count header
                    headers = dict(message.headers or {})
                    headers["x-retries"] = retry_count + 1

                    await self.channel.default_exchange.publish(
                        Message(
                            body=message.body,
                            headers=headers,
                            delivery_mode=DeliveryMode.PERSISTENT,
                        ),
                        routing_key=self.retry_name,
                    )
                    await message.ack()
                    logging.info(
                        f"Message sent to retry queue (retry {retry_count + 1})"
                    )

                else:
                    # Send to DLQ
                    await self.channel.default_exchange.publish(
                        Message(
                            body=message.body,
                            headers=message.headers,
                            delivery_mode=DeliveryMode.PERSISTENT,
                        ),
                        routing_key=self.dlq_name,
                    )
                    await message.ack()
                    logging.warning(
                        "Message sent to dead-letter queue after max retries."
                    )

        if not self.queue:
            await self.connect()

        await self.queue.consume(on_message, no_ack=False)
        logging.info("Consumer is listening...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            logging.info("Connection closed.")
