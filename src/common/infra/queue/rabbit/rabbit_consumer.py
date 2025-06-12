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

logging.basicConfig(level=logging.INFO)

MAX_RETRIES = 5
DLQ_QUEUE = "my_queue_dlq"
RETRY_QUEUE = "my_queue_retry"
RETRY_EXCHANGE = "my_retry_exchange"
DLX_EXCHANGE = "my_dlq_exchange"
MAIN_QUEUE = "requests"
RETRY_DELAY_MS = 5000  # 5 seconds delay for retries


class RabbitMQConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None

    async def connect(self):
        self.connection = await connect_robust("amqp://localhost/")
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
        # Declare exchanges
        self.retry_exchange = await self.channel.declare_exchange(
            RETRY_EXCHANGE, ExchangeType.DIRECT, durable=True
        )
        self.dlx_exchange = await self.channel.declare_exchange(
            DLX_EXCHANGE, ExchangeType.DIRECT, durable=True
        )

        self.queue = await self.channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": RETRY_EXCHANGE,
            },
        )
        self.retry_queue = await self.channel.declare_queue(
            RETRY_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": RETRY_DELAY_MS,
                "x-dead-letter-exchange": "",  # default exchange routes back to main queue
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )

        self.dlq_queue = await self.channel.declare_queue(
            DLQ_QUEUE,
            durable=True,
        )

        await self.retry_queue.bind(self.retry_exchange, routing_key=RETRY_QUEUE)

        await self.dlq_queue.bind(self.dlx_exchange, routing_key=DLQ_QUEUE)

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

                if retry_count < MAX_RETRIES:
                    # Republish to retry queue with incremented retry count header
                    headers = dict(message.headers or {})
                    headers["x-retries"] = retry_count + 1

                    await self.channel.default_exchange.publish(
                        Message(
                            body=message.body,
                            headers=headers,
                            delivery_mode=DeliveryMode.PERSISTENT,
                        ),
                        routing_key=RETRY_QUEUE,
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
                        routing_key=DLQ_QUEUE,
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
