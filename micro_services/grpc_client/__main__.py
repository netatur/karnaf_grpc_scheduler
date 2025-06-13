import asyncio
import logging

from common.classes.callback_request import CallbackRequest
from common.config import (
    GRPC_HOST,
    GRPC_PORT,
    REQUEST_QUEUE,
    REQUEST_DLQ_QUEUE,
    REQUEST_DLX_EXCHANGE,
    REQUEST_RETRY_EXCHANGE,
)
from common.infra.queue.rabbit.rabbit_consumer import RabbitMQConsumer
from micro_services.grpc_client.grpc_client import GrpcClient


async def handle_message(message: dict):
    request = CallbackRequest.model_validate(message)
    client.send_schedule_request(request.id, request.url_callback, request.time)
    logging.info(f"Received callback request: {request}")


async def driver() -> None:
    await consumer.consume(handle_message)
    await asyncio.Event().wait()


if __name__ == "__main__":
    client = GrpcClient(host=GRPC_HOST, port=GRPC_PORT)
    consumer = RabbitMQConsumer(
        REQUEST_QUEUE, REQUEST_DLQ_QUEUE, REQUEST_RETRY_EXCHANGE, REQUEST_DLX_EXCHANGE
    )
    asyncio.run(driver())
