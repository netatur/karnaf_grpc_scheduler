import asyncio
import logging

from common.classes.callback_request import CallbackRequest
from common.config import (
    GRPC_HOST,
    GRPC_PORT,
    REQUEST_QUEUE,
)
from common.infra.queue.rabbit.rabbit_consumer import RabbitMQConsumer
from micro_services.grpc_client.grpc_client import GrpcClient


async def handle_message(message: dict) -> None:
    request = CallbackRequest.model_validate(message)
    client.send_schedule_request(request.id, request.url_callback, request.time)
    logging.info(f"Received callback request: {request}")


async def driver() -> None:
    await consumer.consume(handle_message)
    await asyncio.Event().wait()


if __name__ == "__main__":
    client = GrpcClient(host=GRPC_HOST, port=GRPC_PORT)
    consumer = RabbitMQConsumer(REQUEST_QUEUE)
    asyncio.run(driver())
