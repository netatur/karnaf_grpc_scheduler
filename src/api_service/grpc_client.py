import asyncio
import logging

import grpc

from common.classes.callback_request import CallbackRequest
from common.config import GRPC_HOST, GRPC_PORT, REQUEST_QUEUE, REQUEST_DLQ_QUEUE, \
    REQUEST_DLX_EXCHANGE, REQUEST_RETRY_EXCHANGE
from common.infra.queue.rabbit.rabbit_consumer import RabbitMQConsumer
from common.proto import scheduler_callback_pb2, scheduler_callback_pb2_grpc


class GrpcClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def send_schedule_request(
            self, callback_id: str, url_callback: str, delay: int
    ) -> None:
        with grpc.insecure_channel(f"{self.host}:{self.port}") as channel:
            stub = scheduler_callback_pb2_grpc.SchedulerStub(channel)
            request = scheduler_callback_pb2.ScheduleRequest(
                id=callback_id, url_callback=url_callback, time=delay
            )
            response = stub.ScheduleCallback(request)
            logging.info(f"Client received: {response.status}")


async def handle_message(message: dict):
    request = CallbackRequest.model_validate(message)
    client.send_schedule_request(request.id, request.url_callback, request.time)
    print(f"Received callback request: {request}")

async def driver() -> None:
    await consumer.consume(handle_message)
    await asyncio.Event().wait()


if __name__ == "__main__":
    client = GrpcClient(host=GRPC_HOST, port=GRPC_PORT)
    consumer = RabbitMQConsumer(REQUEST_QUEUE, REQUEST_DLQ_QUEUE, REQUEST_RETRY_EXCHANGE, REQUEST_DLX_EXCHANGE)
    asyncio.run(driver())
