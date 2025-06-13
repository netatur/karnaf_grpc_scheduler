from concurrent import futures

import grpc.aio

from common.config import SCHEDULER_QUEUE, SCHEDULER_RETRY_EXCHANGE, SCHEDULER_DELAYED_EXCHANGE, \
    SCHEDULER_DELAYED_ROUTING_KEY, SCHEDULER_DLX_EXCHANGE
from common.infra.queue.rabbit.rabbit_producer import RabbitMQProducer
from common.proto import scheduler_callback_pb2
from common.proto import scheduler_callback_pb2_grpc


class RequestManager(scheduler_callback_pb2_grpc.SchedulerServicer):

    def __init__(self, producer: RabbitMQProducer):
        self.producer = producer

    async def ScheduleCallback(
            self, request: scheduler_callback_pb2.ScheduleRequest, context
    ) -> scheduler_callback_pb2.ScheduleResponse:
        print(f"Got request {request}")
        key = {"id": request.id, "url": request.url_callback}
        await self.producer.publish(message=key, delay_ms=request.time)
        return scheduler_callback_pb2.ScheduleResponse(status="200")


async def serve(grpc_port: int) -> None:
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    producer = RabbitMQProducer(SCHEDULER_QUEUE, SCHEDULER_DLX_EXCHANGE, SCHEDULER_RETRY_EXCHANGE, SCHEDULER_DELAYED_EXCHANGE,
                                SCHEDULER_DELAYED_ROUTING_KEY)
    request_manager = RequestManager(producer)
    scheduler_callback_pb2_grpc.add_SchedulerServicer_to_server(request_manager, server)
    server.add_insecure_port(f"[::]:{grpc_port}")
    await server.start()
    await server.wait_for_termination()
