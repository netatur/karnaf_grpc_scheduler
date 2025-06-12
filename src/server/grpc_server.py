import json
import time
from concurrent import futures

import grpc

from common.infra.set.abstract_set import AbstractSet
from common.infra.set.redis_set import RedisSet
from common.proto import scheduler_callback_pb2
from common.proto import scheduler_callback_pb2_grpc


class RequestManager(scheduler_callback_pb2_grpc.SchedulerServicer):
    def __init__(self, set: AbstractSet) -> None:
        self.set = set

    def ScheduleCallback(
        self, request: scheduler_callback_pb2.ScheduleRequest, context
    ) -> scheduler_callback_pb2.ScheduleResponse:
        key = {"id": request.id, "url": request.url_callback}
        sending_time = time.time() + request.time
        self.set.add("callbacks", {json.dumps(key): sending_time})
        return scheduler_callback_pb2.ScheduleResponse(status="200")


def serve(grpc_port: int) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    request_manager = RequestManager(RedisSet())
    scheduler_callback_pb2_grpc.add_SchedulerServicer_to_server(request_manager, server)
    server.add_insecure_port(f"[::]:{grpc_port}")
    server.start()
    server.wait_for_termination()
