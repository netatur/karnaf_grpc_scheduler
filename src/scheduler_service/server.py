import json
import time
from concurrent import futures

import grpc
from redis import Redis

import scheduler_callback_pb2
import scheduler_callback_pb2_grpc


class RequestManager(scheduler_callback_pb2_grpc.SchedulerServicer):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    def ScheduleCallback(
        self, request: scheduler_callback_pb2.ScheduleRequest, context
    ) -> scheduler_callback_pb2.ScheduleResponse:
        key = {"id": request.id, "url": request.url_callback}
        sending_time = time.time() + request.time
        self.redis.zadd("callbacks", {json.dumps(key): sending_time})
        return scheduler_callback_pb2.ScheduleResponse(status="200")


def serve(grpc_port: int) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    request_manager = RequestManager(redis=Redis())
    scheduler_callback_pb2_grpc.add_SchedulerServicer_to_server(request_manager, server)
    server.add_insecure_port(f"[::]:{grpc_port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    grpc_port = 50051
    serve(grpc_port)
