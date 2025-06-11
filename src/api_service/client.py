import logging

import grpc

import scheduler_callback_pb2
import scheduler_callback_pb2_grpc


class Client:
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
