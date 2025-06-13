import logging

import requests

from common.infra.queue.rabbit.rabbit_consumer import RabbitMQConsumer


class SchedulerHandler:
    def __init__(self, consumer: RabbitMQConsumer) -> None:
        self.consumer = consumer

    @staticmethod
    async def send_request(request: dict) -> None:
        try:
            requests.post(request["url"], json={"id": request["id"]}, timeout=5)
        except (ConnectionError, Exception) as e:
            logging.error(f"Error sending {request['id']}: {e}")
            raise e
