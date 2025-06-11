import json
import time

import requests

from common.infra.set.abstract_set import AbstractSet
from common.infra.set.redis_set import RedisSet

VALUES_NAME = "callbacks"
RETRY_IN_SECONDS = 10


class SchedulerHandler:
    def __init__(self, set: AbstractSet) -> None:
        self.set = set

    @staticmethod
    def send_request(request: dict) -> None:
        try:
            requests.post(request["url"], json={"id": request["id"]}, timeout=5)
        except (ConnectionError, Exception) as e:
            print(f"Error sending {request['id']}: {e}")
            raise e

    def handle(self):
        while True:
            now = time.time()
            sorted_callback = self.set.get(VALUES_NAME, 0, now)
            for callback in sorted_callback:
                try:
                    self.send_request(json.loads(callback))
                except Exception:
                    self.set.add(VALUES_NAME, {callback: now + RETRY_IN_SECONDS})
                else:
                    self.set.remove(VALUES_NAME, callback)

            time.sleep(1)


if __name__ == "__main__":
    scheduler_handler = SchedulerHandler(RedisSet())
    scheduler_handler.handle()
