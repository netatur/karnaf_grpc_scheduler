import json
import time

import requests
from redis import Redis

VALUES_NAME = "callbacks"
RETRY_IN_SECONDS = 10


class SchedulerHandler:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

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
            sorted_callback = self.redis.zrangebyscore(VALUES_NAME, 0, now)
            for callback in sorted_callback:
                try:
                    self.send_request(json.loads(callback))
                except Exception:
                    self.redis.zadd(VALUES_NAME, {callback: now + RETRY_IN_SECONDS})
                else:
                    self.redis.zrem(VALUES_NAME, callback)

            time.sleep(1)


if __name__ == "__main__":
    scheduler_handler = SchedulerHandler(redis=Redis())
    scheduler_handler.handle()
