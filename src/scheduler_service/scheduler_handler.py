import json
import time

import requests

from common.config import CALLBACKS_VALUE, RETRY_IN_SECONDS
from common.infra.set.abstract_set import AbstractSet


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
            sorted_callback = self.set.get(CALLBACKS_VALUE, 0, now)
            for callback in sorted_callback:
                try:
                    self.send_request(json.loads(callback))
                except Exception:
                    self.set.add(CALLBACKS_VALUE, {callback: now + RETRY_IN_SECONDS})
                else:
                    self.set.remove(CALLBACKS_VALUE, callback)

            time.sleep(1)
