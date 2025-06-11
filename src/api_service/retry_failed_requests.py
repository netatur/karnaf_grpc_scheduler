import json
import logging
import time

import requests

from api_service.classes.callback_request import CallbackRequest
from common.config import FAILED_REQUEST_VALUE, API_HOST, API_PORT
from common.exceptions import RETRY_FAILED_REQUEST_ERROR
from common.infra.set.redis_set import RedisSet

redis = RedisSet()
url = f"http://{API_HOST}:{API_PORT}/register"


def handle():
    while True:
        now = time.time()
        failed_requests = redis.get(FAILED_REQUEST_VALUE, 0, now)
        for callback in failed_requests:
            try:
                requests.post(url, json=json.loads(callback.decode('utf-8')))
                redis.remove(FAILED_REQUEST_VALUE, callback)
            except Exception as e:
                logging.exception("Failed callback request. Add alert!")
                raise RETRY_FAILED_REQUEST_ERROR
        time.sleep(10)


if __name__ == "__main__":
    handle()