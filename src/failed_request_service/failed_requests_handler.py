import json
import logging
import time

import requests

from common.config import FAILED_REQUEST_VALUE, API_HOST, API_PORT
from common.exceptions import RetryFailedRequestError
from common.infra.set.redis_set import RedisSet

redis = RedisSet()
url = f"http://{API_HOST}:{API_PORT}/register"


def handle():
    while True:
        now = time.time()
        failed_requests = redis.get(FAILED_REQUEST_VALUE, 0, now)
        for callback in failed_requests:
            try:
                requests.post(url, json=json.loads(callback.decode("utf-8")))
                redis.remove(FAILED_REQUEST_VALUE, callback)
            except Exception:
                logging.exception("Failed callback request. **Add alert!**")
                raise RetryFailedRequestError
        time.sleep(10)
