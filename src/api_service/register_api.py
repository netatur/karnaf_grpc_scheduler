import json
import logging
import time

import uvicorn
from fastapi import Response, status, FastAPI

from common.classes.callback_request import CallbackRequest
from api_service.client import Client
from common.config import FAILED_REQUEST_VALUE, GRPC_HOST, GRPC_PORT, API_HOST, API_PORT
from common.exceptions import RetryFailedRequestError
from common.infra.set.redis_set import RedisSet

app = FastAPI()
client = Client(host=GRPC_HOST, port=GRPC_PORT)
redis = RedisSet()


def save_failed_request(request: CallbackRequest) -> None:
    try:
        redis.add(FAILED_REQUEST_VALUE, {json.dumps(request.model_dump()): time.time()})
    except Exception:
        logging.exception("Failed to save failed request. **Add alert!**")
        raise RetryFailedRequestError


@app.post("/register")
async def register_callback(req: CallbackRequest):
    try:
        client.send_schedule_request(req.id, req.url_callback, req.time)
    except Exception:
        save_failed_request(req)
    return Response(status_code=status.HTTP_200_OK)


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
