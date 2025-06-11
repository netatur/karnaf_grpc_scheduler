import json
import threading
import time

import uvicorn
from fastapi import Response, status, FastAPI

from api_service import retry_failed_requests
from api_service.classes.callback_request import CallbackRequest
from api_service.client import Client
from common.config import FAILED_REQUEST_VALUE, GRPC_HOST, GRPC_PORT, API_HOST, API_PORT
from common.infra.set.redis_set import RedisSet

app = FastAPI()
client = Client(host=GRPC_HOST, port=GRPC_PORT)
redis = RedisSet()


@app.post("/register")
async def register_callback(req: CallbackRequest):
    try:
        client.send_schedule_request(req.id, req.url_callback, req.time)
    except Exception as e:
        redis.add(FAILED_REQUEST_VALUE, {json.dumps(req.model_dump()): time.time()})
    return Response(status_code=status.HTTP_200_OK)


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
