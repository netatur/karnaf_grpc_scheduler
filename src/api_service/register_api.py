import uvicorn
from fastapi import Response, status, FastAPI

from api_service.classes.callback_request import CallbackRequest
from api_service.client import Client
from common.infra.set.redis_set import RedisSet

app = FastAPI()
client = Client(host="localhost", port=50051)
redis = RedisSet()


@app.post("/register")
async def register_callback(req: CallbackRequest):
    print(req)
    try:
        client.send_schedule_request(req.id, req.url_callback, req.time)
    except Exception as e:
        print(e)
    return Response(status_code=status.HTTP_200_OK)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
