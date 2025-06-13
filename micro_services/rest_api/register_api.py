from fastapi import Response, status, FastAPI

from common.classes.callback_request import CallbackRequest
from common.config import (
    REQUEST_QUEUE,
)
from common.infra.queue.rabbit.rabbit_producer import RabbitMQProducer

app = FastAPI()
producer = RabbitMQProducer(
    REQUEST_QUEUE
)


@app.post("/register")
async def register_callback(req: CallbackRequest):
    await producer.publish(req.model_dump())
    return Response(status_code=status.HTTP_200_OK)
