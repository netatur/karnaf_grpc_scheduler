import uvicorn
from fastapi import Response, status, FastAPI

from common.classes.callback_request import CallbackRequest
from common.config import API_HOST, API_PORT, REQUEST_QUEUE, REQUEST_DLQ_QUEUE, REQUEST_DLX_EXCHANGE, REQUEST_DELAYED_EXCHANGE, \
    REQUEST_DELAYED_ROUTING_KEY
from common.infra.queue.rabbit.rabbit_producer import RabbitMQProducer

app = FastAPI()
producer = RabbitMQProducer(REQUEST_QUEUE, REQUEST_DLQ_QUEUE, REQUEST_DLX_EXCHANGE, REQUEST_DELAYED_EXCHANGE, REQUEST_DELAYED_ROUTING_KEY)


@app.post("/register")
async def register_callback(req: CallbackRequest):
    await producer.publish(req.model_dump())
    return Response(status_code=status.HTTP_200_OK)


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
