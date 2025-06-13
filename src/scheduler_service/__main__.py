import asyncio

from common.config import SCHEDULER_QUEUE, SCHEDULER_DLQ_QUEUE, SCHEDULER_RETRY_QUEUE, SCHEDULER_DLX_EXCHANGE, \
    SCHEDULER_RETRY_EXCHANGE
from common.infra.queue.rabbit.rabbit_consumer import RabbitMQConsumer
from scheduler_service.scheduler_handler import SchedulerHandler


async def driver() -> None:
    await consumer.consume(scheduler_handler.send_request)
    await asyncio.Event().wait()


if __name__ == "__main__":
    consumer = RabbitMQConsumer(SCHEDULER_QUEUE, SCHEDULER_DLQ_QUEUE, SCHEDULER_RETRY_EXCHANGE, SCHEDULER_DLX_EXCHANGE)
    scheduler_handler = SchedulerHandler(consumer)
    asyncio.run(driver())
