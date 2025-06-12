from common.infra.set.redis_set import RedisSet
from scheduler_service.scheduler_handler import SchedulerHandler

if __name__ == "__main__":
    scheduler_handler = SchedulerHandler(RedisSet())
    scheduler_handler.handle()
