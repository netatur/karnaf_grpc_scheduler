from typing import Awaitable

from redis import Redis

from common.infra.set.abstract_set import AbstractSet


class RedisSet(AbstractSet):
    def __init__(self):
        self.redis = Redis()

    def add(self, key: str, row: dict):
        self.redis.zadd(key, row)

    def remove(self, key: str, row: bytes):
        self.redis.zrem(key, row)

    def get(self, key: str, min_score: float, max_score: float) -> Awaitable:
        return self.redis.zrangebyscore(key, min_score, max_score)
