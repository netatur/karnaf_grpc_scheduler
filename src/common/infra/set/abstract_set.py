from typing import Awaitable


class AbstractSet:
    def add(self, key: str, row: dict) -> None:
        pass

    def remove(self, key: str, row: bytes) -> None:
        pass

    def get(self, key: str, min_score: float, max_score: float) -> Awaitable:
        pass
