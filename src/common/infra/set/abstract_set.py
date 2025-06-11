from abc import ABC, abstractmethod
from typing import Awaitable


class AbstractSet(ABC):
    @abstractmethod
    def add(self, key: str, row: dict) -> None:
        pass

    @abstractmethod
    def remove(self, key: str, row: bytes) -> None:
        pass

    @abstractmethod
    def get(self, key: str, min_score: float, max_score: float) -> Awaitable:
        pass
