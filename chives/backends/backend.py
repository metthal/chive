from abc import ABC, abstractmethod
from typing import Any, Optional


class ResultsBackend(ABC):
    def __init__(self, url: str, pool_size: int):
        self.url: str = url
        self.pool_size: int = pool_size

    @abstractmethod
    async def ready(self, task_id: str) -> bool:
        raise NotImplementedError(f"{type(self).__name__}.exists() not implemented")

    @abstractmethod
    async def get(self, task_id: str, timeout: Optional[int] = None) -> Any:
        raise NotImplementedError(f"{type(self).__name__}.get() not implemented")

    @abstractmethod
    async def store(self, task_id: str, data: Any):
        raise NotImplementedError(f"{type(self).__name__}.store() not implemented")


class NoBackend(ResultsBackend):
    def __init__(self):
        super(NoBackend, self).__init__("", 0)

    async def ready(self, task_id: str) -> bool:
        return False

    async def get(self, task_id: str, timeout: Optional[int] = None) -> Any:
        return None

    async def store(self, task_id: str, data: Any):
        pass
