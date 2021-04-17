from typing import Any, Optional

from chive.backends.backend import ResultsBackend
from chive.task import TaskResult, TaskStatus


class Result:
    def __init__(self, backend: ResultsBackend, task_id: str):
        self.backend: ResultsBackend = backend
        self.task_id: str = task_id

    async def ready(self) -> bool:
        return await self.backend.ready(self.task_id)

    async def get(self, timeout: Optional[int] = None) -> Any:
        result = await self.backend.get(self.task_id, timeout=timeout)
        if result.status == TaskStatus.FAILURE:
            raise result.error
        return result.result
