from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class TaskStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRIED = "retried"


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None


class Task:
    pass
    #def __init__(self, name: str):
    #    self.name = name

    #async def execute(self):
    #    pass

    #async def on_succcess(self):
    #    pass
