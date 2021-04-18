import dataclasses
import inspect
import uuid

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Union, Type

from chive.exceptions import RetryTask


class TaskStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRIED = "retried"


@dataclass
class TaskParams:
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    retries: int = 0

    def asdict(self) -> dict:
        return dataclasses.asdict(self)


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None

    def asdict(self) -> dict:
        return dataclasses.asdict(self)


class TaskMixin:
    def __init__(self, name: str, params: TaskParams):
        self.name = name
        self.params = params


class SyncTask(TaskMixin):
    def run(self) -> TaskResult:
        result: TaskResult = TaskResult(task_id=self.params.task_id)

        try:
            result.result = self.execute(*self.params.args, **self.params.kwargs)
            result.status = TaskStatus.SUCCESS
            self.on_success(result.result)
        except RetryTask:
            result.status = TaskStatus.RETRIED
            self.on_retry(self.params.retries + 1)
        except Exception as err:
            result.error = err
            result.status = TaskStatus.FAILURE
            self.on_failure(result.error)
        finally:
            if result.status != TaskStatus.RETRIED:
                self.on_finished(
                    result.status,
                    result.result
                    if result.status == TaskStatus.SUCCESS
                    else result.error,
                )
            return result

    def execute(self, *args, **kwargs):
        pass

    def on_success(self, result: Any):
        pass

    def on_failure(self, error: Exception):
        pass

    def on_finished(self, status: TaskStatus, data: Union[Any, Exception]):
        pass

    def on_retry(self, retries: int):
        pass


class AsyncTask(TaskMixin):
    async def run(self) -> TaskResult:
        result: TaskResult = TaskResult(task_id=self.params.task_id)

        try:
            result.result = await self.execute(*self.params.args, **self.params.kwargs)
            result.status = TaskStatus.SUCCESS
            await self.on_success(result.result)
        except RetryTask:
            result.status = TaskStatus.RETRIED
            await self.on_retry(self.params.retries + 1)
        except Exception as err:
            result.error = err
            result.status = TaskStatus.FAILURE
            await self.on_failure(result.error)
        finally:
            if result.status != TaskStatus.RETRIED:
                await self.on_finished(
                    result.status,
                    result.result
                    if result.status == TaskStatus.SUCCESS
                    else result.error,
                )
            return result

    async def execute(self, *args, **kwargs):
        pass

    async def on_success(self, result: Any):
        pass

    async def on_failure(self, error: Exception):
        pass

    async def on_finished(self, status: TaskStatus, data: Union[Any, Exception]):
        pass

    async def on_retry(self, retries: int):
        pass


@dataclass
class TaskSpec:
    name: str
    task: Union[Callable, Type[AsyncTask], Type[SyncTask]]

    @property
    def task_type(self):
        return self.task.__wrapped__ if hasattr(self.task, "__wrapped__") else self.task

    @property
    def is_async(self):
        return (not self.is_class and inspect.iscoroutinefunction(self.task_type)) or (
            self.is_class and issubclass(self.task_type, AsyncTask)
        )

    @property
    def is_class(self):
        return inspect.isclass(self.task_type)

    @property
    def callable(self):
        return self.task_type.execute if self.is_class else self.task_type

    def create_task(self, params: TaskParams) -> Union[AsyncTask, SyncTask]:
        if self.is_class:
            return self.task_type(self.name, params)
        elif self.is_async:

            class _AsyncTask(AsyncTask):
                async def execute(s, *args, **kwargs):
                    return await self.callable(*args, **kwargs)

            return _AsyncTask(self.name, params)
        else:

            class _SyncTask(SyncTask):
                def execute(s, *args, **kwargs):
                    return self.callable(*args, **kwargs)

            return _SyncTask(self.name, params)
