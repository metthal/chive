import aio_pika
import asyncio
import functools
import inspect
import logging
import ujson
import uuid

from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, Type, Union

from chive.backends import ResultsBackendFactory
from chive.exceptions import RetryTask
from chive.common import RMQConnectionPool
from chive.result import Result
from chive.task import AsyncTask, SyncTask, TaskParams, TaskResult, TaskSpec, TaskStatus
from chive.utils import retry


logger = logging.getLogger("chive")


class TaskRegistry:
    tasks: Dict[str, TaskSpec] = {}


class Chive:
    def __init__(
        self,
        name: str,
        broker: str,
        concurrency: int = 1,
        results: Optional[str] = None,
    ):
        self.name: str = name
        self.broker: str = broker
        self.concurrency: int = concurrency
        self._rmq_pool = RMQConnectionPool(self.broker)
        self._stop_event = asyncio.Event()
        self._sync_task_pool = ProcessPoolExecutor(max_workers=self.concurrency)
        self._results_backend = ResultsBackendFactory.from_string(
            results, self.concurrency
        )

    async def init(self):
        await asyncio.gather(self._rmq_pool.start(), self._results_backend.start())

    async def cleanup(self):
        await asyncio.gather(self._rmq_pool.stop(), self._results_backend.stop())

    async def start_worker(self, task_name: str):
        logger.info(f"Starting worker for task '{task_name}'...")
        async with self._rmq_pool.channel() as channel:
            queue, _ = await self._init_rmq_objects(channel, task_name)
            await queue.consume(self._process_task)

    async def wait_for_stop(self):
        await self._stop_event.wait()

    async def stop_worker(self):
        logger.info(f"Stopping worker...")
        self._stop_event.set()

    async def submit_task(self, name: str, **kwargs) -> Result:
        logging.debug(f"Submitting task '{name}' with args '{kwargs}'")
        async with self._rmq_pool.channel() as channel:
            _, exchange = await self._init_rmq_objects(channel, name)

            params: TaskParams = TaskParams(**kwargs)
            logging.debug(f"  --> task_id: {params.task_id}")
            await exchange.publish(
                aio_pika.Message(body=self._serialize_task_params(name, params)),
                routing_key=f"task.{name}",
            )

            return Result(self._results_backend, params.task_id)

    def task(self, name: Optional[str] = None):
        def decorator(
            task: Union[Callable, Type[AsyncTask], Type[SyncTask]]
        ) -> Callable:
            task_name, wrapper = self._register_task(task, name=name)
            TaskRegistry.tasks[task_name] = TaskSpec(name=task_name, task=wrapper)
            return wrapper

        return decorator

    def register_task(
        self,
        task: Union[Callable, Type[AsyncTask], Type[SyncTask]],
        name: Optional[str] = None,
    ) -> Callable:
        task_name, wrapper = self._register_task(task, name=name)
        TaskRegistry.tasks[task_name] = TaskSpec(name=task_name, task=task)
        return wrapper

    async def _process_task(self, message: aio_pika.IncomingMessage):
        async with message.process():
            name, params = self._deserialize_task_params(message.body)
            spec: TaskSpec = TaskRegistry.tasks[name]

            logger.info(
                "Received task '{}'{}".format(
                    params.task_id,
                    f" (try #{params.retries})" if params.retries > 0 else "",
                )
            )

            result: TaskResult = await self._run_task(spec, params)
            if result.status == TaskStatus.RETRIED:
                params.retries += 1
                if params.retries <= 5:
                    await self.submit_task(name, **params.asdict())

            if result.status != TaskStatus.RETRIED:
                logger.info(f"Storing result for task '{result.task_id}'")
                await retry(
                    self._results_backend.store,
                    args=(result.task_id, result),
                    max_wait_time=60.0,
                    retry_callback=self._log_results_backend_retry,
                )

            logger.info(
                "Finished task '{}' with status '{}'{}".format(
                    result.task_id,
                    result.status,
                    f" (try #{params.retries})" if params.retries > 0 else "",
                )
            )

    async def _run_task(self, spec: TaskSpec, params: TaskParams) -> TaskResult:
        if spec.is_async:
            task = spec.create_task(params)
            return await task.run()  # type: ignore
        else:
            return await asyncio.get_event_loop().run_in_executor(
                self._sync_task_pool,
                functools.partial(_run_sync_task, spec, params),
            )

    def _register_task(
        self,
        task: Union[Callable, Type[AsyncTask], Type[SyncTask]],
        name: Optional[str] = None,
    ) -> Tuple[str, Callable]:
        task_name = name or task.__name__

        @functools.wraps(task)
        async def wrapper(*args, **kwargs) -> Result:
            return await self.submit_task(task_name, args=args, kwargs=kwargs)

        return task_name, wrapper

    def _serialize_task_params(self, name: str, params: TaskParams) -> bytes:
        return ujson.dumps({"name": name, "params": params.asdict()}).encode("utf-8")

    def _deserialize_task_params(self, data: bytes) -> Tuple[str, TaskParams]:
        task_json = ujson.loads(data.decode("utf-8"))
        return task_json["name"], TaskParams(**task_json["params"])

    async def _init_rmq_objects(
        self, channel: aio_pika.Channel, task_name: str
    ) -> Tuple[aio_pika.Queue, aio_pika.Exchange]:
        await channel.set_qos(prefetch_count=self.concurrency)
        queue = await channel.declare_queue(task_name, durable=True)
        exchange = await channel.declare_exchange(
            "chive", aio_pika.exchange.ExchangeType.TOPIC, durable=True
        )
        await queue.bind("chive", routing_key=f"task.{task_name}")
        return queue, exchange

    def _log_results_backend_retry(
        self, err: Exception, try_number: int, wait_time: float
    ):
        logger.error(
            f"Failed to store the result in the results backend (try #{try_number}, next try in {wait_time} seconds) ({repr(err)})"
        )


def _run_sync_task(spec: TaskSpec, params: TaskParams) -> TaskResult:
    task = spec.create_task(params)
    return task.run()  # type: ignore
