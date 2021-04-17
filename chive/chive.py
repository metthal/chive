import aio_pika
import asyncio
import functools
import inspect
import logging
import ujson
import uuid

from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, Union

from chive.backends import ResultsBackendFactory
from chive.common import RMQConnectionPool
from chive.result import Result
from chive.utils import retry


logger = logging.getLogger("chive")


@dataclass
class TaskSpec:
    name: str
    func: Callable
    wrapped: bool


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
        await asyncio.gather(
            self._rmq_pool.start(),
            self._results_backend.start()
        )

    async def cleanup(self):
        await asyncio.gather(
            self._rmq_pool.stop(),
            self._results_backend.stop()
        )

    async def start_worker(self, task_name: str):
        logger.info(f"Starting worker for task '{task_name}'...")
        async with self._rmq_pool.channel() as channel:
            task_name = f"task.{task_name}"
            queue, _ = await self._init_rmq_objects(channel, task_name)
            await queue.consume(self._process_task)

    async def wait_for_stop(self):
        await self._stop_event.wait()

    async def stop_worker(self):
        logger.info(f"Stopping worker...")
        self._stop_event.set()

    async def submit_task(
        self,
        task_name: str,
        args: Union[Tuple[Any, ...], List[Any]],
        kwargs: Dict[str, Any],
    ) -> Result:
        logging.debug(
            f"Submitting task '{task_name}' with args '{args}' and kwargs '{kwargs}'"
        )
        async with self._rmq_pool.channel() as channel:
            _, exchange = await self._init_rmq_objects(channel, task_name)

            task_id = str(uuid.uuid4())
            logging.debug(f"  --> task_id: {task_id}")
            await exchange.publish(
                aio_pika.Message(
                    body=ujson.dumps(
                        {
                            "id": task_id,
                            "name": task_name,
                            "args": args,
                            "kwargs": kwargs,
                        }
                    ).encode("utf-8")
                ),
                routing_key=task_name,
            )

            return Result(self._results_backend, task_id)

    def task(self, name: Optional[str] = None):
        def decorator(func: Callable) -> Callable:
            task_name: str = "task.{}".format(name or func.__name__)

            @functools.wraps(func)
            async def wrapper(*args, **kwargs) -> Result:
                return await self.submit_task(task_name, args=args, kwargs=kwargs)

            TaskRegistry.tasks[task_name] = TaskSpec(
                name=task_name, func=wrapper, wrapped=True
            )
            return wrapper

        return decorator

    def register_task(self, func: Callable, name: Optional[str] = None) -> Callable:
        task_name: str = "task.{}".format(name or func.__name__)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Result:
            return await self.submit_task(task_name, args=args, kwargs=kwargs)

        TaskRegistry.tasks[task_name] = TaskSpec(
            name=task_name, func=func, wrapped=False
        )
        return wrapper

    async def run_task(self, spec: TaskSpec, task) -> Any:
        func = spec.func.__wrapped__ if spec.wrapped else spec.func  # type: ignore
        if inspect.iscoroutinefunction(func):
            return await func(*task["args"], **task["kwargs"])
        else:
            return await asyncio.get_event_loop().run_in_executor(
                self._sync_task_pool,
                functools.partial(_run_sync_task, spec, task),
            )

    async def _process_task(self, message: aio_pika.IncomingMessage):
        async with message.process():
            task: dict = ujson.loads(message.body.decode("utf-8"))
            logger.info(f"Received task '{task['id']}'")
            spec: TaskSpec = TaskRegistry.tasks[task["name"]]

            result = await self.run_task(spec, task)
            logger.info(f"Storing result for task '{task['id']}'")

            await retry(
                self._results_backend.store,
                args=(task["id"], result),
                max_wait_time=60.0,
                retry_callback=self._log_results_backend_retry,
            )

            logger.info(f"Finished task '{task['id']}'")

    async def _init_rmq_objects(
        self, channel: aio_pika.Channel, task_name: str
    ) -> Tuple[aio_pika.Queue, aio_pika.Exchange]:
        await channel.set_qos(prefetch_count=self.concurrency)
        queue = await channel.declare_queue(task_name, durable=True)
        exchange = await channel.declare_exchange(
            "chive", aio_pika.exchange.ExchangeType.TOPIC, durable=True
        )
        await queue.bind("chive", routing_key=task_name)
        return queue, exchange

    def _log_results_backend_retry(
        self, err: Exception, try_number: int, wait_time: float
    ):
        logger.error(
            f"Failed to store the result in the results backend (try #{try_number}, next try in {wait_time} seconds) ({repr(err)})"
        )


def _run_sync_task(spec: TaskSpec, task: dict):
    func = spec.func.__wrapped__ if spec.wrapped else spec.func  # type: ignore
    return spec.func(*task["args"], **task["kwargs"])
