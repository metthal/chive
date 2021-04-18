import aioredis  # type: ignore
import asyncio
import logging
import pickle
import ujson

from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Union

from chive.backends.backend import ResultsBackend
from chive.utils import retry


logger = logging.getLogger("chive")


@dataclass
class Subscription:
    task_id: str
    timeout_in: Optional[int] = None
    result: Optional[asyncio.Future] = None
    _task: Optional[asyncio.Task] = None
    _channel: Optional[aioredis.Channel] = None
    _new: Optional[bool] = None

    def __hash__(self):
        return hash(self.task_id)

    def __eq__(self, rhs: object):
        if isinstance(rhs, Subscription):
            return self.task_id == rhs.task_id
        elif isinstance(rhs, str):
            return self.task_id == rhs
        else:
            raise AttributeError(f"Can't compare Subscription and {type(rhs).__name__}")


class RedisBackend(ResultsBackend):
    def __init__(self, url: str, pool_size: int):
        super(RedisBackend, self).__init__(url, pool_size)
        self._started: bool = False
        self._connection: aioredis.ConnectionsPool = None
        self._pubsub_connection: aioredis.ConnectionsPool = None
        self._subscription_loop_task: Optional[asyncio.Task] = None
        self._subscription_list: Set[Subscription] = set()
        self._subscription_table: Dict[asyncio.Task, Subscription] = {}
        self._non_empty_sub_list_event: asyncio.Event = asyncio.Event()
        self._tasks: Set[asyncio.Task] = set()

    async def start(self):
        logger.info("Starting Redis results backend...")
        self._started = True
        self._connection = await aioredis.create_pool(self.url, maxsize=self.pool_size)
        await self._reconnect_pubsub_connection()
        self._subscription_loop_task = asyncio.get_event_loop().create_task(
            self._subscription_loop()
        )

    async def stop(self):
        logger.info("Stopping Redis results backend...")
        self._started = False
        self._non_empty_sub_list_event.set()
        if self._subscription_loop_task is not None:
            await self._subscription_loop_task
        if self._pubsub_connection is not None:
            self._pubsub_connection.close()
            await self._pubsub_connection.wait_closed()
        if self._connection is not None:
            self._connection.close()
            await self._connection.wait_closed()

    async def ready(self, task_id: str) -> bool:
        return await self._connection.exists(self._task_key(task_id))

    async def get(self, task_id: str, timeout: Optional[int] = None) -> Optional[Any]:
        # Run GET first because we might save some time and not run the whole SUBSCRIBE machinery.
        logger.debug(f"Trying to obtain task '{task_id}'")
        task_key = self._task_key(task_id)
        result = await self._connection.execute("get", task_key)
        if result is not None:
            logger.debug(f"Task '{task_id}' already exists in Redis backend")
            return self._get_result(result)

        # If we haven't found the result yet then add ourselves to the subscription list so that
        # subscription loop picks us up and SUBSCRIBEs us. At the same time we wake up subscription loop
        # in case it was blocked by subscription list being empty.
        logger.debug(
            f"Task '{task_id}' does not exist in Redis backend. Subscribing for changes..."
        )
        loop = asyncio.get_event_loop()
        sub = Subscription(
            task_id=task_id, timeout_in=timeout, result=loop.create_future()
        )
        self._subscription_list.add(sub)
        self._non_empty_sub_list_event.set()

        try:
            return self._get_result(await asyncio.wait_for(sub.result, timeout=timeout))  # type: ignore
        except asyncio.TimeoutError:
            return None

    async def store(self, task_id: str, data: dict):
        task_key = self._task_key(task_id)
        result_object = self._create_result_object(data)
        await self._connection.execute("setex", task_key, 120, result_object)
        await self._connection.execute("publish", task_key, result_object)

    async def _subscription_loop(self):
        logger.info("Starting subscription loop...")
        loop = asyncio.get_event_loop()

        while self._started:
            # If there are no subscribers then it's pointless to loop endlessly.
            # Just block the execution until there is some subscriber available.
            # This piece of code is unblocked by trying to obtain a result of some task.
            if not self._subscription_list:
                await self._non_empty_sub_list_event.wait()
                self._non_empty_sub_list_event.clear()
                continue

            new_subs = False
            for sub in self._subscription_list:
                # This is a brand new subscriber if it doesn't have task assigned.
                # We'll create a channel for subscriptions, start listening on the channel
                # and register the subscription. New subscriber will also be flagged
                # as a new subscriber so that we can later recognize it.
                if sub._task is None:
                    sub._channel = aioredis.Channel(
                        self._task_key(sub.task_id), is_pattern=False
                    )
                    sub._task = loop.create_task(sub._channel.wait_message())
                    sub._new = True
                    self._subscription_table[sub._task] = sub
                    self._tasks.add(sub._task)
                    new_subs = True
                else:
                    sub._new = False

            # If there are any new subscribers then resend SUBSCRIBE command.
            if new_subs:
                logger.debug(
                    "Subscribing to channels\n{}".format(
                        "\n".join(
                            [
                                sub._channel.name.decode("utf-8")
                                for sub in self._subscription_list
                            ]
                        )
                    )
                )
                self._pubsub_connection.execute_pubsub(
                    "subscribe", *[sub._channel for sub in self._subscription_list]
                )

            # Wait for any of the subscriptions channels to receive any message (or to be cancelled).
            # If there are any new subscribers then set the timeout to 0.1s instead of 10s.
            # This is because what might have happened is that we've checked the presence of the key
            # and it didn't exist. But until we got the point where we started subscription channel,
            # there could have been PUBLISH which we might have missed. So we set the really low timeout
            # to recheck the presence of keys almost instantly and introduced guarantee of delivery.
            done, pending = await asyncio.wait(
                self._tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=10 if not new_subs else 0.1,
            )

            # Remove tasks which have been finished (by either being accepted or cancelled).
            self._tasks.difference_update(done)
            to_unsubscribe = []

            try:
                while done:
                    finished_task = done.pop()
                    sub = self._subscription_table[finished_task]
                    if not finished_task.cancelled():
                        result = await sub._channel.get()
                    else:
                        result = None

                    logger.debug(result)
                    # This actually set the future of the object that user is waiting on in Result.get()
                    if not (sub.result.cancelled() or sub.result.done()):
                        sub.result.set_result(result)

                    to_unsubscribe.append(
                        self._subscription_table[finished_task]._channel
                    )
                    self._subscription_list.remove(sub)
                    del self._subscription_table[finished_task]
            except aioredis.errors.ChannelClosedError:
                await retry(self._reconnect_pubsub_connection, max_wait_time=60.0)
                continue

            # Here we resolve the situation mentioned above about missing possible PUBLISH between initial check
            # of the key and SUBSCRIBE. Do it only for new subscribers so that we don't issue huge MGET commands
            # each iteration.
            if new_subs:
                new_subs_list = [sub for sub in self._subscription_list if sub._new]
                # New subscriber might have been removed before because it obtained the message in the meantime.
                if new_subs_list:
                    values = await self._connection.execute(
                        "mget", *[self._task_key(sub.task_id) for sub in new_subs_list]
                    )
                    for sub, value in filter(
                        lambda x: x[1] is not None, zip(new_subs_list, values)
                    ):
                        # If we just cancel the task it will be naturally cancelled in the next iteration of wait()
                        # will pick it up and it will be removed as if the user have given up or we got PUBLISH.
                        if not (sub.result.cancelled() or sub.result.done()):
                            sub.result.set_result(value)
                        if not sub._task.done():
                            sub._task.cancel()

            # In the end, UNSUBSCRIBE channels which already received their message to clean up Redis.
            if to_unsubscribe:
                await self._pubsub_connection.execute_pubsub(
                    "unsubscribe", *to_unsubscribe
                )

        logger.info("Stopping Subscription loop...")

    def _task_key(self, task_id: str) -> str:
        return f"task:{task_id}"

    def _create_result_object(self, data: dict) -> bytes:
        return pickle.dumps(data)

    def _get_result(self, data: bytes) -> dict:
        return pickle.loads(data)

    async def _reconnect_pubsub_connection(self):
        self._pubsub_connection = await aioredis.create_pool(self.url, maxsize=2)
        # Mark all existing subscriptions as new one an cancel their tasks.
        # In case of a reconnect we want to reinitialize all channels and
        # since we might miss PUBLISH again (like with a new subscriber),
        # we want to resend initial MGET to ensure delivery.
        for sub in self._subscription_list:
            if not sub._task.done():
                if not sub._task.cancelled():
                    sub._task.cancel()
            del self._subscription_table[sub._task]
            sub._task = None
            sub._new = True
        self._tasks.clear()


class RedisSentinelBackend(RedisBackend):
    pass
