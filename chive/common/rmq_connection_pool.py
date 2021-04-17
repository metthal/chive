import aio_pika
import aio_pika.pool
import asyncio

from contextlib import asynccontextmanager
from typing import Optional


class RMQConnectionPool:
    def __init__(self, url: str):
        self.url: str = url
        self._connection_pool: Optional[aio_pika.pool.Pool] = None
        self._channel_pool: Optional[aio_pika.pool.Pool] = None

    async def start(self):
        if self._connection_pool is not None or self._channel_pool is not None:
            await self.stop()
        self._connection_pool = aio_pika.pool.Pool(self._get_connection, max_size=16)
        self._channel_pool = aio_pika.pool.Pool(self._get_channel, max_size=128)

    async def stop(self):
        if self._connection_pool is not None:
            await self._connection_pool.close()
            self._connection_pool = None
        if self._channel_pool is not None:
            await self._channel_pool.close()
            self._channel_pool = None

    @asynccontextmanager
    async def channel(self):
        if self._channel_pool is None:
            raise ConnectionError("RMQConnectionPool is not connected")

        async with self._channel_pool.acquire() as channel:
            yield channel

    async def _get_connection(self) -> aio_pika.RobustConnection:
        return await aio_pika.connect_robust(self.url)

    async def _get_channel(self) -> aio_pika.Channel:
        if self._connection_pool is None:
            raise ConnectionError("RMQConnectionPool is not connected")

        async with self._connection_pool.acquire() as connection:
            return await connection.channel()
