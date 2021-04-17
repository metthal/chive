import aioredis
import aio_pika
import asyncio
import pytest

from chives import Chives
from chives.utils import retry


pytest_plugins = ["docker_compose"]


async def rabbitmq_is_live(rabbitmq_url: str):
    connection = await aio_pika.connect_robust(rabbitmq_url)
    await connection.close()


async def redis_is_live(redis_url: str):
    connection = await aioredis.create_redis(redis_url)
    connection.close()


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def chives(request, function_scoped_container_getter):
    rabbitmq = function_scoped_container_getter.get("rabbitmq").network_info[0]
    redis = function_scoped_container_getter.get("redis").network_info[0]

    rabbitmq_url = f"amqp://guest:guest@localhost:{rabbitmq.host_port}/%2F"
    redis_url = f"redis://localhost:{redis.host_port}/0"

    try:
        await retry(
            rabbitmq_is_live,
            args=(rabbitmq_url,),
            wait_exponential=1.0,
            max_retries=20,
        )
    except Exception as err:
        assert False, f"Failed to start RabbitMQ container {repr(err)}"

    try:
        await retry(
            redis_is_live,
            args=(redis_url,),
            wait_exponential=1.0,
            max_retries=20,
        )
    except Exception as err:
        assert False, f"Failed to start Redis container {repr(err)}"

    try:
        app = Chives(request.node.name, rabbitmq_url, results=redis_url)
        await app.init()
        yield app
    finally:
        await app.cleanup()
