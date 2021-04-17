import asyncio
import pytest


@pytest.mark.asyncio
async def test_simple_async_task(chive):
    @chive.task()
    async def plus(x, y):
        return x + y

    try:
        await chive.start_worker("plus")
        future = await plus(5, 6)
        result = await future.get(timeout=30)
        assert result == 11
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
