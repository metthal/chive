import asyncio
import pytest


@pytest.mark.asyncio
async def test_async_task(chive):
    @chive.task(max_priority=10)
    async def plus(x, y):
        return x + y

    try:
        futures = []
        for index, (x, y) in enumerate([(1, 2), (10, 20), (100, 200)]):
            fut = await chive.submit_task("plus", args=(x, y), priority=index)
            futures.append(fut.get(timeout=20))

        await chive.start_worker("plus")

        results = [await result for result in asyncio.as_completed(futures, timeout=20)]
        assert results == [300, 30, 3]
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
