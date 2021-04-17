import asyncio
import pytest


def mult(x, y):
    return x * y


@pytest.mark.asyncio
async def test_simple_sync_task(chives):
    mult_task = chives.register_task(mult, "mult")

    try:
        await chives.start_worker("mult")
        future = await mult_task(100, 6)
        result = await future.get(timeout=30)
        assert result == 600
    finally:
        await chives.stop_worker()
        await chives.wait_for_stop()
