import pytest


def mult(x, y):
    return x * y


@pytest.mark.asyncio
async def test_sync_task(chive):
    mult_task = chive.register_task(mult, "mult")

    try:
        await chive.start_worker("mult")
        future = await mult_task(100, 6)
        result = await future.get(timeout=30)
        assert result == 600
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
