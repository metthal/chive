import asyncio
import pytest


def mult(x, y):
    raise ValueError("Test error")


@pytest.mark.asyncio
async def test_sync_task_error(chive):
    mult_task = chive.register_task(mult, "mult")

    try:
        await chive.start_worker("mult")
        future = await mult_task(100, 6)
        await future.get(timeout=30)
    except ValueError as err:
        assert str(err) == "Test error"
    except Exception as err:
        assert False, f"Unexpected exception has been raised ({repr(err)})"
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
