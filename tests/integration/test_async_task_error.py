import pytest


@pytest.mark.asyncio
async def test_async_task_error(chive):
    @chive.task()
    async def plus(x, y):
        raise ValueError("Test error")

    try:
        await chive.start_worker("plus")
        future = await plus(5, 6)
        await future.get(timeout=30)
    except ValueError as err:
        assert str(err) == "Test error"
    except Exception as err:
        assert False, f"Unexpected exception has been raised ({repr(err)})"
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
