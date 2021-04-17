import pytest

from chive.exceptions import RetryTask


counter = 0


@pytest.mark.asyncio
async def test_async_task_with_retry(chive):
    @chive.task()
    async def fail_for_while():
        global counter
        counter += 1
        if counter == 3:
            return "Success!"
        raise RetryTask

    try:
        await chive.start_worker("fail_for_while")
        future = await fail_for_while()
        result = await future.get(timeout=30)
        assert result == "Success!"
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
