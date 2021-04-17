import pytest

from chive.exceptions import RetryTask


counter = 0


def fail_for_while():
    global counter
    counter += 1
    if counter == 3:
        return "Success!"
    raise RetryTask


@pytest.mark.asyncio
async def test_sync_task_with_retry(chive):
    fail_for_while_task = chive.register_task(fail_for_while)

    try:
        await chive.start_worker("fail_for_while")
        future = await fail_for_while_task()
        result = await future.get(timeout=30)
        assert result == "Success!"
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
