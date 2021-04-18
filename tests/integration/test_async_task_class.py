import pytest

from chive.task import AsyncTask, TaskStatus


collected_events = []


class MyTask(AsyncTask):
    async def execute(self, *args, **kwargs):
        collected_events.append(("execute", args, kwargs))
        return "RESULT"

    async def on_success(self, result):
        collected_events.append(("on_success", result))

    async def on_failure(self, error):
        collected_events.append(("on_failure", error))

    async def on_finished(self, status, result):
        collected_events.append(("on_finished", status, result))

    async def on_retry(self, retries):
        collected_events.append(("on_retry", retries))


@pytest.mark.asyncio
async def test_async_task_class(chive):
    chive.register_task(MyTask, "mytask")

    try:
        await chive.start_worker("mytask")
        future = await chive.submit_task("mytask", args=[1, 2, 3], kwargs={"a": 1, "b": 2, "c": 3})
        result = await future.get(timeout=30)
        assert result == "RESULT"
        assert collected_events == [
            ("execute", (1, 2, 3), {"a": 1, "b": 2, "c": 3}),
            ("on_success", "RESULT"),
            ("on_finished", TaskStatus.SUCCESS, "RESULT"),
        ]
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
