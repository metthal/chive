import pytest

from chive.task import AsyncTask, TaskStatus


class Type:
    def __init__(self, what):
        self.what = what

    def __eq__(self, rhs):
        return self.what == type(rhs)

    def __hash__(self):
        return hash(self.what)


collected_events = []


class MyTask(AsyncTask):
    async def execute(self, *args, **kwargs):
        collected_events.append(("execute", args, kwargs))
        raise ValueError("Test error")

    async def on_success(self, result):
        collected_events.append(("on_success", result))

    async def on_failure(self, error):
        collected_events.append(("on_failure", error))

    async def on_finished(self, status, result):
        collected_events.append(("on_finished", status, result))

    async def on_retry(self, retries):
        collected_events.append(("on_retry", retries))


@pytest.mark.asyncio
async def test_async_task_class_error(chive):
    chive.register_task(MyTask, "mytask")

    try:
        await chive.start_worker("mytask")
        future = await chive.submit_task("mytask", args=[1, 2, 3], kwargs={"a": 1, "b": 2, "c": 3})
        await future.get(timeout=30)
    except ValueError as err:
        assert str(err) == "Test error"
        assert collected_events == [
            ("execute", (1, 2, 3), {"a": 1, "b": 2, "c": 3}),
            ("on_failure", Type(ValueError)),
            ("on_finished", TaskStatus.FAILURE, Type(ValueError)),
        ]
    except Exception as err:
        assert False, f"Unexpected exception has been raised ({repr(err)})"
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
