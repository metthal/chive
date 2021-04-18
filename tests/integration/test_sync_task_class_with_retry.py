import multiprocessing
import pytest

from chive.exceptions import RetryTask
from chive.task import SyncTask, TaskStatus


counter = 0
collected_events = multiprocessing.Queue()


class MyTask(SyncTask):
    def execute(self, *args, **kwargs):
        collected_events.put(("execute", args, kwargs))
        global counter
        counter += 1
        if counter == 3:
            return "Success!"
        raise RetryTask

    def on_success(self, result):
        collected_events.put(("on_success", result))

    def on_failure(self, error):
        collected_events.put(("on_failure", error))

    def on_finished(self, status, result):
        collected_events.put(("on_finished", status, result))
        collected_events.put(None)

    def on_retry(self, retries):
        collected_events.put(("on_retry", retries))


@pytest.mark.asyncio
async def test_sync_task_class_with_retry(chive):
    chive.register_task(MyTask, "mytask")

    try:
        await chive.start_worker("mytask")
        future = await chive.submit_task("mytask", args=[1, 2, 3], kwargs={"a": 1, "b": 2, "c": 3})
        result = await future.get(timeout=30)
        assert result == "Success!"

        events = []
        while True:
            try:
                event = collected_events.get(timeout=10)
                if event is None:
                    break
                events.append(event)
            except queue.Empty:
                break

        assert collected_events.empty()
        assert events == [
            ("execute", (1, 2, 3), {"a": 1, "b": 2, "c": 3}),
            ("on_retry", 1),
            ("execute", (1, 2, 3), {"a": 1, "b": 2, "c": 3}),
            ("on_retry", 2),
            ("execute", (1, 2, 3), {"a": 1, "b": 2, "c": 3}),
            ("on_success", "Success!"),
            ("on_finished", TaskStatus.SUCCESS, "Success!"),
        ]
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
