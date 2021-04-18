import multiprocessing
import pytest

from chive.task import SyncTask, TaskStatus


class Type:
    def __init__(self, what):
        self.what = what

    def __eq__(self, rhs):
        return self.what == type(rhs)

    def __hash__(self):
        return hash(self.what)


collected_events = multiprocessing.Queue()


class MyTask(SyncTask):
    def execute(self, *args, **kwargs):
        collected_events.put(("execute", args, kwargs))
        raise ValueError("Test error")

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
async def test_sync_task_class_error(chive):
    chive.register_task(MyTask, "mytask")

    try:
        await chive.start_worker("mytask")
        future = await chive.submit_task("mytask", args=[1, 2, 3], kwargs={"a": 1, "b": 2, "c": 3})
        await future.get(timeout=30)
    except ValueError as err:
        assert str(err) == "Test error"

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
            ("on_failure", Type(ValueError)),
            ("on_finished", TaskStatus.FAILURE, Type(ValueError)),
        ]
    except Exception as err:
        assert False, f"Unexpected exception has been raised ({repr(err)})"
    finally:
        await chive.stop_worker()
        await chive.wait_for_stop()
