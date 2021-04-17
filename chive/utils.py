import asyncio
import logging

from typing import Any, Callable, Coroutine, Optional, Tuple, Type, TypeVar


T = TypeVar("T")


logger = logging.getLogger("chive")


async def retry(
    coro: Callable[..., Coroutine[T, T, T]],
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
    wait_time: float = 1.0,
    wait_exponential: float = 2.0,
    max_wait_time: float = 300.0,
    max_retries: Optional[int] = None,
    retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    retry_callback: Optional[Callable[[Exception, int, float], None]] = None,
) -> T:
    args = args or tuple()
    kwargs = kwargs or {}
    retry_on = retry_on or (Exception,)
    retry_callback = retry_callback or (lambda _1, _2, _3: None)
    if max_retries is not None and max_retries <= 0:
        max_retries = None

    try_number = 1
    while max_retries is None or try_number <= max_retries:
        try:
            logger.error("HERE")
            return await coro(*args, **kwargs)
        except retry_on as err:
            #logger.error(f"Caught exception {repr(err)}")
            retry_callback(err, try_number, wait_time)
            await asyncio.sleep(wait_time)
            try_number += 1
            logger.error(f"max_retries: {max_retries}, try_number: {try_number}, max_retries: {max_retries}")
            if max_retries is not None and try_number > max_retries:
                raise
            wait_time = min(wait_time * wait_exponential, max_wait_time)

    return None  # type: ignore
