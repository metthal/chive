from typing import Optional

from .backend import NoBackend
from .redis import RedisBackend


class ResultsBackendFactory:
    @staticmethod
    def from_string(url: Optional[str], pool_size: int):
        if url is None:
            return NoBackend()
        elif url.startswith("redis"):
            return RedisBackend(url, pool_size)
        # elif url.startswith("sentinel"):
        #    return RedisSentinelBackend(url, pool_size)
