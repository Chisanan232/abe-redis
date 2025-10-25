"""Queue backend implementations for abstract backend.

This package contains message queue backend implementations that integrate
with the abstract backend's plugin system.
"""

from queue.service.redis import RedisMessageQueueBackend

__all__ = ["RedisMessageQueueBackend"]
