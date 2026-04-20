from __future__ import annotations

from collections import deque
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


class BaseQueue:
    """In-memory FIFO queue for passing contracts between pipeline stages.

    Replaces the previous Redis-backed implementation; contracts are held in a
    thread-local deque and never serialised to an external broker.  The API is
    intentionally kept identical to the old interface so call sites require no
    changes.
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._queue: deque[Any] = deque()

    @property
    def name(self) -> str:
        """Logical name of this queue."""
        return self._name

    def enqueue(self, item: Any) -> bool:
        """Append *item* to the back of the queue.

        Returns:
            True (always succeeds for in-memory storage).
        """
        self._queue.append(item)
        logger.debug("Enqueued item to %s (size=%d)", self._name, len(self._queue))
        return True

    def dequeue(self) -> Optional[Any]:
        """Remove and return the front item, or *None* if the queue is empty."""
        if not self._queue:
            return None
        item = self._queue.popleft()
        logger.debug("Dequeued item from %s (size=%d)", self._name, len(self._queue))
        return item

    def size(self) -> int:
        """Return the number of items currently in the queue."""
        return len(self._queue)

    def clear(self) -> None:
        """Discard all items in the queue."""
        self._queue.clear()

    def get_queue_status(self) -> dict[str, Any]:
        """Return a status snapshot suitable for logging."""
        return {"name": self._name, "size": self.size()}
