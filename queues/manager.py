from __future__ import annotations

from typing import Any
import logging

from .fetching import FetchingQueue
from .processing import ProcessingQueue
from .analysis import AnalysisQueue
from .visualization import VisualizationQueue

logger = logging.getLogger(__name__)


class QueueManager:
    """Owns one instance of each pipeline queue.

    Provides a single access point for queue status reporting and bulk
    operations (e.g. clearing all queues between runs).
    """

    def __init__(self) -> None:
        self.fetching = FetchingQueue()
        self.processing = ProcessingQueue()
        self.analysis = AnalysisQueue()
        self.visualization = VisualizationQueue()
        self._queues = (
            self.fetching,
            self.processing,
            self.analysis,
            self.visualization,
        )

    def clear_all(self) -> None:
        """Discard every item in every queue."""
        for q in self._queues:
            q.clear()
        logger.info("All pipeline queues cleared")

    def get_status(self) -> dict[str, Any]:
        """Return a combined status snapshot for all queues."""
        return {q.name: q.get_queue_status() for q in self._queues}
