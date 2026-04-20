from __future__ import annotations

from typing import Optional
import logging

from .base import BaseQueue
from data_contracts import VisualizationContract

logger = logging.getLogger(__name__)


class VisualizationQueue(BaseQueue):
    """Queue for :class:`~data_contracts.VisualizationContract` objects."""

    def __init__(self) -> None:
        super().__init__("visualization")

    def enqueue_visualization_contract(self, contract: VisualizationContract) -> bool:
        """Add *contract* to the queue.

        Args:
            contract: A validated :class:`VisualizationContract` instance.

        Returns:
            True on success, False if *contract* has the wrong type.
        """
        if not isinstance(contract, VisualizationContract):
            logger.error("Expected VisualizationContract, got %s", type(contract).__name__)
            return False
        return self.enqueue(contract)

    def dequeue_visualization_contract(self) -> Optional[VisualizationContract]:
        """Remove and return the front :class:`VisualizationContract`, or *None*."""
        return self.dequeue()
