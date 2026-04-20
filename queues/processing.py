from __future__ import annotations

from typing import Optional
import logging

from .base import BaseQueue
from data_contracts import ProcessingContract

logger = logging.getLogger(__name__)


class ProcessingQueue(BaseQueue):
    """Queue for :class:`~data_contracts.ProcessingContract` objects."""

    def __init__(self) -> None:
        super().__init__("processing")

    def enqueue_processing_contract(self, contract: ProcessingContract) -> bool:
        """Add *contract* to the queue.

        Args:
            contract: A validated :class:`ProcessingContract` instance.

        Returns:
            True on success, False if *contract* has the wrong type.
        """
        if not isinstance(contract, ProcessingContract):
            logger.error("Expected ProcessingContract, got %s", type(contract).__name__)
            return False
        return self.enqueue(contract)

    def dequeue_processing_contract(self) -> Optional[ProcessingContract]:
        """Remove and return the front :class:`ProcessingContract`, or *None*."""
        return self.dequeue()
