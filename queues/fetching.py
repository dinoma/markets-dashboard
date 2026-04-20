from __future__ import annotations

from typing import Any, Optional
import logging

from .base import BaseQueue
from data_contracts import FetchingContract

logger = logging.getLogger(__name__)


class FetchingQueue(BaseQueue):
    """Queue for :class:`~data_contracts.FetchingContract` objects."""

    def __init__(self) -> None:
        super().__init__("fetching")

    def enqueue_fetching_contract(self, contract: FetchingContract) -> bool:
        """Add *contract* to the queue.

        Args:
            contract: A validated :class:`FetchingContract` instance.

        Returns:
            True on success, False if *contract* has the wrong type.
        """
        if not isinstance(contract, FetchingContract):
            logger.error("Expected FetchingContract, got %s", type(contract).__name__)
            return False
        return self.enqueue(contract)

    def dequeue_fetching_contract(self) -> Optional[FetchingContract]:
        """Remove and return the front :class:`FetchingContract`, or *None*."""
        return self.dequeue()
