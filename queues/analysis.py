from __future__ import annotations

from typing import Optional
import logging

from .base import BaseQueue
from data_contracts import AnalysisContract

logger = logging.getLogger(__name__)


class AnalysisQueue(BaseQueue):
    """Queue for :class:`~data_contracts.AnalysisContract` objects."""

    def __init__(self) -> None:
        super().__init__("analysis")

    def enqueue_analysis_contract(self, contract: AnalysisContract) -> bool:
        """Add *contract* to the queue.

        Args:
            contract: A validated :class:`AnalysisContract` instance.

        Returns:
            True on success, False if *contract* has the wrong type.
        """
        if not isinstance(contract, AnalysisContract):
            logger.error("Expected AnalysisContract, got %s", type(contract).__name__)
            return False
        return self.enqueue(contract)

    def dequeue_analysis_contract(self) -> Optional[AnalysisContract]:
        """Remove and return the front :class:`AnalysisContract`, or *None*."""
        return self.dequeue()
