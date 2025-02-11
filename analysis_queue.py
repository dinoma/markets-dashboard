from typing import Optional
from base_queue import BaseQueue
from data_contracts import AnalysisContract
import logging

class AnalysisQueue(BaseQueue):
    """Specialized queue for handling AnalysisContract messages"""
    
    def __init__(self):
        super().__init__('analysis_queue')
        self.logger = logging.getLogger('analysis_queue')
        
    def enqueue_analysis_contract(self, contract: AnalysisContract) -> bool:
        """
        Enqueue an AnalysisContract message
        
        Args:
            contract (AnalysisContract): The contract to enqueue
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not isinstance(contract, AnalysisContract):
            self.logger.error("Invalid contract type - must be AnalysisContract")
            return False
            
        try:
            # Convert contract to dict and enqueue
            return self.enqueue(contract.dict())
        except Exception as e:
            self.logger.error(f"Failed to enqueue AnalysisContract: {e}")
            return False
            
    def dequeue_analysis_contract(self) -> Optional[AnalysisContract]:
        """
        Dequeue an AnalysisContract message
        
        Returns:
            Optional[AnalysisContract]: The dequeued contract or None if empty/failed
        """
        message = self.dequeue()
        if not message:
            return None
            
        try:
            # Convert dict back to AnalysisContract
            return AnalysisContract(**message)
        except Exception as e:
            self.logger.error(f"Failed to parse AnalysisContract: {e}")
            return None
            
    def get_queue_status(self) -> dict:
        """
        Get current status of the analysis queue
        
        Returns:
            dict: Queue status information
        """
        return {
            'queue_name': self.queue_name,
            'size': self.size(),
            'connected': self.redis is not None
        }
