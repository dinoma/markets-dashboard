from typing import Optional, Dict, Any
from base_queue import BaseQueue
from data_contracts import AnalysisContract
import logging
import json
import pandas as pd

class AnalysisQueue(BaseQueue):
    """Specialized queue for handling AnalysisContract messages with enhanced debugging"""
    
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
            # Convert contract to dict with proper DataFrame serialization
            contract_dict = contract.to_dict()
            return self.enqueue(contract_dict)
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
            if 'processed_data' in message and isinstance(message['processed_data'], list):
                # Convert processed_data back to DataFrame
                message['processed_data'] = pd.DataFrame(message['processed_data'])
            return AnalysisContract(**message)
        except Exception as e:
            self.logger.error(f"Failed to parse AnalysisContract: {e}")
            return None
            
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current status of the analysis queue
        
        Returns:
            dict: Queue status information including:
                - queue_name: Name of the queue
                - size: Number of items in queue
                - connected: Whether connected to Redis
                - last_error: Last error message (if any)
        """
        status = {
            'queue_name': self.queue_name,
            'size': self.size(),
            'connected': self.redis is not None,
            'last_error': self.last_error if hasattr(self, 'last_error') else None
        }
        
        # Add detailed queue info if connected
        if self.redis is not None:
            try:
                status.update({
                    'memory_usage': self.redis.info('memory')['used_memory_human'],
                    'queue_memory': self.redis.memory_usage(self.queue_name),
                    'pending_messages': self.redis.xlen(self.queue_name)
                })
            except Exception as e:
                self.logger.error(f"Error getting queue details: {e}")
                status['queue_details_error'] = str(e)
                
        return status

    def clear_queue(self) -> bool:
        """
        Clear all messages from the analysis queue
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.redis.delete(self.queue_name)
            return True
        except Exception as e:
            self.logger.error(f"Failed to clear analysis queue: {e}")
            return False

    def peek_next_contract(self) -> Optional[Dict[str, Any]]:
        """
        Peek at the next contract in the queue without removing it
        
        Returns:
            Optional[Dict]: The next contract data or None if empty
        """
        try:
            messages = self.redis.xrange(self.queue_name, count=1)
            if messages:
                return json.loads(messages[0][1]['data'])
            return None
        except Exception as e:
            self.logger.error(f"Failed to peek at next contract: {e}")
            return None
