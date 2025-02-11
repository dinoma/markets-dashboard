from typing import Dict
from .base import BaseQueue
from .fetching import FetchingQueue
from .processing import ProcessingQueue
from .analysis import AnalysisQueue
from .visualization import VisualizationQueue
from threading import Thread
import time
import logging

class QueueManager:
    """Manages all queues and worker threads"""
    
    def __init__(self):
        self.queues = {
            'fetching': FetchingQueue(),
            'processing': ProcessingQueue(),
            'analysis': AnalysisQueue(),
            'visualization': VisualizationQueue()
        }
        self.workers = {}
        self.running = False
        self.logger = logging.getLogger('queue_manager')
        
    def start_workers(self):
        """Start worker threads for each queue"""
        self.running = True
        for stage, queue in self.queues.items():
            worker = Thread(target=self._worker_loop, args=(stage,))
            worker.daemon = True
            worker.start()
            self.workers[stage] = worker
            self.logger.info(f"Started worker for {stage} queue")
            
    def stop_workers(self):
        """Stop all worker threads"""
        self.running = False
        for worker in self.workers.values():
            worker.join()
        self.logger.info("All workers stopped")
            
    def _worker_loop(self, stage: str):
        """Worker thread main loop"""
        queue = self.queues[stage]
        while self.running:
            try:
                # Process messages from queue
                if stage == 'fetching':
                    contract = queue.dequeue_fetching_contract()
                    if contract:
                        # Process fetching contract
                        pass
                elif stage == 'processing':
                    contract = queue.dequeue_processing_contract()
                    if contract:
                        # Process processing contract
                        pass
                elif stage == 'analysis':
                    contract = queue.dequeue_analysis_contract()
                    if contract:
                        # Process analysis contract
                        pass
                elif stage == 'visualization':
                    contract = queue.dequeue_visualization_contract()
                    if contract:
                        # Process visualization contract
                        pass
                        
                # Sleep briefly to prevent CPU overuse
                time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Error in {stage} worker: {e}")
