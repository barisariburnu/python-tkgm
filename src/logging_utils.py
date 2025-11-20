"""
Logging Utilities - Batch Logging for Performance

Optimized logging to reduce log spam by 99%.
"""

import time
from loguru import logger


class BatchLogger:
    """
    Smart batch logger - reduces log spam by 99%
    
    Instead of logging every record (10k logs), logs at intervals (~100 logs).
    
    Usage:
        batch_logger = BatchLogger("Inserting parcels", total=1000, interval=100)
        for i, parcel in enumerate(parcels, 1):
            insert(parcel)
            batch_logger.log_progress(i)
        batch_logger.finalize(success_count=950, error_count=50)
    """
    
    def __init__(self, operation: str, total: int, interval: int = 100):
        """
        Initialize batch logger
        
        Args:
            operation: Operation name (e.g., "Inserting parcels")
            total: Total number of items to process
            interval: Log progress every N items (default: 100)
        """
        self.operation = operation
        self.total = total
        self.interval = interval
        self.start_time = time.time()
        
        logger.info(f"🚀 Starting: {operation} ({total} items)")
    
    def log_progress(self, current: int):
        """
        Log progress at intervals
        
        Only logs when:
        - current is multiple of interval (e.g., 100, 200, 300)
        - current equals total (final item)
        
        This reduces 10,000 logs to ~100 logs (99% reduction!)
        """
        if current % self.interval == 0 or current == self.total:
            elapsed = time.time() - self.start_time
            percentage = (current / self.total * 100) if self.total > 0 else 0
            rate = current / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"📊 {self.operation}: {current}/{self.total} ({percentage:.1f}%) "
                f"- {rate:.1f} items/s"
            )
    
    def finalize(self, success_count: int, error_count: int = 0, skip_count: int = 0):
        """
        Log final summary with statistics
        
        Args:
            success_count: Number of successfully processed items
            error_count: Number of failed items
            skip_count: Number of skipped items
        """
        duration = time.time() - self.start_time
        rate = success_count / duration if duration > 0 else 0
        
        logger.info(
            f"✅ {self.operation} completed in {duration:.2f}s\n"
            f"   Total: {self.total}, Success: {success_count}, "
            f"Errors: {error_count}, Skipped: {skip_count}\n"
            f"   Rate: {rate:.1f} items/s"
        )
