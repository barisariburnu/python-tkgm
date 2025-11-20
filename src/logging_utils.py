"""
Logging Utilities - Performance & Context Tracking

Optimized logging helpers for better observability.
"""

import time
import functools
from contextlib import contextmanager
from typing import Any, Callable
from loguru import logger


@contextmanager
def log_execution_time(operation_name: str, log_level: str = "INFO"):
    """
    Context manager to log execution time
    
    Usage:
        with log_execution_time("Database insert"):
            db.insert_batch(records)
    """
    start = time.time()
    logger.log(log_level, f"Starting: {operation_name}")
    
    try:
        yield
    finally:
        duration = time.time() - start
        logger.log(log_level, f"✅ {operation_name} completed in {duration:.2f}s")


def log_batch_progress(current: int, total: int, interval: int = 100, operation: str = "Processing"):
    """
    Log progress at intervals (reduces log spam)
    
    Usage:
        for i, item in enumerate(items, 1):
            process(item)
            log_batch_progress(i, len(items), interval=50)
    """
    if current % interval == 0 or current == total:
        percentage = (current / total * 100) if total > 0 else 0
        logger.info(f"{operation}: {current}/{total} ({percentage:.1f}%)")


def performance_logger(operation_name: str = None):
    """
    Decorator to log function execution time
    
    Usage:
        @performance_logger("Save to database")
        def save_records(records):
            ...
    """
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or func.__name__
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start = time.time()
            logger.debug(f"Starting: {op_name}")
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                logger.info(f"✅ {op_name} completed in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(f"❌ {op_name} failed after {duration:.2f}s: {e}")
                raise
        
        return wrapper
    return decorator


class BatchLogger:
    """
    Smart batch logger - reduces log spam
    
    Usage:
        batch_logger = BatchLogger("Inserting parcels", total=1000)
        for i, parcel in enumerate(parcels):
            insert(parcel)
            batch_logger.log_progress(i + 1)
        batch_logger.finalize(success_count=950, error_count=50)
    """
    
    def __init__(self, operation: str, total: int, interval: int = 100):
        self.operation = operation
        self.total = total
        self.interval = interval
        self.start_time = time.time()
        
        logger.info(f"🚀 Starting: {operation} ({total} items)")
    
    def log_progress(self, current: int):
        """Log progress at intervals"""
        if current % self.interval == 0 or current == self.total:
            elapsed = time.time() - self.start_time
            percentage = (current / self.total * 100) if self.total > 0 else 0
            rate = current / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"📊 {self.operation}: {current}/{self.total} ({percentage:.1f}%) "
                f"- {rate:.1f} items/s"
            )
    
    def finalize(self, success_count: int, error_count: int = 0, skip_count: int = 0):
        """Log final summary"""
        duration = time.time() - self.start_time
        rate = success_count / duration if duration > 0 else 0
        
        logger.info(
            f"✅ {self.operation} completed in {duration:.2f}s\n"
            f"   Total: {self.total}, Success: {success_count}, "
            f"Errors: {error_count}, Skipped: {skip_count}\n"
            f"   Rate: {rate:.1f} items/s"
        )


# Convenience function for common logging patterns
def log_summary(operation: str, **metrics):
    """
    Log a summary with metrics
    
    Usage:
        log_summary("Database sync", 
                   total=1000, saved=950, errors=30, skipped=20,
                   duration=45.2)
    """
    summary_lines = [f"📋 {operation} Summary:"]
    for key, value in metrics.items():
        summary_lines.append(f"   {key}: {value}")
    
    logger.info("\n".join(summary_lines))
