"""
Logging utility for the trading system.
Provides structured logging with file and console output.
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


class DailyRotatingFileHandler(logging.FileHandler):
    """
    Custom file handler that rotates log files daily based on date in filename.
    Creates a new file when the date changes.
    """
    
    def __init__(self, log_dir, base_filename, mode='a', encoding='utf-8', delay=False):
        """
        Initialize daily rotating file handler.
        
        Args:
            log_dir: Directory for log files
            base_filename: Base filename (without date), e.g., "trading_engine"
            mode: File mode
            encoding: File encoding
            delay: Delay file creation until first write
        """
        self.log_dir = Path(log_dir)
        self.base_filename = base_filename
        self.current_date = None
        
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(exist_ok=True)
        
        # Get initial file path
        today = datetime.now().strftime('%Y%m%d')
        self.current_date = today
        initial_file = self.log_dir / f"{self.base_filename}_{today}.log"
        
        # Call parent with the initial file path
        super().__init__(str(initial_file), mode, encoding, delay)
    
    def _get_file_path(self):
        """Get current log file path based on today's date."""
        today = datetime.now().strftime('%Y%m%d')
        return self.log_dir / f"{self.base_filename}_{today}.log"
    
    def emit(self, record):
        """Emit a log record, switching to new file if date changed."""
        # Check if date has changed
        today = datetime.now().strftime('%Y%m%d')
        current_file = self._get_file_path()
        
        # If date changed, switch to new file
        if self.current_date != today:
            # Close current file
            if self.stream:
                self.flush()
                self.stream.close()
            
            # Update to new file
            self.current_date = today
            self.baseFilename = str(current_file)
            self.stream = self._open()
        
        # Call parent emit
        super().emit(record)


class TradingLogger:
    """
    Centralized logging system for the trading engine.
    Supports file logging with rotation and console output.
    """
    
    _logger = None
    _initialized = False
    
    @classmethod
    def setup_logger(cls, log_dir="logs", log_level=logging.INFO, log_to_console=True):
        """
        Initialize the logger with file and console handlers.
        
        Args:
            log_dir: Directory to store log files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_to_console: Whether to output logs to console
        """
        if cls._initialized:
            return cls._logger
        
        # Create logs directory if it doesn't exist
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        # Create logger
        logger = logging.getLogger("trading_engine")
        logger.setLevel(log_level)
        
        # Prevent duplicate logs
        if logger.handlers:
            return logger
        
        # Log format: Time | Level | Component | Message
        log_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler with daily rotation (creates new file when date changes)
        file_handler = DailyRotatingFileHandler(
            log_dir=log_dir,
            base_filename="trading_engine",
            mode='a',
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
        
        # Console handler
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(log_format)
            logger.addHandler(console_handler)
        
        cls._logger = logger
        cls._initialized = True
        
        initial_log_file = file_handler.baseFilename
        logger.info(f"Logger initialized | Log file: {initial_log_file}")
        return logger
    
    @classmethod
    def get_logger(cls, name=None):
        """
        Get logger instance. Initializes if not already done.
        
        Args:
            name: Optional logger name (for component-specific logging)
        
        Returns:
            Logger instance
        """
        if not cls._initialized:
            cls.setup_logger()
        
        if name:
            return logging.getLogger(f"trading_engine.{name}")
        return cls._logger or logging.getLogger("trading_engine")
    
    @classmethod
    def get_component_logger(cls, component_name):
        """
        Get a logger for a specific component.
        
        Args:
            component_name: Name of the component (e.g., 'strategy', 'broker', 'execution')
        
        Returns:
            Component-specific logger
        """
        return cls.get_logger(component_name)


# Convenience functions for easy access
def get_logger(name=None):
    """Get logger instance"""
    return TradingLogger.get_logger(name)


def get_component_logger(component_name):
    """Get component-specific logger"""
    return TradingLogger.get_component_logger(component_name)


# Initialize logger on import
logger = TradingLogger.get_logger()

