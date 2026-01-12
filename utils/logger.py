"""
Logging utility for the trading system.
Provides structured logging with file and console output.
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


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
        
        # File handler with rotation (10MB per file, keep 5 backups)
        log_file = log_path / f"trading_engine_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
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
        
        logger.info(f"Logger initialized | Log file: {log_file}")
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

