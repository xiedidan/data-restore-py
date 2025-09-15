"""
Logging utilities for Oracle to PostgreSQL migration tool.
"""

import logging
import sys
from typing import Optional
from datetime import datetime


class Logger:
    """Enhanced logger with progress tracking capabilities."""
    
    def __init__(self, log_level: str = "INFO", log_file: Optional[str] = None, name: str = "migration"):
        """Initialize logger with specified level and optional file output."""
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        self._last_progress_length = 0
    
    def info(self, message: str) -> None:
        """Log info message."""
        self._clear_progress_line()
        self.logger.info(message)
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        self._clear_progress_line()
        self.logger.debug(message)
    
    def warning(self, message: str) -> None:
        """Log warning message."""
        self._clear_progress_line()
        self.logger.warning(message)
    
    def error(self, message: str, exception: Optional[Exception] = None) -> None:
        """Log error message with optional exception details."""
        self._clear_progress_line()
        if exception:
            self.logger.error(f"{message}: {str(exception)}", exc_info=True)
        else:
            self.logger.error(message)
    
    def progress(self, current: int, total: int, message: str = "") -> None:
        """Display progress information."""
        if total <= 0:
            return
        
        percentage = (current / total) * 100
        bar_length = 30
        filled_length = int(bar_length * current // total)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        progress_text = f"\r[{bar}] {percentage:.1f}% ({current}/{total})"
        if message:
            progress_text += f" - {message}"
        
        # Clear previous progress line
        self._clear_progress_line()
        
        # Print new progress
        print(progress_text, end='', flush=True)
        self._last_progress_length = len(progress_text)
    
    def progress_complete(self, message: str = "Complete") -> None:
        """Mark progress as complete and move to next line."""
        self._clear_progress_line()
        self.info(message)
    
    def progress_step(self, current: int, total: int, step_name: str, file_name: str = "") -> None:
        """Display progress with step information for better user feedback."""
        if total <= 0:
            return
        
        percentage = (current / total) * 100
        bar_length = 25  # Slightly shorter to make room for step info
        filled_length = int(bar_length * current // total)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        progress_text = f"\r[{bar}] {percentage:.1f}% ({current}/{total})"
        if file_name:
            progress_text += f" | {file_name}"
        if step_name:
            progress_text += f" | {step_name}"
        
        # Clear previous progress line
        self._clear_progress_line()
        
        # Print new progress
        print(progress_text, end='', flush=True)
        self._last_progress_length = len(progress_text)
    
    def _clear_progress_line(self) -> None:
        """Clear the current progress line."""
        if self._last_progress_length > 0:
            print('\r' + ' ' * self._last_progress_length + '\r', end='', flush=True)
            self._last_progress_length = 0
    
    def section(self, title: str) -> None:
        """Log a section header."""
        self._clear_progress_line()
        separator = "=" * 60
        self.logger.info(separator)
        self.logger.info(f" {title}")
        self.logger.info(separator)
    
    def subsection(self, title: str) -> None:
        """Log a subsection header."""
        self._clear_progress_line()
        separator = "-" * 40
        self.logger.info(separator)
        self.logger.info(f" {title}")
        self.logger.info(separator)


class TimedLogger:
    """Context manager for timing operations."""
    
    def __init__(self, logger: Logger, operation_name: str):
        self.logger = logger
        self.operation_name = operation_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.operation_name}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(f"Completed {self.operation_name} in {duration.total_seconds():.2f} seconds")
        else:
            self.logger.error(f"Failed {self.operation_name} after {duration.total_seconds():.2f} seconds")


def get_logger(name: str, log_level: str = "INFO", log_file: Optional[str] = None) -> Logger:
    """Get a configured logger instance."""
    return Logger(log_level, log_file, name)