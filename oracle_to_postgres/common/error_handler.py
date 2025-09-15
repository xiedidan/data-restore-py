"""
Error handling and retry utilities for Oracle to PostgreSQL migration tool.
"""

import time
import functools
from typing import Callable, Any, Optional, Type, Tuple
from dataclasses import dataclass
from enum import Enum

from .logger import Logger


class ErrorType(Enum):
    """Types of errors that can occur during migration."""
    FILE_ACCESS = "file_access"
    ENCODING_DETECTION = "encoding_detection"
    SQL_PARSING = "sql_parsing"
    API_CALL = "api_call"
    DATABASE_CONNECTION = "database_connection"
    DATABASE_EXECUTION = "database_execution"
    NETWORK = "network"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


@dataclass
class ErrorContext:
    """Context information for error handling."""
    error_type: ErrorType
    operation: str
    file_path: Optional[str] = None
    table_name: Optional[str] = None
    sql_statement: Optional[str] = None
    additional_info: Optional[dict] = None


class ErrorHandler:
    """Centralized error handling and retry logic."""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0, 
                 backoff_multiplier: float = 2.0, logger: Optional[Logger] = None):
        """
        Initialize error handler.
        
        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Initial delay between retries in seconds
            backoff_multiplier: Multiplier for exponential backoff
            logger: Optional logger instance
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_multiplier = backoff_multiplier
        self.logger = logger or Logger()
        
        # Track error statistics
        self.error_counts = {}
        self.retry_counts = {}
    
    def retry_on_failure(self, func: Callable, *args, 
                        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
                        context: Optional[ErrorContext] = None,
                        **kwargs) -> Any:
        """
        Execute function with retry logic on failure.
        
        Args:
            func: Function to execute
            *args: Function arguments
            retryable_exceptions: Tuple of exception types that should trigger retry
            context: Error context for logging
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            try:
                return func(*args, **kwargs)
                
            except retryable_exceptions as e:
                last_exception = e
                
                # Log the error
                operation = context.operation if context else func.__name__
                self._log_error(e, operation, attempt, context)
                
                # Update statistics
                self._update_error_stats(e, operation)
                
                # Don't retry on last attempt
                if attempt >= self.max_retries:
                    break
                
                # Calculate delay with exponential backoff
                delay = self.retry_delay * (self.backoff_multiplier ** attempt)
                self.logger.info(f"Retrying {operation} in {delay:.1f} seconds (attempt {attempt + 2}/{self.max_retries + 1})")
                time.sleep(delay)
        
        # All attempts failed
        self._handle_final_failure(last_exception, context)
        raise last_exception
    
    def handle_api_error(self, error: Exception, context: str) -> None:
        """
        Handle API-specific errors.
        
        Args:
            error: The exception that occurred
            context: Context description
        """
        error_context = ErrorContext(
            error_type=ErrorType.API_CALL,
            operation=context
        )
        
        self._log_error(error, context, 0, error_context)
        self._update_error_stats(error, context)
        
        # Provide specific guidance for common API errors
        error_str = str(error).lower()
        
        if "authentication" in error_str or "unauthorized" in error_str:
            self.logger.error("API authentication failed. Please check your API key.")
        elif "rate limit" in error_str or "429" in error_str:
            self.logger.error("API rate limit exceeded. Consider reducing request frequency.")
        elif "timeout" in error_str:
            self.logger.error("API request timed out. Consider increasing timeout value.")
        elif "network" in error_str or "connection" in error_str:
            self.logger.error("Network connection failed. Please check your internet connection.")
    
    def handle_db_error(self, error: Exception, sql: str) -> None:
        """
        Handle database-specific errors.
        
        Args:
            error: The exception that occurred
            sql: SQL statement that caused the error
        """
        error_context = ErrorContext(
            error_type=ErrorType.DATABASE_EXECUTION,
            operation="database_execution",
            sql_statement=sql[:200] + "..." if len(sql) > 200 else sql
        )
        
        self._log_error(error, "database_execution", 0, error_context)
        self._update_error_stats(error, "database_execution")
        
        # Provide specific guidance for common database errors
        error_str = str(error).lower()
        
        if "connection" in error_str:
            self.logger.error("Database connection failed. Please check connection parameters.")
        elif "syntax error" in error_str:
            self.logger.error("SQL syntax error. The generated DDL may need manual review.")
        elif "permission" in error_str or "access denied" in error_str:
            self.logger.error("Database permission error. Please check user privileges.")
        elif "already exists" in error_str:
            self.logger.warning("Object already exists. Consider using DROP IF EXISTS option.")
    
    def handle_file_error(self, error: Exception, file_path: str, operation: str) -> None:
        """
        Handle file-related errors.
        
        Args:
            error: The exception that occurred
            file_path: Path to the file that caused the error
            operation: Operation being performed
        """
        error_context = ErrorContext(
            error_type=ErrorType.FILE_ACCESS,
            operation=operation,
            file_path=file_path
        )
        
        self._log_error(error, operation, 0, error_context)
        self._update_error_stats(error, operation)
        
        # Provide specific guidance for common file errors
        if isinstance(error, FileNotFoundError):
            self.logger.error(f"File not found: {file_path}")
        elif isinstance(error, PermissionError):
            self.logger.error(f"Permission denied accessing file: {file_path}")
        elif isinstance(error, UnicodeDecodeError):
            self.logger.error(f"Encoding error reading file: {file_path}. Try specifying encoding explicitly.")
    
    def _log_error(self, error: Exception, operation: str, attempt: int, 
                  context: Optional[ErrorContext] = None) -> None:
        """Log error with context information."""
        if attempt == 0:
            log_level = "error"
            message = f"Error in {operation}: {str(error)}"
        else:
            log_level = "warning"
            message = f"Retry {attempt} failed for {operation}: {str(error)}"
        
        # Add context information
        if context:
            if context.file_path:
                message += f" (file: {context.file_path})"
            if context.table_name:
                message += f" (table: {context.table_name})"
            if context.sql_statement:
                message += f" (SQL: {context.sql_statement[:100]}...)"
        
        if log_level == "error":
            self.logger.error(message, error)
        else:
            self.logger.warning(message)
    
    def _update_error_stats(self, error: Exception, operation: str) -> None:
        """Update error statistics."""
        error_type = type(error).__name__
        
        # Update error counts
        if error_type not in self.error_counts:
            self.error_counts[error_type] = 0
        self.error_counts[error_type] += 1
        
        # Update retry counts
        if operation not in self.retry_counts:
            self.retry_counts[operation] = 0
        self.retry_counts[operation] += 1
    
    def _handle_final_failure(self, error: Exception, context: Optional[ErrorContext] = None) -> None:
        """Handle final failure after all retries exhausted."""
        operation = context.operation if context else "unknown_operation"
        self.logger.error(f"All retry attempts failed for {operation}. Final error: {str(error)}")
        
        # Provide recovery suggestions
        if context:
            if context.error_type == ErrorType.API_CALL:
                self.logger.info("Suggestion: Check API key, network connection, and service status")
            elif context.error_type == ErrorType.DATABASE_CONNECTION:
                self.logger.info("Suggestion: Verify database credentials and network connectivity")
            elif context.error_type == ErrorType.FILE_ACCESS:
                self.logger.info("Suggestion: Check file permissions and disk space")
    
    def get_error_summary(self) -> dict:
        """
        Get summary of errors encountered.
        
        Returns:
            Dictionary with error statistics
        """
        return {
            "error_counts": dict(self.error_counts),
            "retry_counts": dict(self.retry_counts),
            "total_errors": sum(self.error_counts.values()),
            "total_retries": sum(self.retry_counts.values())
        }
    
    def reset_stats(self) -> None:
        """Reset error statistics."""
        self.error_counts.clear()
        self.retry_counts.clear()


def retry_on_exception(*exception_types, max_retries: int = 3, 
                      delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator for automatic retry on specified exceptions.
    
    Args:
        *exception_types: Exception types that should trigger retry
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier for exponential backoff
    
    Returns:
        Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            handler = ErrorHandler(max_retries, delay, backoff)
            return handler.retry_on_failure(
                func, *args, 
                retryable_exceptions=exception_types or (Exception,),
                **kwargs
            )
        return wrapper
    return decorator


def handle_exceptions(error_handler: ErrorHandler, context: ErrorContext):
    """
    Decorator for centralized exception handling.
    
    Args:
        error_handler: ErrorHandler instance
        context: Error context information
    
    Returns:
        Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if context.error_type == ErrorType.API_CALL:
                    error_handler.handle_api_error(e, context.operation)
                elif context.error_type == ErrorType.DATABASE_EXECUTION:
                    error_handler.handle_db_error(e, context.sql_statement or "")
                elif context.error_type == ErrorType.FILE_ACCESS:
                    error_handler.handle_file_error(e, context.file_path or "", context.operation)
                else:
                    error_handler._log_error(e, context.operation, 0, context)
                raise
        return wrapper
    return decorator