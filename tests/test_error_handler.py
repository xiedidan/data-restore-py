"""
Tests for error handling functionality.
"""

import pytest
from unittest.mock import Mock, patch
import time
from oracle_to_postgres.common.error_handler import (
    ErrorHandler, ErrorContext, ErrorType, retry_on_exception
)


class TestErrorHandler:
    """Test cases for ErrorHandler class."""
    
    def test_error_handler_initialization(self):
        """Test error handler initialization."""
        handler = ErrorHandler(max_retries=5, retry_delay=2.0, backoff_multiplier=1.5)
        
        assert handler.max_retries == 5
        assert handler.retry_delay == 2.0
        assert handler.backoff_multiplier == 1.5
        assert handler.error_counts == {}
        assert handler.retry_counts == {}
    
    def test_successful_function_execution(self):
        """Test successful function execution without retries."""
        handler = ErrorHandler()
        
        def successful_function(x, y):
            return x + y
        
        result = handler.retry_on_failure(successful_function, 5, 3)
        assert result == 8
    
    def test_retry_on_transient_failure(self):
        """Test retry mechanism with transient failures."""
        handler = ErrorHandler(max_retries=3, retry_delay=0.1)
        
        # Mock function that fails twice then succeeds
        mock_func = Mock(side_effect=[Exception("Fail 1"), Exception("Fail 2"), "Success"])
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = handler.retry_on_failure(mock_func)
        
        assert result == "Success"
        assert mock_func.call_count == 3
    
    def test_retry_exhaustion(self):
        """Test behavior when all retries are exhausted."""
        handler = ErrorHandler(max_retries=2, retry_delay=0.1)
        
        # Mock function that always fails
        mock_func = Mock(side_effect=Exception("Always fails"))
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            with pytest.raises(Exception, match="Always fails"):
                handler.retry_on_failure(mock_func)
        
        assert mock_func.call_count == 3  # Initial + 2 retries
    
    def test_retry_with_specific_exceptions(self):
        """Test retry only on specific exception types."""
        handler = ErrorHandler(max_retries=2, retry_delay=0.1)
        
        # Mock function that raises ValueError
        mock_func = Mock(side_effect=ValueError("Specific error"))
        
        with patch('time.sleep'):
            # Should retry on ValueError
            with pytest.raises(ValueError):
                handler.retry_on_failure(
                    mock_func, 
                    retryable_exceptions=(ValueError, ConnectionError)
                )
        
        assert mock_func.call_count == 3  # Initial + 2 retries
        
        # Reset mock
        mock_func.reset_mock()
        mock_func.side_effect = TypeError("Different error")
        
        # Should not retry on TypeError
        with pytest.raises(TypeError):
            handler.retry_on_failure(
                mock_func,
                retryable_exceptions=(ValueError, ConnectionError)
            )
        
        assert mock_func.call_count == 1  # Only initial attempt
    
    def test_error_context_logging(self):
        """Test error logging with context information."""
        mock_logger = Mock()
        handler = ErrorHandler(max_retries=1, retry_delay=0.1, logger=mock_logger)
        
        context = ErrorContext(
            error_type=ErrorType.FILE_ACCESS,
            operation="read_file",
            file_path="/test/file.sql"
        )
        
        mock_func = Mock(side_effect=FileNotFoundError("File not found"))
        
        with patch('time.sleep'):
            with pytest.raises(FileNotFoundError):
                handler.retry_on_failure(mock_func, context=context)
        
        # Check that error was logged with context
        assert mock_logger.error.called or mock_logger.warning.called
    
    def test_api_error_handling(self):
        """Test API-specific error handling."""
        mock_logger = Mock()
        handler = ErrorHandler(logger=mock_logger)
        
        # Test authentication error
        auth_error = Exception("Authentication failed")
        handler.handle_api_error(auth_error, "api_call")
        
        # Check that specific guidance was provided
        error_calls = [call.args[0] for call in mock_logger.error.call_args_list]
        assert any("API key" in call for call in error_calls)
    
    def test_database_error_handling(self):
        """Test database-specific error handling."""
        mock_logger = Mock()
        handler = ErrorHandler(logger=mock_logger)
        
        # Test connection error
        conn_error = Exception("Connection failed")
        handler.handle_db_error(conn_error, "SELECT * FROM users")
        
        # Check that specific guidance was provided
        error_calls = [call.args[0] for call in mock_logger.error.call_args_list]
        assert any("connection" in call.lower() for call in error_calls)
    
    def test_file_error_handling(self):
        """Test file-specific error handling."""
        mock_logger = Mock()
        handler = ErrorHandler(logger=mock_logger)
        
        # Test file not found error
        file_error = FileNotFoundError("File not found")
        handler.handle_file_error(file_error, "/test/file.sql", "read_file")
        
        # Check that specific guidance was provided
        error_calls = [call.args[0] for call in mock_logger.error.call_args_list]
        assert any("not found" in call for call in error_calls)
    
    def test_error_statistics(self):
        """Test error statistics tracking."""
        handler = ErrorHandler(max_retries=1, retry_delay=0.1)
        
        mock_func = Mock(side_effect=ValueError("Test error"))
        
        with patch('time.sleep'):
            with pytest.raises(ValueError):
                handler.retry_on_failure(mock_func)
        
        # Check statistics
        stats = handler.get_error_summary()
        assert stats['total_errors'] > 0
        assert 'ValueError' in stats['error_counts']
    
    def test_reset_statistics(self):
        """Test resetting error statistics."""
        handler = ErrorHandler()
        
        # Add some statistics
        handler.error_counts['TestError'] = 5
        handler.retry_counts['test_operation'] = 3
        
        # Reset
        handler.reset_stats()
        
        assert handler.error_counts == {}
        assert handler.retry_counts == {}
        
        stats = handler.get_error_summary()
        assert stats['total_errors'] == 0
        assert stats['total_retries'] == 0


class TestErrorContext:
    """Test cases for ErrorContext class."""
    
    def test_error_context_creation(self):
        """Test ErrorContext creation with various parameters."""
        context = ErrorContext(
            error_type=ErrorType.API_CALL,
            operation="generate_ddl",
            table_name="users",
            additional_info={"api_endpoint": "/v1/chat"}
        )
        
        assert context.error_type == ErrorType.API_CALL
        assert context.operation == "generate_ddl"
        assert context.table_name == "users"
        assert context.file_path is None
        assert context.additional_info["api_endpoint"] == "/v1/chat"


class TestRetryDecorator:
    """Test cases for retry decorator."""
    
    def test_retry_decorator_success(self):
        """Test retry decorator with successful function."""
        @retry_on_exception(ValueError, max_retries=2, delay=0.1)
        def successful_function(x):
            return x * 2
        
        result = successful_function(5)
        assert result == 10
    
    def test_retry_decorator_with_retries(self):
        """Test retry decorator with transient failures."""
        call_count = 0
        
        @retry_on_exception(ValueError, max_retries=2, delay=0.1)
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Transient error")
            return "Success"
        
        with patch('time.sleep'):
            result = failing_function()
        
        assert result == "Success"
        assert call_count == 3
    
    def test_retry_decorator_exhaustion(self):
        """Test retry decorator when retries are exhausted."""
        @retry_on_exception(ValueError, max_retries=1, delay=0.1)
        def always_failing_function():
            raise ValueError("Always fails")
        
        with patch('time.sleep'):
            with pytest.raises(ValueError, match="Always fails"):
                always_failing_function()
    
    def test_retry_decorator_non_retryable_exception(self):
        """Test retry decorator with non-retryable exception."""
        @retry_on_exception(ValueError, max_retries=2, delay=0.1)
        def function_with_type_error():
            raise TypeError("Not retryable")
        
        # Should not retry TypeError
        with pytest.raises(TypeError, match="Not retryable"):
            function_with_type_error()


class TestErrorTypes:
    """Test cases for ErrorType enum."""
    
    def test_error_type_values(self):
        """Test ErrorType enum values."""
        assert ErrorType.FILE_ACCESS.value == "file_access"
        assert ErrorType.API_CALL.value == "api_call"
        assert ErrorType.DATABASE_CONNECTION.value == "database_connection"
        assert ErrorType.SQL_PARSING.value == "sql_parsing"
        assert ErrorType.UNKNOWN.value == "unknown"