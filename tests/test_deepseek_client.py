"""
Tests for DeepSeek API client functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from oracle_to_postgres.common.deepseek_client import DeepSeekClient, DDLGenerationResult


class TestDeepSeekClient:
    """Test cases for DeepSeekClient class."""
    
    def test_client_initialization(self):
        """Test client initialization with various parameters."""
        client = DeepSeekClient(api_key="test-key")
        
        assert client.api_key == "test-key"
        assert client.base_url == "https://api.deepseek.com"
        assert client.timeout == 30
        assert client.max_retries == 3
        assert "Bearer test-key" in client.headers["Authorization"]
    
    def test_client_initialization_custom_params(self):
        """Test client initialization with custom parameters."""
        client = DeepSeekClient(
            api_key="custom-key",
            base_url="https://custom.api.com",
            timeout=60,
            max_retries=5
        )
        
        assert client.api_key == "custom-key"
        assert client.base_url == "https://custom.api.com"
        assert client.timeout == 60
        assert client.max_retries == 5
    
    def test_build_prompt(self):
        """Test prompt building for DDL generation."""
        client = DeepSeekClient(api_key="test-key")
        
        sample_inserts = [
            "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');",
            "INSERT INTO users (id, name, email) VALUES (2, 'Jane', 'jane@example.com');"
        ]
        
        prompt = client._build_prompt("users", sample_inserts)
        
        assert "users" in prompt
        assert "CREATE TABLE" in prompt
        assert "PostgreSQL" in prompt
        assert "john@example.com" in prompt
        assert "jane@example.com" in prompt
    
    def test_build_prompt_with_many_samples(self):
        """Test prompt building with many sample inserts (should limit to 10)."""
        client = DeepSeekClient(api_key="test-key")
        
        # Create 15 sample inserts
        sample_inserts = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User{i}');"
            for i in range(1, 16)
        ]
        
        prompt = client._build_prompt("users", sample_inserts)
        
        # Should only include first 10 samples
        assert "User1" in prompt
        assert "User10" in prompt
        assert "User15" not in prompt
    
    def test_clean_ddl_content(self):
        """Test DDL content cleaning."""
        client = DeepSeekClient(api_key="test-key")
        
        # Test with markdown code blocks
        content_with_markdown = """```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);
```"""
        
        cleaned = client._clean_ddl_content(content_with_markdown)
        assert not cleaned.startswith('```')
        assert not cleaned.endswith('```')
        assert cleaned.endswith(';')
        assert "CREATE TABLE users" in cleaned
    
    def test_clean_ddl_content_without_semicolon(self):
        """Test DDL content cleaning when semicolon is missing."""
        client = DeepSeekClient(api_key="test-key")
        
        content = "CREATE TABLE users (id INTEGER)"
        cleaned = client._clean_ddl_content(content)
        
        assert cleaned.endswith(';')
    
    def test_validate_ddl_content(self):
        """Test DDL content validation."""
        client = DeepSeekClient(api_key="test-key")
        
        # Valid DDL
        valid_ddl = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));"
        assert client._validate_ddl_content(valid_ddl) is True
        
        # Invalid DDL (missing CREATE TABLE)
        invalid_ddl = "SELECT * FROM users;"
        assert client._validate_ddl_content(invalid_ddl) is False
        
        # Invalid DDL (missing parentheses)
        invalid_ddl2 = "CREATE TABLE users"
        assert client._validate_ddl_content(invalid_ddl2) is False
    
    @patch('requests.post')
    def test_successful_api_request(self, mock_post):
        """Test successful API request."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));'
                }
            }],
            'usage': {'total_tokens': 150}
        }
        mock_post.return_value = mock_response
        
        client = DeepSeekClient(api_key="test-key")
        sample_inserts = ["INSERT INTO users (id, name) VALUES (1, 'John');"]
        
        result = client.generate_ddl("users", sample_inserts)
        
        assert result.success is True
        assert "CREATE TABLE users" in result.ddl_content
        assert result.tokens_used == 150
        assert result.error_message is None
    
    @patch('requests.post')
    def test_api_request_with_authentication_error(self, mock_post):
        """Test API request with authentication error."""
        # Mock 401 response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        
        client = DeepSeekClient(api_key="invalid-key")
        sample_inserts = ["INSERT INTO users (id, name) VALUES (1, 'John');"]
        
        result = client.generate_ddl("users", sample_inserts)
        
        assert result.success is False
        assert "authentication failed" in result.error_message.lower()
    
    @patch('requests.post')
    def test_api_request_with_rate_limit(self, mock_post):
        """Test API request with rate limiting."""
        # Mock 429 response followed by success
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.text = "Rate limit exceeded"
        
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'CREATE TABLE users (id INTEGER);'
                }
            }]
        }
        
        # First call returns 429, second call returns 200
        mock_post.side_effect = [mock_response_429, mock_response_200]
        
        client = DeepSeekClient(api_key="test-key", max_retries=2)
        sample_inserts = ["INSERT INTO users (id) VALUES (1);"]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = client.generate_ddl("users", sample_inserts)
        
        assert result.success is True
        assert mock_post.call_count == 2
    
    @patch('requests.post')
    def test_api_request_timeout(self, mock_post):
        """Test API request timeout handling."""
        # Mock timeout exception
        mock_post.side_effect = Exception("Request timeout")
        
        client = DeepSeekClient(api_key="test-key", max_retries=1)
        sample_inserts = ["INSERT INTO users (id) VALUES (1);"]
        
        result = client.generate_ddl("users", sample_inserts)
        
        assert result.success is False
        assert "timeout" in result.error_message.lower() or "failed" in result.error_message.lower()
    
    @patch('requests.post')
    def test_api_request_invalid_json_response(self, mock_post):
        """Test handling of invalid JSON response."""
        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_post.return_value = mock_response
        
        client = DeepSeekClient(api_key="test-key", max_retries=1)
        sample_inserts = ["INSERT INTO users (id) VALUES (1);"]
        
        result = client.generate_ddl("users", sample_inserts)
        
        assert result.success is False
        assert "json" in result.error_message.lower() or "failed" in result.error_message.lower()
    
    @patch('requests.post')
    def test_api_response_parsing_error(self, mock_post):
        """Test handling of API response parsing errors."""
        # Mock response with missing expected fields
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'invalid_structure': True
        }
        mock_post.return_value = mock_response
        
        client = DeepSeekClient(api_key="test-key")
        sample_inserts = ["INSERT INTO users (id) VALUES (1);"]
        
        result = client.generate_ddl("users", sample_inserts)
        
        assert result.success is False
        assert result.error_message is not None
    
    @patch('requests.post')
    def test_test_connection_success(self, mock_post):
        """Test successful connection test."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        client = DeepSeekClient(api_key="test-key")
        
        result = client.test_connection()
        
        assert result is True
    
    @patch('requests.post')
    def test_test_connection_failure(self, mock_post):
        """Test failed connection test."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response
        
        client = DeepSeekClient(api_key="invalid-key")
        
        result = client.test_connection()
        
        assert result is False
    
    def test_get_usage_info(self):
        """Test getting usage information."""
        client = DeepSeekClient(
            api_key="test-key",
            base_url="https://custom.api.com",
            timeout=60,
            max_retries=5
        )
        
        usage_info = client.get_usage_info()
        
        assert usage_info['api_key_configured'] is True
        assert usage_info['base_url'] == "https://custom.api.com"
        assert usage_info['timeout'] == 60
        assert usage_info['max_retries'] == 5
    
    def test_get_usage_info_no_api_key(self):
        """Test usage info when no API key is configured."""
        client = DeepSeekClient(api_key="")
        
        usage_info = client.get_usage_info()
        
        assert usage_info['api_key_configured'] is False


class TestDDLGenerationResult:
    """Test cases for DDLGenerationResult dataclass."""
    
    def test_successful_result_creation(self):
        """Test creation of successful DDL generation result."""
        result = DDLGenerationResult(
            success=True,
            ddl_content="CREATE TABLE users (id INTEGER);",
            api_response_time=1.5,
            tokens_used=100
        )
        
        assert result.success is True
        assert result.ddl_content == "CREATE TABLE users (id INTEGER);"
        assert result.error_message is None
        assert result.api_response_time == 1.5
        assert result.tokens_used == 100
    
    def test_failed_result_creation(self):
        """Test creation of failed DDL generation result."""
        result = DDLGenerationResult(
            success=False,
            ddl_content="",
            error_message="API call failed",
            api_response_time=0.5
        )
        
        assert result.success is False
        assert result.ddl_content == ""
        assert result.error_message == "API call failed"
        assert result.api_response_time == 0.5
        assert result.tokens_used is None