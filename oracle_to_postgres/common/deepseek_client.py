"""
DeepSeek API client for DDL generation in Oracle to PostgreSQL migration tool.
"""

import json
import time
from typing import List, Optional, Dict, Any
import requests
from dataclasses import dataclass

from .logger import Logger


@dataclass
class DDLGenerationResult:
    """Result of DDL generation from DeepSeek API."""
    success: bool
    ddl_content: str
    error_message: Optional[str] = None
    api_response_time: float = 0.0
    tokens_used: Optional[int] = None


class DeepSeekClient:
    """Client for interacting with DeepSeek API to generate PostgreSQL DDL."""
    
    def __init__(self, api_key: str, base_url: str = "https://api.deepseek.com", 
                 model: str = "deepseek-reasoner", timeout: int = 30, max_retries: int = 3, 
                 logger: Optional[Logger] = None):
        """
        Initialize DeepSeek API client.
        
        Args:
            api_key: DeepSeek API key
            base_url: API base URL
            model: DeepSeek model to use
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            logger: Optional logger instance
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = logger or Logger()
        
        # API endpoints
        self.chat_endpoint = f"{self.base_url}/v1/chat/completions"
        
        # Request headers
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def generate_ddl(self, table_name: str, sample_inserts: List[str]) -> DDLGenerationResult:
        """
        Generate PostgreSQL DDL from sample INSERT statements.
        
        Args:
            table_name: Name of the table
            sample_inserts: List of sample INSERT statements
            
        Returns:
            DDLGenerationResult with generated DDL or error information
        """
        start_time = time.time()
        
        try:
            self.logger.debug(f"Building prompt for table {table_name} with {len(sample_inserts)} sample statements")
            
            # Build the prompt
            prompt = self._build_prompt(table_name, sample_inserts)
            
            self.logger.debug(f"Sending request to DeepSeek API for table {table_name}")
            
            # Make API request with retries
            response_data = self._make_api_request(prompt)
            
            self.logger.debug(f"Parsing DeepSeek response for table {table_name}")
            
            # Parse the response
            ddl_content = self._parse_response(response_data)
            
            response_time = time.time() - start_time
            self.logger.debug(f"DDL generation completed for {table_name} in {response_time:.2f}s")
            
            return DDLGenerationResult(
                success=True,
                ddl_content=ddl_content,
                api_response_time=response_time,
                tokens_used=response_data.get('usage', {}).get('total_tokens')
            )
            
        except Exception as e:
            response_time = time.time() - start_time
            error_msg = f"DDL generation failed for table {table_name}: {str(e)}"
            self.logger.error(error_msg, e)
            
            return DDLGenerationResult(
                success=False,
                ddl_content="",
                error_message=error_msg,
                api_response_time=response_time
            )
    
    def _build_prompt(self, table_name: str, sample_inserts: List[str]) -> str:
        """
        Build the prompt for DDL generation.
        
        Args:
            table_name: Name of the table
            sample_inserts: List of sample INSERT statements
            
        Returns:
            Formatted prompt string
        """
        # Limit the number of sample inserts to avoid token limits
        max_samples = min(len(sample_inserts), 10)
        limited_samples = sample_inserts[:max_samples]
        
        prompt = f"""You are a database migration expert. I need you to generate a PostgreSQL CREATE TABLE statement based on the following Oracle INSERT statements.

Table Name: {table_name}

Sample INSERT statements:
"""
        
        for i, insert_stmt in enumerate(limited_samples, 1):
            # Clean up the insert statement
            clean_stmt = insert_stmt.strip()
            if not clean_stmt.endswith(';'):
                clean_stmt += ';'
            prompt += f"{i}. {clean_stmt}\n"
        
        prompt += """
Please analyze these INSERT statements and generate a PostgreSQL CREATE TABLE statement that:

1. Uses appropriate PostgreSQL data types (INTEGER, VARCHAR, TEXT, DECIMAL, TIMESTAMP, BOOLEAN, etc.)
2. Infers column names from the INSERT statements
3. Sets reasonable column lengths for VARCHAR fields based on the sample data
4. Includes NOT NULL constraints where appropriate based on the data patterns
5. Uses proper PostgreSQL syntax and naming conventions
6. Adds a primary key constraint on the 'id' column if it exists
7. Includes comments explaining any assumptions made

Requirements:
- Use double quotes for column names to preserve case sensitivity
- Choose the most appropriate PostgreSQL data type for each column
- For VARCHAR columns, add a reasonable length limit (add 50% buffer to max observed length)
- For DECIMAL/NUMERIC columns, infer appropriate precision and scale
- Add NOT NULL constraints only if you're confident based on the sample data
- If you see date/timestamp patterns, use appropriate PostgreSQL date/time types

Please respond with ONLY the CREATE TABLE statement, no additional explanation or markdown formatting.
"""
        
        return prompt
    
    def _make_api_request(self, prompt: str) -> Dict[str, Any]:
        """
        Make API request to DeepSeek with retry logic.
        
        Args:
            prompt: The prompt to send to the API
            
        Returns:
            API response data
            
        Raises:
            Exception: If all retry attempts fail
        """
        request_data = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,  # Low temperature for consistent results
            "max_tokens": 2000,  # Sufficient for DDL generation
            "stream": False
        }
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying DeepSeek API request (attempt {attempt + 1}/{self.max_retries})")
                else:
                    self.logger.debug(f"Making DeepSeek API request using model {self.model}")
                
                response = requests.post(
                    self.chat_endpoint,
                    headers=self.headers,
                    json=request_data,
                    timeout=self.timeout
                )
                
                # Check for HTTP errors
                if response.status_code == 401:
                    raise Exception("Invalid API key or authentication failed")
                elif response.status_code == 429:
                    # Rate limit - wait and retry
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.logger.warning(f"Rate limit hit, waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)
                    continue
                elif response.status_code >= 400:
                    raise Exception(f"API request failed with status {response.status_code}: {response.text}")
                
                # Parse JSON response
                response_data = response.json()
                
                # Check for API errors
                if 'error' in response_data:
                    raise Exception(f"API error: {response_data['error']}")
                
                return response_data
                
            except requests.exceptions.Timeout:
                last_exception = Exception(f"API request timeout after {self.timeout} seconds")
                self.logger.warning(f"Request timeout on attempt {attempt + 1}")
                
            except requests.exceptions.ConnectionError:
                last_exception = Exception("Failed to connect to DeepSeek API")
                self.logger.warning(f"Connection error on attempt {attempt + 1}")
                
            except requests.exceptions.RequestException as e:
                last_exception = Exception(f"Request error: {str(e)}")
                self.logger.warning(f"Request error on attempt {attempt + 1}: {str(e)}")
                
            except json.JSONDecodeError:
                last_exception = Exception("Invalid JSON response from API")
                self.logger.warning(f"JSON decode error on attempt {attempt + 1}")
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            
            # Wait before retry (except on last attempt)
            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                time.sleep(wait_time)
        
        # All retries failed
        raise last_exception or Exception("All API request attempts failed")
    
    def _parse_response(self, response_data: Dict[str, Any]) -> str:
        """
        Parse the API response to extract DDL content.
        
        Args:
            response_data: Raw API response data
            
        Returns:
            Extracted DDL content
            
        Raises:
            Exception: If response format is invalid
        """
        try:
            # Extract the generated content
            choices = response_data.get('choices', [])
            if not choices:
                raise Exception("No choices in API response")
            
            message = choices[0].get('message', {})
            content = message.get('content', '').strip()
            
            if not content:
                raise Exception("Empty content in API response")
            
            # Clean up the content
            ddl_content = self._clean_ddl_content(content)
            
            # Validate that it looks like a CREATE TABLE statement
            if not self._validate_ddl_content(ddl_content):
                raise Exception("Generated content does not appear to be a valid CREATE TABLE statement")
            
            return ddl_content
            
        except KeyError as e:
            raise Exception(f"Invalid response format: missing key {e}")
        except Exception as e:
            raise Exception(f"Failed to parse API response: {str(e)}")
    
    def _clean_ddl_content(self, content: str) -> str:
        """
        Clean up the DDL content from API response.
        
        Args:
            content: Raw content from API
            
        Returns:
            Cleaned DDL content
        """
        # Remove markdown code blocks if present
        if content.startswith('```'):
            lines = content.split('\n')
            # Remove first line (```sql or similar)
            if lines[0].startswith('```'):
                lines = lines[1:]
            # Remove last line if it's ```
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            content = '\n'.join(lines)
        
        # Remove extra whitespace
        content = content.strip()
        
        # Ensure it ends with semicolon
        if not content.endswith(';'):
            content += ';'
        
        return content
    
    def _validate_ddl_content(self, content: str) -> bool:
        """
        Validate that the content looks like a CREATE TABLE statement.
        
        Args:
            content: DDL content to validate
            
        Returns:
            True if content appears to be valid DDL
        """
        content_upper = content.upper().strip()
        
        # Basic validation
        if not content_upper.startswith('CREATE TABLE'):
            return False
        
        # Check for required elements
        required_elements = ['(', ')', ';']
        for element in required_elements:
            if element not in content:
                return False
        
        return True
    
    def test_connection(self) -> bool:
        """
        Test the API connection and authentication.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Simple test request
            test_prompt = "Please respond with 'OK' to confirm the connection."
            
            request_data = {
                "model": self.model,
                "messages": [{"role": "user", "content": test_prompt}],
                "max_tokens": 10
            }
            
            response = requests.post(
                self.chat_endpoint,
                headers=self.headers,
                json=request_data,
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_usage_info(self) -> Dict[str, Any]:
        """
        Get API usage information (if available).
        
        Returns:
            Dictionary with usage information
        """
        # This would depend on DeepSeek API's usage endpoint
        # For now, return basic info
        return {
            "api_key_configured": bool(self.api_key),
            "base_url": self.base_url,
            "timeout": self.timeout,
            "max_retries": self.max_retries
        }