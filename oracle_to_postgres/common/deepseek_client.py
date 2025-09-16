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
                 max_samples: int = 10, auto_fallback: bool = True, logger: Optional[Logger] = None):
        """
        Initialize DeepSeek API client.
        
        Args:
            api_key: DeepSeek API key
            base_url: API base URL
            model: DeepSeek model to use
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            max_samples: Maximum number of sample INSERT statements to use
            logger: Optional logger instance
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_samples = max_samples
        self.auto_fallback = auto_fallback
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
            
            self.logger.debug(f"Sending request to DeepSeek API for table {table_name} (timeout: {self.timeout}s)")
            
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
        max_samples = min(len(sample_inserts), self.max_samples)
        limited_samples = sample_inserts[:max_samples]
        
        prompt = f"""Task: Generate PostgreSQL CREATE TABLE DDL

Table Name: {table_name}

Oracle INSERT Statements:
"""
        
        for i, insert_stmt in enumerate(limited_samples, 1):
            # Clean up the insert statement
            clean_stmt = insert_stmt.strip()
            if not clean_stmt.endswith(';'):
                clean_stmt += ';'
            prompt += f"{clean_stmt}\n"
        
        prompt += f"""
INSTRUCTIONS:
Analyze the INSERT statements above and generate a PostgreSQL CREATE TABLE statement for table "{table_name}".

Requirements:
1. Use PostgreSQL data types: INTEGER, BIGINT, VARCHAR(n), TEXT, DECIMAL(p,s), TIMESTAMP, DATE, BOOLEAN
2. Infer column names and types from INSERT values
3. Set VARCHAR lengths with 50% buffer over max observed length
4. DO NOT add NOT NULL constraints - this is for data analysis, allow all columns to be nullable
5. DO NOT add PRIMARY KEY constraints - this is for data analysis, not production use
6. DO NOT add any CHECK constraints or other restrictions
7. Use double quotes for column names
8. Keep the DDL simple and permissive for data import

OUTPUT FORMAT:
Return ONLY the CREATE TABLE statement. No explanations, no markdown, no reasoning text.
Start directly with "CREATE TABLE" and end with the semicolon.

Example format:
CREATE TABLE "{table_name}" (
    "column1" INTEGER,
    "column2" VARCHAR(100),
    "column3" TEXT
);
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
        
        # Add special parameters for reasoner model
        if self.model == "deepseek-reasoner":
            request_data["temperature"] = 0.0  # Even lower temperature for reasoner
            request_data["max_tokens"] = 4000  # More tokens for reasoning + answer
        
        # For reasoner model, use longer timeout if not already set
        actual_timeout = self.timeout
        original_model = self.model
        
        if self.model == "deepseek-reasoner" and self.timeout < 60:
            actual_timeout = max(self.timeout, 60)
            self.logger.debug(f"Using extended timeout for reasoner model: {actual_timeout}s")
        
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
                    timeout=actual_timeout
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
                self.logger.warning(f"Request timeout on attempt {attempt + 1} (waited {self.timeout}s)")
                
            except requests.exceptions.ConnectionError as e:
                last_exception = Exception(f"Failed to connect to DeepSeek API: {str(e)}")
                self.logger.warning(f"Connection error on attempt {attempt + 1}: {str(e)}")
                
            except requests.exceptions.RequestException as e:
                last_exception = Exception(f"Request error: {str(e)}")
                self.logger.warning(f"Request error on attempt {attempt + 1}: {str(e)}")
                
            except json.JSONDecodeError as e:
                last_exception = Exception(f"Invalid JSON response from API: {str(e)}")
                self.logger.warning(f"JSON decode error on attempt {attempt + 1}: {str(e)}")
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                
                # If using reasoner model and getting parsing errors, try fallback to chat model
                if (self.auto_fallback and 
                    self.model == "deepseek-reasoner" and 
                    "Empty content" in str(e) and 
                    attempt == self.max_retries - 1):
                    self.logger.warning("Reasoner model failed, trying fallback to deepseek-chat")
                    request_data["model"] = "deepseek-chat"
                    # Give it one more try with chat model
                    try:
                        response = requests.post(
                            self.chat_endpoint,
                            headers=self.headers,
                            json=request_data,
                            timeout=actual_timeout
                        )
                        
                        if response.status_code == 200:
                            response_data = response.json()
                            if 'error' not in response_data:
                                return response_data
                    except Exception as fallback_e:
                        self.logger.warning(f"Fallback to chat model also failed: {fallback_e}")
            
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
            # Log the raw response for debugging
            self.logger.debug(f"Raw API response keys: {list(response_data.keys())}")
            
            # Extract the generated content
            choices = response_data.get('choices', [])
            if not choices:
                self.logger.error(f"No choices in API response. Response: {response_data}")
                raise Exception("No choices in API response")
            
            self.logger.debug(f"First choice keys: {list(choices[0].keys())}")
            
            message = choices[0].get('message', {})
            if not message:
                self.logger.error(f"No message in choice. Choice: {choices[0]}")
                raise Exception("No message in API response choice")
            
            self.logger.debug(f"Message keys: {list(message.keys())}")
            
            # Try different content extraction methods for different model types
            content = ""
            
            # Method 1: Standard content field (for chat models)
            if 'content' in message and message.get('content'):
                content = message.get('content', '').strip()
                self.logger.debug("Extracted content from 'content' field")
            
            # Method 2: For reasoner model, try reasoning_content first, then look for final answer
            elif self.model == "deepseek-reasoner":
                # Check if there's a reasoning_content field
                if 'reasoning_content' in message:
                    reasoning_text = message.get('reasoning_content', '')
                    self.logger.debug(f"Found reasoning_content: {reasoning_text[:200]}...")
                    
                    # Try to extract CREATE TABLE from reasoning content
                    content = self._extract_ddl_from_reasoning(reasoning_text)
                    if content:
                        self.logger.debug("Extracted DDL from reasoning_content")
                
                # If no content found in reasoning, check other fields
                if not content:
                    for field in ['content', 'text', 'answer', 'result']:
                        if field in message and message.get(field):
                            content = message.get(field, '').strip()
                            self.logger.debug(f"Extracted content from '{field}' field")
                            break
            
            # Method 3: Check if content is in a different structure
            elif isinstance(message.get('content'), dict):
                content_dict = message.get('content', {})
                content = content_dict.get('text', '').strip()
                self.logger.debug("Extracted content from nested content.text")
            
            # Method 4: Check for text field directly
            elif 'text' in message:
                content = message.get('text', '').strip()
                self.logger.debug("Extracted content from 'text' field")
            
            if not content:
                self.logger.error(f"Empty content in message. Message keys: {list(message.keys())}")
                # Try to extract any text-like content from the message
                for key, value in message.items():
                    if isinstance(value, str) and len(value.strip()) > 10:
                        self.logger.warning(f"Found potential content in '{key}' field: {value[:100]}...")
                        content = value.strip()
                        break
                
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
    
    def _extract_ddl_from_reasoning(self, reasoning_text: str) -> str:
        """
        Extract CREATE TABLE statement from reasoning content.
        
        Args:
            reasoning_text: The reasoning content from deepseek-reasoner
            
        Returns:
            Extracted DDL statement or empty string if not found
        """
        if not reasoning_text:
            return ""
        
        # Look for CREATE TABLE statements in the reasoning text
        import re
        
        # Pattern to match CREATE TABLE statements
        patterns = [
            r'CREATE TABLE[^;]+;',
            r'create table[^;]+;',
            r'CREATE\s+TABLE\s+[^;]+;'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, reasoning_text, re.IGNORECASE | re.DOTALL)
            if matches:
                # Return the first (and hopefully only) CREATE TABLE statement
                ddl = matches[0].strip()
                self.logger.debug(f"Extracted DDL from reasoning: {ddl[:100]}...")
                return ddl
        
        # If no complete CREATE TABLE found, look for partial DDL that might be at the end
        lines = reasoning_text.split('\n')
        ddl_lines = []
        in_create_table = False
        
        for line in lines:
            line = line.strip()
            if re.match(r'CREATE\s+TABLE', line, re.IGNORECASE):
                in_create_table = True
                ddl_lines = [line]
            elif in_create_table:
                ddl_lines.append(line)
                if line.endswith(';'):
                    # End of CREATE TABLE statement
                    ddl = '\n'.join(ddl_lines)
                    self.logger.debug(f"Extracted multi-line DDL from reasoning: {ddl[:100]}...")
                    return ddl
        
        return ""
    
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
                timeout=self.timeout
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