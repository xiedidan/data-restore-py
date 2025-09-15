"""
Tests for configuration management.
"""

import pytest
import tempfile
import os
from oracle_to_postgres.common.config import Config, DeepSeekConfig, PostgreSQLConfig


def test_config_defaults():
    """Test default configuration values."""
    config = Config()
    
    assert config.source_directory == ""
    assert config.ddl_directory == "./ddl"
    assert config.sample_lines == 100
    assert config.target_encoding == "utf-8"
    
    assert config.deepseek.base_url == "https://api.deepseek.com"
    assert config.deepseek.timeout == 30
    assert config.deepseek.max_retries == 3
    
    assert config.postgresql.host == "localhost"
    assert config.postgresql.port == 5432
    assert config.postgresql.schema == "public"
    
    assert config.performance.max_workers == 4
    assert config.performance.batch_size == 1000
    
    assert config.logging.level == "INFO"


def test_config_from_yaml():
    """Test loading configuration from YAML file."""
    yaml_content = """
source_directory: "/test/path"
sample_lines: 50
deepseek:
  api_key: "test-key"
  timeout: 60
postgresql:
  host: "testhost"
  database: "testdb"
  username: "testuser"
performance:
  max_workers: 8
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        
        try:
            config = Config.from_file(f.name)
            
            assert config.source_directory == "/test/path"
            assert config.sample_lines == 50
            assert config.deepseek.api_key == "test-key"
            assert config.deepseek.timeout == 60
            assert config.postgresql.host == "testhost"
            assert config.postgresql.database == "testdb"
            assert config.postgresql.username == "testuser"
            assert config.performance.max_workers == 8
        finally:
            os.unlink(f.name)


def test_config_validation():
    """Test configuration validation."""
    config = Config()
    
    # Should fail validation due to missing required fields
    with pytest.raises(ValueError) as exc_info:
        config.validate()
    
    error_message = str(exc_info.value)
    assert "Source directory is required" in error_message
    assert "DeepSeek API key is required" in error_message
    assert "PostgreSQL database name is required" in error_message
    assert "PostgreSQL username is required" in error_message


def test_config_validation_success():
    """Test successful configuration validation."""
    config = Config()
    config.source_directory = "/tmp"  # Use existing directory
    config.deepseek.api_key = "test-key"
    config.postgresql.database = "testdb"
    config.postgresql.username = "testuser"
    
    # Should not raise any exception
    config.validate()


def test_config_file_not_found():
    """Test handling of missing configuration file."""
    with pytest.raises(FileNotFoundError):
        Config.from_file("nonexistent.yaml")