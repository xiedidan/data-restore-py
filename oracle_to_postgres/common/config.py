"""
Configuration management for Oracle to PostgreSQL migration tool.
"""

import argparse
import os
from dataclasses import dataclass, field
from typing import Optional
import yaml


@dataclass
class DeepSeekConfig:
    """DeepSeek API configuration."""
    api_key: str = ""
    base_url: str = "https://api.deepseek.com"
    timeout: int = 30
    max_retries: int = 3


@dataclass
class PostgreSQLConfig:
    """PostgreSQL database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    schema: str = "public"
    username: str = ""
    password: str = ""


@dataclass
class PerformanceConfig:
    """Performance and optimization configuration."""
    max_workers: int = 4
    batch_size: int = 1000
    memory_limit_mb: int = 1024


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    file: str = "./migration.log"


@dataclass
class Config:
    """Main configuration class for the migration tool."""
    
    # Basic configuration
    source_directory: str = ""
    ddl_directory: str = "./ddl"
    sample_lines: int = 100
    target_encoding: str = "utf-8"
    
    # Component configurations
    deepseek: DeepSeekConfig = field(default_factory=DeepSeekConfig)
    postgresql: PostgreSQLConfig = field(default_factory=PostgreSQLConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """Load configuration from YAML file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        config = cls()
        
        # Basic configuration
        config.source_directory = data.get('source_directory', config.source_directory)
        config.ddl_directory = data.get('ddl_directory', config.ddl_directory)
        config.sample_lines = data.get('sample_lines', config.sample_lines)
        config.target_encoding = data.get('target_encoding', config.target_encoding)
        
        # DeepSeek configuration
        if 'deepseek' in data:
            deepseek_data = data['deepseek']
            config.deepseek.api_key = deepseek_data.get('api_key', config.deepseek.api_key)
            config.deepseek.base_url = deepseek_data.get('base_url', config.deepseek.base_url)
            config.deepseek.timeout = deepseek_data.get('timeout', config.deepseek.timeout)
            config.deepseek.max_retries = deepseek_data.get('max_retries', config.deepseek.max_retries)
        
        # PostgreSQL configuration
        if 'postgresql' in data:
            pg_data = data['postgresql']
            config.postgresql.host = pg_data.get('host', config.postgresql.host)
            config.postgresql.port = pg_data.get('port', config.postgresql.port)
            config.postgresql.database = pg_data.get('database', config.postgresql.database)
            config.postgresql.schema = pg_data.get('schema', config.postgresql.schema)
            config.postgresql.username = pg_data.get('username', config.postgresql.username)
            config.postgresql.password = pg_data.get('password', config.postgresql.password)
        
        # Performance configuration
        if 'performance' in data:
            perf_data = data['performance']
            config.performance.max_workers = perf_data.get('max_workers', config.performance.max_workers)
            config.performance.batch_size = perf_data.get('batch_size', config.performance.batch_size)
            config.performance.memory_limit_mb = perf_data.get('memory_limit_mb', config.performance.memory_limit_mb)
        
        # Logging configuration
        if 'logging' in data:
            log_data = data['logging']
            config.logging.level = log_data.get('level', config.logging.level)
            config.logging.file = log_data.get('file', config.logging.file)
        
        return config
    
    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'Config':
        """Create configuration from command line arguments."""
        config = cls()
        
        # Update configuration with command line arguments
        if hasattr(args, 'source_directory') and args.source_directory:
            config.source_directory = args.source_directory
        if hasattr(args, 'ddl_directory') and args.ddl_directory:
            config.ddl_directory = args.ddl_directory
        if hasattr(args, 'sample_lines') and args.sample_lines:
            config.sample_lines = args.sample_lines
        if hasattr(args, 'target_encoding') and args.target_encoding:
            config.target_encoding = args.target_encoding
        
        # PostgreSQL arguments
        if hasattr(args, 'pg_host') and args.pg_host:
            config.postgresql.host = args.pg_host
        if hasattr(args, 'pg_port') and args.pg_port:
            config.postgresql.port = args.pg_port
        if hasattr(args, 'pg_database') and args.pg_database:
            config.postgresql.database = args.pg_database
        if hasattr(args, 'pg_schema') and args.pg_schema:
            config.postgresql.schema = args.pg_schema
        if hasattr(args, 'pg_username') and args.pg_username:
            config.postgresql.username = args.pg_username
        if hasattr(args, 'pg_password') and args.pg_password:
            config.postgresql.password = args.pg_password
        
        # DeepSeek API arguments
        if hasattr(args, 'deepseek_api_key') and args.deepseek_api_key:
            config.deepseek.api_key = args.deepseek_api_key
        if hasattr(args, 'deepseek_base_url') and args.deepseek_base_url:
            config.deepseek.base_url = args.deepseek_base_url
        
        # Performance arguments
        if hasattr(args, 'max_workers') and args.max_workers:
            config.performance.max_workers = args.max_workers
        if hasattr(args, 'batch_size') and args.batch_size:
            config.performance.batch_size = args.batch_size
        
        # Logging arguments
        if hasattr(args, 'log_level') and args.log_level:
            config.logging.level = args.log_level
        if hasattr(args, 'log_file') and args.log_file:
            config.logging.file = args.log_file
        
        return config
    
    def merge_with_file(self, config_path: str) -> 'Config':
        """Merge current configuration with file configuration."""
        if os.path.exists(config_path):
            file_config = self.from_file(config_path)
            # Command line arguments take precedence over file configuration
            # Only update empty/default values
            if not self.source_directory:
                self.source_directory = file_config.source_directory
            if not self.deepseek.api_key:
                self.deepseek.api_key = file_config.deepseek.api_key
            if not self.postgresql.database:
                self.postgresql.database = file_config.postgresql.database
            if not self.postgresql.username:
                self.postgresql.username = file_config.postgresql.username
            if not self.postgresql.password:
                self.postgresql.password = file_config.postgresql.password
        
        return self
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        errors = []
        
        if not self.source_directory:
            errors.append("Source directory is required")
        elif not os.path.exists(self.source_directory):
            errors.append(f"Source directory does not exist: {self.source_directory}")
        
        if not self.deepseek.api_key:
            errors.append("DeepSeek API key is required")
        
        if not self.postgresql.database:
            errors.append("PostgreSQL database name is required")
        
        if not self.postgresql.username:
            errors.append("PostgreSQL username is required")
        
        if self.sample_lines <= 0:
            errors.append("Sample lines must be greater than 0")
        
        if self.performance.max_workers <= 0:
            errors.append("Max workers must be greater than 0")
        
        if self.performance.batch_size <= 0:
            errors.append("Batch size must be greater than 0")
        
        if errors:
            raise ValueError("Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors))


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common command line arguments to parser."""
    parser.add_argument(
        '--config', '-c',
        type=str,
        default='config.yaml',
        help='Configuration file path (default: config.yaml)'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log file path (default: ./migration.log)'
    )


def add_source_arguments(parser: argparse.ArgumentParser) -> None:
    """Add source-related command line arguments."""
    parser.add_argument(
        '--source-directory', '-s',
        type=str,
        help='Directory containing Oracle SQL dump files (can be specified in config file)'
    )
    parser.add_argument(
        '--sample-lines',
        type=int,
        default=100,
        help='Number of lines to sample from each file (default: 100)'
    )


def add_deepseek_arguments(parser: argparse.ArgumentParser) -> None:
    """Add DeepSeek API command line arguments."""
    parser.add_argument(
        '--deepseek-api-key',
        type=str,
        help='DeepSeek API key'
    )
    parser.add_argument(
        '--deepseek-base-url',
        type=str,
        help='DeepSeek API base URL'
    )


def add_postgresql_arguments(parser: argparse.ArgumentParser) -> None:
    """Add PostgreSQL command line arguments."""
    parser.add_argument(
        '--pg-host',
        type=str,
        default='localhost',
        help='PostgreSQL host (default: localhost)'
    )
    parser.add_argument(
        '--pg-port',
        type=int,
        default=5432,
        help='PostgreSQL port (default: 5432)'
    )
    parser.add_argument(
        '--pg-database',
        type=str,
        required=True,
        help='PostgreSQL database name'
    )
    parser.add_argument(
        '--pg-schema',
        type=str,
        default='public',
        help='PostgreSQL schema (default: public)'
    )
    parser.add_argument(
        '--pg-username',
        type=str,
        help='PostgreSQL username'
    )
    parser.add_argument(
        '--pg-password',
        type=str,
        help='PostgreSQL password'
    )