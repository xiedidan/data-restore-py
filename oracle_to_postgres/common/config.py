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
    model: str = "deepseek-reasoner"  # Default to reasoner model
    timeout: int = 30
    max_retries: int = 3
    auto_fallback: bool = True  # Auto fallback to chat model if reasoner fails


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
    show_progress_steps: bool = True  # Show detailed progress steps


@dataclass
class TableCreationConfig:
    """Table creation configuration."""
    drop_existing: bool = False  # Drop existing tables before creating
    stop_on_error: bool = False  # Stop execution on first error
    dry_run: bool = False  # Only show what would be done, don't execute


@dataclass
class Config:
    """Main configuration class for the migration tool."""
    
    # Basic configuration
    source_directory: str = ""
    ddl_directory: str = "./ddl"
    sample_lines: int = 100
    max_insert_samples: int = 20  # Maximum INSERT statements to send to DeepSeek
    target_encoding: str = "utf-8"
    
    # Component configurations
    deepseek: DeepSeekConfig = field(default_factory=DeepSeekConfig)
    postgresql: PostgreSQLConfig = field(default_factory=PostgreSQLConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    table_creation: TableCreationConfig = field(default_factory=TableCreationConfig)
    
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
        config.max_insert_samples = data.get('max_insert_samples', config.max_insert_samples)
        config.target_encoding = data.get('target_encoding', config.target_encoding)
        
        # DeepSeek configuration
        if 'deepseek' in data:
            deepseek_data = data['deepseek']
            config.deepseek.api_key = deepseek_data.get('api_key', config.deepseek.api_key)
            config.deepseek.base_url = deepseek_data.get('base_url', config.deepseek.base_url)
            config.deepseek.model = deepseek_data.get('model', config.deepseek.model)
            config.deepseek.timeout = deepseek_data.get('timeout', config.deepseek.timeout)
            config.deepseek.max_retries = deepseek_data.get('max_retries', config.deepseek.max_retries)
            config.deepseek.auto_fallback = deepseek_data.get('auto_fallback', config.deepseek.auto_fallback)
        
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
            config.logging.show_progress_steps = log_data.get('show_progress_steps', config.logging.show_progress_steps)
        
        # Table creation configuration
        if 'table_creation' in data:
            table_data = data['table_creation']
            config.table_creation.drop_existing = table_data.get('drop_existing', config.table_creation.drop_existing)
            config.table_creation.stop_on_error = table_data.get('stop_on_error', config.table_creation.stop_on_error)
            config.table_creation.dry_run = table_data.get('dry_run', config.table_creation.dry_run)
        
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
        if hasattr(args, 'sample_lines') and args.sample_lines is not None:
            config.sample_lines = args.sample_lines
        if hasattr(args, 'target_encoding') and args.target_encoding:
            config.target_encoding = args.target_encoding
        
        # PostgreSQL arguments
        if hasattr(args, 'pg_host') and args.pg_host is not None:
            config.postgresql.host = args.pg_host
        if hasattr(args, 'pg_port') and args.pg_port is not None:
            config.postgresql.port = args.pg_port
        if hasattr(args, 'pg_database') and args.pg_database is not None:
            config.postgresql.database = args.pg_database
        if hasattr(args, 'pg_schema') and args.pg_schema is not None:
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
        if hasattr(args, 'deepseek_model') and args.deepseek_model:
            config.deepseek.model = args.deepseek_model
        if hasattr(args, 'deepseek_timeout') and args.deepseek_timeout:
            config.deepseek.timeout = args.deepseek_timeout
        if hasattr(args, 'deepseek_max_retries') and args.deepseek_max_retries:
            config.deepseek.max_retries = args.deepseek_max_retries
        
        # Performance arguments
        if hasattr(args, 'max_workers') and args.max_workers:
            config.performance.max_workers = args.max_workers
        if hasattr(args, 'batch_size') and args.batch_size:
            config.performance.batch_size = args.batch_size
        
        # Logging arguments
        if hasattr(args, 'log_level') and args.log_level is not None:
            config.logging.level = args.log_level
        if hasattr(args, 'log_file') and args.log_file is not None:
            config.logging.file = args.log_file
        if hasattr(args, 'simple_progress') and args.simple_progress:
            config.logging.show_progress_steps = False
        
        # Table creation arguments
        if hasattr(args, 'drop_existing') and args.drop_existing:
            config.table_creation.drop_existing = args.drop_existing
        if hasattr(args, 'stop_on_error') and args.stop_on_error:
            config.table_creation.stop_on_error = args.stop_on_error
        if hasattr(args, 'dry_run') and args.dry_run:
            config.table_creation.dry_run = args.dry_run
        
        return config
    
    def merge_with_file(self, config_path: str) -> 'Config':
        """Merge current configuration with file configuration."""
        if os.path.exists(config_path):
            file_config = self.from_file(config_path)
            # Command line arguments take precedence over file configuration
            # Only update empty/default values
            
            # Basic configuration
            if not self.source_directory:
                self.source_directory = file_config.source_directory
            if self.sample_lines == 100:  # Default value, use file config
                self.sample_lines = file_config.sample_lines
            if self.max_insert_samples == 20:  # Default value, use file config
                self.max_insert_samples = file_config.max_insert_samples
            if self.target_encoding == "utf-8":  # Default value, use file config
                self.target_encoding = file_config.target_encoding
            
            # DeepSeek configuration
            if not self.deepseek.api_key:
                self.deepseek.api_key = file_config.deepseek.api_key
            if self.deepseek.base_url == "https://api.deepseek.com":  # Default value
                self.deepseek.base_url = file_config.deepseek.base_url
            if self.deepseek.model == "deepseek-reasoner":  # Default value
                self.deepseek.model = file_config.deepseek.model
            if self.deepseek.timeout == 30:  # Default value, use file config
                self.deepseek.timeout = file_config.deepseek.timeout
            if self.deepseek.max_retries == 3:  # Default value
                self.deepseek.max_retries = file_config.deepseek.max_retries
            if self.deepseek.auto_fallback == True:  # Default value
                self.deepseek.auto_fallback = file_config.deepseek.auto_fallback
            
            # PostgreSQL configuration
            if not self.postgresql.database:
                self.postgresql.database = file_config.postgresql.database
            if not self.postgresql.username:
                self.postgresql.username = file_config.postgresql.username
            if not self.postgresql.password:
                self.postgresql.password = file_config.postgresql.password
            if self.postgresql.host == "localhost":  # Default value
                self.postgresql.host = file_config.postgresql.host
            if self.postgresql.port == 5432:  # Default value
                self.postgresql.port = file_config.postgresql.port
            if self.postgresql.schema == "public":  # Default value
                self.postgresql.schema = file_config.postgresql.schema
            
            # Performance configuration
            if self.performance.max_workers == 4:  # Default value
                self.performance.max_workers = file_config.performance.max_workers
            if self.performance.batch_size == 1000:  # Default value
                self.performance.batch_size = file_config.performance.batch_size
            if self.performance.memory_limit_mb == 1024:  # Default value
                self.performance.memory_limit_mb = file_config.performance.memory_limit_mb
            
            # Logging configuration
            if self.logging.level == "INFO":  # Default value
                self.logging.level = file_config.logging.level
            if self.logging.file == "./migration.log":  # Default value
                self.logging.file = file_config.logging.file
            if self.logging.show_progress_steps == True:  # Default value
                self.logging.show_progress_steps = file_config.logging.show_progress_steps
            
            # Table creation configuration
            if self.table_creation.drop_existing == False:  # Default value
                self.table_creation.drop_existing = file_config.table_creation.drop_existing
            if self.table_creation.stop_on_error == False:  # Default value
                self.table_creation.stop_on_error = file_config.table_creation.stop_on_error
            if self.table_creation.dry_run == False:  # Default value
                self.table_creation.dry_run = file_config.table_creation.dry_run
        
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
        help='Logging level (default: from config file or INFO)'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log file path (default: ./migration.log)'
    )
    parser.add_argument(
        '--simple-progress',
        action='store_true',
        help='Use simple progress display instead of detailed steps'
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
        help='Number of lines to sample from each file (default: from config file or 100)'
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
    parser.add_argument(
        '--deepseek-model',
        type=str,
        choices=['deepseek-reasoner', 'deepseek-chat', 'deepseek-coder'],
        help='DeepSeek model to use (default: deepseek-reasoner)'
    )
    parser.add_argument(
        '--deepseek-timeout',
        type=int,
        help='DeepSeek API timeout in seconds'
    )
    parser.add_argument(
        '--deepseek-max-retries',
        type=int,
        help='Maximum number of DeepSeek API retry attempts'
    )


def add_table_creation_arguments(parser: argparse.ArgumentParser) -> None:
    """Add table creation command line arguments."""
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing tables before creating new ones (can be specified in config file)'
    )
    parser.add_argument(
        '--stop-on-error',
        action='store_true',
        help='Stop execution on first error (can be specified in config file)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing DDL (can be specified in config file)'
    )


def add_postgresql_arguments(parser: argparse.ArgumentParser) -> None:
    """Add PostgreSQL command line arguments."""
    parser.add_argument(
        '--pg-host',
        type=str,
        help='PostgreSQL host (default: from config file or localhost)'
    )
    parser.add_argument(
        '--pg-port',
        type=int,
        help='PostgreSQL port (default: from config file or 5432)'
    )
    parser.add_argument(
        '--pg-database',
        type=str,
        help='PostgreSQL database name (can be specified in config file)'
    )
    parser.add_argument(
        '--pg-schema',
        type=str,
        help='PostgreSQL schema (default: from config file or public)'
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