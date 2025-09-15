#!/usr/bin/env python3
"""
Oracle to PostgreSQL Migration Setup Script

This script helps users set up the migration environment by:
1. Creating necessary directories
2. Copying configuration templates
3. Validating dependencies
4. Testing database connectivity

Usage:
    python setup_migration.py --environment dev|prod|test
    python setup_migration.py --config-only
"""

import argparse
import os
import shutil
import sys
from pathlib import Path


def create_directories():
    """Create necessary directories for migration."""
    directories = ['ddl', 'reports', 'logs']
    
    print("Creating directories...")
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"  ✓ Created {directory}/")


def copy_config_template(environment=None):
    """Copy appropriate configuration template."""
    if environment:
        source_config = f"examples/config-{environment}.yaml"
        if not os.path.exists(source_config):
            print(f"  ✗ Environment config not found: {source_config}")
            return False
    else:
        source_config = "config.yaml.template"
    
    target_config = "config.yaml"
    
    if os.path.exists(target_config):
        response = input(f"config.yaml already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("  - Skipped config.yaml (already exists)")
            return True
    
    try:
        shutil.copy2(source_config, target_config)
        print(f"  ✓ Copied {source_config} to config.yaml")
        return True
    except FileNotFoundError:
        print(f"  ✗ Template not found: {source_config}")
        return False


def check_dependencies():
    """Check if required Python packages are installed."""
    required_packages = [
        'yaml', 'psycopg2', 'requests', 'chardet'
    ]
    
    print("Checking dependencies...")
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} (missing)")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Install with: pip install -r requirements.txt")
        return False
    
    return True


def test_config_loading():
    """Test if configuration can be loaded."""
    if not os.path.exists('config.yaml'):
        print("  - No config.yaml found, skipping config test")
        return True
    
    try:
        # Add project root to path
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        from oracle_to_postgres.common.config import Config
        config = Config.from_file('config.yaml')
        print("  ✓ Configuration file loads successfully")
        
        # Basic validation (will show what's missing)
        try:
            config.validate()
            print("  ✓ Configuration validation passed")
        except ValueError as e:
            print(f"  ! Configuration needs updates:")
            for line in str(e).split('\n'):
                if line.strip().startswith('-'):
                    print(f"    {line.strip()}")
        
        return True
    except Exception as e:
        print(f"  ✗ Configuration error: {e}")
        return False


def test_database_connection():
    """Test PostgreSQL database connection."""
    if not os.path.exists('config.yaml'):
        print("  - No config.yaml found, skipping database test")
        return True
    
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        from oracle_to_postgres.common.config import Config
        from oracle_to_postgres.common.database import DatabaseManager, ConnectionInfo
        
        config = Config.from_file('config.yaml')
        
        # Skip if database config is not complete
        if not all([config.postgresql.database, config.postgresql.username]):
            print("  - Database configuration incomplete, skipping connection test")
            return True
        
        connection_info = ConnectionInfo(
            host=config.postgresql.host,
            port=config.postgresql.port,
            database=config.postgresql.database,
            username=config.postgresql.username,
            password=config.postgresql.password,
            schema=config.postgresql.schema
        )
        
        db_manager = DatabaseManager(connection_info)
        db_manager.connect()
        db_manager.disconnect()
        
        print("  ✓ Database connection successful")
        return True
        
    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        print("    Update config.yaml with correct database settings")
        return False


def show_next_steps():
    """Show user what to do next."""
    print("\n" + "="*60)
    print("SETUP COMPLETE!")
    print("="*60)
    
    print("\nNext steps:")
    print("1. Edit config.yaml with your specific settings:")
    print("   - source_directory: Path to your Oracle SQL files")
    print("   - deepseek.api_key: Your DeepSeek API key")
    print("   - postgresql.*: Your PostgreSQL connection details")
    
    print("\n2. Test the configuration:")
    print("   python setup_migration.py --test-only")
    
    print("\n3. Run the migration:")
    print("   python analyze_sql.py --config config.yaml")
    print("   python create_tables.py --config config.yaml")
    print("   python import_data.py --config config.yaml")
    
    print("\n4. For help, see documentation:")
    print("   - docs/USAGE.md - Detailed usage instructions")
    print("   - docs/EXAMPLES.md - Example configurations")
    print("   - docs/TROUBLESHOOTING.md - Common issues and solutions")


def main():
    parser = argparse.ArgumentParser(
        description="Set up Oracle to PostgreSQL migration environment"
    )
    parser.add_argument(
        '--environment', '-e',
        choices=['dev', 'prod', 'test'],
        help='Use environment-specific configuration template'
    )
    parser.add_argument(
        '--config-only',
        action='store_true',
        help='Only copy configuration template'
    )
    parser.add_argument(
        '--test-only',
        action='store_true',
        help='Only test existing configuration'
    )
    
    args = parser.parse_args()
    
    print("Oracle to PostgreSQL Migration Setup")
    print("="*40)
    
    success = True
    
    if args.test_only:
        print("\nTesting existing configuration...")
        success &= check_dependencies()
        success &= test_config_loading()
        success &= test_database_connection()
    else:
        if not args.config_only:
            create_directories()
            success &= check_dependencies()
        
        success &= copy_config_template(args.environment)
        
        if not args.config_only:
            success &= test_config_loading()
    
    if success and not args.test_only:
        show_next_steps()
    elif success and args.test_only:
        print("\n✓ All tests passed! Your configuration is ready.")
    else:
        print("\n✗ Setup completed with issues. Please review the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()