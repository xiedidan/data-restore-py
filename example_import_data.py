#!/usr/bin/env python3
"""
Example usage of the import_data.py script.
"""

import os
import tempfile
import csv
import json
from pathlib import Path

def create_sample_files():
    """Create sample files for demonstration."""
    # Create temporary directory structure
    temp_dir = tempfile.mkdtemp(prefix='oracle_import_demo_')
    
    # Create source directory with sample SQL files
    source_dir = os.path.join(temp_dir, 'sql_files')
    os.makedirs(source_dir, exist_ok=True)
    
    # Create output directory
    output_dir = os.path.join(temp_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    # Sample SQL files with Oracle-specific syntax
    sql_files = {
        'users.sql': """
-- Users table data
INSERT INTO ORACLE_DB.USERS VALUES (1, 'John Doe', 'john@example.com', SYSDATE);
INSERT INTO ORACLE_DB.USERS VALUES (2, 'Jane Smith', 'jane@example.com', TO_DATE('2023-01-01', 'YYYY-MM-DD'));
INSERT INTO ORACLE_DB.USERS VALUES (3, 'Bob Johnson', 'bob@example.com', SYSDATE);
        """,
        'orders.sql': """
-- Orders table data  
INSERT INTO ORACLE_DB.ORDERS VALUES (1, 1, TO_DATE('2023-01-15', 'YYYY-MM-DD'), 100.50);
INSERT INTO ORACLE_DB.ORDERS VALUES (2, 2, TO_DATE('2023-01-16', 'YYYY-MM-DD'), 250.75);
INSERT INTO ORACLE_DB.ORDERS VALUES (3, 1, SYSDATE, 75.25);
        """,
        'products.sql': """
-- Products table data
INSERT INTO ORACLE_DB.PRODUCTS VALUES (1, 'Widget A', 'High quality widget', 25.99);
INSERT INTO ORACLE_DB.PRODUCTS VALUES (2, 'Widget B', 'Premium widget', 35.99);
INSERT INTO ORACLE_DB.PRODUCTS VALUES (3, 'Widget C', 'Deluxe widget', 45.99);
        """
    }
    
    # Write SQL files
    for filename, content in sql_files.items():
        file_path = os.path.join(source_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content.strip())
    
    # Create sample encoding analysis report
    encoding_report = os.path.join(output_dir, 'encoding_analysis.csv')
    with open(encoding_report, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['file_path', 'encoding', 'confidence'])
        for filename in sql_files.keys():
            file_path = os.path.join(source_dir, filename)
            writer.writerow([file_path, 'utf-8', '0.99'])
    
    return temp_dir, source_dir, output_dir


def create_sample_config(temp_dir, source_dir, output_dir):
    """Create sample configuration file."""
    config_content = f"""
# Oracle to PostgreSQL Import Configuration

# Source settings
source_dir: "{source_dir}"
source_db: "ORACLE_DB"

# Target settings  
target_db: "postgres_db"
target_schema: "public"

# Database connection
db_host: "localhost"
db_port: 5432
db_user: "postgres"
db_password: "your_password_here"

# Performance settings
max_workers: 4
batch_size: 1000

# Encoding settings
default_encoding: "utf-8"
target_encoding: "utf-8"

# Output settings
output_dir: "{output_dir}"
log_level: "INFO"
verbose: true
"""
    
    config_file = os.path.join(temp_dir, 'import_config.yaml')
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write(config_content.strip())
    
    return config_file


def demonstrate_command_line_usage(source_dir, output_dir):
    """Demonstrate command line usage examples."""
    print("\n" + "="*60)
    print("COMMAND LINE USAGE EXAMPLES")
    print("="*60)
    
    print("\n1. Basic usage with command line parameters:")
    print("-" * 50)
    print(f"""python import_data.py \\
    --source-dir "{source_dir}" \\
    --target-db postgres_db \\
    --target-schema public \\
    --source-db ORACLE_DB \\
    --db-host localhost \\
    --db-port 5432 \\
    --db-user postgres \\
    --db-password your_password \\
    --output-dir "{output_dir}" \\
    --max-workers 4 \\
    --batch-size 1000""")
    
    print("\n2. Using configuration file:")
    print("-" * 50)
    print("python import_data.py --config import_config.yaml")
    
    print("\n3. With custom performance settings:")
    print("-" * 50)
    print(f"""python import_data.py \\
    --source-dir "{source_dir}" \\
    --target-db postgres_db \\
    --db-password your_password \\
    --max-workers 8 \\
    --batch-size 5000 \\
    --log-level DEBUG \\
    --verbose""")
    
    print("\n4. With encoding settings:")
    print("-" * 50)
    print(f"""python import_data.py \\
    --source-dir "{source_dir}" \\
    --target-db postgres_db \\
    --db-password your_password \\
    --default-encoding gbk \\
    --target-encoding utf-8""")


def show_sample_files(source_dir, output_dir):
    """Show sample files created."""
    print("\n" + "="*60)
    print("SAMPLE FILES CREATED")
    print("="*60)
    
    print(f"\nSource directory: {source_dir}")
    print("SQL files:")
    for file in os.listdir(source_dir):
        if file.endswith('.sql'):
            file_path = os.path.join(source_dir, file)
            print(f"  - {file}")
            
            # Show first few lines of each file
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()[:3]
                for line in lines:
                    if line.strip():
                        print(f"    {line.strip()}")
            print()
    
    print(f"\nOutput directory: {output_dir}")
    print("Files:")
    for file in os.listdir(output_dir):
        print(f"  - {file}")
        
        if file == 'encoding_analysis.csv':
            file_path = os.path.join(output_dir, file)
            with open(file_path, 'r', encoding='utf-8') as f:
                print(f"    Content preview:")
                for i, line in enumerate(f):
                    if i < 3:  # Show first 3 lines
                        print(f"      {line.strip()}")


def show_expected_output():
    """Show what the expected output would look like."""
    print("\n" + "="*60)
    print("EXPECTED OUTPUT")
    print("="*60)
    
    print("\nWhen you run the import_data.py script, you would see:")
    print("-" * 50)
    
    sample_output = """
2025-09-15 14:00:00 - migration - INFO - Starting data import process
2025-09-15 14:00:00 - migration - INFO - Loading encoding information...
2025-09-15 14:00:00 - migration - INFO - Loaded encoding information for 3 files
2025-09-15 14:00:00 - migration - INFO - Discovering SQL files...
2025-09-15 14:00:00 - migration - INFO - Discovered 3 SQL files in /path/to/sql_files
2025-09-15 14:00:00 - migration - INFO - Creating import tasks...
2025-09-15 14:00:00 - migration - INFO - Created 3 import tasks
2025-09-15 14:00:00 - migration - INFO - Starting parallel import of 3 files with 4 workers
2025-09-15 14:00:01 - migration - INFO - Import Progress: 33.3% (1/3 files) - Elapsed: 1.0s, Remaining: 2.0s
2025-09-15 14:00:02 - migration - INFO - Import Progress: 66.7% (2/3 files) - Elapsed: 2.0s, Remaining: 1.0s
2025-09-15 14:00:03 - migration - INFO - Import Progress: 100.0% (3/3 files) - Elapsed: 3.0s, Remaining: 0.0s
2025-09-15 14:00:03 - migration - INFO - Parallel import completed: 3/3 files successful, 9 records processed
2025-09-15 14:00:03 - migration - INFO - Data import process completed
2025-09-15 14:00:03 - migration - INFO - Import report generated: /output/import_report.json
2025-09-15 14:00:03 - migration - INFO - Import summary generated: /output/import_summary.csv

============================================================
DATA IMPORT SUMMARY
============================================================
Total files processed: 3
Successful files: 3
Failed files: 0
Total records: 9
Successful records: 9
Failed records: 0
Total processing time: 3.25 seconds
Throughput: 2.8 records/second
Success rate: 100.0%

File Details:
------------------------------------------------------------
✓ SUCCESS users.sql
  Table: users
  Records: 3 processed, 0 failed
  Time: 1.05s

✓ SUCCESS orders.sql
  Table: orders
  Records: 3 processed, 0 failed
  Time: 1.10s

✓ SUCCESS products.sql
  Table: products
  Records: 3 processed, 0 failed
  Time: 1.10s

All files imported successfully!
"""
    
    print(sample_output.strip())
    
    print("\n\nGenerated report files:")
    print("-" * 50)
    print("1. import_report.json - Detailed JSON report with:")
    print("   - Summary statistics")
    print("   - Individual file results")
    print("   - Error details")
    print("   - Warning messages")
    
    print("\n2. import_summary.csv - CSV summary with:")
    print("   - File name, table name, success status")
    print("   - Records processed and failed counts")
    print("   - Processing time for each file")
    print("   - Error messages (if any)")


def show_sql_transformations():
    """Show examples of SQL transformations."""
    print("\n" + "="*60)
    print("SQL TRANSFORMATIONS")
    print("="*60)
    
    transformations = [
        {
            'description': 'Database name replacement',
            'original': "INSERT INTO ORACLE_DB.USERS VALUES (1, 'John');",
            'transformed': 'INSERT INTO "postgres_db"."public"."USERS" VALUES (1, \'John\');'
        },
        {
            'description': 'Oracle SYSDATE to PostgreSQL NOW()',
            'original': "INSERT INTO USERS VALUES (1, 'John', SYSDATE);",
            'transformed': "INSERT INTO \"public\".\"USERS\" VALUES (1, 'John', NOW());"
        },
        {
            'description': 'Oracle TO_DATE to PostgreSQL timestamp',
            'original': "INSERT INTO ORDERS VALUES (1, TO_DATE('2023-01-01', 'YYYY-MM-DD'));",
            'transformed': "INSERT INTO \"public\".\"ORDERS\" VALUES (1, '2023-01-01'::timestamp);"
        }
    ]
    
    for i, transform in enumerate(transformations, 1):
        print(f"\n{i}. {transform['description']}:")
        print(f"   Original:    {transform['original']}")
        print(f"   Transformed: {transform['transformed']}")


def cleanup_demo_files(temp_dir):
    """Clean up demonstration files."""
    import shutil
    try:
        shutil.rmtree(temp_dir)
        print(f"\nDemo files cleaned up: {temp_dir}")
    except Exception as e:
        print(f"\nFailed to clean up demo files: {e}")


def main():
    """Main demonstration function."""
    print("Oracle to PostgreSQL Data Import Script Demonstration")
    print("=" * 60)
    
    try:
        # Create sample files
        print("Creating sample demonstration files...")
        temp_dir, source_dir, output_dir = create_sample_files()
        config_file = create_sample_config(temp_dir, source_dir, output_dir)
        
        print(f"Demo files created in: {temp_dir}")
        
        # Show sample files
        show_sample_files(source_dir, output_dir)
        
        # Show command line usage
        demonstrate_command_line_usage(source_dir, output_dir)
        
        # Show SQL transformations
        show_sql_transformations()
        
        # Show expected output
        show_expected_output()
        
        print("\n" + "="*60)
        print("NEXT STEPS")
        print("="*60)
        print("\n1. Set up your PostgreSQL database connection")
        print("2. Update the database password in the configuration")
        print("3. Run the import_data.py script with your actual SQL files")
        print("4. Monitor the progress and check the generated reports")
        
        print(f"\nConfiguration file created: {config_file}")
        print("You can use this as a template for your actual import.")
        
        # Ask if user wants to keep demo files
        try:
            keep_files = input("\nKeep demo files for testing? (y/N): ").lower().strip()
            if keep_files != 'y':
                cleanup_demo_files(temp_dir)
            else:
                print(f"Demo files kept in: {temp_dir}")
        except KeyboardInterrupt:
            cleanup_demo_files(temp_dir)
    
    except Exception as e:
        print(f"Error during demonstration: {e}")


if __name__ == "__main__":
    main()