#!/usr/bin/env python3
"""
Example usage of the parallel data importer.
"""

import os
import tempfile
from oracle_to_postgres.common.parallel_importer import (
    ImportTask, ImportResult, ImportProgress, ParallelImporter
)
from oracle_to_postgres.common.logger import Logger
from oracle_to_postgres.common.database import DatabaseManager, ConnectionInfo
from oracle_to_postgres.common.sql_rewriter import SQLRewriter


def create_sample_sql_files():
    """Create sample SQL files for testing."""
    temp_files = []
    
    # Create sample SQL content
    sql_contents = [
        """
        INSERT INTO users VALUES (1, 'John Doe', 'john@example.com');
        INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com');
        INSERT INTO users VALUES (3, 'Bob Johnson', 'bob@example.com');
        """,
        """
        INSERT INTO orders VALUES (1, 1, '2023-01-01', 100.00);
        INSERT INTO orders VALUES (2, 2, '2023-01-02', 150.50);
        INSERT INTO orders VALUES (3, 1, '2023-01-03', 75.25);
        """,
        """
        INSERT INTO products VALUES (1, 'Widget A', 'Description A', 25.99);
        INSERT INTO products VALUES (2, 'Widget B', 'Description B', 35.99);
        INSERT INTO products VALUES (3, 'Widget C', 'Description C', 45.99);
        """
    ]
    
    table_names = ['users', 'orders', 'products']
    
    for i, (content, table_name) in enumerate(zip(sql_contents, table_names)):
        with tempfile.NamedTemporaryFile(
            mode='w', 
            suffix=f'_{table_name}.sql', 
            delete=False, 
            encoding='utf-8'
        ) as f:
            f.write(content)
            temp_files.append((f.name, table_name))
    
    return temp_files


def progress_callback(progress: ImportProgress):
    """Progress callback function."""
    print(f"Progress: {progress.completion_percentage:.1f}% "
          f"({progress.completed_files}/{progress.total_files} files) "
          f"- Current: {progress.current_file}")


def main():
    """Main example function."""
    print("Parallel Data Importer Example")
    print("=" * 40)
    
    # Initialize logger
    logger = Logger()
    
    # Create sample SQL files
    print("Creating sample SQL files...")
    temp_files = create_sample_sql_files()
    
    try:
        # Create import tasks
        import_tasks = []
        for file_path, table_name in temp_files:
            task = ImportTask(
                file_path=file_path,
                table_name=table_name,
                encoding='utf-8'
            )
            import_tasks.append(task)
        
        print(f"Created {len(import_tasks)} import tasks")
        
        # Note: In a real scenario, you would initialize these with actual database connections
        # For this example, we'll create mock instances
        
        # Mock database manager (would be real in production)
        class MockDatabaseManager:
            def get_connection(self):
                class MockConnection:
                    def __enter__(self):
                        return self
                    def __exit__(self, *args):
                        pass
                    def cursor(self):
                        class MockCursor:
                            def __enter__(self):
                                return self
                            def __exit__(self, *args):
                                pass
                            def execute(self, sql):
                                print(f"  Executing: {sql[:50]}...")
                    
                    def commit(self):
                        pass
                        return MockCursor()
                return MockConnection()
        
        # Mock SQL rewriter (would be real in production)
        class MockSQLRewriter:
            def rewrite_insert_statement(self, statement):
                # Simple mock rewriting - just return the statement
                return statement.replace('INSERT INTO', 'INSERT INTO "public".')
        
        # Initialize components
        db_manager = MockDatabaseManager()
        sql_rewriter = MockSQLRewriter()
        
        # Create parallel importer
        parallel_importer = ParallelImporter(
            db_manager=db_manager,
            sql_rewriter=sql_rewriter,
            max_workers=2,  # Use 2 workers for this example
            batch_size=10,  # Small batch size for demonstration
            logger=logger
        )
        
        print("\nStarting parallel import...")
        
        # Import files with progress monitoring
        results = parallel_importer.import_files(
            import_tasks=import_tasks,
            progress_callback=progress_callback
        )
        
        print("\nImport Results:")
        print("-" * 40)
        
        for result in results:
            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print(f"{status} {os.path.basename(result.file_path)}")
            print(f"  Table: {result.table_name}")
            print(f"  Records processed: {result.records_processed}")
            print(f"  Records failed: {result.records_failed}")
            print(f"  Processing time: {result.processing_time:.2f}s")
            
            if result.warnings:
                print(f"  Warnings: {len(result.warnings)}")
            
            if result.error_message:
                print(f"  Error: {result.error_message}")
            
            print()
        
        # Display statistics
        stats = parallel_importer.get_statistics()
        print("Overall Statistics:")
        print("-" * 40)
        print(f"Total files: {stats['total_files']}")
        print(f"Successful files: {stats['successful_files']}")
        print(f"Failed files: {stats['failed_files']}")
        print(f"Total records: {stats['total_records']}")
        print(f"Successful records: {stats['successful_records']}")
        print(f"Failed records: {stats['failed_records']}")
        print(f"Total time: {stats['total_time']:.2f}s")
        
        # Calculate throughput
        if stats['total_time'] > 0:
            throughput = stats['successful_records'] / stats['total_time']
            print(f"Throughput: {throughput:.1f} records/second")
    
    finally:
        # Clean up temporary files
        print("\nCleaning up temporary files...")
        for file_path, _ in temp_files:
            try:
                os.unlink(file_path)
                print(f"  Deleted: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"  Failed to delete {file_path}: {e}")


if __name__ == "__main__":
    main()