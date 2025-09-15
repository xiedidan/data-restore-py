#!/usr/bin/env python3
"""
Oracle to PostgreSQL Data Import Script

This script imports data from Oracle SQL dump files to PostgreSQL database.
It reads encoding information from analysis reports, rewrites SQL statements,
and performs parallel data import with progress monitoring.

Usage:
    python import_data.py --config config.yaml
    python import_data.py --source-dir /path/to/sql/files --target-db mydb --target-schema public
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.config import Config
from oracle_to_postgres.common.logger import Logger
from oracle_to_postgres.common.database import DatabaseManager, ConnectionInfo
from oracle_to_postgres.common.sql_rewriter import SQLRewriter
from oracle_to_postgres.common.parallel_importer import (
    ParallelImporter, ImportTask, ImportResult, ImportProgress
)
from oracle_to_postgres.common.report import ReportGenerator
from oracle_to_postgres.common.error_handler import ErrorHandler, ErrorContext, ErrorType


class DataImporter:
    """Main data importer class that coordinates the import process."""
    
    def __init__(self, config: Config):
        """
        Initialize data importer.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.logger = Logger()
        self.error_handler = ErrorHandler(logger=self.logger)
        self.report_generator = ReportGenerator()
        
        # Initialize components
        self._init_database_manager()
        self._init_sql_rewriter()
        self._init_parallel_importer()
        
        # Statistics
        self.import_stats = {
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'start_time': 0,
            'end_time': 0,
            'total_time': 0
        }
    
    def _init_database_manager(self):
        """Initialize database manager."""
        try:
            connection_info = ConnectionInfo(
                host=self.config.postgresql.host,
                port=self.config.postgresql.port,
                database=self.config.postgresql.database,
                username=self.config.postgresql.username,
                password=self.config.postgresql.password,
                schema=self.config.postgresql.schema
            )
            
            self.logger.info(f"Config max_workers: {self.config.performance.max_workers}")
            self.db_manager = DatabaseManager(
                connection_info=connection_info,
                pool_size=self.config.performance.max_workers,
                logger=self.logger
            )
            
            # Test connection
            if not self.db_manager.test_connection():
                raise Exception("Failed to connect to PostgreSQL database")
                
            self.logger.info("Database connection established successfully")
            
        except Exception as e:
            self.error_handler.handle_db_error(e, "database_initialization")
            raise
    
    def _init_sql_rewriter(self):
        """Initialize SQL rewriter."""
        self.sql_rewriter = SQLRewriter(
            source_db="oracle",  # Default source database type
            target_db="postgresql",  # Default target database type
            target_schema=self.config.postgresql.schema,
            logger=self.logger
        )
        
        self.logger.info(f"SQL rewriter initialized: oracle -> postgresql")
    
    def _init_parallel_importer(self):
        """Initialize parallel importer."""
        self.parallel_importer = ParallelImporter(
            db_manager=self.db_manager,
            sql_rewriter=self.sql_rewriter,
            max_workers=self.config.performance.max_workers,
            batch_size=self.config.performance.batch_size,
            logger=self.logger
        )
        
        self.logger.info(f"Parallel importer initialized with {self.config.performance.max_workers} workers")
    
    def load_encoding_report(self) -> Dict[str, str]:
        """
        Load encoding information from analysis report.
        
        Returns:
            Dictionary mapping file paths to their encodings
        """
        encoding_map = {}
        
        # Look for the most recent encoding analysis report
        reports_dir = Path("./reports")
        if not reports_dir.exists():
            self.logger.warning("Reports directory not found")
            return encoding_map
        
        # Find all encoding analysis CSV files
        encoding_files = list(reports_dir.glob("encoding_analysis*.csv"))
        
        if not encoding_files:
            self.logger.warning("No encoding analysis reports found in reports directory")
            self.logger.info("Will attempt to detect encoding for each file during import")
            return encoding_map
        
        # Use the most recent report (sort by filename which includes timestamp)
        report_file = sorted(encoding_files)[-1]
        self.logger.info(f"Using encoding report: {report_file}")
        
        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Handle both old and new report formats
                    file_path = row.get('file_path', '')
                    file_name = row.get('file_name', '')
                    encoding = row.get('encoding', 'utf-8')
                    
                    # If we have file_name but not file_path, construct the path
                    if file_name and not file_path:
                        # Assume files are in the source directory
                        source_dir = Path(self.config.source_directory)
                        file_path = str(source_dir / file_name)
                    
                    if file_path and encoding:
                        encoding_map[file_path] = encoding
                        # Also map by filename for easier lookup
                        if file_name:
                            encoding_map[file_name] = encoding
            
            self.logger.info(f"Loaded encoding information for {len(encoding_map)} files from {report_file.name}")
            
        except Exception as e:
            self.logger.error(f"Failed to load encoding report: {str(e)}")
            self.error_handler.handle_file_error(e, str(report_file))
        
        return encoding_map
    
    def detect_file_encoding(self, file_path: str) -> str:
        """
        Detect encoding of a file using chardet.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Detected encoding or 'utf-8' as fallback
        """
        try:
            import chardet
            
            with open(file_path, 'rb') as f:
                # Read first 10KB for encoding detection
                raw_data = f.read(10240)
                
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            self.logger.debug(f"Detected encoding for {file_path}: {encoding} (confidence: {confidence:.2f})")
            
            # Use utf-8 if confidence is too low
            if confidence < 0.7:
                self.logger.warning(f"Low confidence ({confidence:.2f}) for encoding detection of {file_path}, using utf-8")
                return 'utf-8'
                
            return encoding
            
        except ImportError:
            self.logger.warning("chardet not available, using utf-8 encoding")
            return 'utf-8'
        except Exception as e:
            self.logger.error(f"Failed to detect encoding for {file_path}: {str(e)}")
            return 'utf-8'
    
    def discover_sql_files(self) -> List[str]:
        """
        Discover SQL files in the source directory.
        
        Returns:
            List of SQL file paths
        """
        sql_files = []
        source_dir = Path(self.config.source_directory)
        
        if not source_dir.exists():
            raise FileNotFoundError(f"Source directory not found: {source_dir}")
        
        # Find all .sql files
        for sql_file in source_dir.rglob('*.sql'):
            if sql_file.is_file():
                sql_files.append(str(sql_file))
        
        self.logger.info(f"Discovered {len(sql_files)} SQL files in {source_dir}")
        return sorted(sql_files)
    
    def create_import_tasks(self, sql_files: List[str], encoding_map: Dict[str, str]) -> List[ImportTask]:
        """
        Create import tasks for SQL files.
        
        Args:
            sql_files: List of SQL file paths
            encoding_map: Mapping of file paths to encodings
            
        Returns:
            List of import tasks
        """
        import_tasks = []
        
        for sql_file in sql_files:
            # Get encoding from report or detect it
            file_name = os.path.basename(sql_file)
            
            # Try to find encoding by full path first, then by filename
            if sql_file in encoding_map:
                encoding = encoding_map[sql_file]
                self.logger.debug(f"Found encoding for {file_name} by full path: {encoding}")
            elif file_name in encoding_map:
                encoding = encoding_map[file_name]
                self.logger.debug(f"Found encoding for {file_name} by filename: {encoding}")
            else:
                encoding = self.detect_file_encoding(sql_file)
                self.logger.debug(f"Detected encoding for {file_name}: {encoding}")
            
            # Extract table name from file name (assuming format like table_name.sql)
            file_name = os.path.basename(sql_file)
            table_name = os.path.splitext(file_name)[0]
            
            # Remove common prefixes/suffixes if present
            if table_name.startswith('insert_'):
                table_name = table_name[7:]
            if table_name.endswith('_data'):
                table_name = table_name[:-5]
            
            task = ImportTask(
                file_path=sql_file,
                table_name=table_name,
                encoding=encoding,
                target_encoding=self.config.target_encoding
            )
            
            import_tasks.append(task)
        
        self.logger.info(f"Created {len(import_tasks)} import tasks")
        return import_tasks
    
    def progress_callback(self, progress: ImportProgress):
        """
        Progress callback for import monitoring.
        
        Args:
            progress: Current import progress
        """
        elapsed = progress.elapsed_time
        remaining = progress.estimated_remaining_time
        
        self.logger.info(
            f"Import Progress: {progress.completion_percentage:.1f}% "
            f"({progress.completed_files}/{progress.total_files} files) "
            f"- Elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s"
        )
        
        if progress.current_file:
            self.logger.debug(f"Currently processing: {progress.current_file}")
    
    def import_data(self) -> List[ImportResult]:
        """
        Execute the data import process.
        
        Returns:
            List of import results
        """
        self.logger.info("Starting data import process")
        self.import_stats['start_time'] = time.time()
        
        try:
            # Load encoding information
            self.logger.info("Loading encoding information...")
            encoding_map = self.load_encoding_report()
            
            # Discover SQL files
            self.logger.info("Discovering SQL files...")
            sql_files = self.discover_sql_files()
            
            if not sql_files:
                self.logger.warning("No SQL files found to import")
                return []
            
            # Create import tasks
            self.logger.info("Creating import tasks...")
            import_tasks = self.create_import_tasks(sql_files, encoding_map)
            
            # Update statistics
            self.import_stats['total_files'] = len(import_tasks)
            
            # Execute parallel import
            self.logger.info(f"Starting parallel import of {len(import_tasks)} files...")
            results = self.parallel_importer.import_files(
                import_tasks=import_tasks,
                progress_callback=self.progress_callback
            )
            
            # Update final statistics
            self._update_final_statistics(results)
            
            self.logger.info("Data import process completed")
            return results
            
        except Exception as e:
            self.logger.error(f"Data import failed: {str(e)}")
            self.error_handler.handle_import_error(e, "data_import")
            raise
        
        finally:
            self.import_stats['end_time'] = time.time()
            self.import_stats['total_time'] = (
                self.import_stats['end_time'] - self.import_stats['start_time']
            )
    
    def _update_final_statistics(self, results: List[ImportResult]):
        """Update final import statistics."""
        for result in results:
            if result.success:
                self.import_stats['successful_files'] += 1
            else:
                self.import_stats['failed_files'] += 1
            
            self.import_stats['successful_records'] += result.records_processed
            self.import_stats['failed_records'] += result.records_failed
            self.import_stats['total_records'] += (
                result.records_processed + result.records_failed
            )
    
    def generate_import_report(self, results: List[ImportResult]) -> str:
        """
        Generate detailed import report.
        
        Args:
            results: List of import results
            
        Returns:
            Path to generated report file
        """
        self.logger.info("Generating import report...")
        
        # Prepare report data
        report_data = {
            'summary': self.import_stats,
            'file_results': [],
            'errors': [],
            'warnings': []
        }
        
        # Process results
        for result in results:
            file_result = {
                'file_name': os.path.basename(result.file_path),
                'file_path': result.file_path,
                'table_name': result.table_name,
                'success': result.success,
                'records_processed': result.records_processed,
                'records_failed': result.records_failed,
                'processing_time': result.processing_time,
                'error_message': result.error_message
            }
            report_data['file_results'].append(file_result)
            
            # Collect errors and warnings
            if result.error_message:
                report_data['errors'].append({
                    'file': result.file_path,
                    'error': result.error_message
                })
            
            if result.warnings:
                for warning in result.warnings:
                    report_data['warnings'].append({
                        'file': result.file_path,
                        'warning': warning
                    })
        
        # Generate report
        report_file = os.path.join("./reports", 'import_report.json')
        self.report_generator.generate_json_report(report_data, report_file)
        
        # Also generate CSV summary
        csv_file = os.path.join("./reports", 'import_summary.csv')
        self._generate_csv_summary(results, csv_file)
        
        self.logger.info(f"Import report generated: {report_file}")
        self.logger.info(f"Import summary generated: {csv_file}")
        
        return report_file
    
    def _generate_csv_summary(self, results: List[ImportResult], csv_file: str):
        """Generate CSV summary of import results."""
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'file_name', 'table_name', 'success', 'records_processed',
                'records_failed', 'processing_time', 'error_message'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    'file_name': os.path.basename(result.file_path),
                    'table_name': result.table_name,
                    'success': result.success,
                    'records_processed': result.records_processed,
                    'records_failed': result.records_failed,
                    'processing_time': f"{result.processing_time:.2f}",
                    'error_message': result.error_message or ''
                })
    
    def print_summary(self, results: List[ImportResult]):
        """Print import summary to console."""
        print("\n" + "="*60)
        print("DATA IMPORT SUMMARY")
        print("="*60)
        
        # Overall statistics
        print(f"Total files processed: {self.import_stats['total_files']}")
        print(f"Successful files: {self.import_stats['successful_files']}")
        print(f"Failed files: {self.import_stats['failed_files']}")
        print(f"Total records: {self.import_stats['total_records']}")
        print(f"Successful records: {self.import_stats['successful_records']}")
        print(f"Failed records: {self.import_stats['failed_records']}")
        print(f"Total processing time: {self.import_stats['total_time']:.2f} seconds")
        
        # Calculate throughput
        if self.import_stats['total_time'] > 0:
            throughput = self.import_stats['successful_records'] / self.import_stats['total_time']
            print(f"Throughput: {throughput:.1f} records/second")
        
        # Success rate
        if self.import_stats['total_files'] > 0:
            success_rate = (self.import_stats['successful_files'] / self.import_stats['total_files']) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        print("\nFile Details:")
        print("-" * 60)
        
        for result in results:
            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print(f"{status} {os.path.basename(result.file_path)}")
            print(f"  Table: {result.table_name}")
            print(f"  Records: {result.records_processed} processed, {result.records_failed} failed")
            print(f"  Time: {result.processing_time:.2f}s")
            
            if result.error_message:
                print(f"  Error: {result.error_message}")
            
            if result.warnings:
                print(f"  Warnings: {len(result.warnings)}")
            
            print()


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Import data from Oracle SQL dump files to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import using configuration file
  python import_data.py --config config.yaml
  
  # Import with command line parameters
  python import_data.py --source-dir /path/to/sql/files \\
                       --target-db mydb \\
                       --target-schema public \\
                       --db-host localhost \\
                       --db-port 5432 \\
                       --db-user postgres \\
                       --db-password secret
  
  # Import with custom settings
  python import_data.py --source-dir /data/oracle_dumps \\
                       --target-db production_db \\
                       --max-workers 8 \\
                       --batch-size 5000 \\
                       --output-dir /reports
        """
    )
    
    # Configuration
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to YAML configuration file'
    )
    
    # Source and target
    parser.add_argument(
        '--source-dir',
        type=str,
        help='Directory containing Oracle SQL dump files'
    )
    
    parser.add_argument(
        '--target-db',
        type=str,
        help='Target PostgreSQL database name'
    )
    
    parser.add_argument(
        '--target-schema',
        type=str,
        default='public',
        help='Target PostgreSQL schema name (default: public)'
    )
    
    parser.add_argument(
        '--source-db',
        type=str,
        help='Source Oracle database name to replace in SQL statements'
    )
    
    # Database connection
    parser.add_argument(
        '--db-host',
        type=str,
        default='localhost',
        help='PostgreSQL host (default: localhost)'
    )
    
    parser.add_argument(
        '--db-port',
        type=int,
        default=5432,
        help='PostgreSQL port (default: 5432)'
    )
    
    parser.add_argument(
        '--db-user',
        type=str,
        default='postgres',
        help='PostgreSQL username (default: postgres)'
    )
    
    parser.add_argument(
        '--db-password',
        type=str,
        help='PostgreSQL password'
    )
    
    # Performance settings
    parser.add_argument(
        '--max-workers',
        type=int,
        help='Maximum number of parallel workers (default: from config file)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Number of records to process in each batch (default: from config file)'
    )
    
    # Encoding settings
    parser.add_argument(
        '--default-encoding',
        type=str,
        default='utf-8',
        help='Default encoding for SQL files (default: utf-8)'
    )
    
    parser.add_argument(
        '--target-encoding',
        type=str,
        default='utf-8',
        help='Target encoding for output files (default: utf-8)'
    )
    
    # Output settings
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Output directory for reports (default: output)'
    )
    
    # Logging
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser


def main():
    """Main function."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        # Load configuration
        if args.config:
            config = Config.from_file(args.config)
        else:
            config = Config()
        
        # Override config with command line arguments
        if args.source_dir:
            config.source_directory = args.source_dir
        if args.target_db:
            config.postgresql.database = args.target_db
        if args.target_schema:
            config.postgresql.schema = args.target_schema
        if args.db_host:
            config.postgresql.host = args.db_host
        if args.db_port:
            config.postgresql.port = args.db_port
        if args.db_user:
            config.postgresql.username = args.db_user
        if args.db_password:
            config.postgresql.password = args.db_password
        if args.max_workers:
            config.performance.max_workers = args.max_workers
        if args.batch_size:
            config.performance.batch_size = args.batch_size
        if args.target_encoding:
            config.target_encoding = args.target_encoding
        if args.log_level:
            config.logging.level = args.log_level
        
        # Validate required parameters
        if not config.source_directory:
            print("Error: Source directory is required (--source-dir or config file)")
            sys.exit(1)
        
        if not config.postgresql.database:
            print("Error: Target database is required (--target-db or config file)")
            sys.exit(1)
        
        if not config.postgresql.password:
            print("Error: Database password is required (--db-password or config file)")
            sys.exit(1)
        
        # Create output directory
        os.makedirs("./reports", exist_ok=True)
        
        # Initialize and run data importer
        importer = DataImporter(config)
        results = importer.import_data()
        
        # Generate reports
        importer.generate_import_report(results)
        
        # Print summary
        importer.print_summary(results)
        
        # Exit with appropriate code
        failed_files = sum(1 for r in results if not r.success)
        if failed_files > 0:
            print(f"\nWarning: {failed_files} files failed to import")
            sys.exit(1)
        else:
            print("\nAll files imported successfully!")
            sys.exit(0)
    
    except KeyboardInterrupt:
        print("\nImport interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        print(f"Import failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()