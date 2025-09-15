#!/usr/bin/env python3
"""
Oracle SQL Analysis Script

Analyzes Oracle SQL dump files to detect encoding and generate PostgreSQL DDL statements
using DeepSeek API.

Usage:
    python analyze_sql.py --source-directory /path/to/dumps --config config.yaml
"""

import argparse
import os
import sys
from typing import List, Dict
from dataclasses import dataclass

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.config import (
    Config, add_common_arguments, add_source_arguments, add_deepseek_arguments
)
from oracle_to_postgres.common.logger import Logger, TimedLogger
from oracle_to_postgres.common.file_scanner import FileScanner, FileInfo
from oracle_to_postgres.common.encoding_detector import EncodingDetector, EncodingResult
from oracle_to_postgres.common.sql_parser import SQLParser, InsertStatement
from oracle_to_postgres.common.deepseek_client import DeepSeekClient, DDLGenerationResult
from oracle_to_postgres.common.report import ReportGenerator
from oracle_to_postgres.common.error_handler import ErrorHandler, ErrorContext, ErrorType


@dataclass
class AnalysisResult:
    """Result of analyzing a single SQL file."""
    file_name: str
    table_name: str
    encoding: str
    file_size_mb: float
    ddl_generated: bool
    ddl_file_path: str = ""
    error_message: str = ""
    processing_time: float = 0.0
    confidence: float = 0.0


class SQLAnalyzer:
    """Main analyzer class for Oracle SQL files."""
    
    def __init__(self, config: Config):
        """Initialize the SQL analyzer with configuration."""
        self.config = config
        self.logger = Logger(
            log_level=config.logging.level,
            log_file=config.logging.file,
            name="analyze_sql"
        )
        
        # Initialize components
        self.file_scanner = FileScanner()
        self.encoding_detector = EncodingDetector(sample_lines=config.sample_lines)
        self.sql_parser = SQLParser()
        self.deepseek_client = DeepSeekClient(
            api_key=config.deepseek.api_key,
            base_url=config.deepseek.base_url,
            timeout=config.deepseek.timeout,
            max_retries=config.deepseek.max_retries,
            logger=self.logger
        )
        self.report_generator = ReportGenerator()
        self.error_handler = ErrorHandler(logger=self.logger)
        
        # Ensure DDL directory exists
        os.makedirs(config.ddl_directory, exist_ok=True)
    
    def analyze_files(self) -> List[AnalysisResult]:
        """
        Analyze all SQL files in the source directory.
        
        Returns:
            List of AnalysisResult objects
        """
        self.logger.section("Starting SQL File Analysis")
        
        # Scan for SQL files
        self.logger.info(f"Scanning directory: {self.config.source_directory}")
        files = self.file_scanner.scan_directory(self.config.source_directory)
        
        if not files:
            self.logger.warning("No SQL files found in source directory")
            return []
        
        self.logger.info(f"Found {len(files)} SQL files to analyze")
        
        # Display file summary
        total_size_bytes, total_size_mb = self.file_scanner.get_total_size(files)
        self.logger.info(f"Total size: {total_size_mb:.2f} MB ({total_size_bytes:,} bytes)")
        
        # Test DeepSeek API connection
        if not self._test_api_connection():
            self.logger.error("DeepSeek API connection test failed. Please check your configuration.")
            return []
        
        # Analyze each file
        results = []
        for i, file_info in enumerate(files, 1):
            self.logger.progress(i, len(files), f"Analyzing {file_info.file_name}")
            
            try:
                result = self._analyze_single_file(file_info)
                results.append(result)
                
                if result.ddl_generated:
                    self.logger.debug(f"✓ Generated DDL for {result.table_name}")
                else:
                    self.logger.warning(f"✗ Failed to generate DDL for {result.table_name}: {result.error_message}")
                    
            except Exception as e:
                self.logger.error(f"Unexpected error analyzing {file_info.file_name}: {str(e)}")
                results.append(AnalysisResult(
                    file_name=file_info.file_name,
                    table_name=self.file_scanner.extract_table_name_from_filename(file_info),
                    encoding="unknown",
                    file_size_mb=file_info.file_size_mb,
                    ddl_generated=False,
                    error_message=f"Unexpected error: {str(e)}"
                ))
        
        self.logger.progress_complete("Analysis complete")
        
        # Generate reports
        self._generate_reports(results)
        
        # Display summary
        self._display_summary(results)
        
        return results
    
    def _analyze_single_file(self, file_info: FileInfo) -> AnalysisResult:
        """Analyze a single SQL file."""
        import time
        start_time = time.time()
        
        table_name = self.file_scanner.extract_table_name_from_filename(file_info)
        
        result = AnalysisResult(
            file_name=file_info.file_name,
            table_name=table_name,
            encoding="unknown",
            file_size_mb=file_info.file_size_mb,
            ddl_generated=False
        )
        
        try:
            # Step 1: Detect encoding
            encoding_result = self._detect_file_encoding(file_info.file_path)
            result.encoding = encoding_result.encoding
            result.confidence = encoding_result.confidence
            
            # Step 2: Parse SQL statements
            sample_inserts = self._parse_sql_file(file_info.file_path, encoding_result.encoding)
            
            if not sample_inserts:
                result.error_message = "No INSERT statements found in file"
                return result
            
            # Step 3: Generate DDL using DeepSeek API
            ddl_result = self._generate_ddl(table_name, sample_inserts)
            
            if ddl_result.success:
                # Step 4: Save DDL to file
                ddl_file_path = self._save_ddl(table_name, ddl_result.ddl_content)
                result.ddl_generated = True
                result.ddl_file_path = ddl_file_path
            else:
                result.error_message = ddl_result.error_message or "DDL generation failed"
            
        except Exception as e:
            result.error_message = str(e)
            self.error_handler.handle_file_error(e, file_info.file_path, "analyze_file")
        
        result.processing_time = time.time() - start_time
        return result
    
    def _detect_file_encoding(self, file_path: str) -> EncodingResult:
        """Detect file encoding with error handling."""
        try:
            return self.encoding_detector.detect_encoding(file_path)
        except Exception as e:
            self.error_handler.handle_file_error(e, file_path, "encoding_detection")
            # Return default encoding as fallback
            return EncodingResult(encoding='utf-8', confidence=0.1)
    
    def _parse_sql_file(self, file_path: str, encoding: str) -> List[str]:
        """Parse SQL file and extract sample INSERT statements."""
        try:
            # Use safe file reading with encoding detection
            content, actual_encoding = self.encoding_detector.read_file_safely(file_path, encoding)
            
            # Log if we had to use error handling
            if ':' in actual_encoding and actual_encoding != encoding:
                self.logger.warning(f"Used encoding fallback for {file_path}: {actual_encoding}")
            
            # Parse INSERT statements
            statements = self.sql_parser.parse_insert_statements(content, actual_encoding.split(':')[0])
            
            # Extract sample statements (limit to avoid token limits)
            sample_statements = []
            for stmt in statements[:10]:  # Limit to first 10 statements
                sample_statements.append(stmt.original_statement)
            
            return sample_statements
            
        except Exception as e:
            self.error_handler.handle_file_error(e, file_path, "sql_parsing")
            return []
    
    def _generate_ddl(self, table_name: str, sample_inserts: List[str]) -> DDLGenerationResult:
        """Generate DDL using DeepSeek API with error handling."""
        try:
            return self.deepseek_client.generate_ddl(table_name, sample_inserts)
        except Exception as e:
            context = ErrorContext(
                error_type=ErrorType.API_CALL,
                operation="generate_ddl",
                table_name=table_name
            )
            self.error_handler.handle_api_error(e, "DDL generation")
            return DDLGenerationResult(
                success=False,
                ddl_content="",
                error_message=str(e)
            )
    
    def _save_ddl(self, table_name: str, ddl_content: str) -> str:
        """Save DDL content to file."""
        ddl_filename = f"create_{table_name}.sql"
        ddl_file_path = os.path.join(self.config.ddl_directory, ddl_filename)
        
        try:
            with open(ddl_file_path, 'w', encoding='utf-8') as f:
                f.write(ddl_content)
            
            return ddl_file_path
            
        except Exception as e:
            self.error_handler.handle_file_error(e, ddl_file_path, "save_ddl")
            raise
    
    def _test_api_connection(self) -> bool:
        """Test DeepSeek API connection."""
        self.logger.info("Testing DeepSeek API connection...")
        
        try:
            if self.deepseek_client.test_connection():
                self.logger.info("✓ DeepSeek API connection successful")
                return True
            else:
                self.logger.error("✗ DeepSeek API connection failed")
                return False
        except Exception as e:
            self.logger.error(f"✗ DeepSeek API connection test error: {str(e)}")
            return False
    
    def _generate_reports(self, results: List[AnalysisResult]) -> None:
        """Generate analysis reports."""
        self.logger.subsection("Generating Reports")
        
        try:
            # Generate encoding analysis CSV report
            csv_path = self.report_generator.generate_encoding_report(results)
            self.logger.info(f"Generated encoding report: {csv_path}")
            
            # Generate summary report
            summary_path = self.report_generator.generate_summary_report(
                results, 
                "analysis_summary.txt",
                "SQL Analysis Summary"
            )
            self.logger.info(f"Generated summary report: {summary_path}")
            
            # Generate JSON report for programmatic access
            json_path = self.report_generator.generate_json_report(
                results,
                "analysis_results.json"
            )
            self.logger.info(f"Generated JSON report: {json_path}")
            
        except Exception as e:
            self.logger.error(f"Error generating reports: {str(e)}")
    
    def _display_summary(self, results: List[AnalysisResult]) -> None:
        """Display analysis summary."""
        self.logger.subsection("Analysis Summary")
        
        total_files = len(results)
        successful_ddl = sum(1 for r in results if r.ddl_generated)
        failed_ddl = total_files - successful_ddl
        
        self.logger.info(f"Total files analyzed: {total_files}")
        self.logger.info(f"DDL generated successfully: {successful_ddl}")
        self.logger.info(f"DDL generation failed: {failed_ddl}")
        
        if total_files > 0:
            success_rate = (successful_ddl / total_files) * 100
            self.logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Display encoding distribution
        encoding_counts = {}
        for result in results:
            encoding = result.encoding
            encoding_counts[encoding] = encoding_counts.get(encoding, 0) + 1
        
        self.logger.info("Encoding distribution:")
        for encoding, count in sorted(encoding_counts.items()):
            self.logger.info(f"  {encoding}: {count} files")
        
        # Display error summary
        error_summary = self.error_handler.get_error_summary()
        if error_summary['total_errors'] > 0:
            self.logger.warning(f"Total errors encountered: {error_summary['total_errors']}")
            self.logger.warning(f"Total retries performed: {error_summary['total_retries']}")


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Analyze Oracle SQL dump files and generate PostgreSQL DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_sql.py --source-directory /path/to/dumps
  python analyze_sql.py -s /path/to/dumps --config custom_config.yaml
  python analyze_sql.py -s /path/to/dumps --sample-lines 50 --log-level DEBUG
        """
    )
    
    # Add argument groups
    add_common_arguments(parser)
    add_source_arguments(parser)
    add_deepseek_arguments(parser)
    
    # Script-specific arguments
    parser.add_argument(
        '--ddl-directory',
        type=str,
        default='./ddl',
        help='Directory to store generated DDL files (default: ./ddl)'
    )
    
    return parser


def main():
    """Main entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = Config.from_args(args)
        
        # Merge with file configuration if exists
        if os.path.exists(args.config):
            config = config.merge_with_file(args.config)
        
        # Override DDL directory if specified
        if args.ddl_directory:
            config.ddl_directory = args.ddl_directory
        
        # Validate configuration
        config.validate()
        
        # Create analyzer and run analysis
        analyzer = SQLAnalyzer(config)
        
        with TimedLogger(analyzer.logger, "SQL file analysis"):
            results = analyzer.analyze_files()
        
        # Exit with appropriate code
        successful_count = sum(1 for r in results if r.ddl_generated)
        if successful_count == 0 and results:
            analyzer.logger.error("No DDL files were generated successfully")
            sys.exit(1)
        elif successful_count < len(results):
            analyzer.logger.warning(f"Some files failed to process ({len(results) - successful_count} failures)")
            sys.exit(2)
        else:
            analyzer.logger.info("All files processed successfully")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()