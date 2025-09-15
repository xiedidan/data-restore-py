"""
Tests for import_data.py script.
"""

import os
import tempfile
import csv
import json
from unittest.mock import Mock, patch, MagicMock
import pytest

# Import the DataImporter class
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oracle_to_postgres.common.config import Config
from oracle_to_postgres.common.parallel_importer import ImportTask, ImportResult


class TestDataImporter:
    """Test DataImporter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create a test config
        self.config = Config()
        self.config.source_dir = "/test/source"
        self.config.target_db = "test_db"
        self.config.target_schema = "public"
        self.config.source_db = "oracle_db"
        self.config.db_host = "localhost"
        self.config.db_port = 5432
        self.config.db_user = "postgres"
        self.config.db_password = "password"
        self.config.max_workers = 2
        self.config.batch_size = 100
        self.config.default_encoding = "utf-8"
        self.config.target_encoding = "utf-8"
        self.config.output_dir = "/test/output"
    
    @patch('import_data.DatabaseManager')
    @patch('import_data.SQLRewriter')
    @patch('import_data.ParallelImporter')
    def test_data_importer_initialization(self, mock_parallel_importer, mock_sql_rewriter, mock_db_manager):
        """Test DataImporter initialization."""
        # Mock database manager test_connection to return True
        mock_db_instance = Mock()
        mock_db_instance.test_connection.return_value = True
        mock_db_manager.return_value = mock_db_instance
        
        # Import here to avoid circular import issues
        from import_data import DataImporter
        
        importer = DataImporter(self.config)
        
        # Verify components were initialized
        assert importer.config == self.config
        assert importer.db_manager == mock_db_instance
        mock_db_manager.assert_called_once()
        mock_sql_rewriter.assert_called_once()
        mock_parallel_importer.assert_called_once()
    
    def test_load_encoding_report_file_exists(self):
        """Test loading encoding report when file exists."""
        from import_data import DataImporter
        
        # Create temporary encoding report
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['file_path', 'encoding', 'confidence'])
            writer.writerow(['/test/file1.sql', 'utf-8', '0.99'])
            writer.writerow(['/test/file2.sql', 'gbk', '0.95'])
            temp_file = f.name
        
        try:
            # Mock the config to point to our temp file
            with patch.object(self.config, 'output_dir', os.path.dirname(temp_file)):
                with patch('os.path.join', return_value=temp_file):
                    with patch('import_data.DatabaseManager'):
                        with patch('import_data.SQLRewriter'):
                            with patch('import_data.ParallelImporter'):
                                # Mock database connection test
                                with patch('import_data.DataImporter._init_database_manager'):
                                    with patch('import_data.DataImporter._init_parallel_importer'):
                                        importer = DataImporter(self.config)
                                        # Manually set the attributes that would be set by the mocked methods
                                        importer.db_manager = Mock()
                                        importer.parallel_importer = Mock()
                                    encoding_map = importer.load_encoding_report()
            
            assert len(encoding_map) == 2
            assert encoding_map['/test/file1.sql'] == 'utf-8'
            assert encoding_map['/test/file2.sql'] == 'gbk'
        
        finally:
            os.unlink(temp_file)
    
    def test_load_encoding_report_file_not_exists(self):
        """Test loading encoding report when file doesn't exist."""
        from import_data import DataImporter
        
        with patch('import_data.DatabaseManager'):
            with patch('import_data.SQLRewriter'):
                with patch('import_data.ParallelImporter'):
                    with patch('import_data.DataImporter._init_database_manager'):
                        with patch('import_data.DataImporter._init_parallel_importer'):
                            importer = DataImporter(self.config)
                            importer.db_manager = Mock()
                            importer.parallel_importer = Mock()
                            encoding_map = importer.load_encoding_report()
        
        assert encoding_map == {}
    
    @patch('import_data.Path')
    def test_discover_sql_files(self, mock_path):
        """Test SQL file discovery."""
        from import_data import DataImporter
        
        # Mock Path behavior
        mock_source_dir = Mock()
        mock_source_dir.exists.return_value = True
        # Create mock file objects
        mock_file1 = Mock()
        mock_file1.is_file.return_value = True
        mock_file1.__str__ = Mock(return_value='/test/file1.sql')
        
        mock_file2 = Mock()
        mock_file2.is_file.return_value = True
        mock_file2.__str__ = Mock(return_value='/test/file2.sql')
        
        mock_dir = Mock()
        mock_dir.is_file.return_value = False
        mock_dir.__str__ = Mock(return_value='/test/subdir')
        
        mock_source_dir.rglob.return_value = [mock_file1, mock_file2, mock_dir]
        mock_path.return_value = mock_source_dir
        
        with patch('import_data.DatabaseManager'):
            with patch('import_data.SQLRewriter'):
                with patch('import_data.ParallelImporter'):
                    with patch('import_data.DataImporter._init_database_manager'):
                        with patch('import_data.DataImporter._init_parallel_importer'):
                            importer = DataImporter(self.config)
                            importer.db_manager = Mock()
                            importer.parallel_importer = Mock()
                            sql_files = importer.discover_sql_files()
        
        assert len(sql_files) == 2
        assert '/test/file1.sql' in sql_files
        assert '/test/file2.sql' in sql_files
    
    def test_create_import_tasks(self):
        """Test creation of import tasks."""
        from import_data import DataImporter
        
        sql_files = ['/test/users.sql', '/test/orders_data.sql', '/test/insert_products.sql']
        encoding_map = {
            '/test/users.sql': 'utf-8',
            '/test/orders_data.sql': 'gbk'
        }
        
        with patch('import_data.DatabaseManager'):
            with patch('import_data.SQLRewriter'):
                with patch('import_data.ParallelImporter'):
                    with patch('import_data.DataImporter._init_database_manager'):
                        with patch('import_data.DataImporter._init_parallel_importer'):
                            importer = DataImporter(self.config)
                            importer.db_manager = Mock()
                            importer.parallel_importer = Mock()
                            tasks = importer.create_import_tasks(sql_files, encoding_map)
        
        assert len(tasks) == 3
        
        # Check first task
        assert tasks[0].file_path == '/test/users.sql'
        assert tasks[0].table_name == 'users'
        assert tasks[0].encoding == 'utf-8'
        
        # Check second task (with encoding from map)
        assert tasks[1].file_path == '/test/orders_data.sql'
        assert tasks[1].table_name == 'orders'  # _data suffix removed
        assert tasks[1].encoding == 'gbk'
        
        # Check third task (with prefix removed)
        assert tasks[2].file_path == '/test/insert_products.sql'
        assert tasks[2].table_name == 'products'  # insert_ prefix removed
        assert tasks[2].encoding == 'utf-8'  # default encoding
    
    @patch('import_data.time.time')
    def test_import_data_success(self, mock_time):
        """Test successful data import."""
        from import_data import DataImporter
        
        # Mock time - return a constant value to avoid StopIteration
        mock_time.return_value = 1000
        
        # Mock results
        mock_results = [
            ImportResult(
                file_path='/test/file1.sql',
                table_name='table1',
                success=True,
                records_processed=100,
                records_failed=0,
                processing_time=2.0
            ),
            ImportResult(
                file_path='/test/file2.sql',
                table_name='table2',
                success=True,
                records_processed=200,
                records_failed=5,
                processing_time=3.0
            )
        ]
        
        with patch('import_data.DatabaseManager'):
            with patch('import_data.SQLRewriter'):
                with patch('import_data.ParallelImporter') as mock_parallel:
                    with patch('import_data.DataImporter._init_database_manager'):
                        # Mock parallel importer
                        mock_parallel_instance = Mock()
                        mock_parallel_instance.import_files.return_value = mock_results
                        mock_parallel.return_value = mock_parallel_instance
                        
                        with patch('import_data.DataImporter._init_parallel_importer'):
                            importer = DataImporter(self.config)
                            importer.db_manager = Mock()
                            importer.parallel_importer = mock_parallel_instance
                            
                            # Mock other methods
                            importer.load_encoding_report = Mock(return_value={})
                            importer.discover_sql_files = Mock(return_value=['/test/file1.sql', '/test/file2.sql'])
                            
                            results = importer.import_data()
        
        assert len(results) == 2
        assert importer.import_stats['total_files'] == 2
        assert importer.import_stats['successful_files'] == 2
        assert importer.import_stats['failed_files'] == 0
        assert importer.import_stats['successful_records'] == 300
        assert importer.import_stats['failed_records'] == 5
        assert importer.import_stats['total_time'] == 0  # Since we used constant time
    
    def test_generate_import_report(self):
        """Test import report generation."""
        from import_data import DataImporter
        
        # Create mock results
        results = [
            ImportResult(
                file_path='/test/file1.sql',
                table_name='table1',
                success=True,
                records_processed=100,
                records_failed=0,
                processing_time=2.0,
                warnings=['Warning 1']
            ),
            ImportResult(
                file_path='/test/file2.sql',
                table_name='table2',
                success=False,
                records_processed=50,
                records_failed=10,
                processing_time=1.0,
                error_message='Connection failed'
            )
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.config.output_dir = temp_dir
            
            with patch('import_data.DatabaseManager'):
                with patch('import_data.SQLRewriter'):
                    with patch('import_data.ParallelImporter'):
                        with patch('import_data.DataImporter._init_database_manager'):
                            with patch('import_data.DataImporter._init_parallel_importer'):
                                importer = DataImporter(self.config)
                                importer.db_manager = Mock()
                                importer.parallel_importer = Mock()
                                importer.import_stats = {
                                'total_files': 2,
                                'successful_files': 1,
                                'failed_files': 1,
                                'total_records': 160,
                                'successful_records': 150,
                                'failed_records': 10,
                                'total_time': 3.0
                            }
                            
                            report_file = importer.generate_import_report(results)
            
            # Check that report files were created
            assert os.path.exists(report_file)
            assert os.path.exists(os.path.join(temp_dir, 'import_summary.csv'))
            
            # Check JSON report content
            with open(report_file, 'r', encoding='utf-8') as f:
                report_data = json.load(f)
            
            assert report_data['summary']['total_files'] == 2
            assert len(report_data['file_results']) == 2
            assert len(report_data['errors']) == 1
            assert len(report_data['warnings']) == 1
            
            # Check CSV report content
            csv_file = os.path.join(temp_dir, 'import_summary.csv')
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            assert len(rows) == 2
            assert rows[0]['file_name'] == 'file1.sql'
            assert rows[0]['success'] == 'True'
            assert rows[1]['file_name'] == 'file2.sql'
            assert rows[1]['success'] == 'False'


class TestCommandLineInterface:
    """Test command line interface."""
    
    def test_argument_parser_creation(self):
        """Test argument parser creation."""
        from import_data import create_argument_parser
        
        parser = create_argument_parser()
        
        # Test with minimal required arguments
        args = parser.parse_args([
            '--source-dir', '/test/source',
            '--target-db', 'testdb',
            '--db-password', 'secret'
        ])
        
        assert args.source_dir == '/test/source'
        assert args.target_db == 'testdb'
        assert args.db_password == 'secret'
        assert args.target_schema == 'public'  # default
        assert args.max_workers == 4  # default
    
    def test_argument_parser_all_options(self):
        """Test argument parser with all options."""
        from import_data import create_argument_parser
        
        parser = create_argument_parser()
        
        args = parser.parse_args([
            '--config', 'config.yaml',
            '--source-dir', '/data/sql',
            '--target-db', 'mydb',
            '--target-schema', 'import',
            '--source-db', 'oracle_prod',
            '--db-host', 'db.example.com',
            '--db-port', '5433',
            '--db-user', 'admin',
            '--db-password', 'secret123',
            '--max-workers', '8',
            '--batch-size', '2000',
            '--default-encoding', 'gbk',
            '--target-encoding', 'utf-8',
            '--output-dir', '/reports',
            '--log-level', 'DEBUG',
            '--verbose'
        ])
        
        assert args.config == 'config.yaml'
        assert args.source_dir == '/data/sql'
        assert args.target_db == 'mydb'
        assert args.target_schema == 'import'
        assert args.source_db == 'oracle_prod'
        assert args.db_host == 'db.example.com'
        assert args.db_port == 5433
        assert args.db_user == 'admin'
        assert args.db_password == 'secret123'
        assert args.max_workers == 8
        assert args.batch_size == 2000
        assert args.default_encoding == 'gbk'
        assert args.target_encoding == 'utf-8'
        assert args.output_dir == '/reports'
        assert args.log_level == 'DEBUG'
        assert args.verbose is True


if __name__ == '__main__':
    pytest.main([__file__])