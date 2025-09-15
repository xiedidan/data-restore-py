"""
Tests for parallel data importer.
"""

import os
import tempfile
import threading
import time
from unittest.mock import Mock, patch, MagicMock
import pytest

from oracle_to_postgres.common.parallel_importer import (
    ImportTask, ImportResult, ImportProgress, ImportProgressMonitor,
    SingleFileImporter, ParallelImporter
)
from oracle_to_postgres.common.database import DatabaseManager, ConnectionInfo
from oracle_to_postgres.common.sql_rewriter import SQLRewriter
from oracle_to_postgres.common.logger import Logger


class TestImportTask:
    """Test ImportTask dataclass."""
    
    def test_import_task_creation(self):
        """Test ImportTask creation."""
        task = ImportTask(
            file_path="/path/to/file.sql",
            table_name="test_table",
            encoding="utf-8"
        )
        
        assert task.file_path == "/path/to/file.sql"
        assert task.table_name == "test_table"
        assert task.encoding == "utf-8"
        assert task.target_encoding == "utf-8"


class TestImportResult:
    """Test ImportResult dataclass."""
    
    def test_import_result_creation(self):
        """Test ImportResult creation."""
        result = ImportResult(
            file_path="/path/to/file.sql",
            table_name="test_table",
            success=True,
            records_processed=100,
            records_failed=5,
            processing_time=2.5
        )
        
        assert result.file_path == "/path/to/file.sql"
        assert result.table_name == "test_table"
        assert result.success is True
        assert result.records_processed == 100
        assert result.records_failed == 5
        assert result.processing_time == 2.5
        assert result.error_message is None
        assert result.warnings == []
    
    def test_import_result_with_warnings(self):
        """Test ImportResult with warnings."""
        warnings = ["Warning 1", "Warning 2"]
        result = ImportResult(
            file_path="/path/to/file.sql",
            table_name="test_table",
            success=False,
            records_processed=50,
            records_failed=10,
            processing_time=1.0,
            error_message="Some error",
            warnings=warnings
        )
        
        assert result.warnings == warnings
        assert result.error_message == "Some error"


class TestImportProgress:
    """Test ImportProgress dataclass."""
    
    def test_progress_calculation(self):
        """Test progress percentage calculation."""
        progress = ImportProgress(
            total_files=10,
            completed_files=3,
            total_records=1000,
            processed_records=300,
            failed_records=10,
            start_time=time.time() - 60  # 1 minute ago
        )
        
        assert progress.completion_percentage == 30.0
        assert progress.elapsed_time >= 59  # Should be around 60 seconds
        assert progress.estimated_remaining_time > 0
    
    def test_progress_zero_files(self):
        """Test progress with zero files."""
        progress = ImportProgress(
            total_files=0,
            completed_files=0,
            total_records=0,
            processed_records=0,
            failed_records=0,
            start_time=time.time()
        )
        
        assert progress.completion_percentage == 0.0
        assert progress.estimated_remaining_time == 0.0


class TestImportProgressMonitor:
    """Test ImportProgressMonitor class."""
    
    def test_progress_monitor_initialization(self):
        """Test progress monitor initialization."""
        logger = Mock(spec=Logger)
        monitor = ImportProgressMonitor(logger)
        
        assert monitor.logger == logger
        assert monitor._progress is None
        assert monitor._callbacks == []
    
    def test_start_monitoring(self):
        """Test starting progress monitoring."""
        monitor = ImportProgressMonitor()
        monitor.start_monitoring(5)
        
        progress = monitor.get_progress()
        assert progress is not None
        assert progress.total_files == 5
        assert progress.completed_files == 0
    
    def test_update_file_started(self):
        """Test updating when file starts."""
        monitor = ImportProgressMonitor()
        monitor.start_monitoring(3)
        
        monitor.update_file_started("/path/to/test.sql")
        
        progress = monitor.get_progress()
        assert progress.current_file == "test.sql"
    
    def test_update_file_completed(self):
        """Test updating when file completes."""
        monitor = ImportProgressMonitor()
        monitor.start_monitoring(3)
        
        result = ImportResult(
            file_path="/path/to/test.sql",
            table_name="test_table",
            success=True,
            records_processed=100,
            records_failed=5,
            processing_time=2.0
        )
        
        monitor.update_file_completed(result)
        
        progress = monitor.get_progress()
        assert progress.completed_files == 1
        assert progress.processed_records == 100
        assert progress.failed_records == 5
        assert progress.current_file == ""
    
    def test_progress_callbacks(self):
        """Test progress callbacks."""
        monitor = ImportProgressMonitor()
        callback_called = []
        
        def test_callback(progress):
            callback_called.append(progress.completed_files)
        
        monitor.add_progress_callback(test_callback)
        monitor.start_monitoring(2)
        
        # Should trigger callback
        monitor.update_file_started("/test.sql")
        
        assert len(callback_called) == 1
    
    def test_thread_safety(self):
        """Test thread safety of progress monitor."""
        monitor = ImportProgressMonitor()
        monitor.start_monitoring(10)
        
        results = []
        
        def worker():
            for i in range(5):
                result = ImportResult(
                    file_path=f"/test{i}.sql",
                    table_name="test_table",
                    success=True,
                    records_processed=10,
                    records_failed=0,
                    processing_time=1.0
                )
                monitor.update_file_completed(result)
                results.append(monitor.get_progress().completed_files)
        
        # Run multiple threads
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        # Should have processed 10 files total
        final_progress = monitor.get_progress()
        assert final_progress.completed_files == 10


class TestSingleFileImporter:
    """Test SingleFileImporter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.mock_sql_rewriter = Mock(spec=SQLRewriter)
        self.mock_logger = Mock(spec=Logger)
        
        self.importer = SingleFileImporter(
            db_manager=self.mock_db_manager,
            sql_rewriter=self.mock_sql_rewriter,
            batch_size=2,
            logger=self.mock_logger
        )
    
    def test_split_sql_statements(self):
        """Test SQL statement splitting."""
        content = """
        INSERT INTO table1 VALUES (1, 'test');
        INSERT INTO table2 VALUES (2, 'test2');
        INSERT INTO table3 VALUES (3, 'test''s value');
        """
        
        statements = self.importer._split_sql_statements(content)
        
        assert len(statements) == 3
        assert "INSERT INTO table1" in statements[0]
        assert "INSERT INTO table2" in statements[1]
        assert "test''s value" in statements[2]
    
    def test_execute_batch_success(self):
        """Test successful batch execution."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock the context manager properly
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_conn
        mock_context_manager.__exit__.return_value = None
        self.mock_db_manager.get_connection.return_value = mock_context_manager
        
        statements = ["INSERT INTO test VALUES (1);", "INSERT INTO test VALUES (2);"]
        
        result = self.importer._execute_batch(statements)
        
        assert result['processed'] == 2
        assert result['failed'] == 0
        assert len(result['warnings']) == 0
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()
    
    def test_execute_batch_with_failures(self):
        """Test batch execution with some failures."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock the context manager properly
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_conn
        mock_context_manager.__exit__.return_value = None
        self.mock_db_manager.get_connection.return_value = mock_context_manager
        
        # Make second statement fail
        mock_cursor.execute.side_effect = [None, Exception("SQL error"), None]
        
        statements = ["INSERT INTO test VALUES (1);", "INVALID SQL;", "INSERT INTO test VALUES (3);"]
        
        result = self.importer._execute_batch(statements)
        
        assert result['processed'] == 2
        assert result['failed'] == 1
        assert len(result['warnings']) == 1
        assert "Failed to execute statement" in result['warnings'][0]
    
    def test_import_file_success(self):
        """Test successful file import."""
        # Create temporary SQL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
            f.write("INSERT INTO test VALUES (1, 'data1');\nINSERT INTO test VALUES (2, 'data2');")
            temp_file = f.name
        
        try:
            # Mock dependencies
            self.mock_sql_rewriter.rewrite_insert_statement.side_effect = lambda x: x
            
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Mock the context manager properly
            mock_context_manager = MagicMock()
            mock_context_manager.__enter__.return_value = mock_conn
            mock_context_manager.__exit__.return_value = None
            self.mock_db_manager.get_connection.return_value = mock_context_manager
            
            task = ImportTask(
                file_path=temp_file,
                table_name="test_table",
                encoding="utf-8"
            )
            
            result = self.importer.import_file(task)
            
            assert result.success is True
            assert result.file_path == temp_file
            assert result.table_name == "test_table"
            assert result.records_processed == 2
            assert result.records_failed == 0
            assert result.processing_time > 0
            
        finally:
            os.unlink(temp_file)
    
    def test_import_file_with_error(self):
        """Test file import with error."""
        task = ImportTask(
            file_path="/nonexistent/file.sql",
            table_name="test_table",
            encoding="utf-8"
        )
        
        result = self.importer.import_file(task)
        
        assert result.success is False
        assert result.records_processed == 0
        assert result.records_failed == 1
        assert result.error_message is not None
        assert "Error importing" in result.error_message


class TestParallelImporter:
    """Test ParallelImporter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.mock_sql_rewriter = Mock(spec=SQLRewriter)
        self.mock_logger = Mock(spec=Logger)
        
        self.importer = ParallelImporter(
            db_manager=self.mock_db_manager,
            sql_rewriter=self.mock_sql_rewriter,
            max_workers=2,
            batch_size=10,
            logger=self.mock_logger
        )
    
    def test_initialization(self):
        """Test parallel importer initialization."""
        assert self.importer.db_manager == self.mock_db_manager
        assert self.importer.sql_rewriter == self.mock_sql_rewriter
        assert self.importer.max_workers == 2
        assert self.importer.batch_size == 10
        assert self.importer.logger == self.mock_logger
        assert isinstance(self.importer.progress_monitor, ImportProgressMonitor)
    
    def test_import_files_empty_list(self):
        """Test importing empty file list."""
        results = self.importer.import_files([])
        
        assert results == []
        self.mock_logger.warning.assert_called_with("No import tasks provided")
    
    @patch('oracle_to_postgres.common.parallel_importer.SingleFileImporter')
    def test_import_files_success(self, mock_single_importer_class):
        """Test successful parallel file import."""
        # Create temporary files
        temp_files = []
        for i in range(3):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
                f.write(f"INSERT INTO test VALUES ({i}, 'data{i}');")
                temp_files.append(f.name)
        
        try:
            # Mock SingleFileImporter
            mock_importer = Mock()
            mock_single_importer_class.return_value = mock_importer
            
            # Create mock results
            mock_results = []
            for i, temp_file in enumerate(temp_files):
                result = ImportResult(
                    file_path=temp_file,
                    table_name=f"table_{i}",
                    success=True,
                    records_processed=10,
                    records_failed=0,
                    processing_time=1.0
                )
                mock_results.append(result)
            
            mock_importer.import_file.side_effect = mock_results
            
            # Create import tasks
            tasks = []
            for i, temp_file in enumerate(temp_files):
                task = ImportTask(
                    file_path=temp_file,
                    table_name=f"table_{i}",
                    encoding="utf-8"
                )
                tasks.append(task)
            
            # Import files
            results = self.importer.import_files(tasks)
            
            assert len(results) == 3
            assert all(result.success for result in results)
            
            # Check statistics
            stats = self.importer.get_statistics()
            assert stats['total_files'] == 3
            assert stats['successful_files'] == 3
            assert stats['failed_files'] == 0
            assert stats['successful_records'] == 30
            
        finally:
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass
    
    def test_progress_callback(self):
        """Test progress callback functionality."""
        callback_calls = []
        
        def progress_callback(progress):
            callback_calls.append(progress.completed_files)
        
        # Add callback
        self.importer.add_progress_callback(progress_callback)
        
        # Simulate progress update
        self.importer.progress_monitor.start_monitoring(2)
        result = ImportResult(
            file_path="/test.sql",
            table_name="test_table",
            success=True,
            records_processed=10,
            records_failed=0,
            processing_time=1.0
        )
        self.importer.progress_monitor.update_file_completed(result)
        
        # Check that callback was called
        assert len(callback_calls) > 0
    
    def test_statistics_tracking(self):
        """Test statistics tracking."""
        # Reset statistics
        self.importer._reset_statistics()
        
        # Update with successful result
        success_result = ImportResult(
            file_path="/test1.sql",
            table_name="test_table",
            success=True,
            records_processed=100,
            records_failed=5,
            processing_time=2.0
        )
        self.importer._update_statistics(success_result)
        
        # Update with failed result
        failed_result = ImportResult(
            file_path="/test2.sql",
            table_name="test_table",
            success=False,
            records_processed=50,
            records_failed=10,
            processing_time=1.0
        )
        self.importer._update_statistics(failed_result)
        
        stats = self.importer.get_statistics()
        assert stats['successful_files'] == 1
        assert stats['failed_files'] == 1
        assert stats['successful_records'] == 150
        assert stats['failed_records'] == 15
        assert stats['total_records'] == 165


if __name__ == '__main__':
    pytest.main([__file__])