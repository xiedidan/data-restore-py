"""
Tests for file scanning functionality.
"""

import pytest
import tempfile
import os
from oracle_to_postgres.common.file_scanner import FileScanner, FileInfo


class TestFileInfo:
    """Test cases for FileInfo class."""
    
    def test_file_info_creation(self):
        """Test FileInfo creation from path."""
        content = "Test content for file size calculation"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                file_info = FileInfo.from_path(f.name)
                
                assert file_info.file_path == f.name
                assert file_info.file_name == os.path.basename(f.name)
                assert file_info.file_size > 0
                assert file_info.file_size_mb > 0
                
            finally:
                os.unlink(f.name)


class TestFileScanner:
    """Test cases for FileScanner class."""
    
    def test_scan_directory_basic(self):
        """Test basic directory scanning."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test SQL files
            test_files = ['table1.sql', 'table2.sql', 'data.txt']
            
            for filename in test_files:
                file_path = os.path.join(temp_dir, filename)
                with open(file_path, 'w') as f:
                    f.write(f"Content of {filename}")
            
            scanner = FileScanner()
            files = scanner.scan_directory(temp_dir)
            
            # Should find only .sql files
            assert len(files) == 2
            sql_files = [f.file_name for f in files]
            assert 'table1.sql' in sql_files
            assert 'table2.sql' in sql_files
            assert 'data.txt' not in sql_files
    
    def test_scan_directory_custom_extensions(self):
        """Test scanning with custom file extensions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files with different extensions
            test_files = ['table1.sql', 'table2.dump', 'data.txt']
            
            for filename in test_files:
                file_path = os.path.join(temp_dir, filename)
                with open(file_path, 'w') as f:
                    f.write(f"Content of {filename}")
            
            scanner = FileScanner(file_extensions=['.sql', '.dump'])
            files = scanner.scan_directory(temp_dir)
            
            # Should find both .sql and .dump files
            assert len(files) == 2
            filenames = [f.file_name for f in files]
            assert 'table1.sql' in filenames
            assert 'table2.dump' in filenames
            assert 'data.txt' not in filenames
    
    def test_scan_directory_recursive(self):
        """Test recursive directory scanning."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create subdirectory
            sub_dir = os.path.join(temp_dir, 'subdir')
            os.makedirs(sub_dir)
            
            # Create files in both directories
            with open(os.path.join(temp_dir, 'root.sql'), 'w') as f:
                f.write("Root file")
            
            with open(os.path.join(sub_dir, 'sub.sql'), 'w') as f:
                f.write("Sub file")
            
            scanner = FileScanner()
            
            # Non-recursive scan
            files_non_recursive = scanner.scan_directory(temp_dir, recursive=False)
            assert len(files_non_recursive) == 1
            assert files_non_recursive[0].file_name == 'root.sql'
            
            # Recursive scan
            files_recursive = scanner.scan_directory(temp_dir, recursive=True)
            assert len(files_recursive) == 2
            filenames = [f.file_name for f in files_recursive]
            assert 'root.sql' in filenames
            assert 'sub.sql' in filenames
    
    def test_scan_directory_not_found(self):
        """Test handling of non-existent directory."""
        scanner = FileScanner()
        
        with pytest.raises(FileNotFoundError):
            scanner.scan_directory("/nonexistent/directory")
    
    def test_scan_directory_not_directory(self):
        """Test handling when path is not a directory."""
        with tempfile.NamedTemporaryFile() as f:
            scanner = FileScanner()
            
            with pytest.raises(ValueError):
                scanner.scan_directory(f.name)
    
    def test_filter_by_size(self):
        """Test filtering files by size."""
        # Create mock FileInfo objects with different sizes
        files = [
            FileInfo("file1.sql", "file1.sql", 1024, 0.001),      # ~1KB
            FileInfo("file2.sql", "file2.sql", 1024*1024, 1.0),   # 1MB
            FileInfo("file3.sql", "file3.sql", 10*1024*1024, 10.0), # 10MB
            FileInfo("file4.sql", "file4.sql", 100*1024*1024, 100.0) # 100MB
        ]
        
        scanner = FileScanner()
        
        # Filter files >= 1MB
        large_files = scanner.filter_by_size(files, min_size_mb=1.0)
        assert len(large_files) == 3
        
        # Filter files between 1MB and 50MB
        medium_files = scanner.filter_by_size(files, min_size_mb=1.0, max_size_mb=50.0)
        assert len(medium_files) == 2
        
        # Filter very small files
        small_files = scanner.filter_by_size(files, max_size_mb=0.1)
        assert len(small_files) == 1
    
    def test_get_total_size(self):
        """Test calculating total size of files."""
        files = [
            FileInfo("file1.sql", "file1.sql", 1024, 0.001),
            FileInfo("file2.sql", "file2.sql", 2048, 0.002)
        ]
        
        scanner = FileScanner()
        total_bytes, total_mb = scanner.get_total_size(files)
        
        assert total_bytes == 3072
        assert abs(total_mb - 0.003) < 0.001  # Allow for floating point precision
    
    def test_extract_table_name_from_filename(self):
        """Test extracting table name from filename."""
        scanner = FileScanner()
        
        test_cases = [
            ("users.sql", "users"),
            ("dump_orders.sql", "orders"),
            ("table_products.sql", "products"),
            ("customers_dump.sql", "customers"),
            ("inventory_export.sql", "inventory"),
            ("export_categories_data.sql", "categories"),
        ]
        
        for filename, expected_table in test_cases:
            file_info = FileInfo(filename, filename, 1024, 0.001)
            table_name = scanner.extract_table_name_from_filename(file_info)
            assert table_name == expected_table
    
    def test_group_files_by_size(self):
        """Test grouping files by size categories."""
        files = [
            FileInfo("small1.sql", "small1.sql", 1024, 0.001),           # small
            FileInfo("small2.sql", "small2.sql", 5*1024*1024, 5.0),      # small
            FileInfo("medium1.sql", "medium1.sql", 20*1024*1024, 20.0),  # medium
            FileInfo("medium2.sql", "medium2.sql", 80*1024*1024, 80.0),  # medium
            FileInfo("large1.sql", "large1.sql", 200*1024*1024, 200.0),  # large
            FileInfo("xlarge1.sql", "xlarge1.sql", 2*1024*1024*1024, 2048.0) # xlarge
        ]
        
        scanner = FileScanner()
        groups = scanner.group_files_by_size(files)
        
        assert len(groups['small']) == 2
        assert len(groups['medium']) == 2
        assert len(groups['large']) == 1
        assert len(groups['xlarge']) == 1
    
    def test_validate_files(self):
        """Test file validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a valid file
            valid_file = os.path.join(temp_dir, 'valid.sql')
            with open(valid_file, 'w') as f:
                f.write("Valid content")
            
            # Create FileInfo objects
            files = [
                FileInfo.from_path(valid_file),
                FileInfo("nonexistent.sql", "nonexistent.sql", 1024, 0.001)
            ]
            
            scanner = FileScanner()
            valid_files = scanner.validate_files(files)
            
            assert len(valid_files) == 1
            assert valid_files[0].file_name == 'valid.sql'