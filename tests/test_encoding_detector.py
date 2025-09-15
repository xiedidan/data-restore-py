"""
Tests for encoding detection functionality.
"""

import pytest
import tempfile
import os
from oracle_to_postgres.common.encoding_detector import EncodingDetector, EncodingConverter, EncodingResult


class TestEncodingDetector:
    """Test cases for EncodingDetector class."""
    
    def test_detect_utf8_file(self):
        """Test detection of UTF-8 encoded file."""
        content = "这是一个UTF-8编码的测试文件\nINSERT INTO users VALUES (1, '张三', 'test@example.com');\n"
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                detector = EncodingDetector(sample_lines=10)
                result = detector.detect_encoding(f.name)
                
                assert result.encoding in ['utf-8', 'utf-8-sig']
                assert result.confidence > 0.5
            finally:
                os.unlink(f.name)
    
    def test_detect_gbk_file(self):
        """Test detection of GBK encoded file."""
        content = "这是一个GBK编码的测试文件\nINSERT INTO users VALUES (1, '张三', 'test@example.com');\n"
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='gbk', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                detector = EncodingDetector(sample_lines=10)
                result = detector.detect_encoding(f.name)
                
                # Should detect some encoding (might be gbk, gb2312, or similar)
                assert result.encoding is not None
                assert result.confidence > 0.0
            finally:
                os.unlink(f.name)
    
    def test_detect_ascii_file(self):
        """Test detection of ASCII encoded file."""
        content = "INSERT INTO users VALUES (1, 'John Doe', 'john@example.com');\n"
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='ascii', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                detector = EncodingDetector(sample_lines=10)
                result = detector.detect_encoding(f.name)
                
                # ASCII should be detected as UTF-8 or ASCII
                assert result.encoding in ['utf-8', 'ascii', 'latin1']
                assert result.confidence > 0.5
            finally:
                os.unlink(f.name)
    
    def test_empty_file(self):
        """Test detection of empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.flush()
            
            try:
                detector = EncodingDetector()
                result = detector.detect_encoding(f.name)
                
                assert result.encoding == 'utf-8'
                assert result.confidence == 1.0
            finally:
                os.unlink(f.name)
    
    def test_file_not_found(self):
        """Test handling of non-existent file."""
        detector = EncodingDetector()
        
        with pytest.raises(FileNotFoundError):
            detector.detect_encoding("nonexistent_file.sql")
    
    def test_sample_lines_limit(self):
        """Test that sample_lines parameter is respected."""
        # Create a file with many lines
        lines = ["INSERT INTO test VALUES ({}, 'data{}');\n".format(i, i) for i in range(1000)]
        content = "".join(lines)
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                detector = EncodingDetector(sample_lines=5)
                result = detector.detect_encoding(f.name)
                
                # Should still detect UTF-8 even with limited sample
                assert result.encoding in ['utf-8', 'ascii']
                assert result.confidence > 0.0
            finally:
                os.unlink(f.name)
    
    def test_detect_multiple_files(self):
        """Test detection of multiple files."""
        files = []
        
        try:
            # Create test files with different encodings
            for i, encoding in enumerate(['utf-8', 'ascii']):
                content = f"Test file {i}\nINSERT INTO test VALUES ({i}, 'data');\n"
                
                with tempfile.NamedTemporaryFile(mode='w', encoding=encoding, delete=False) as f:
                    f.write(content)
                    f.flush()
                    files.append(f.name)
            
            detector = EncodingDetector()
            results = detector.detect_multiple_files([f.name for f in files])
            
            assert len(results) == 2
            for file_path, result in results:
                assert isinstance(result, EncodingResult)
                assert result.encoding is not None
                
        finally:
            for file_path in files:
                if os.path.exists(file_path):
                    os.unlink(file_path)
    
    def test_validate_encoding(self):
        """Test encoding validation."""
        content = "这是一个UTF-8编码的测试文件\n"
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                detector = EncodingDetector()
                
                # Should validate UTF-8 successfully
                assert detector.validate_encoding(f.name, 'utf-8') is True
                
                # Should fail for incompatible encoding
                assert detector.validate_encoding(f.name, 'ascii') is False
                
            finally:
                os.unlink(f.name)


class TestEncodingConverter:
    """Test cases for EncodingConverter class."""
    
    def test_convert_file_encoding(self):
        """Test file encoding conversion."""
        content = "Test content\nINSERT INTO test VALUES (1, 'data');\n"
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as source_file:
            source_file.write(content)
            source_file.flush()
            
            with tempfile.NamedTemporaryFile(delete=False) as target_file:
                try:
                    converter = EncodingConverter()
                    success = converter.convert_file_encoding(
                        source_file.name, 
                        target_file.name, 
                        target_encoding='utf-8',
                        source_encoding='utf-8'
                    )
                    
                    assert success is True
                    
                    # Verify target file content
                    with open(target_file.name, 'r', encoding='utf-8') as f:
                        converted_content = f.read()
                        assert converted_content == content
                        
                finally:
                    os.unlink(source_file.name)
                    os.unlink(target_file.name)
    
    def test_get_file_encoding_info(self):
        """Test getting comprehensive file encoding information."""
        content = "Test file\nINSERT INTO test VALUES (1, 'data');\n"
        
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as f:
            f.write(content)
            f.flush()
            
            try:
                converter = EncodingConverter()
                info = converter.get_file_encoding_info(f.name)
                
                assert 'file_path' in info
                assert 'file_size' in info
                assert 'file_size_mb' in info
                assert 'detected_encoding' in info
                assert 'confidence' in info
                assert 'is_valid' in info
                
                assert info['file_path'] == f.name
                assert info['file_size'] > 0
                assert info['detected_encoding'] is not None
                
            finally:
                os.unlink(f.name)
    
    def test_get_file_encoding_info_error(self):
        """Test error handling in get_file_encoding_info."""
        converter = EncodingConverter()
        info = converter.get_file_encoding_info("nonexistent_file.sql")
        
        assert 'error' in info
        assert info['detected_encoding'] == 'unknown'
        assert info['confidence'] == 0.0
        assert info['is_valid'] is False


class TestEncodingResult:
    """Test cases for EncodingResult dataclass."""
    
    def test_encoding_result_creation(self):
        """Test EncodingResult creation and attributes."""
        result = EncodingResult(encoding='utf-8', confidence=0.95)
        
        assert result.encoding == 'utf-8'
        assert result.confidence == 0.95
        assert result.raw_encoding is None
    
    def test_encoding_result_with_raw(self):
        """Test EncodingResult with raw encoding."""
        result = EncodingResult(
            encoding='utf-8', 
            confidence=0.95, 
            raw_encoding='UTF-8'
        )
        
        assert result.encoding == 'utf-8'
        assert result.confidence == 0.95
        assert result.raw_encoding == 'UTF-8'