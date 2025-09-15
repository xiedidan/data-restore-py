"""
File encoding detection utilities for Oracle to PostgreSQL migration tool.
"""

import chardet
import os
from typing import Optional, Tuple, List
from dataclasses import dataclass


@dataclass
class EncodingResult:
    """Result of encoding detection."""
    encoding: str
    confidence: float
    raw_encoding: Optional[str] = None  # Original chardet result
    
    
class EncodingDetector:
    """File encoding detector with configurable sampling."""
    
    # Common encodings to try in order of preference
    COMMON_ENCODINGS = [
        'utf-8',
        'utf-8-sig',  # UTF-8 with BOM
        'gbk',        # GBK is superset of GB2312, try first for Chinese
        'gb18030',    # Even more comprehensive Chinese encoding
        'gb2312',     # Fallback for older Chinese files
        'big5',       # Traditional Chinese
        'latin1',
        'cp1252',
        'iso-8859-1',
        'ascii'
    ]
    
    def __init__(self, sample_lines: int = 100, min_confidence: float = 0.7):
        """
        Initialize encoding detector.
        
        Args:
            sample_lines: Number of lines to sample from file for detection
            min_confidence: Minimum confidence threshold for encoding detection
        """
        self.sample_lines = sample_lines
        self.min_confidence = min_confidence
    
    def detect_encoding(self, file_path: str) -> EncodingResult:
        """
        Detect file encoding by sampling the first N lines.
        
        Args:
            file_path: Path to the file to analyze
            
        Returns:
            EncodingResult with detected encoding and confidence
            
        Raises:
            FileNotFoundError: If file doesn't exist
            PermissionError: If file cannot be read
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"Cannot read file: {file_path}")
        
        # Read sample data from file
        sample_data = self._read_sample_data(file_path)
        
        if not sample_data:
            # Empty file, assume UTF-8
            return EncodingResult(encoding='utf-8', confidence=1.0)
        
        # Try chardet first
        chardet_result = self._detect_with_chardet(sample_data)
        
        # If chardet gives high confidence result, use it
        if chardet_result and chardet_result.confidence >= self.min_confidence:
            detected_encoding = chardet_result.encoding.lower()
            
            # If detected as GB2312, try GBK first (GBK is superset of GB2312)
            if detected_encoding == 'gb2312':
                try:
                    sample_data.decode('gbk')
                    return EncodingResult(
                        encoding='gbk',
                        confidence=chardet_result.confidence,
                        raw_encoding=chardet_result.encoding
                    )
                except (UnicodeDecodeError, UnicodeError):
                    pass  # Fall back to original detection
            
            return EncodingResult(
                encoding=detected_encoding,
                confidence=chardet_result.confidence,
                raw_encoding=chardet_result.encoding
            )
        
        # Try common encodings manually
        manual_result = self._try_common_encodings(sample_data)
        if manual_result:
            return manual_result
        
        # Fallback to chardet result even if low confidence
        if chardet_result:
            return EncodingResult(
                encoding=chardet_result.encoding.lower(),
                confidence=chardet_result.confidence,
                raw_encoding=chardet_result.encoding
            )
        
        # Ultimate fallback to UTF-8
        return EncodingResult(encoding='utf-8', confidence=0.1)
    
    def _read_sample_data(self, file_path: str) -> bytes:
        """Read sample data from file for encoding detection."""
        sample_data = b''
        lines_read = 0
        
        try:
            with open(file_path, 'rb') as f:
                while lines_read < self.sample_lines:
                    line = f.readline()
                    if not line:  # EOF
                        break
                    sample_data += line
                    lines_read += 1
                    
                    # Limit sample size to prevent memory issues
                    if len(sample_data) > 1024 * 1024:  # 1MB limit
                        break
        except Exception as e:
            raise IOError(f"Error reading file {file_path}: {str(e)}")
        
        return sample_data
    
    def _detect_with_chardet(self, data: bytes) -> Optional[dict]:
        """Use chardet library for encoding detection."""
        try:
            result = chardet.detect(data)
            if result and result['encoding']:
                # Create a simple object to hold the result
                class ChardetResult:
                    def __init__(self, encoding: str, confidence: float):
                        self.encoding = encoding
                        self.confidence = confidence
                
                return ChardetResult(result['encoding'], result['confidence'])
        except Exception:
            pass
        
        return None
    
    def _try_common_encodings(self, data: bytes) -> Optional[EncodingResult]:
        """Try to decode data with common encodings."""
        for encoding in self.COMMON_ENCODINGS:
            try:
                # Try to decode the sample data
                decoded = data.decode(encoding)
                
                # Check if decoded text looks reasonable
                if self._is_reasonable_text(decoded):
                    # Calculate a simple confidence based on successful decoding
                    confidence = 0.8 if encoding in ['utf-8', 'utf-8-sig'] else 0.6
                    return EncodingResult(encoding=encoding, confidence=confidence)
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        # Try with error handling strategies
        for encoding in self.COMMON_ENCODINGS:
            for error_strategy in ['ignore', 'replace']:
                try:
                    decoded = data.decode(encoding, errors=error_strategy)
                    if self._is_reasonable_text(decoded):
                        # Lower confidence for error-handled decoding
                        confidence = 0.4 if error_strategy == 'replace' else 0.3
                        return EncodingResult(
                            encoding=f"{encoding}:{error_strategy}", 
                            confidence=confidence
                        )
                except (UnicodeDecodeError, UnicodeError):
                    continue
        
        return None
    
    def _is_reasonable_text(self, text: str) -> bool:
        """Check if decoded text looks reasonable."""
        if not text:
            return True
        
        # Check for too many control characters (except common ones)
        control_chars = sum(1 for c in text if ord(c) < 32 and c not in '\n\r\t')
        control_ratio = control_chars / len(text)
        
        # If more than 10% control characters, probably wrong encoding
        if control_ratio > 0.1:
            return False
        
        # Check for replacement characters (indicates encoding issues)
        if '\ufffd' in text:
            return False
        
        return True
    
    def detect_multiple_files(self, file_paths: List[str]) -> List[Tuple[str, EncodingResult]]:
        """
        Detect encoding for multiple files.
        
        Args:
            file_paths: List of file paths to analyze
            
        Returns:
            List of tuples (file_path, EncodingResult)
        """
        results = []
        
        for file_path in file_paths:
            try:
                encoding_result = self.detect_encoding(file_path)
                results.append((file_path, encoding_result))
            except Exception as e:
                # Create error result
                error_result = EncodingResult(
                    encoding='utf-8',  # Default fallback
                    confidence=0.0
                )
                results.append((file_path, error_result))
        
        return results
    
    def validate_encoding(self, file_path: str, encoding: str) -> bool:
        """
        Validate if a file can be read with specified encoding.
        
        Args:
            file_path: Path to the file
            encoding: Encoding to validate
            
        Returns:
            True if file can be read with the encoding, False otherwise
        """
        try:
            sample_data = self._read_sample_data(file_path)
            sample_data.decode(encoding)
            return True
        except (UnicodeDecodeError, UnicodeError, IOError):
            return False
    
    def read_file_safely(self, file_path: str, encoding: str = None, max_size_mb: int = 100) -> Tuple[str, str]:
        """
        Safely read file content with automatic encoding detection and error handling.
        
        Args:
            file_path: Path to the file to read
            encoding: Specific encoding to use (auto-detect if None)
            max_size_mb: Maximum file size to read in MB (for performance)
            
        Returns:
            Tuple of (content, actual_encoding_used)
            
        Raises:
            IOError: If file cannot be read at all
        """
        if encoding is None:
            detection_result = self.detect_encoding(file_path)
            encoding = detection_result.encoding
        
        # Parse encoding and error strategy if specified
        error_strategy = 'strict'
        if ':' in encoding:
            encoding, error_strategy = encoding.split(':', 1)
        
        # Check file size for performance optimization
        file_size = os.path.getsize(file_path)
        max_bytes = max_size_mb * 1024 * 1024
        
        try:
            with open(file_path, 'r', encoding=encoding, errors=error_strategy) as f:
                if file_size > max_bytes:
                    # For large files, read only the first part
                    content = f.read(max_bytes)
                else:
                    content = f.read()
            return content, f"{encoding}:{error_strategy}" if error_strategy != 'strict' else encoding
        except UnicodeDecodeError as e:
            # Try with GBK first if original was GB2312
            if encoding == 'gb2312':
                try:
                    with open(file_path, 'r', encoding='gbk', errors='strict') as f:
                        if file_size > max_bytes:
                            content = f.read(max_bytes)
                        else:
                            content = f.read()
                    return content, 'gbk'
                except Exception:
                    pass
            
            # Try with replace strategy
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    if file_size > max_bytes:
                        content = f.read(max_bytes)
                    else:
                        content = f.read()
                return content, f"{encoding}:replace"
            except Exception:
                # Try with ignore strategy
                try:
                    with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                        if file_size > max_bytes:
                            content = f.read(max_bytes)
                        else:
                            content = f.read()
                    return content, f"{encoding}:ignore"
                except Exception:
                    # Last resort: try UTF-8 with replace
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                            if file_size > max_bytes:
                                content = f.read(max_bytes)
                            else:
                                content = f.read()
                        return content, "utf-8:replace"
                    except Exception as final_e:
                        raise IOError(f"Cannot read file {file_path} with any encoding strategy: {final_e}")
    
    def read_file_sample_safely(self, file_path: str, encoding: str = None, sample_lines: int = 1000) -> Tuple[str, str]:
        """
        Safely read a sample of file content for analysis purposes.
        Much faster for large files as it only reads the first N lines.
        
        Args:
            file_path: Path to the file to read
            encoding: Specific encoding to use (auto-detect if None)
            sample_lines: Number of lines to read from the beginning
            
        Returns:
            Tuple of (sample_content, actual_encoding_used)
        """
        if encoding is None:
            detection_result = self.detect_encoding(file_path)
            encoding = detection_result.encoding
        
        # Parse encoding and error strategy if specified
        error_strategy = 'strict'
        if ':' in encoding:
            encoding, error_strategy = encoding.split(':', 1)
        
        try:
            lines = []
            with open(file_path, 'r', encoding=encoding, errors=error_strategy) as f:
                for i, line in enumerate(f):
                    if i >= sample_lines:
                        break
                    lines.append(line)
            content = ''.join(lines)
            return content, f"{encoding}:{error_strategy}" if error_strategy != 'strict' else encoding
        except UnicodeDecodeError:
            # Try with GBK first if original was GB2312
            if encoding == 'gb2312':
                try:
                    lines = []
                    with open(file_path, 'r', encoding='gbk', errors='strict') as f:
                        for i, line in enumerate(f):
                            if i >= sample_lines:
                                break
                            lines.append(line)
                    content = ''.join(lines)
                    return content, 'gbk'
                except Exception:
                    pass
            
            # Try with replace strategy
            try:
                lines = []
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    for i, line in enumerate(f):
                        if i >= sample_lines:
                            break
                        lines.append(line)
                content = ''.join(lines)
                return content, f"{encoding}:replace"
            except Exception:
                # Last resort: try UTF-8 with replace
                try:
                    lines = []
                    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                        for i, line in enumerate(f):
                            if i >= sample_lines:
                                break
                            lines.append(line)
                    content = ''.join(lines)
                    return content, "utf-8:replace"
                except Exception as final_e:
                    raise IOError(f"Cannot read file {file_path} with any encoding strategy: {final_e}")


class EncodingConverter:
    """Utility for converting file encodings."""
    
    def __init__(self, detector: Optional[EncodingDetector] = None):
        """Initialize with optional custom detector."""
        self.detector = detector or EncodingDetector()
    
    def convert_file_encoding(self, source_path: str, target_path: str, 
                            target_encoding: str = 'utf-8',
                            source_encoding: Optional[str] = None) -> bool:
        """
        Convert file from one encoding to another.
        
        Args:
            source_path: Source file path
            target_path: Target file path
            target_encoding: Target encoding (default: utf-8)
            source_encoding: Source encoding (auto-detect if None)
            
        Returns:
            True if conversion successful, False otherwise
        """
        try:
            # Detect source encoding if not provided
            if source_encoding is None:
                detection_result = self.detector.detect_encoding(source_path)
                source_encoding = detection_result.encoding
            
            # Read source file
            with open(source_path, 'r', encoding=source_encoding) as source_file:
                content = source_file.read()
            
            # Write target file
            with open(target_path, 'w', encoding=target_encoding) as target_file:
                target_file.write(content)
            
            return True
            
        except Exception:
            return False
    
    def get_file_encoding_info(self, file_path: str) -> dict:
        """
        Get comprehensive encoding information for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with encoding information
        """
        try:
            detection_result = self.detector.detect_encoding(file_path)
            file_size = os.path.getsize(file_path)
            
            return {
                'file_path': file_path,
                'file_size': file_size,
                'file_size_mb': file_size / (1024 * 1024),
                'detected_encoding': detection_result.encoding,
                'confidence': detection_result.confidence,
                'raw_encoding': detection_result.raw_encoding,
                'is_valid': detection_result.confidence > 0.5
            }
        except Exception as e:
            return {
                'file_path': file_path,
                'file_size': 0,
                'file_size_mb': 0.0,
                'detected_encoding': 'unknown',
                'confidence': 0.0,
                'raw_encoding': None,
                'is_valid': False,
                'error': str(e)
            }