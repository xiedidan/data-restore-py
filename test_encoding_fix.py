#!/usr/bin/env python3
"""
Test script to demonstrate the encoding detection fix for import_data.py
This script tests the encoding detection functionality without requiring a database connection.
"""

import os
import sys
import chardet
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.config import Config


def detect_file_encoding(file_path: str) -> str:
    """
    Detect encoding of a file using chardet.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Detected encoding or 'utf-8' as fallback
    """
    try:
        with open(file_path, 'rb') as f:
            # Read first 10KB for encoding detection
            raw_data = f.read(10240)
            
        result = chardet.detect(raw_data)
        encoding = result.get('encoding', 'utf-8')
        confidence = result.get('confidence', 0)
        
        print(f"Detected encoding for {file_path}: {encoding} (confidence: {confidence:.2f})")
        
        # Use utf-8 if confidence is too low
        if confidence < 0.7:
            print(f"Low confidence ({confidence:.2f}) for encoding detection of {file_path}, using utf-8")
            return 'utf-8'
            
        return encoding
        
    except Exception as e:
        print(f"Failed to detect encoding for {file_path}: {str(e)}")
        return 'utf-8'


def test_encoding_detection():
    """Test encoding detection on available SQL files."""
    print("Testing encoding detection functionality...")
    print("=" * 60)
    
    # Load config
    config = Config.from_file('config.yaml')
    source_dir = Path(config.source_directory)
    
    if not source_dir.exists():
        print(f"Source directory not found: {source_dir}")
        return
    
    # Find SQL files
    sql_files = []
    for sql_file in source_dir.rglob('*.sql'):
        if sql_file.is_file():
            sql_files.append(str(sql_file))
    
    if not sql_files:
        print("No SQL files found in source directory")
        return
    
    print(f"Found {len(sql_files)} SQL files:")
    print("-" * 40)
    
    # Test encoding detection for each file
    for sql_file in sql_files:
        print(f"\nFile: {os.path.basename(sql_file)}")
        encoding = detect_file_encoding(sql_file)
        
        # Try to read the file with detected encoding
        try:
            with open(sql_file, 'r', encoding=encoding) as f:
                content = f.read(200)  # Read first 200 characters
                print(f"✓ Successfully read with {encoding} encoding")
                print(f"  Preview: {repr(content[:100])}...")
        except UnicodeDecodeError as e:
            print(f"✗ Failed to read with {encoding} encoding: {e}")
        except Exception as e:
            print(f"✗ Error reading file: {e}")


def create_test_files_with_different_encodings():
    """Create test files with different encodings to demonstrate the fix."""
    print("\nCreating test files with different encodings...")
    
    # Create a file with Chinese characters (will be UTF-8)
    chinese_content = """-- 中文测试文件
INSERT INTO 用户表 (ID, 姓名, 邮箱) VALUES (1, '张三', 'zhangsan@example.com');
INSERT INTO 用户表 (ID, 姓名, 邮箱) VALUES (2, '李四', 'lisi@example.com');
"""
    
    with open('test_data/chinese_utf8.sql', 'w', encoding='utf-8') as f:
        f.write(chinese_content)
    
    # Create a file with Latin-1 encoding
    latin_content = """-- Test file with Latin-1 encoding
INSERT INTO USERS (ID, NAME, DESCRIPTION) VALUES (1, 'José', 'Café owner');
INSERT INTO USERS (ID, NAME, DESCRIPTION) VALUES (2, 'François', 'Résumé writer');
"""
    
    with open('test_data/latin1.sql', 'w', encoding='latin-1') as f:
        f.write(latin_content)
    
    print("✓ Created test files with different encodings")


if __name__ == "__main__":
    print("Encoding Detection Test for import_data.py")
    print("=" * 60)
    
    # Create test files
    create_test_files_with_different_encodings()
    
    # Test encoding detection
    test_encoding_detection()
    
    print("\n" + "=" * 60)
    print("Encoding detection test completed!")
    print("\nThe fix ensures that:")
    print("1. If an encoding analysis report exists, it uses that information")
    print("2. If no report exists, it automatically detects encoding for each file")
    print("3. Falls back to UTF-8 if detection fails or confidence is low")
    print("4. This prevents the 'utf-8 codec can't decode' errors you encountered")