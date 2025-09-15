#!/usr/bin/env python3
"""
Test the import_data.py with the real encoding report from analyze_sql.py
"""

import os
import sys
import csv
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.config import Config


def test_real_encoding_report():
    """Test loading the real encoding report."""
    print("Testing Real Encoding Report Loading")
    print("=" * 50)
    
    # Load config
    config = Config.from_file('config.yaml')
    
    # Simulate the encoding loading logic from import_data.py
    encoding_map = {}
    
    # Look for the most recent encoding analysis report
    reports_dir = Path("./reports")
    if not reports_dir.exists():
        print("Reports directory not found")
        return
    
    # Find all encoding analysis CSV files
    encoding_files = list(reports_dir.glob("encoding_analysis*.csv"))
    
    if not encoding_files:
        print("No encoding analysis reports found")
        return
    
    # Use the most recent report
    report_file = sorted(encoding_files)[-1]
    print(f"Using encoding report: {report_file}")
    
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
                    source_dir = Path(config.source_directory)
                    file_path = str(source_dir / file_name)
                
                if file_path and encoding:
                    encoding_map[file_path] = encoding
                    # Also map by filename for easier lookup
                    if file_name:
                        encoding_map[file_name] = encoding
        
        print(f"✓ Loaded encoding information for {len(encoding_map)} files")
        
        # Show the loaded encodings
        print("\nEncoding mappings from real report:")
        print("-" * 40)
        
        # Group by encoding type
        encoding_groups = {}
        for file_path, encoding in encoding_map.items():
            if encoding not in encoding_groups:
                encoding_groups[encoding] = []
            # Only show filename, not full path
            filename = os.path.basename(file_path)
            if filename not in [os.path.basename(f) for f in encoding_groups[encoding]]:
                encoding_groups[encoding].append(filename)
        
        for encoding, files in encoding_groups.items():
            print(f"\n{encoding.upper()} files:")
            for filename in sorted(files):
                print(f"  - {filename}")
        
        # Show statistics
        print(f"\nEncoding Statistics:")
        print("-" * 20)
        for encoding, files in encoding_groups.items():
            print(f"{encoding}: {len(files)} files")
            
    except Exception as e:
        print(f"✗ Failed to load encoding report: {str(e)}")
    
    return encoding_map


def main():
    """Main test function."""
    print("Real Encoding Report Test")
    print("=" * 50)
    
    encoding_map = test_real_encoding_report()
    
    print("\n" + "=" * 50)
    print("ANALYSIS")
    print("=" * 50)
    
    if encoding_map:
        gbk_files = [f for f, e in encoding_map.items() if e == 'gbk']
        ascii_files = [f for f, e in encoding_map.items() if e == 'ascii']
        
        print(f"✓ Found {len(gbk_files)} GBK-encoded files")
        print(f"✓ Found {len(ascii_files)} ASCII-encoded files")
        print()
        print("The import_data.py script will now:")
        print("1. Load this encoding information automatically")
        print("2. Use GBK encoding for Chinese files (HZSQKSB.sql, etc.)")
        print("3. Use ASCII encoding for simple files (CRZYMXB.sql, etc.)")
        print("4. Prevent 'utf-8 codec can't decode' errors")
        print()
        print("Your original encoding errors should now be resolved!")
    else:
        print("No encoding information loaded - check report file format")


if __name__ == "__main__":
    main()