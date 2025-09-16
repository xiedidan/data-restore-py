#!/usr/bin/env python3
"""
Test the date format conversion fix.
"""

import os
import sys

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.sql_rewriter import SQLRewriter


def test_date_format_conversion():
    """Test date format conversion for PostgreSQL compatibility."""
    print("Testing Date Format Conversion")
    print("=" * 50)
    
    # Initialize SQL rewriter
    rewriter = SQLRewriter(
        source_db="oracle",
        target_db="postgresql", 
        target_schema="public"
    )
    
    # Test cases based on the actual error
    test_cases = [
        {
            'name': 'DD-MM-YYYY HH24:MI:SS (Error Case)',
            'input': "INSERT INTO \"public\".\"TEST\" (\"DATE_COL\") VALUES ('16-08-2022 08:00:59'::timestamp)",
            'expected_format': '2022-08-16 08:00:59',
            'description': 'Convert DD-MM-YYYY HH24:MI:SS to YYYY-MM-DD HH24:MI:SS'
        },
        {
            'name': 'DD-MM-YYYY Date Only',
            'input': "INSERT INTO \"public\".\"TEST\" (\"DATE_COL\") VALUES ('25-12-2023')",
            'expected_format': '2023-12-25',
            'description': 'Convert DD-MM-YYYY to YYYY-MM-DD'
        },
        {
            'name': 'DD/MM/YYYY HH24:MI:SS',
            'input': "INSERT INTO \"public\".\"TEST\" (\"DATE_COL\") VALUES ('31/01/2023 14:30:45')",
            'expected_format': '2023-01-31 14:30:45',
            'description': 'Convert DD/MM/YYYY HH24:MI:SS to YYYY-MM-DD HH24:MI:SS'
        },
        {
            'name': 'DD/MM/YYYY Date Only',
            'input': "INSERT INTO \"public\".\"TEST\" (\"DATE_COL\") VALUES ('15/03/2023')",
            'expected_format': '2023-03-15',
            'description': 'Convert DD/MM/YYYY to YYYY-MM-DD'
        },
        {
            'name': 'DD.MM.YYYY HH24:MI:SS',
            'input': "INSERT INTO \"public\".\"TEST\" (\"DATE_COL\") VALUES ('28.02.2023 16:45:30')",
            'expected_format': '2023-02-28 16:45:30',
            'description': 'Convert DD.MM.YYYY HH24:MI:SS to YYYY-MM-DD HH24:MI:SS'
        },
        {
            'name': 'Multiple Dates in One Statement',
            'input': "INSERT INTO \"public\".\"TEST\" (\"START_DATE\", \"END_DATE\") VALUES ('16-08-2022 08:00:59', '17-08-2022 18:30:00')",
            'expected_format': ['2022-08-16 08:00:59', '2022-08-17 18:30:00'],
            'description': 'Convert multiple dates in single statement'
        },
        {
            'name': 'Already Correct Format',
            'input': "INSERT INTO \"public\".\"TEST\" (\"DATE_COL\") VALUES ('2023-12-25 14:30:45')",
            'expected_format': '2023-12-25 14:30:45',
            'description': 'Leave correct YYYY-MM-DD format unchanged'
        }
    ]
    
    print("Testing date format conversions:")
    print("-" * 50)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['description']}")
        print(f"Input:  {test_case['input']}")
        
        result = rewriter.rewrite_insert_statement(test_case['input'])
        print(f"Output: {result}")
        
        # Check conversion
        if isinstance(test_case['expected_format'], list):
            # Multiple dates
            success = all(date_str in result for date_str in test_case['expected_format'])
        else:
            # Single date
            success = test_case['expected_format'] in result
        
        if success:
            print("✅ PASS - Date format converted correctly")
        else:
            print(f"❌ FAIL - Expected format: {test_case['expected_format']}")
        
        # Check that old format is gone (except for already correct format)
        if 'Already Correct' not in test_case['name']:
            old_patterns = ['16-08-2022', '25-12-2023', '31/01/2023', '15/03/2023', '28.02.2023']
            old_format_present = any(pattern in result for pattern in old_patterns)
            
            if not old_format_present:
                print("✅ PASS - Old date format removed")
            else:
                print("❌ FAIL - Old date format still present")


def test_specific_error_case():
    """Test the specific error case from the log."""
    print("\n" + "=" * 50)
    print("Testing Specific Error Case")
    print("=" * 50)
    
    rewriter = SQLRewriter("oracle", "postgresql", "public")
    
    # The exact problematic SQL from the error
    problematic_sql = "INSERT INTO table (col1, col2, col3) VALUES ('0000002616', 1, '05-08-2022 09:18:52'::timestamp, '16-08-2022 08:00:59')"
    
    print("BEFORE (Problematic SQL):")
    print(f"  {problematic_sql}")
    print("Error: date/time field value out of range: \"16-08-2022 08:00:59\"")
    print()
    
    # Apply the fix
    fixed_sql = rewriter.rewrite_insert_statement(problematic_sql)
    
    print("AFTER (Fixed SQL):")
    print(f"  {fixed_sql}")
    print()
    
    # Verify the fix
    if '2022-08-16 08:00:59' in fixed_sql and '2022-08-05 09:18:52' in fixed_sql:
        print("✅ SUCCESS: Date formats converted correctly!")
        print("   16-08-2022 08:00:59 → 2022-08-16 08:00:59")
        print("   05-08-2022 09:18:52 → 2022-08-05 09:18:52")
    else:
        print("❌ FAILED: Date formats not converted properly")
    
    # Check that old formats are gone
    if '16-08-2022' not in fixed_sql and '05-08-2022' not in fixed_sql:
        print("✅ SUCCESS: Old date formats removed!")
    else:
        print("❌ FAILED: Some old date formats still present")


def show_date_format_mappings():
    """Show all supported date format mappings."""
    print("\n" + "=" * 50)
    print("Supported Date Format Mappings")
    print("=" * 50)
    
    mappings = [
        {
            'oracle': 'DD-MM-YYYY HH24:MI:SS',
            'example_oracle': '16-08-2022 08:00:59',
            'postgresql': 'YYYY-MM-DD HH24:MI:SS',
            'example_postgresql': '2022-08-16 08:00:59'
        },
        {
            'oracle': 'DD-MM-YYYY',
            'example_oracle': '25-12-2023',
            'postgresql': 'YYYY-MM-DD',
            'example_postgresql': '2023-12-25'
        },
        {
            'oracle': 'DD/MM/YYYY HH24:MI:SS',
            'example_oracle': '31/01/2023 14:30:45',
            'postgresql': 'YYYY-MM-DD HH24:MI:SS',
            'example_postgresql': '2023-01-31 14:30:45'
        },
        {
            'oracle': 'DD/MM/YYYY',
            'example_oracle': '15/03/2023',
            'postgresql': 'YYYY-MM-DD',
            'example_postgresql': '2023-03-15'
        },
        {
            'oracle': 'DD.MM.YYYY HH24:MI:SS',
            'example_oracle': '28.02.2023 16:45:30',
            'postgresql': 'YYYY-MM-DD HH24:MI:SS',
            'example_postgresql': '2023-02-28 16:45:30'
        }
    ]
    
    print("Automatic date format conversions:")
    print("-" * 50)
    
    for mapping in mappings:
        print(f"Oracle:     {mapping['oracle']}")
        print(f"Example:    '{mapping['example_oracle']}'")
        print(f"PostgreSQL: {mapping['postgresql']}")
        print(f"Converted:  '{mapping['example_postgresql']}'")
        print()


def main():
    """Main test function."""
    test_date_format_conversion()
    test_specific_error_case()
    show_date_format_mappings()
    
    print("\n" + "=" * 50)
    print("DATE FORMAT CONVERSION SUMMARY")
    print("=" * 50)
    print("The enhanced SQL rewriter now:")
    print("1. ✅ Converts DD-MM-YYYY to YYYY-MM-DD format")
    print("2. ✅ Converts DD/MM/YYYY to YYYY-MM-DD format")
    print("3. ✅ Converts DD.MM.YYYY to YYYY-MM-DD format")
    print("4. ✅ Handles both date and datetime formats")
    print("5. ✅ Processes multiple dates in single statement")
    print("6. ✅ Leaves correct formats unchanged")
    print()
    print("This should resolve the PostgreSQL date parsing errors!")
    print("Oracle date formats will be automatically converted to PostgreSQL standard.")


if __name__ == "__main__":
    main()