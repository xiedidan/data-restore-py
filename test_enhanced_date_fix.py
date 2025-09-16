#!/usr/bin/env python3
"""
Test enhanced date format conversion with explicit casting.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.sql_rewriter import SQLRewriter
from oracle_to_postgres.common.logger import Logger

def test_enhanced_date_conversion():
    """Test enhanced date format conversion with explicit casting."""
    
    logger = Logger()
    rewriter = SQLRewriter("ORACLE_DB", "postgres_db", "public", logger)
    
    # Test cases with problematic date formats
    test_cases = [
        {
            'name': 'DD-MM-YYYY HH24:MI:SS with timestamp cast',
            'input': "INSERT INTO test_table VALUES ('13-07-2022 16:50:00'::timestamp)",
            'expected_contains': ['2022-07-13 16:50:00', '::timestamp']
        },
        {
            'name': 'DD-MM-YYYY HH24:MI:SS without cast',
            'input': "INSERT INTO test_table VALUES ('13-07-2022 16:50:00')",
            'expected_contains': ['2022-07-13 16:50:00', '::timestamp']
        },
        {
            'name': 'DD-MM-YYYY without cast',
            'input': "INSERT INTO test_table VALUES ('13-07-2022')",
            'expected_contains': ['2022-07-13', '::date']
        },
        {
            'name': 'Multiple date formats in one statement',
            'input': "INSERT INTO test_table VALUES ('12-07-2022 09:17:22', '13-07-2022 16:50:00')",
            'expected_contains': ['2022-07-12 09:17:22', '2022-07-13 16:50:00', '::timestamp']
        },
        {
            'name': 'DD/MM/YYYY format',
            'input': "INSERT INTO test_table VALUES ('13/07/2022 16:50:00')",
            'expected_contains': ['2022-07-13 16:50:00', '::timestamp']
        },
        {
            'name': 'DD.MM.YYYY format',
            'input': "INSERT INTO test_table VALUES ('13.07.2022')",
            'expected_contains': ['2022-07-13', '::date']
        },
        {
            'name': 'Already correct format should get cast',
            'input': "INSERT INTO test_table VALUES ('2022-07-13 16:50:00')",
            'expected_contains': ['2022-07-13 16:50:00', '::timestamp']
        },
        {
            'name': 'Complex INSERT with schema and table mapping',
            'input': "INSERT INTO EMR_HIS.V_HIS_PATIENT (id, created_date, updated_date) VALUES (1, '12-07-2022 09:17:22', '13-07-2022 16:50:00')",
            'expected_contains': ['public', 'PATIENT', '2022-07-12 09:17:22', '2022-07-13 16:50:00', '::timestamp']
        }
    ]
    
    print("Testing Enhanced Date Format Conversion")
    print("=" * 50)
    
    all_passed = True
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['name']}")
        print(f"Input:  {test_case['input']}")
        
        # Apply rewriting
        result = rewriter.rewrite_insert_statement(test_case['input'])
        print(f"Output: {result}")
        
        # Check if all expected strings are present
        test_passed = True
        for expected in test_case['expected_contains']:
            if expected not in result:
                print(f"❌ Missing expected string: '{expected}'")
                test_passed = False
        
        if test_passed:
            print("✅ PASSED")
        else:
            print("❌ FAILED")
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests PASSED!")
    else:
        print("❌ Some tests FAILED!")
    
    # Show rewrite statistics
    stats = rewriter.get_rewrite_statistics()
    if stats:
        print("\nRewrite Statistics:")
        for rule, count in stats.items():
            print(f"  {rule}: {count}")
    
    return all_passed

def test_real_world_example():
    """Test with the actual error case from the log."""
    
    logger = Logger()
    rewriter = SQLRewriter("ORACLE_DB", "postgres_db", "public", logger)
    
    # The actual problematic SQL from the error log
    problematic_sql = """INSERT INTO EMR_HIS.V_HIS_PATIENT VALUES ('0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-2022 16:50:00')"""
    
    print("\nTesting Real-World Example")
    print("=" * 50)
    print(f"Original SQL: {problematic_sql}")
    
    result = rewriter.rewrite_insert_statement(problematic_sql)
    print(f"Rewritten SQL: {result}")
    
    # Check key transformations
    checks = [
        ('Schema mapping', 'public' in result),
        ('Table name mapping', 'PATIENT' in result and 'V_HIS_PATIENT' not in result),
        ('First date converted', '2022-07-12 09:17:22' in result),
        ('Second date converted', '2022-07-13 16:50:00' in result),
        ('Timestamp cast added', result.count('::timestamp') >= 2),
        ('Column quoting', '"' in result)
    ]
    
    all_good = True
    for check_name, passed in checks:
        status = "✅" if passed else "❌"
        print(f"{status} {check_name}: {passed}")
        if not passed:
            all_good = False
    
    return all_good

if __name__ == "__main__":
    print("Enhanced Date Format Conversion Test")
    print("=" * 60)
    
    test1_passed = test_enhanced_date_conversion()
    test2_passed = test_real_world_example()
    
    print("\n" + "=" * 60)
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED! Date format conversion is working correctly.")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED! Please check the implementation.")
        sys.exit(1)