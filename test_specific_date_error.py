#!/usr/bin/env python3
"""
Test the specific date error case from the production log.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.sql_rewriter import SQLRewriter
from oracle_to_postgres.common.logger import Logger

def test_specific_error_case():
    """Test the exact error case from the production log."""
    
    logger = Logger()
    rewriter = SQLRewriter("ORACLE_DB", "postgres_db", "public", logger)
    
    # The exact error case from the log
    error_sql = """INSERT INTO EMR_HIS.V_HIS_PATIENT VALUES ('0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-2022 16:50:00')"""
    
    print("Testing Specific Date Error Case")
    print("=" * 50)
    print("Original Error:")
    print('date/time field value out of range: "13-07-2022 16:50:00"')
    print("LINE 2: ...0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-202...")
    print("HINT:  Perhaps you need a different \"datestyle\" setting.")
    print()
    
    print("Original SQL:")
    print(error_sql)
    print()
    
    # Apply the fix
    fixed_sql = rewriter.rewrite_insert_statement(error_sql)
    print("Fixed SQL:")
    print(fixed_sql)
    print()
    
    # Verify the fix
    checks = [
        {
            'name': 'Schema mapped to public',
            'check': 'public' in fixed_sql and 'EMR_HIS' not in fixed_sql,
            'expected': True
        },
        {
            'name': 'V_HIS_ prefix removed',
            'check': 'PATIENT' in fixed_sql and 'V_HIS_PATIENT' not in fixed_sql,
            'expected': True
        },
        {
            'name': 'First date converted (12-07-2022 -> 2022-07-12)',
            'check': '2022-07-12 09:17:22' in fixed_sql,
            'expected': True
        },
        {
            'name': 'Second date converted (13-07-2022 -> 2022-07-13)',
            'check': '2022-07-13 16:50:00' in fixed_sql,
            'expected': True
        },
        {
            'name': 'Both dates have timestamp cast',
            'check': fixed_sql.count('::timestamp') == 2,
            'expected': True
        },
        {
            'name': 'No problematic DD-MM-YYYY format remains',
            'check': '13-07-2022' not in fixed_sql and '12-07-2022' not in fixed_sql,
            'expected': True
        },
        {
            'name': 'Table and schema properly quoted',
            'check': '"public"."PATIENT"' in fixed_sql,
            'expected': True
        }
    ]
    
    all_passed = True
    print("Verification Results:")
    print("-" * 30)
    
    for check in checks:
        result = check['check']
        status = "✅ PASS" if result == check['expected'] else "❌ FAIL"
        print(f"{status} {check['name']}")
        if result != check['expected']:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 SUCCESS: The date format error has been fixed!")
        print("The SQL should now work correctly with PostgreSQL.")
    else:
        print("❌ FAILURE: Some checks failed. Please review the implementation.")
    
    return all_passed

def test_postgresql_compatibility():
    """Test that the fixed SQL is PostgreSQL compatible."""
    
    logger = Logger()
    rewriter = SQLRewriter("ORACLE_DB", "postgres_db", "public", logger)
    
    # Test various problematic date formats
    test_cases = [
        "INSERT INTO test VALUES ('13-07-2022 16:50:00')",
        "INSERT INTO test VALUES ('01-01-2023')",
        "INSERT INTO test VALUES ('31/12/2022 23:59:59')",
        "INSERT INTO test VALUES ('15.03.2023 12:30:45')",
    ]
    
    print("\nTesting PostgreSQL Compatibility")
    print("=" * 50)
    
    all_compatible = True
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\nTest {i}:")
        print(f"Input:  {sql}")
        
        fixed = rewriter.rewrite_insert_statement(sql)
        print(f"Output: {fixed}")
        
        # Check for PostgreSQL compatibility
        has_explicit_cast = '::timestamp' in fixed or '::date' in fixed
        has_iso_format = any(f'{year}-{month:02d}-{day:02d}' in fixed 
                           for year in range(2020, 2030) 
                           for month in range(1, 13) 
                           for day in range(1, 32))
        
        if has_explicit_cast and has_iso_format:
            print("✅ PostgreSQL compatible")
        else:
            print("❌ May have compatibility issues")
            all_compatible = False
    
    return all_compatible

if __name__ == "__main__":
    print("Specific Date Error Fix Test")
    print("=" * 60)
    
    test1_passed = test_specific_error_case()
    test2_passed = test_postgresql_compatibility()
    
    print("\n" + "=" * 60)
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED!")
        print("The date format error should be resolved.")
        print("You can now re-run your import process.")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED!")
        print("Please check the implementation before proceeding.")
        sys.exit(1)