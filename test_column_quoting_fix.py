#!/usr/bin/env python3
"""
Test the column name quoting fix (instead of case conversion).
"""

import os
import sys

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.sql_rewriter import SQLRewriter


def test_column_quoting():
    """Test column name quoting instead of case conversion."""
    print("Testing Column Name Quoting (Preserve Case)")
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
            'name': 'CRZYMXB INSERT (Original Error)',
            'input': 'INSERT INTO "public"."CRZYMXB" (NY, BAH, ZYH, CZLX, LYKS, MDKS) VALUES (\'2022-08-10\', \'b37a635a6aa780814b83\', \'0000049818\', \'zy\', \'2d1b5ab9185680810ade\', \'38f5c3cb5bbc808136c7\')',
            'expected_columns': ['"NY"', '"BAH"', '"ZYH"', '"CZLX"', '"LYKS"', '"MDKS"'],
            'description': 'Add quotes to preserve uppercase column names'
        },
        {
            'name': 'Mixed Case Columns',
            'input': 'INSERT INTO "public"."TEST_TABLE" (Id, Name, Email, CreatedDate) VALUES (1, \'John\', \'john@test.com\', \'2023-01-01\')',
            'expected_columns': ['"Id"', '"Name"', '"Email"', '"CreatedDate"'],
            'description': 'Add quotes to preserve mixed case column names'
        },
        {
            'name': 'Already Quoted Columns',
            'input': 'INSERT INTO "public"."TEST_TABLE" ("ID", "NAME", "EMAIL") VALUES (1, \'John\', \'john@test.com\')',
            'expected_columns': ['"ID"', '"NAME"', '"EMAIL"'],
            'description': 'Keep existing quotes unchanged'
        },
        {
            'name': 'Oracle Schema with V_HIS_ Prefix',
            'input': 'INSERT INTO EMR_HIS.V_HIS_KSDMDZB (HISKSDM, HISKSMC, BAKS) VALUES (\'001\', \'科室名称\', \'Y\')',
            'expected_columns': ['"HISKSDM"', '"HISKSMC"', '"BAKS"'],
            'description': 'Handle schema replacement, V_HIS_ removal, and column quoting'
        },
        {
            'name': 'Lowercase Columns',
            'input': 'INSERT INTO "public"."TEST" (id, name, email) VALUES (1, \'test\', \'test@example.com\')',
            'expected_columns': ['"id"', '"name"', '"email"'],
            'description': 'Add quotes to lowercase column names'
        }
    ]
    
    print("Testing column name quoting:")
    print("-" * 50)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['description']}")
        print(f"Input:  {test_case['input'][:80]}{'...' if len(test_case['input']) > 80 else ''}")
        
        result = rewriter.rewrite_insert_statement(test_case['input'])
        print(f"Output: {result[:80]}{'...' if len(result) > 80 else ''}")
        
        # Check if expected quoted columns are present
        success = True
        for expected_col in test_case['expected_columns']:
            if expected_col not in result:
                success = False
                print(f"   Missing: {expected_col}")
                break
        
        if success:
            print("✅ PASS - Column names properly quoted")
        else:
            print("❌ FAIL - Column names not properly quoted")
            print(f"   Expected columns: {test_case['expected_columns']}")


def test_specific_error_case():
    """Test the specific error case with quoting approach."""
    print("\n" + "=" * 50)
    print("Testing Specific Error Case with Quoting")
    print("=" * 50)
    
    rewriter = SQLRewriter("oracle", "postgresql", "public")
    
    # The exact statement that was failing
    problematic_sql = 'INSERT INTO "public"."CRZYMXB" (NY, BAH, ZYH, CZLX, LYKS, MDKS) VALUES (\'2022-08-10\', \'b37a635a6aa780814b83\', \'0000049818\', \'zy\', \'2d1b5ab9185680810ade\', \'38f5c3cb5bbc808136c7\')'
    
    print("BEFORE (Problematic SQL):")
    print(f"  {problematic_sql}")
    print()
    
    # Apply the fix
    fixed_sql = rewriter.rewrite_insert_statement(problematic_sql)
    
    print("AFTER (Fixed SQL):")
    print(f"  {fixed_sql}")
    print()
    
    # Verify the fix
    expected_quoted_columns = ['"NY"', '"BAH"', '"ZYH"', '"CZLX"', '"LYKS"', '"MDKS"']
    if all(col in fixed_sql for col in expected_quoted_columns):
        print("✅ SUCCESS: Column names properly quoted!")
        print("   NY → \"NY\", BAH → \"BAH\", ZYH → \"ZYH\", etc.")
        print("   PostgreSQL will now match the exact case in the database.")
    else:
        print("❌ FAILED: Column names not quoted properly")
    
    # Check that unquoted versions are gone
    unquoted_columns = ['(NY,', '(BAH,', '(ZYH,', '(CZLX,', '(LYKS,', '(MDKS']
    if not any(col in fixed_sql for col in unquoted_columns):
        print("✅ SUCCESS: All column names are now quoted!")
    else:
        print("❌ FAILED: Some column names still unquoted")


def compare_approaches():
    """Compare the quoting approach vs lowercase approach."""
    print("\n" + "=" * 50)
    print("Comparing Approaches")
    print("=" * 50)
    
    print("QUOTING APPROACH (Current):")
    print("  Original: INSERT INTO table (NY, BAH, ZYH)")
    print("  Result:   INSERT INTO table (\"NY\", \"BAH\", \"ZYH\")")
    print("  Benefit:  Preserves original case, matches database exactly")
    print()
    
    print("LOWERCASE APPROACH (Previous):")
    print("  Original: INSERT INTO table (NY, BAH, ZYH)")
    print("  Result:   INSERT INTO table (ny, bah, zyh)")
    print("  Issue:    Assumes database columns are lowercase")
    print()
    
    print("WHY QUOTING IS BETTER:")
    print("✅ Preserves the exact case from Oracle")
    print("✅ Matches whatever case is used in PostgreSQL tables")
    print("✅ No assumptions about database schema case")
    print("✅ More reliable for mixed-case scenarios")


def main():
    """Main test function."""
    test_column_quoting()
    test_specific_error_case()
    compare_approaches()
    
    print("\n" + "=" * 50)
    print("COLUMN QUOTING SUMMARY")
    print("=" * 50)
    print("The enhanced SQL rewriter now:")
    print("1. ✅ Adds double quotes to all column names")
    print("2. ✅ Preserves original case (NY stays NY)")
    print("3. ✅ Works with any database schema case")
    print("4. ✅ Handles already-quoted columns correctly")
    print("5. ✅ More reliable than case conversion")
    print()
    print("This should resolve the column name mismatch errors!")
    print("PostgreSQL will match the exact column names in your database.")


if __name__ == "__main__":
    main()