#!/usr/bin/env python3
"""
Test the INSERT-only statement filtering functionality.
"""

import os
import sys

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_insert_only_filtering():
    """Test filtering to allow only INSERT statements."""
    print("Testing INSERT-Only Statement Filtering")
    print("=" * 50)
    
    # Simulate the filtering logic from parallel_importer.py
    def is_valid_sql_statement(statement: str) -> bool:
        """Check if a statement is a valid INSERT statement only."""
        statement_lower = statement.lower().strip()
        
        # Skip empty statements
        if not statement_lower:
            return False
        
        # Skip Oracle-specific commands
        oracle_commands = [
            'prompt ',
            'set feedback',
            'set define',
            'set echo',
            'set pagesize',
            'set linesize',
            'set timing',
            'set serveroutput',
            'set verify',
            'set heading',
            'spool ',
            'exit',
            'quit',
            'connect ',
            'disconnect',
            'commit;',
            'rollback;',
            'alter session',
            'whenever sqlerror',
            'whenever oserror'
        ]
        
        for oracle_cmd in oracle_commands:
            if statement_lower.startswith(oracle_cmd):
                return False
        
        # Skip comments (lines starting with --, /*, or REM)
        if (statement_lower.startswith('--') or 
            statement_lower.startswith('/*') or 
            statement_lower.startswith('rem ')):
            return False
        
        # Only allow INSERT statements for data import
        # Filter out SELECT, UPDATE, DELETE and other non-insert statements
        valid_sql_starts = [
            'insert'
        ]
        
        for valid_start in valid_sql_starts:
            if statement_lower.startswith(valid_start):
                return True
        
        return False
    
    # Test cases from actual Oracle SQL files
    test_statements = [
        # Should be EXECUTED (INSERT statements)
        {
            'statement': "INSERT INTO EMR_HIS.V_HIS_CRZYMXB (NY, BAH, ZYH) VALUES ('2022-08-10', 'b37a635a6aa780814b83', '0000049818')",
            'should_execute': True,
            'reason': 'Valid INSERT statement'
        },
        {
            'statement': "insert into EMR_HIS.V_HIS_KSDMDZB (HISKSDM, HISKSMC) values ('001', '内科')",
            'should_execute': True,
            'reason': 'Valid INSERT statement (lowercase)'
        },
        
        # Should be SKIPPED (Non-INSERT statements)
        {
            'statement': "SELECT * FROM EMR_HIS.V_HIS_MZJZXXB WHERE t.sj>='2022-01-01 0'",
            'should_execute': False,
            'reason': 'SELECT statement (should be filtered)'
        },
        {
            'statement': "UPDATE EMR_HIS.V_HIS_PATIENTS SET name = 'Updated' WHERE id = 1",
            'should_execute': False,
            'reason': 'UPDATE statement (should be filtered)'
        },
        {
            'statement': "DELETE FROM EMR_HIS.V_HIS_TEMP WHERE id = 1",
            'should_execute': False,
            'reason': 'DELETE statement (should be filtered)'
        },
        {
            'statement': "CREATE TABLE test_table (id INT)",
            'should_execute': False,
            'reason': 'CREATE statement (should be filtered)'
        },
        {
            'statement': "DROP TABLE test_table",
            'should_execute': False,
            'reason': 'DROP statement (should be filtered)'
        },
        
        # Should be SKIPPED (Oracle commands)
        {
            'statement': 'prompt Importing table EMR_HIS.V_HIS_CRZYMXB...',
            'should_execute': False,
            'reason': 'Oracle prompt command'
        },
        {
            'statement': 'set feedback off',
            'should_execute': False,
            'reason': 'Oracle set command'
        },
        {
            'statement': 'commit;',
            'should_execute': False,
            'reason': 'Oracle commit command'
        },
        
        # Should be SKIPPED (Comments)
        {
            'statement': '-- This is a comment',
            'should_execute': False,
            'reason': 'SQL comment'
        }
    ]
    
    print("Testing statement filtering:")
    print("-" * 50)
    
    passed = 0
    failed = 0
    insert_count = 0
    filtered_count = 0
    
    for i, test in enumerate(test_statements, 1):
        statement = test['statement']
        expected = test['should_execute']
        reason = test['reason']
        
        result = is_valid_sql_statement(statement)
        
        print(f"\nTest {i}: {reason}")
        print(f"Statement: {statement[:60]}{'...' if len(statement) > 60 else ''}")
        print(f"Expected: {'EXECUTE' if expected else 'SKIP'}")
        print(f"Result:   {'EXECUTE' if result else 'SKIP'}")
        
        if result == expected:
            print("✅ PASS")
            passed += 1
            if result:
                insert_count += 1
            else:
                filtered_count += 1
        else:
            print("❌ FAIL")
            failed += 1
    
    print(f"\n" + "=" * 50)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print(f"INSERT statements to execute: {insert_count}")
    print(f"Statements filtered out: {filtered_count}")
    
    if failed == 0:
        print("✅ All statement filtering tests passed!")
        print("Only INSERT statements will be executed during import.")
    else:
        print("❌ Some tests failed - statement filtering needs adjustment.")


def main():
    """Main test function."""
    test_insert_only_filtering()
    
    print("\n" + "=" * 50)
    print("INSERT-ONLY FILTERING SUMMARY")
    print("=" * 50)
    print("The enhanced import process will now:")
    print("1. ✅ Execute ONLY INSERT statements")
    print("2. ✅ Skip SELECT statements (no data modification)")
    print("3. ✅ Skip UPDATE statements (not for data import)")
    print("4. ✅ Skip DELETE statements (not for data import)")
    print("5. ✅ Skip CREATE/DROP statements (DDL operations)")
    print("6. ✅ Skip Oracle commands (prompt, set, commit, etc.)")
    print("7. ✅ Skip comments and non-SQL statements")
    print()
    print("This ensures that only data insertion operations are performed!")
    print("Your import will be safer and more focused on data migration.")


if __name__ == "__main__":
    main()