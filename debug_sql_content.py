#!/usr/bin/env python3
"""
Debug the SQL content rewriting process.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.sql_rewriter import SQLRewriter
from oracle_to_postgres.common.logger import Logger

def debug_sql_content():
    """Debug the SQL content rewriting process."""
    
    logger = Logger()
    rewriter = SQLRewriter("ORACLE_DB", "postgres_db", "public", logger)
    
    # Test content with multiple statements
    test_content = """-- Test SQL with various Oracle date formats
INSERT INTO EMR_HIS.V_HIS_PATIENT VALUES ('0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-2022 16:50:00');
INSERT INTO EMR_HIS.V_HIS_DOCTOR VALUES ('DOC001', 'Dr. Smith', '01-01-2023');
COMMIT;"""
    
    print("Debugging SQL Content Rewriting")
    print("=" * 50)
    print("Original content:")
    print(test_content)
    print()
    
    # Split into statements
    statements = rewriter._split_sql_statements(test_content)
    print(f"Split into {len(statements)} statements:")
    for i, stmt in enumerate(statements, 1):
        print(f"{i}. '{stmt.strip()}'")
    print()
    
    # Process each statement
    rewritten_statements = []
    for i, statement in enumerate(statements, 1):
        if statement.strip():
            print(f"Processing statement {i}: {statement.strip()}")
            
            if rewriter._is_insert_statement(statement):
                print("  -> Identified as INSERT statement")
                rewritten = rewriter.rewrite_insert_statement(statement)
                print(f"  -> Rewritten: {rewritten}")
            else:
                print("  -> Not an INSERT statement, applying general rules")
                rewritten = rewriter._apply_general_rules(statement)
                print(f"  -> Rewritten: {rewritten}")
            
            rewritten_statements.append(rewritten)
            print()
    
    # Join back together
    final_result = '\n'.join(rewritten_statements)
    print("Final result:")
    print(final_result)
    
    # Compare with direct method call
    print("\nDirect rewrite_sql_content call:")
    direct_result = rewriter.rewrite_sql_content(test_content)
    print(direct_result)

if __name__ == "__main__":
    debug_sql_content()