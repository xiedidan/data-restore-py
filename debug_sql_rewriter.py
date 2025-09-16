#!/usr/bin/env python3
"""
Debug the SQL rewriter step by step.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.sql_rewriter import SQLRewriter
from oracle_to_postgres.common.logger import Logger

def debug_rewriter():
    """Debug the SQL rewriter step by step."""
    
    logger = Logger()
    rewriter = SQLRewriter("ORACLE_DB", "postgres_db", "public", logger)
    
    # Test the problematic statement
    test_statement = "INSERT INTO EMR_HIS.V_HIS_PATIENT VALUES ('0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-2022 16:50:00');"
    
    print("Debugging SQL Rewriter")
    print("=" * 50)
    print(f"Original: {test_statement}")
    print()
    
    # Apply rules one by one to see what happens
    current = test_statement
    
    for i, rule in enumerate(rewriter.rewrite_rules):
        import re
        old_current = current
        current = re.sub(rule.pattern, rule.replacement, current, flags=rule.flags)
        
        if old_current != current:
            print(f"Rule {i+1}: {rule.description}")
            print(f"  Pattern: {rule.pattern}")
            print(f"  Before:  {old_current}")
            print(f"  After:   {current}")
            print()
    
    print("Final result:")
    print(current)
    
    # Also test the full rewrite method
    print("\nUsing full rewrite_insert_statement method:")
    full_result = rewriter.rewrite_insert_statement(test_statement)
    print(full_result)

if __name__ == "__main__":
    debug_rewriter()