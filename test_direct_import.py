#!/usr/bin/env python3

import sys
import os

# Add current directory to path
sys.path.insert(0, os.getcwd())

print("Testing direct import...")

# Try to import the module directly
try:
    import oracle_to_postgres.common.parallel_importer as pi
    print("Module imported successfully")
    
    # Check what's available
    attrs = [x for x in dir(pi) if not x.startswith('_')]
    print(f"Available attributes: {attrs}")
    
    # Try to access ImportTask directly
    if hasattr(pi, 'ImportTask'):
        print("ImportTask found!")
        task_class = pi.ImportTask
        print(f"ImportTask class: {task_class}")
        
        # Try to create an instance
        task = task_class(
            file_path="/test.sql",
            table_name="test_table",
            encoding="utf-8"
        )
        print(f"Task created: {task}")
    else:
        print("ImportTask NOT found")
        
        # Let's see what's actually in the module
        print("Module contents:")
        for name in dir(pi):
            if not name.startswith('_'):
                obj = getattr(pi, name)
                print(f"  {name}: {type(obj)} = {obj}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()