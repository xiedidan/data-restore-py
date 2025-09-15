#!/usr/bin/env python3

import sys
import traceback

print("Testing parallel_importer imports...")

try:
    print("1. Testing basic imports...")
    import os
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dataclasses import dataclass
    from typing import Dict, List, Optional, Tuple, Callable
    from queue import Queue
    import psycopg2
    from psycopg2.extras import execute_batch
    print("   Basic imports OK")
    
    print("2. Testing local imports...")
    from oracle_to_postgres.common.logger import Logger
    from oracle_to_postgres.common.database import DatabaseManager
    from oracle_to_postgres.common.sql_rewriter import SQLRewriter
    from oracle_to_postgres.common.encoding_detector import EncodingDetector
    print("   Local imports OK")
    
    print("3. Testing dataclass definition...")
    
    @dataclass
    class TestImportTask:
        """Single file import task."""
        file_path: str
        table_name: str
        encoding: str
        target_encoding: str = 'utf-8'
    
    print("   Dataclass definition OK")
    
    print("4. Testing module import...")
    import oracle_to_postgres.common.parallel_importer as pi
    print("   Module import OK")
    print("   Available in module:", [x for x in dir(pi) if not x.startswith('_')])
    
    print("5. Testing direct class import...")
    exec("from oracle_to_postgres.common.parallel_importer import ImportTask")
    print("   Direct import OK")
    
except Exception as e:
    print(f"Error at step: {e}")
    traceback.print_exc()