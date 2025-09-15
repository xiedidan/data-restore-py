"""
Minimal test for parallel importer classes.
"""

from dataclasses import dataclass
from typing import Optional, List

@dataclass
class ImportTask:
    """Single file import task."""
    file_path: str
    table_name: str
    encoding: str
    target_encoding: str = 'utf-8'

print("ImportTask defined successfully")

if __name__ == "__main__":
    task = ImportTask(
        file_path="/test.sql",
        table_name="test_table", 
        encoding="utf-8"
    )
    print(f"Task created: {task}")