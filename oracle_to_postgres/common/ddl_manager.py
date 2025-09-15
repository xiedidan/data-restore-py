"""
DDL management and execution utilities for Oracle to PostgreSQL migration.
"""

import os
import glob
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re
import time

from .database import DatabaseManager, ExecutionResult, DDLExecutor
from .logger import Logger
from .error_handler import ErrorHandler


@dataclass
class DDLFile:
    """Information about a DDL file."""
    file_path: str
    file_name: str
    table_name: str
    file_size: int
    
    @classmethod
    def from_path(cls, file_path: str) -> 'DDLFile':
        """Create DDLFile from file path."""
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Extract table name from filename (remove create_ prefix and .sql suffix)
        table_name = file_name
        if table_name.startswith('create_'):
            table_name = table_name[7:]  # Remove 'create_' prefix
        if table_name.endswith('.sql'):
            table_name = table_name[:-4]  # Remove '.sql' suffix
        
        return cls(
            file_path=file_path,
            file_name=file_name,
            table_name=table_name,
            file_size=file_size
        )


@dataclass
class DDLExecutionResult:
    """Result of DDL execution."""
    table_name: str
    success: bool
    execution_time: float = 0.0
    error_message: str = ""
    ddl_file_path: str = ""
    table_created: bool = False
    table_dropped: bool = False


class DDLManager:
    """Manager for DDL file operations and execution."""
    
    def __init__(self, ddl_directory: str, db_manager: DatabaseManager, 
                 logger: Optional[Logger] = None):
        """
        Initialize DDL manager.
        
        Args:
            ddl_directory: Directory containing DDL files
            db_manager: Database manager instance
            logger: Optional logger instance
        """
        self.ddl_directory = ddl_directory
        self.db_manager = db_manager
        self.logger = logger or Logger()
        self.error_handler = ErrorHandler(logger=self.logger)
        self.ddl_executor = DDLExecutor(db_manager)
    
    def scan_ddl_files(self) -> List[DDLFile]:
        """
        Scan DDL directory for DDL files.
        
        Returns:
            List of DDLFile objects
        """
        if not os.path.exists(self.ddl_directory):
            self.logger.warning(f"DDL directory does not exist: {self.ddl_directory}")
            return []
        
        # Find all .sql files in DDL directory
        pattern = os.path.join(self.ddl_directory, "*.sql")
        sql_files = glob.glob(pattern)
        
        ddl_files = []
        for file_path in sql_files:
            try:
                ddl_file = DDLFile.from_path(file_path)
                ddl_files.append(ddl_file)
            except Exception as e:
                self.logger.warning(f"Error processing DDL file {file_path}: {str(e)}")
        
        # Sort by table name for consistent ordering
        ddl_files.sort(key=lambda f: f.table_name)
        
        return ddl_files
    
    def validate_ddl_files(self, ddl_files: List[DDLFile]) -> List[DDLFile]:
        """
        Validate DDL files for basic syntax and structure.
        
        Args:
            ddl_files: List of DDL files to validate
            
        Returns:
            List of valid DDL files
        """
        valid_files = []
        
        for ddl_file in ddl_files:
            try:
                if self._validate_single_ddl_file(ddl_file):
                    valid_files.append(ddl_file)
                else:
                    self.logger.warning(f"DDL file validation failed: {ddl_file.file_name}")
            except Exception as e:
                self.logger.error(f"Error validating DDL file {ddl_file.file_name}: {str(e)}")
        
        return valid_files
    
    def _validate_single_ddl_file(self, ddl_file: DDLFile) -> bool:
        """Validate a single DDL file."""
        try:
            with open(ddl_file.file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            if not content:
                self.logger.warning(f"DDL file is empty: {ddl_file.file_name}")
                return False
            
            # Basic validation - should contain CREATE TABLE
            if not re.search(r'CREATE\s+TABLE', content, re.IGNORECASE):
                self.logger.warning(f"DDL file does not contain CREATE TABLE: {ddl_file.file_name}")
                return False
            
            # Check for balanced parentheses
            open_parens = content.count('(')
            close_parens = content.count(')')
            if open_parens != close_parens:
                self.logger.warning(f"Unbalanced parentheses in DDL file: {ddl_file.file_name}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error reading DDL file {ddl_file.file_path}: {str(e)}")
            return False
    
    def analyze_dependencies(self, ddl_files: List[DDLFile]) -> List[DDLFile]:
        """
        Analyze and sort DDL files by dependencies.
        
        Args:
            ddl_files: List of DDL files to analyze
            
        Returns:
            List of DDL files sorted by dependency order
        """
        # For now, implement simple sorting by table name
        # In a more advanced implementation, we would parse foreign key references
        
        dependencies = {}
        
        for ddl_file in ddl_files:
            try:
                deps = self._extract_dependencies(ddl_file)
                dependencies[ddl_file.table_name] = deps
            except Exception as e:
                self.logger.warning(f"Error analyzing dependencies for {ddl_file.table_name}: {str(e)}")
                dependencies[ddl_file.table_name] = []
        
        # Topological sort (simplified)
        sorted_files = self._topological_sort(ddl_files, dependencies)
        
        return sorted_files
    
    def _extract_dependencies(self, ddl_file: DDLFile) -> List[str]:
        """Extract table dependencies from DDL file."""
        dependencies = []
        
        try:
            with open(ddl_file.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Look for REFERENCES clauses (foreign keys)
            references_pattern = r'REFERENCES\s+(?:"?(\w+)"?\.)?"?(\w+)"?'
            matches = re.findall(references_pattern, content, re.IGNORECASE)
            
            for match in matches:
                # match[1] is the table name (match[0] might be schema)
                referenced_table = match[1]
                if referenced_table and referenced_table != ddl_file.table_name:
                    dependencies.append(referenced_table)
            
        except Exception as e:
            self.logger.debug(f"Error extracting dependencies from {ddl_file.file_path}: {str(e)}")
        
        return list(set(dependencies))  # Remove duplicates
    
    def _topological_sort(self, ddl_files: List[DDLFile], 
                         dependencies: Dict[str, List[str]]) -> List[DDLFile]:
        """Perform topological sort of DDL files based on dependencies."""
        # Create mapping from table name to DDL file
        table_to_file = {f.table_name: f for f in ddl_files}
        
        # Simple topological sort implementation
        visited = set()
        temp_visited = set()
        result = []
        
        def visit(table_name: str):
            if table_name in temp_visited:
                # Circular dependency detected, skip
                return
            
            if table_name in visited:
                return
            
            temp_visited.add(table_name)
            
            # Visit dependencies first
            for dep in dependencies.get(table_name, []):
                if dep in table_to_file:
                    visit(dep)
            
            temp_visited.remove(table_name)
            visited.add(table_name)
            
            # Add to result if we have the DDL file
            if table_name in table_to_file:
                result.append(table_to_file[table_name])
        
        # Visit all tables
        for ddl_file in ddl_files:
            if ddl_file.table_name not in visited:
                visit(ddl_file.table_name)
        
        # Add any remaining files that weren't processed
        for ddl_file in ddl_files:
            if ddl_file not in result:
                result.append(ddl_file)
        
        return result
    
    def execute_ddl_files(self, ddl_files: List[DDLFile], 
                         drop_existing: bool = False,
                         stop_on_error: bool = False) -> List[DDLExecutionResult]:
        """
        Execute DDL files to create tables.
        
        Args:
            ddl_files: List of DDL files to execute
            drop_existing: Whether to drop existing tables
            stop_on_error: Whether to stop on first error
            
        Returns:
            List of DDLExecutionResult objects
        """
        results = []
        
        self.logger.info(f"Executing {len(ddl_files)} DDL files...")
        self.logger.info(f"Configuration: drop_existing={drop_existing}, stop_on_error={stop_on_error}")
        
        for i, ddl_file in enumerate(ddl_files, 1):
            self.logger.progress(i, len(ddl_files), f"Creating table {ddl_file.table_name}")
            
            result = self._execute_single_ddl(ddl_file, drop_existing)
            results.append(result)
            
            if not result.success:
                self.logger.error(f"✗ Failed to create table {ddl_file.table_name}: {result.error_message}")
                
                if stop_on_error:
                    self.logger.error("Stopping execution due to error")
                    break
            else:
                self.logger.debug(f"✓ Created table {ddl_file.table_name}")
        
        self.logger.progress_complete("DDL execution complete")
        
        return results
    
    def _execute_single_ddl(self, ddl_file: DDLFile, drop_existing: bool) -> DDLExecutionResult:
        """Execute a single DDL file."""
        start_time = time.time()
        
        result = DDLExecutionResult(
            table_name=ddl_file.table_name,
            success=False,
            ddl_file_path=ddl_file.file_path
        )
        
        try:
            # Check if table already exists
            table_exists = self.db_manager.table_exists(ddl_file.table_name)
            self.logger.debug(f"Table {ddl_file.table_name} exists: {table_exists}")
            
            # Debug: Show where the table actually exists
            if not table_exists:
                self._debug_table_location(ddl_file.table_name)
            
            if table_exists and drop_existing:
                # Drop existing table
                self.logger.info(f"Dropping existing table {ddl_file.table_name}")
                drop_result = self.db_manager.drop_table(ddl_file.table_name)
                result.table_dropped = drop_result.success
                
                if not drop_result.success:
                    result.error_message = f"Failed to drop existing table: {drop_result.error_message}"
                    return result
                else:
                    self.logger.info(f"✓ Dropped existing table {ddl_file.table_name}")
            elif table_exists and not drop_existing:
                self.logger.warning(f"Table {ddl_file.table_name} already exists, skipping (drop_existing=False)")
                result.error_message = "Table already exists (use --drop-existing to replace)"
                return result
            
            # Execute DDL
            execution_result = self.ddl_executor.create_table_from_file(
                ddl_file.file_path, 
                drop_if_exists=False  # We already handled dropping above
            )
            
            result.success = execution_result.success
            result.table_created = execution_result.success
            
            if not execution_result.success:
                result.error_message = execution_result.error_message
            
        except Exception as e:
            result.error_message = str(e)
            self.error_handler.handle_file_error(e, ddl_file.file_path, "execute_ddl")
        
        result.execution_time = time.time() - start_time
        return result
    
    def _debug_table_location(self, table_name: str) -> None:
        """Debug method to show where a table actually exists."""
        try:
            debug_sql = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_name = %s
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(debug_sql, (table_name,))
                    results = cursor.fetchall()
                    
                    if results:
                        self.logger.debug(f"Table {table_name} found in schemas:")
                        for row in results:
                            schema = self.db_manager._extract_value(row, 0)
                            name = self.db_manager._extract_value(row, 1)
                            self.logger.debug(f"  - {schema}.{name}")
                    else:
                        self.logger.debug(f"Table {table_name} not found in any schema")
        except Exception as e:
            self.logger.debug(f"Error checking table location: {e}")
    
    def get_execution_summary(self, results: List[DDLExecutionResult]) -> Dict[str, any]:
        """
        Get summary of DDL execution results.
        
        Args:
            results: List of execution results
            
        Returns:
            Dictionary with summary statistics
        """
        total_files = len(results)
        successful = sum(1 for r in results if r.success)
        failed = total_files - successful
        tables_created = sum(1 for r in results if r.table_created)
        tables_dropped = sum(1 for r in results if r.table_dropped)
        total_time = sum(r.execution_time for r in results)
        
        return {
            'total_files': total_files,
            'successful': successful,
            'failed': failed,
            'success_rate': (successful / total_files * 100) if total_files > 0 else 0,
            'tables_created': tables_created,
            'tables_dropped': tables_dropped,
            'total_execution_time': total_time,
            'average_execution_time': total_time / total_files if total_files > 0 else 0
        }
    
    def cleanup_failed_tables(self, results: List[DDLExecutionResult]) -> int:
        """
        Clean up tables that failed to create properly.
        
        Args:
            results: List of execution results
            
        Returns:
            Number of tables cleaned up
        """
        cleanup_count = 0
        
        for result in results:
            if not result.success and result.table_created:
                # Table was partially created but failed, try to clean up
                try:
                    drop_result = self.db_manager.drop_table(result.table_name, if_exists=True)
                    if drop_result.success:
                        cleanup_count += 1
                        self.logger.info(f"Cleaned up failed table: {result.table_name}")
                except Exception as e:
                    self.logger.warning(f"Failed to cleanup table {result.table_name}: {str(e)}")
        
        return cleanup_count