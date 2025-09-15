"""
PostgreSQL database connection and management utilities.
"""

import psycopg2
import psycopg2.pool
from psycopg2 import sql
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from dataclasses import dataclass
import time

from .logger import Logger
from .error_handler import ErrorHandler, ErrorContext, ErrorType


@dataclass
class ConnectionInfo:
    """PostgreSQL connection information."""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "public"
    
    def get_dsn(self) -> str:
        """Get database connection string."""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.username} password={self.password}"


@dataclass
class ExecutionResult:
    """Result of SQL execution."""
    success: bool
    affected_rows: int = 0
    execution_time: float = 0.0
    error_message: str = ""
    sql_statement: str = ""


class DatabaseManager:
    """PostgreSQL database connection and operation manager."""
    
    def __init__(self, connection_info: ConnectionInfo, 
                 pool_size: int = 5, logger: Optional[Logger] = None):
        """
        Initialize database manager.
        
        Args:
            connection_info: Database connection information
            pool_size: Connection pool size
            logger: Optional logger instance
        """
        self.connection_info = connection_info
        self.pool_size = pool_size
        self.logger = logger or Logger()
        self.error_handler = ErrorHandler(logger=self.logger)
        
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._connection_tested = False
    
    def initialize_pool(self) -> None:
        """Initialize connection pool."""
        try:
            self.logger.info("Initializing PostgreSQL connection pool...")
            
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size,
                dsn=self.connection_info.get_dsn(),
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            
            self.logger.info(f"✓ Connection pool initialized (size: {self.pool_size})")
            
        except Exception as e:
            self.error_handler.handle_db_error(e, "initialize_pool")
            raise
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info("Testing PostgreSQL connection...")
            
            # Create a temporary connection for testing
            conn = psycopg2.connect(self.connection_info.get_dsn())
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                self.logger.info(f"✓ Connected to PostgreSQL: {version}")
            
            conn.close()
            self._connection_tested = True
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Connection test failed: {str(e)}")
            self.error_handler.handle_db_error(e, "test_connection")
            return False
    
    @contextmanager
    def get_connection(self):
        """
        Get database connection from pool.
        
        Yields:
            Database connection
        """
        if not self._pool:
            self.initialize_pool()
        
        conn = None
        try:
            conn = self._pool.getconn()
            if conn:
                yield conn
            else:
                raise Exception("Failed to get connection from pool")
        except Exception as e:
            self.error_handler.handle_db_error(e, "get_connection")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)
    
    def execute_sql(self, sql_statement: str, parameters: Optional[Tuple] = None,
                   fetch_results: bool = False) -> ExecutionResult:
        """
        Execute SQL statement.
        
        Args:
            sql_statement: SQL statement to execute
            parameters: Optional parameters for parameterized queries
            fetch_results: Whether to fetch and return results
            
        Returns:
            ExecutionResult with execution details
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_statement, parameters)
                    
                    affected_rows = cursor.rowcount
                    results = None
                    
                    if fetch_results and cursor.description:
                        results = cursor.fetchall()
                    
                    conn.commit()
                    
                    execution_time = time.time() - start_time
                    
                    return ExecutionResult(
                        success=True,
                        affected_rows=affected_rows,
                        execution_time=execution_time,
                        sql_statement=sql_statement[:200] + "..." if len(sql_statement) > 200 else sql_statement
                    )
        
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            
            self.error_handler.handle_db_error(e, sql_statement)
            
            return ExecutionResult(
                success=False,
                execution_time=execution_time,
                error_message=error_msg,
                sql_statement=sql_statement[:200] + "..." if len(sql_statement) > 200 else sql_statement
            )
    
    def execute_ddl(self, ddl_statement: str) -> ExecutionResult:
        """
        Execute DDL statement (CREATE, DROP, ALTER).
        
        Args:
            ddl_statement: DDL statement to execute
            
        Returns:
            ExecutionResult with execution details
        """
        self.logger.debug(f"Executing DDL: {ddl_statement[:100]}...")
        return self.execute_sql(ddl_statement)
    
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if table exists.
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if None)
            
        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.connection_info.schema
        
        check_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(check_sql, (schema, table_name))
                    return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error checking table existence: {str(e)}")
            return False
    
    def drop_table(self, table_name: str, schema: str = None, 
                  if_exists: bool = True) -> ExecutionResult:
        """
        Drop table.
        
        Args:
            table_name: Name of the table to drop
            schema: Schema name (uses default if None)
            if_exists: Whether to use IF EXISTS clause
            
        Returns:
            ExecutionResult with execution details
        """
        schema = schema or self.connection_info.schema
        
        if if_exists:
            drop_sql = f'DROP TABLE IF EXISTS "{schema}"."{table_name}";'
        else:
            drop_sql = f'DROP TABLE "{schema}"."{table_name}";'
        
        self.logger.debug(f"Dropping table: {schema}.{table_name}")
        return self.execute_ddl(drop_sql)
    
    def get_table_info(self, table_name: str, schema: str = None) -> Dict[str, Any]:
        """
        Get table information.
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if None)
            
        Returns:
            Dictionary with table information
        """
        schema = schema or self.connection_info.schema
        
        info_sql = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(info_sql, (schema, table_name))
                    columns = cursor.fetchall()
                    
                    return {
                        'schema': schema,
                        'table_name': table_name,
                        'columns': [dict(col) for col in columns],
                        'column_count': len(columns)
                    }
        except Exception as e:
            self.logger.error(f"Error getting table info: {str(e)}")
            return {}
    
    def list_tables(self, schema: str = None) -> List[str]:
        """
        List tables in schema.
        
        Args:
            schema: Schema name (uses default if None)
            
        Returns:
            List of table names
        """
        schema = schema or self.connection_info.schema
        
        list_sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(list_sql, (schema,))
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error listing tables: {str(e)}")
            return []
    
    def execute_batch(self, sql_statements: List[str], 
                     stop_on_error: bool = False) -> List[ExecutionResult]:
        """
        Execute multiple SQL statements.
        
        Args:
            sql_statements: List of SQL statements to execute
            stop_on_error: Whether to stop execution on first error
            
        Returns:
            List of ExecutionResult objects
        """
        results = []
        
        for i, statement in enumerate(sql_statements, 1):
            self.logger.debug(f"Executing statement {i}/{len(sql_statements)}")
            
            result = self.execute_sql(statement)
            results.append(result)
            
            if not result.success and stop_on_error:
                self.logger.error(f"Stopping batch execution due to error in statement {i}")
                break
        
        return results
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get database information.
        
        Returns:
            Dictionary with database information
        """
        info_queries = {
            'version': "SELECT version();",
            'current_database': "SELECT current_database();",
            'current_schema': "SELECT current_schema();",
            'current_user': "SELECT current_user;",
            'server_encoding': "SHOW server_encoding;",
            'client_encoding': "SHOW client_encoding;"
        }
        
        info = {}
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for key, query in info_queries.items():
                        try:
                            cursor.execute(query)
                            info[key] = cursor.fetchone()[0]
                        except Exception as e:
                            info[key] = f"Error: {str(e)}"
        except Exception as e:
            self.logger.error(f"Error getting database info: {str(e)}")
            info['error'] = str(e)
        
        return info
    
    def close_pool(self) -> None:
        """Close connection pool."""
        if self._pool:
            self.logger.info("Closing connection pool...")
            self._pool.closeall()
            self._pool = None
            self.logger.info("✓ Connection pool closed")
    
    def __enter__(self):
        """Context manager entry."""
        if not self._connection_tested:
            if not self.test_connection():
                raise Exception("Database connection test failed")
        
        if not self._pool:
            self.initialize_pool()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_pool()


class DDLExecutor:
    """Specialized executor for DDL operations."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize DDL executor."""
        self.db_manager = db_manager
        self.logger = db_manager.logger
    
    def create_table_from_file(self, ddl_file_path: str, 
                              drop_if_exists: bool = False) -> ExecutionResult:
        """
        Create table from DDL file.
        
        Args:
            ddl_file_path: Path to DDL file
            drop_if_exists: Whether to drop table if it exists
            
        Returns:
            ExecutionResult with execution details
        """
        try:
            # Read DDL content
            with open(ddl_file_path, 'r', encoding='utf-8') as f:
                ddl_content = f.read().strip()
            
            if not ddl_content:
                return ExecutionResult(
                    success=False,
                    error_message="DDL file is empty",
                    sql_statement=ddl_file_path
                )
            
            # Extract table name for drop operation
            if drop_if_exists:
                table_name = self._extract_table_name_from_ddl(ddl_content)
                if table_name:
                    self.logger.debug(f"Dropping existing table: {table_name}")
                    drop_result = self.db_manager.drop_table(table_name)
                    if not drop_result.success:
                        self.logger.warning(f"Failed to drop table {table_name}: {drop_result.error_message}")
            
            # Execute DDL
            return self.db_manager.execute_ddl(ddl_content)
            
        except Exception as e:
            return ExecutionResult(
                success=False,
                error_message=str(e),
                sql_statement=ddl_file_path
            )
    
    def _extract_table_name_from_ddl(self, ddl_content: str) -> Optional[str]:
        """Extract table name from DDL content."""
        import re
        
        # Simple regex to extract table name from CREATE TABLE statement
        match = re.search(r'CREATE\s+TABLE\s+(?:"?(\w+)"?\.)?"?(\w+)"?', ddl_content, re.IGNORECASE)
        if match:
            return match.group(2)  # Return table name (group 2)
        
        return None