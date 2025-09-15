"""
Tests for database connection and management functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from oracle_to_postgres.common.database import (
    DatabaseManager, ConnectionInfo, ExecutionResult, DDLExecutor
)


class TestConnectionInfo:
    """Test cases for ConnectionInfo class."""
    
    def test_connection_info_creation(self):
        """Test ConnectionInfo creation."""
        conn_info = ConnectionInfo(
            host="localhost",
            port=5432,
            database="testdb",
            username="testuser",
            password="testpass",
            schema="public"
        )
        
        assert conn_info.host == "localhost"
        assert conn_info.port == 5432
        assert conn_info.database == "testdb"
        assert conn_info.username == "testuser"
        assert conn_info.password == "testpass"
        assert conn_info.schema == "public"
    
    def test_get_dsn(self):
        """Test DSN generation."""
        conn_info = ConnectionInfo(
            host="localhost",
            port=5432,
            database="testdb",
            username="testuser",
            password="testpass"
        )
        
        dsn = conn_info.get_dsn()
        
        assert "host=localhost" in dsn
        assert "port=5432" in dsn
        assert "dbname=testdb" in dsn
        assert "user=testuser" in dsn
        assert "password=testpass" in dsn


class TestExecutionResult:
    """Test cases for ExecutionResult class."""
    
    def test_successful_result(self):
        """Test successful execution result."""
        result = ExecutionResult(
            success=True,
            affected_rows=5,
            execution_time=1.5,
            sql_statement="SELECT * FROM users"
        )
        
        assert result.success is True
        assert result.affected_rows == 5
        assert result.execution_time == 1.5
        assert result.error_message == ""
        assert "SELECT" in result.sql_statement
    
    def test_failed_result(self):
        """Test failed execution result."""
        result = ExecutionResult(
            success=False,
            error_message="Table does not exist",
            sql_statement="SELECT * FROM nonexistent"
        )
        
        assert result.success is False
        assert result.affected_rows == 0
        assert result.error_message == "Table does not exist"


@patch('psycopg2.connect')
@patch('psycopg2.pool.ThreadedConnectionPool')
class TestDatabaseManager:
    """Test cases for DatabaseManager class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.conn_info = ConnectionInfo(
            host="localhost",
            port=5432,
            database="testdb",
            username="testuser",
            password="testpass"
        )
    
    def test_database_manager_initialization(self, mock_pool, mock_connect):
        """Test DatabaseManager initialization."""
        db_manager = DatabaseManager(self.conn_info, pool_size=3)
        
        assert db_manager.connection_info == self.conn_info
        assert db_manager.pool_size == 3
        assert db_manager._pool is None
        assert db_manager._connection_tested is False
    
    def test_test_connection_success(self, mock_pool, mock_connect):
        """Test successful connection test."""
        # Mock successful connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.0"]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        result = db_manager.test_connection()
        
        assert result is True
        assert db_manager._connection_tested is True
        mock_connect.assert_called_once()
        mock_conn.close.assert_called_once()
    
    def test_test_connection_failure(self, mock_pool, mock_connect):
        """Test failed connection test."""
        # Mock connection failure
        mock_connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager(self.conn_info)
        result = db_manager.test_connection()
        
        assert result is False
        assert db_manager._connection_tested is False
    
    def test_initialize_pool(self, mock_pool, mock_connect):
        """Test connection pool initialization."""
        db_manager = DatabaseManager(self.conn_info, pool_size=5)
        db_manager.initialize_pool()
        
        mock_pool.assert_called_once()
        assert db_manager._pool is not None
    
    def test_execute_sql_success(self, mock_pool, mock_connect):
        """Test successful SQL execution."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 3
        mock_cursor.description = None
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        result = db_manager.execute_sql("INSERT INTO users VALUES (1, 'John')")
        
        assert result.success is True
        assert result.affected_rows == 3
        assert result.execution_time > 0
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
    
    def test_execute_sql_failure(self, mock_pool, mock_connect):
        """Test failed SQL execution."""
        # Mock connection that raises exception
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("SQL error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        result = db_manager.execute_sql("INVALID SQL")
        
        assert result.success is False
        assert "SQL error" in result.error_message
        assert result.execution_time > 0
    
    def test_table_exists_true(self, mock_pool, mock_connect):
        """Test table existence check when table exists."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [True]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        exists = db_manager.table_exists("users")
        
        assert exists is True
        mock_cursor.execute.assert_called_once()
    
    def test_table_exists_false(self, mock_pool, mock_connect):
        """Test table existence check when table doesn't exist."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [False]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        exists = db_manager.table_exists("nonexistent")
        
        assert exists is False
    
    def test_drop_table(self, mock_pool, mock_connect):
        """Test table dropping."""
        # Mock successful execution
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        result = db_manager.drop_table("users", if_exists=True)
        
        assert result.success is True
        mock_cursor.execute.assert_called_once()
        
        # Check that IF EXISTS was used
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "IF EXISTS" in executed_sql
    
    def test_list_tables(self, mock_pool, mock_connect):
        """Test listing tables."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [["users"], ["orders"], ["products"]]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        tables = db_manager.list_tables()
        
        assert tables == ["users", "orders", "products"]
        mock_cursor.execute.assert_called_once()
    
    def test_execute_batch(self, mock_pool, mock_connect):
        """Test batch SQL execution."""
        # Mock successful execution
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock pool
        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        db_manager._pool = mock_pool_instance
        
        statements = [
            "INSERT INTO users VALUES (1, 'John')",
            "INSERT INTO users VALUES (2, 'Jane')"
        ]
        
        results = db_manager.execute_batch(statements)
        
        assert len(results) == 2
        assert all(r.success for r in results)
        assert mock_cursor.execute.call_count == 2
    
    def test_context_manager(self, mock_pool, mock_connect):
        """Test DatabaseManager as context manager."""
        # Mock successful connection test
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.0"]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        db_manager = DatabaseManager(self.conn_info)
        
        with db_manager as db:
            assert db is db_manager
            assert db._connection_tested is True
        
        # Pool should be closed after context exit
        # (We can't easily test this with mocks, but the method should be called)


class TestDDLExecutor:
    """Test cases for DDLExecutor class."""
    
    def test_ddl_executor_initialization(self):
        """Test DDLExecutor initialization."""
        mock_db_manager = Mock()
        executor = DDLExecutor(mock_db_manager)
        
        assert executor.db_manager is mock_db_manager
    
    def test_extract_table_name_from_ddl(self):
        """Test table name extraction from DDL."""
        mock_db_manager = Mock()
        executor = DDLExecutor(mock_db_manager)
        
        # Test simple CREATE TABLE
        ddl1 = "CREATE TABLE users (id INTEGER, name VARCHAR(100));"
        table_name1 = executor._extract_table_name_from_ddl(ddl1)
        assert table_name1 == "users"
        
        # Test CREATE TABLE with schema
        ddl2 = 'CREATE TABLE "public"."orders" (id INTEGER);'
        table_name2 = executor._extract_table_name_from_ddl(ddl2)
        assert table_name2 == "orders"
        
        # Test CREATE TABLE with quoted names
        ddl3 = 'CREATE TABLE "user_data" (id INTEGER);'
        table_name3 = executor._extract_table_name_from_ddl(ddl3)
        assert table_name3 == "user_data"
    
    @patch('builtins.open', create=True)
    def test_create_table_from_file_success(self, mock_open):
        """Test successful table creation from file."""
        # Mock file content
        ddl_content = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));"
        mock_open.return_value.__enter__.return_value.read.return_value = ddl_content
        
        # Mock database manager
        mock_db_manager = Mock()
        mock_db_manager.execute_ddl.return_value = ExecutionResult(success=True)
        
        executor = DDLExecutor(mock_db_manager)
        result = executor.create_table_from_file("/path/to/ddl.sql")
        
        assert result.success is True
        mock_db_manager.execute_ddl.assert_called_once_with(ddl_content)
    
    @patch('builtins.open', create=True)
    def test_create_table_from_file_with_drop(self, mock_open):
        """Test table creation with drop existing."""
        # Mock file content
        ddl_content = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
        mock_open.return_value.__enter__.return_value.read.return_value = ddl_content
        
        # Mock database manager
        mock_db_manager = Mock()
        mock_db_manager.drop_table.return_value = ExecutionResult(success=True)
        mock_db_manager.execute_ddl.return_value = ExecutionResult(success=True)
        
        executor = DDLExecutor(mock_db_manager)
        result = executor.create_table_from_file("/path/to/ddl.sql", drop_if_exists=True)
        
        assert result.success is True
        mock_db_manager.drop_table.assert_called_once_with("users")
        mock_db_manager.execute_ddl.assert_called_once_with(ddl_content)
    
    @patch('builtins.open', create=True)
    def test_create_table_from_empty_file(self, mock_open):
        """Test handling of empty DDL file."""
        # Mock empty file
        mock_open.return_value.__enter__.return_value.read.return_value = ""
        
        mock_db_manager = Mock()
        executor = DDLExecutor(mock_db_manager)
        
        result = executor.create_table_from_file("/path/to/empty.sql")
        
        assert result.success is False
        assert "empty" in result.error_message.lower()
    
    @patch('builtins.open', create=True)
    def test_create_table_file_not_found(self, mock_open):
        """Test handling of missing DDL file."""
        # Mock file not found
        mock_open.side_effect = FileNotFoundError("File not found")
        
        mock_db_manager = Mock()
        executor = DDLExecutor(mock_db_manager)
        
        result = executor.create_table_from_file("/path/to/missing.sql")
        
        assert result.success is False
        assert "not found" in result.error_message.lower()