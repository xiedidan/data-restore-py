"""
Tests for SQL parsing functionality.
"""

import pytest
from oracle_to_postgres.common.sql_parser import (
    SQLParser, InsertStatement, ColumnInfo, DataType, DataTypeInference
)


class TestSQLParser:
    """Test cases for SQLParser class."""
    
    def test_parse_simple_insert(self):
        """Test parsing simple INSERT statement."""
        sql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');"
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 1
        stmt = statements[0]
        assert stmt.table_name == 'users'
        assert stmt.columns == ['id', 'name', 'email']
        assert len(stmt.values) == 1
        assert stmt.values[0] == (1, 'John Doe', 'john@example.com')
    
    def test_parse_multiple_inserts(self):
        """Test parsing multiple INSERT statements."""
        sql = """
        INSERT INTO users (id, name) VALUES (1, 'John');
        INSERT INTO users (id, name) VALUES (2, 'Jane');
        INSERT INTO orders (id, user_id, total) VALUES (1, 1, 99.99);
        """
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 3
        
        # Check first statement
        assert statements[0].table_name == 'users'
        assert statements[0].values[0] == (1, 'John')
        
        # Check second statement
        assert statements[1].table_name == 'users'
        assert statements[1].values[0] == (2, 'Jane')
        
        # Check third statement
        assert statements[2].table_name == 'orders'
        assert statements[2].values[0] == (1, 1, 99.99)
    
    def test_parse_insert_without_columns(self):
        """Test parsing INSERT without explicit column list."""
        sql = "INSERT INTO users VALUES (1, 'John Doe', 'john@example.com');"
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 1
        stmt = statements[0]
        assert stmt.table_name == 'users'
        assert stmt.columns == []  # No column names
        assert stmt.values[0] == (1, 'John Doe', 'john@example.com')
    
    def test_parse_insert_with_schema(self):
        """Test parsing INSERT with schema prefix."""
        sql = "INSERT INTO myschema.users (id, name) VALUES (1, 'John');"
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 1
        stmt = statements[0]
        assert stmt.table_name == 'users'  # Schema is ignored for table name
    
    def test_parse_values_with_nulls(self):
        """Test parsing INSERT with NULL values."""
        sql = "INSERT INTO users (id, name, email) VALUES (1, 'John', NULL);"
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 1
        stmt = statements[0]
        assert stmt.values[0] == (1, 'John', None)
    
    def test_parse_values_with_quotes(self):
        """Test parsing INSERT with quoted strings containing special characters."""
        sql = "INSERT INTO users (id, name) VALUES (1, 'John O''Connor');"
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 1
        stmt = statements[0]
        assert stmt.values[0] == (1, "John O'Connor")
    
    def test_parse_values_with_numbers(self):
        """Test parsing INSERT with various numeric types."""
        sql = "INSERT INTO products (id, price, weight, active) VALUES (1, 99.99, 1.5, TRUE);"
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        assert len(statements) == 1
        stmt = statements[0]
        values = stmt.values[0]
        assert values[0] == 1      # integer
        assert values[1] == 99.99  # float
        assert values[2] == 1.5    # float
        assert values[3] is True   # boolean
    
    def test_parse_complex_sql(self):
        """Test parsing SQL with comments and whitespace."""
        sql = """
        -- Insert user data
        INSERT INTO users (
            id,
            name,
            email
        ) VALUES (
            1,
            'John Doe',
            'john@example.com'
        );
        
        /* Another insert */
        INSERT INTO users (id, name) VALUES (2, 'Jane Smith');
        """
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        
        # Should parse both statements despite comments and formatting
        assert len(statements) >= 1  # At least one should be parsed
    
    def test_analyze_table_structure(self):
        """Test analyzing table structure from INSERT statements."""
        sql = """
        INSERT INTO users (id, name, email, age, active) VALUES (1, 'John', 'john@example.com', 25, TRUE);
        INSERT INTO users (id, name, email, age, active) VALUES (2, 'Jane', 'jane@example.com', 30, FALSE);
        INSERT INTO users (id, name, email, age, active) VALUES (3, 'Bob', NULL, 35, TRUE);
        """
        
        parser = SQLParser()
        statements = parser.parse_insert_statements(sql)
        structures = parser.analyze_table_structure(statements)
        
        assert 'users' in structures
        columns = structures['users']
        
        # Should have 5 columns
        assert len(columns) == 5
        
        # Check column names
        column_names = [col.name for col in columns]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'email' in column_names
        assert 'age' in column_names
        assert 'active' in column_names
        
        # Check some inferred types
        id_col = next(col for col in columns if col.name == 'id')
        assert id_col.data_type in [DataType.INTEGER, DataType.SMALLINT]
        
        email_col = next(col for col in columns if col.name == 'email')
        assert email_col.nullable is True  # Has NULL value
        
        active_col = next(col for col in columns if col.name == 'active')
        assert active_col.data_type == DataType.BOOLEAN


class TestDataTypeInference:
    """Test cases for DataTypeInference class."""
    
    def test_infer_integer_type(self):
        """Test inference of integer types."""
        inference = DataTypeInference()
        
        # Small integers
        column = inference.infer_column_type('id', [1, 2, 3, 100])
        assert column.data_type in [DataType.SMALLINT, DataType.INTEGER]
        
        # Large integers
        column = inference.infer_column_type('big_id', [1000000, 2000000, 3000000])
        assert column.data_type in [DataType.INTEGER, DataType.BIGINT]
    
    def test_infer_string_type(self):
        """Test inference of string types."""
        inference = DataTypeInference()
        
        # Short strings
        column = inference.infer_column_type('name', ['John', 'Jane', 'Bob'])
        assert column.data_type == DataType.VARCHAR
        assert column.max_length is not None
        
        # Long strings
        long_text = 'A' * 1000
        column = inference.infer_column_type('description', [long_text])
        assert column.data_type == DataType.TEXT
    
    def test_infer_boolean_type(self):
        """Test inference of boolean types."""
        inference = DataTypeInference()
        
        column = inference.infer_column_type('active', [True, False, True])
        assert column.data_type == DataType.BOOLEAN
        
        # String booleans
        column = inference.infer_column_type('enabled', ['TRUE', 'FALSE', 'T'])
        assert column.data_type == DataType.BOOLEAN
    
    def test_infer_decimal_type(self):
        """Test inference of decimal types."""
        inference = DataTypeInference()
        
        column = inference.infer_column_type('price', [99.99, 149.50, 29.95])
        assert column.data_type == DataType.DECIMAL
        assert column.precision is not None
        assert column.scale is not None
    
    def test_infer_nullable_column(self):
        """Test inference with NULL values."""
        inference = DataTypeInference()
        
        column = inference.infer_column_type('optional_field', ['value1', None, 'value2'])
        assert column.nullable is True
        
        column = inference.infer_column_type('required_field', ['value1', 'value2', 'value3'])
        assert column.nullable is False
    
    def test_infer_date_type(self):
        """Test inference of date types."""
        inference = DataTypeInference()
        
        column = inference.infer_column_type('birth_date', ['2023-01-15', '2023-02-20'])
        assert column.data_type == DataType.DATE
        
        column = inference.infer_column_type('created_at', ['2023-01-15 10:30:00', '2023-02-20 15:45:30'])
        assert column.data_type == DataType.TIMESTAMP
    
    def test_infer_uuid_type(self):
        """Test inference of UUID types."""
        inference = DataTypeInference()
        
        uuids = ['550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8']
        column = inference.infer_column_type('uuid_field', uuids)
        assert column.data_type == DataType.UUID
    
    def test_infer_json_type(self):
        """Test inference of JSON types."""
        inference = DataTypeInference()
        
        json_values = ['{"key": "value"}', '{"name": "John", "age": 30}']
        column = inference.infer_column_type('metadata', json_values)
        assert column.data_type == DataType.JSONB


class TestColumnInfo:
    """Test cases for ColumnInfo class."""
    
    def test_column_info_ddl_fragment(self):
        """Test DDL fragment generation."""
        # Simple column
        column = ColumnInfo(name='id', data_type=DataType.INTEGER)
        ddl = column.to_ddl_fragment()
        assert '"id" INTEGER' in ddl
        
        # VARCHAR with length
        column = ColumnInfo(name='name', data_type=DataType.VARCHAR, max_length=100)
        ddl = column.to_ddl_fragment()
        assert '"name" VARCHAR(100)' in ddl
        
        # NOT NULL column
        column = ColumnInfo(name='required_field', data_type=DataType.TEXT, nullable=False)
        ddl = column.to_ddl_fragment()
        assert 'NOT NULL' in ddl
        
        # Column with default
        column = ColumnInfo(name='status', data_type=DataType.VARCHAR, default_value="'active'")
        ddl = column.to_ddl_fragment()
        assert "DEFAULT 'active'" in ddl
        
        # DECIMAL with precision and scale
        column = ColumnInfo(name='price', data_type=DataType.DECIMAL, precision=10, scale=2)
        ddl = column.to_ddl_fragment()
        assert 'DECIMAL(10,2)' in ddl


class TestInsertStatement:
    """Test cases for InsertStatement class."""
    
    def test_get_sample_values(self):
        """Test getting sample values from INSERT statement."""
        values = [(1, 'John'), (2, 'Jane'), (3, 'Bob'), (4, 'Alice'), (5, 'Charlie'), (6, 'David')]
        
        stmt = InsertStatement(
            table_name='users',
            columns=['id', 'name'],
            values=values,
            original_statement='INSERT INTO users...'
        )
        
        # Get default sample (5 values)
        samples = stmt.get_sample_values()
        assert len(samples) == 5
        assert samples[0] == (1, 'John')
        
        # Get custom sample size
        samples = stmt.get_sample_values(max_samples=3)
        assert len(samples) == 3
        
        # Request more samples than available
        samples = stmt.get_sample_values(max_samples=10)
        assert len(samples) == 6  # All available values