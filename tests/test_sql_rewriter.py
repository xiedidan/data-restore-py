"""
Tests for SQL rewriting functionality.
"""

import pytest
import tempfile
import os
from oracle_to_postgres.common.sql_rewriter import (
    SQLRewriter, BatchSQLRewriter, PostgreSQLCompatibilityChecker, RewriteRule
)
from oracle_to_postgres.common.logger import Logger


class TestSQLRewriter:
    """Test cases for SQLRewriter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.rewriter = SQLRewriter(
            source_db="ORCL",
            target_db="postgres_db", 
            target_schema="public"
        )
    
    def test_database_name_replacement(self):
        """Test database name replacement in SQL statements."""
        sql = "INSERT INTO ORCL.users (id, name) VALUES (1, 'test')"
        result = self.rewriter.rewrite_insert_statement(sql)
        assert '"postgres_db".' in result
        assert 'ORCL.' not in result
    
    def test_schema_qualification(self):
        """Test schema qualification for table references."""
        sql = "INSERT INTO users (id, name) VALUES (1, 'test')"
        result = self.rewriter.rewrite_insert_statement(sql)
        assert '"public"."users"' in result
    
    def test_oracle_date_conversion(self):
        """Test Oracle TO_DATE conversion to PostgreSQL."""
        sql = "INSERT INTO events (id, created_date) VALUES (1, TO_DATE('2023-01-01', 'YYYY-MM-DD'))"
        result = self.rewriter.rewrite_insert_statement(sql)
        assert "TO_DATE" not in result
        assert "'2023-01-01'::timestamp" in result
    
    def test_sysdate_conversion(self):
        """Test Oracle SYSDATE conversion to PostgreSQL NOW()."""
        sql = "INSERT INTO logs (id, timestamp) VALUES (1, SYSDATE)"
        result = self.rewriter.rewrite_insert_statement(sql)
        assert "SYSDATE" not in result
        assert "NOW()" in result
    
    def test_dual_table_removal(self):
        """Test Oracle DUAL table reference removal."""
        sql = "SELECT 1 FROM DUAL"
        result = self.rewriter._apply_general_rules(sql)
        assert "FROM DUAL" not in result
    
    def test_sequence_nextval_conversion(self):
        """Test Oracle sequence NEXTVAL conversion."""
        sql = "INSERT INTO users (id, name) VALUES (user_seq.NEXTVAL, 'test')"
        result = self.rewriter.rewrite_insert_statement(sql)
        assert "user_seq.NEXTVAL" not in result
        assert "nextval('user_seq')" in result
    
    def test_nvl_to_coalesce(self):
        """Test Oracle NVL to PostgreSQL COALESCE conversion."""
        sql = "SELECT NVL(name, 'Unknown') FROM users"
        result = self.rewriter._apply_general_rules(sql)
        assert "NVL(" not in result
        assert "COALESCE(" in result
    
    def test_data_type_conversions(self):
        """Test Oracle data type conversions."""
        test_cases = [
            ("VARCHAR2(100)", "VARCHAR(100)"),
            ("NUMBER(10,2)", "NUMERIC(10,2)"),
            ("CLOB", "TEXT"),
            ("BLOB", "BYTEA")
        ]
        
        for oracle_type, postgres_type in test_cases:
            sql = f"CREATE TABLE test (col {oracle_type})"
            result = self.rewriter._apply_general_rules(sql)
            assert postgres_type in result
            assert oracle_type not in result
    
    def test_empty_string_handling(self):
        """Test Oracle empty string to NULL conversion."""
        sql = "SELECT * FROM users WHERE name = ''"
        result = self.rewriter._apply_general_rules(sql)
        assert "= ''" not in result
        assert "IS NULL" in result
    
    def test_rownum_to_limit(self):
        """Test Oracle ROWNUM to PostgreSQL LIMIT conversion."""
        sql = "SELECT * FROM users WHERE ROWNUM <= 10"
        result = self.rewriter._apply_general_rules(sql)
        assert "ROWNUM" not in result
        assert "LIMIT 10" in result
    
    def test_complex_insert_statement(self):
        """Test complex INSERT statement rewriting."""
        sql = """INSERT INTO ORCL.user_logs (
            id, user_id, action, created_date, status
        ) VALUES (
            log_seq.NEXTVAL, 
            123, 
            'LOGIN', 
            TO_DATE('2023-01-01 10:30:00', 'YYYY-MM-DD HH24:MI:SS'),
            NVL(?, 'ACTIVE')
        )"""
        
        result = self.rewriter.rewrite_insert_statement(sql)
        
        # Check all transformations
        assert '"postgres_db".' in result
        assert '"public"."user_logs"' in result
        assert "nextval('log_seq')" in result
        assert "'2023-01-01 10:30:00'::timestamp" in result
        assert "COALESCE(?, 'ACTIVE')" in result
    
    def test_sql_statement_splitting(self):
        """Test SQL statement splitting functionality."""
        content = """
        INSERT INTO users (id, name) VALUES (1, 'John');
        INSERT INTO users (id, name) VALUES (2, 'Jane');
        UPDATE users SET status = 'active' WHERE id = 1;
        """
        
        statements = self.rewriter._split_sql_statements(content)
        assert len(statements) == 3
        assert all("INSERT" in stmt or "UPDATE" in stmt for stmt in statements)
    
    def test_custom_rule_addition(self):
        """Test adding custom rewrite rules."""
        self.rewriter.add_custom_rule(
            pattern=r'\bCUSTOM_FUNC\b',
            replacement='pg_custom_func',
            description="Custom function replacement"
        )
        
        sql = "SELECT CUSTOM_FUNC(col) FROM table"
        result = self.rewriter._apply_general_rules(sql)
        assert "CUSTOM_FUNC" not in result
        assert "pg_custom_func" in result
    
    def test_rewrite_statistics(self):
        """Test rewrite statistics tracking."""
        self.rewriter.reset_statistics()
        
        sql = "INSERT INTO ORCL.users (id, created) VALUES (1, SYSDATE)"
        self.rewriter.rewrite_insert_statement(sql)
        
        stats = self.rewriter.get_rewrite_statistics()
        assert len(stats) > 0
        assert any("database name" in desc for desc in stats.keys())
        assert any("SYSDATE" in desc for desc in stats.keys())


class TestBatchSQLRewriter:
    """Test cases for BatchSQLRewriter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.batch_rewriter = BatchSQLRewriter(
            source_db="ORCL",
            target_db="postgres_db",
            target_schema="public"
        )
    
    def test_batch_file_rewriting(self):
        """Test batch file rewriting functionality."""
        # Create temporary files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Source file
            source_file = os.path.join(temp_dir, "source.sql")
            with open(source_file, 'w', encoding='utf-8') as f:
                f.write("INSERT INTO ORCL.users (id, name) VALUES (1, 'test');")
            
            # Target file
            target_file = os.path.join(temp_dir, "target.sql")
            
            # Rewrite files
            file_mappings = [(source_file, target_file, 'utf-8')]
            results = self.batch_rewriter.rewrite_files(file_mappings)
            
            assert len(results) == 1
            assert results[0] is True
            
            # Check target file content
            with open(target_file, 'r', encoding='utf-8') as f:
                content = f.read()
                assert '"postgres_db".' in content
                assert '"public"."users"' in content
    
    def test_combined_statistics(self):
        """Test combined statistics from batch rewriting."""
        with tempfile.TemporaryDirectory() as temp_dir:
            source_file = os.path.join(temp_dir, "source.sql")
            with open(source_file, 'w', encoding='utf-8') as f:
                f.write("INSERT INTO ORCL.users (id, created) VALUES (1, SYSDATE);")
            
            target_file = os.path.join(temp_dir, "target.sql")
            file_mappings = [(source_file, target_file, 'utf-8')]
            
            self.batch_rewriter.rewrite_files(file_mappings)
            stats = self.batch_rewriter.get_combined_statistics()
            
            assert 'rewrite_rules_applied' in stats
            assert 'total_transformations' in stats
            assert 'rules_used' in stats
            assert stats['total_transformations'] > 0


class TestPostgreSQLCompatibilityChecker:
    """Test cases for PostgreSQLCompatibilityChecker class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.checker = PostgreSQLCompatibilityChecker()
    
    def test_connect_by_detection(self):
        """Test detection of Oracle CONNECT BY queries."""
        sql = """
        SELECT level, employee_id, manager_id, last_name
        FROM employees
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id
        """
        
        issues = self.checker.check_compatibility(sql)
        connect_by_issues = [i for i in issues if 'CONNECT BY' in i['issue']]
        start_with_issues = [i for i in issues if 'START WITH' in i['issue']]
        
        assert len(connect_by_issues) > 0
        assert len(start_with_issues) > 0
    
    def test_rowid_detection(self):
        """Test detection of Oracle ROWID usage."""
        sql = "SELECT ROWID, * FROM users WHERE ROWID = 'AAABBBaaaBBBaaa'"
        
        issues = self.checker.check_compatibility(sql)
        rowid_issues = [i for i in issues if 'ROWID' in i['issue']]
        
        assert len(rowid_issues) > 0
        assert 'ctid' in rowid_issues[0]['suggestion']
    
    def test_decode_function_detection(self):
        """Test detection of Oracle DECODE function."""
        sql = "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM users"
        
        issues = self.checker.check_compatibility(sql)
        decode_issues = [i for i in issues if 'DECODE' in i['issue']]
        
        assert len(decode_issues) > 0
        assert 'CASE WHEN' in decode_issues[0]['suggestion']
    
    def test_multiple_issues_detection(self):
        """Test detection of multiple compatibility issues."""
        sql = """
        SELECT ROWID, DECODE(status, 'A', 'Active') as status_desc
        FROM users 
        WHERE ROWNUM <= 10
        START WITH parent_id IS NULL
        CONNECT BY PRIOR id = parent_id
        """
        
        issues = self.checker.check_compatibility(sql)
        
        # Should detect multiple different issues
        issue_types = set(issue['matched_text'].upper() for issue in issues)
        assert len(issue_types) >= 4  # ROWID, DECODE, ROWNUM, START WITH, CONNECT BY
    
    def test_compatibility_report_generation(self):
        """Test compatibility report generation."""
        sql = "SELECT ROWID FROM users WHERE ROWNUM <= 5"
        
        issues = self.checker.check_compatibility(sql)
        report = self.checker.generate_compatibility_report(issues)
        
        assert "PostgreSQL Compatibility Issues Found" in report
        assert "ROWID" in report
        assert "ROWNUM" in report
        assert "Suggestion:" in report
    
    def test_no_issues_report(self):
        """Test report when no compatibility issues are found."""
        sql = "SELECT id, name FROM users WHERE status = 'active' LIMIT 10"
        
        issues = self.checker.check_compatibility(sql)
        report = self.checker.generate_compatibility_report(issues)
        
        assert "No compatibility issues found" in report


class TestRewriteRule:
    """Test cases for RewriteRule dataclass."""
    
    def test_rewrite_rule_creation(self):
        """Test RewriteRule creation and attributes."""
        rule = RewriteRule(
            pattern=r'\bTEST\b',
            replacement='REPLACEMENT',
            description='Test rule'
        )
        
        assert rule.pattern == r'\bTEST\b'
        assert rule.replacement == 'REPLACEMENT'
        assert rule.description == 'Test rule'
        assert rule.flags == 2  # re.IGNORECASE


if __name__ == '__main__':
    pytest.main([__file__])