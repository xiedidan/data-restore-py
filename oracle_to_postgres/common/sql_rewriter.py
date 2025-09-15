"""
SQL statement rewriting utilities for Oracle to PostgreSQL migration.
"""

import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import os

from .logger import Logger
from .encoding_detector import EncodingConverter


@dataclass
class RewriteRule:
    """Rule for SQL rewriting."""
    pattern: str
    replacement: str
    description: str
    flags: int = re.IGNORECASE


class SQLRewriter:
    """SQL statement rewriter for Oracle to PostgreSQL conversion."""
    
    def __init__(self, source_db: str, target_db: str, target_schema: str = "public",
                 logger: Optional[Logger] = None):
        """
        Initialize SQL rewriter.
        
        Args:
            source_db: Source database name (Oracle)
            target_db: Target database name (PostgreSQL)
            target_schema: Target schema name
            logger: Optional logger instance
        """
        self.source_db = source_db
        self.target_db = target_db
        self.target_schema = target_schema
        self.logger = logger or Logger()
        
        # Initialize rewrite rules
        self.rewrite_rules = self._initialize_rewrite_rules()
        
        # Statistics
        self.rewrite_stats = {}
    
    def _initialize_rewrite_rules(self) -> List[RewriteRule]:
        """Initialize SQL rewrite rules."""
        rules = []
        
        # Database name replacement
        rules.append(RewriteRule(
            pattern=rf'\b{re.escape(self.source_db)}\.',
            replacement=f'"{self.target_db}".',
            description="Replace source database name with target database name"
        ))
        
        # Schema-qualified table references
        rules.append(RewriteRule(
            pattern=r'INSERT\s+INTO\s+(\w+)\.(\w+)',
            replacement=rf'INSERT INTO "{self.target_schema}"."\2"',
            description="Add schema qualification to INSERT statements"
        ))
        
        # Oracle-specific date formats
        rules.append(RewriteRule(
            pattern=r"TO_DATE\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
            replacement=r"'\1'::timestamp",
            description="Convert Oracle TO_DATE to PostgreSQL timestamp cast"
        ))
        
        # Oracle SYSDATE to PostgreSQL NOW()
        rules.append(RewriteRule(
            pattern=r'\bSYSDATE\b',
            replacement='NOW()',
            description="Replace Oracle SYSDATE with PostgreSQL NOW()"
        ))
        
        # Oracle DUAL table
        rules.append(RewriteRule(
            pattern=r'\bFROM\s+DUAL\b',
            replacement='',
            description="Remove Oracle DUAL table references"
        ))
        
        # Oracle sequence NEXTVAL
        rules.append(RewriteRule(
            pattern=r'(\w+)\.NEXTVAL',
            replacement=r"nextval('\1')",
            description="Convert Oracle sequence NEXTVAL to PostgreSQL function"
        ))
        
        # Oracle NVL to PostgreSQL COALESCE
        rules.append(RewriteRule(
            pattern=r'\bNVL\s*\(',
            replacement='COALESCE(',
            description="Replace Oracle NVL with PostgreSQL COALESCE"
        ))
        
        # Oracle DECODE to PostgreSQL CASE
        # This is a simplified conversion - complex DECODE statements may need manual review
        rules.append(RewriteRule(
            pattern=r'\bDECODE\s*\(',
            replacement='CASE ',
            description="Convert Oracle DECODE to PostgreSQL CASE (may need manual review)"
        ))
        
        # Oracle VARCHAR2 to PostgreSQL VARCHAR
        rules.append(RewriteRule(
            pattern=r'\bVARCHAR2\b',
            replacement='VARCHAR',
            description="Replace Oracle VARCHAR2 with PostgreSQL VARCHAR"
        ))
        
        # Oracle NUMBER to PostgreSQL NUMERIC
        rules.append(RewriteRule(
            pattern=r'\bNUMBER\b',
            replacement='NUMERIC',
            description="Replace Oracle NUMBER with PostgreSQL NUMERIC"
        ))
        
        # Oracle CLOB to PostgreSQL TEXT
        rules.append(RewriteRule(
            pattern=r'\bCLOB\b',
            replacement='TEXT',
            description="Replace Oracle CLOB with PostgreSQL TEXT"
        ))
        
        # Oracle BLOB to PostgreSQL BYTEA
        rules.append(RewriteRule(
            pattern=r'\bBLOB\b',
            replacement='BYTEA',
            description="Replace Oracle BLOB with PostgreSQL BYTEA"
        ))
        
        # Oracle empty string handling (Oracle treats '' as NULL)
        rules.append(RewriteRule(
            pattern=r"=\s*''",
            replacement="IS NULL",
            description="Convert Oracle empty string comparison to NULL check"
        ))
        
        # Oracle ROWNUM to PostgreSQL LIMIT (simplified)
        rules.append(RewriteRule(
            pattern=r'\bROWNUM\s*<=?\s*(\d+)',
            replacement=r'LIMIT \1',
            description="Convert Oracle ROWNUM to PostgreSQL LIMIT (simplified)"
        ))
        
        return rules
    
    def rewrite_insert_statement(self, statement: str) -> str:
        """
        Rewrite a single INSERT statement.
        
        Args:
            statement: Original INSERT statement
            
        Returns:
            Rewritten INSERT statement
        """
        rewritten = statement
        
        # Apply all rewrite rules
        for rule in self.rewrite_rules:
            old_statement = rewritten
            rewritten = re.sub(rule.pattern, rule.replacement, rewritten, flags=rule.flags)
            
            # Track statistics
            if old_statement != rewritten:
                self.rewrite_stats[rule.description] = self.rewrite_stats.get(rule.description, 0) + 1
        
        # Additional specific processing for INSERT statements
        rewritten = self._process_insert_specific(rewritten)
        
        return rewritten
    
    def _process_insert_specific(self, statement: str) -> str:
        """Process INSERT-specific transformations."""
        # Ensure proper schema qualification
        insert_pattern = r'INSERT\s+INTO\s+(?!")[^.\s]+(?!["])'
        
        def add_schema(match):
            table_ref = match.group(0)
            # Extract table name
            table_name = table_ref.split()[-1]  # Get the last word (table name)
            return f'INSERT INTO "{self.target_schema}"."{table_name}"'
        
        statement = re.sub(insert_pattern, add_schema, statement, flags=re.IGNORECASE)
        
        return statement
    
    def rewrite_sql_file(self, source_file: str, target_file: str, 
                        source_encoding: str, target_encoding: str = 'utf-8') -> bool:
        """
        Rewrite an entire SQL file.
        
        Args:
            source_file: Path to source SQL file
            target_file: Path to target SQL file
            source_encoding: Source file encoding
            target_encoding: Target file encoding
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read source file
            with open(source_file, 'r', encoding=source_encoding) as f:
                content = f.read()
            
            # Rewrite content
            rewritten_content = self.rewrite_sql_content(content)
            
            # Convert encoding if needed
            if source_encoding != target_encoding:
                # The content is already in Unicode, just write with target encoding
                pass
            
            # Write target file
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            with open(target_file, 'w', encoding=target_encoding) as f:
                f.write(rewritten_content)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error rewriting SQL file {source_file}: {str(e)}")
            return False
    
    def rewrite_sql_content(self, content: str) -> str:
        """
        Rewrite SQL content.
        
        Args:
            content: Original SQL content
            
        Returns:
            Rewritten SQL content
        """
        # Split content into statements
        statements = self._split_sql_statements(content)
        
        # Rewrite each statement
        rewritten_statements = []
        for statement in statements:
            if statement.strip():
                if self._is_insert_statement(statement):
                    rewritten = self.rewrite_insert_statement(statement)
                else:
                    # Apply general rules to non-INSERT statements
                    rewritten = self._apply_general_rules(statement)
                
                rewritten_statements.append(rewritten)
        
        return '\n'.join(rewritten_statements)
    
    def _split_sql_statements(self, content: str) -> List[str]:
        """Split SQL content into individual statements."""
        statements = []
        current_statement = ""
        in_string = False
        escape_next = False
        
        for char in content:
            if escape_next:
                current_statement += char
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                current_statement += char
                continue
            
            if char == "'" and not escape_next:
                in_string = not in_string
            
            current_statement += char
            
            if char == ';' and not in_string:
                if current_statement.strip():
                    statements.append(current_statement.strip())
                current_statement = ""
        
        # Add remaining statement if any
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return statements
    
    def _is_insert_statement(self, statement: str) -> bool:
        """Check if statement is an INSERT statement."""
        return re.match(r'^\s*INSERT\s+INTO', statement, re.IGNORECASE) is not None
    
    def _apply_general_rules(self, statement: str) -> str:
        """Apply general rewrite rules to any statement."""
        rewritten = statement
        
        # Apply rules that are not INSERT-specific
        general_rules = [rule for rule in self.rewrite_rules 
                        if "INSERT" not in rule.description]
        
        for rule in general_rules:
            old_statement = rewritten
            rewritten = re.sub(rule.pattern, rule.replacement, rewritten, flags=rule.flags)
            
            # Track statistics
            if old_statement != rewritten:
                self.rewrite_stats[rule.description] = self.rewrite_stats.get(rule.description, 0) + 1
        
        return rewritten
    
    def add_custom_rule(self, pattern: str, replacement: str, description: str) -> None:
        """
        Add a custom rewrite rule.
        
        Args:
            pattern: Regex pattern to match
            replacement: Replacement string
            description: Description of the rule
        """
        rule = RewriteRule(
            pattern=pattern,
            replacement=replacement,
            description=description
        )
        self.rewrite_rules.append(rule)
        self.logger.info(f"Added custom rewrite rule: {description}")
    
    def get_rewrite_statistics(self) -> Dict[str, int]:
        """Get rewrite statistics."""
        return dict(self.rewrite_stats)
    
    def reset_statistics(self) -> None:
        """Reset rewrite statistics."""
        self.rewrite_stats.clear()


class BatchSQLRewriter:
    """Batch SQL file rewriter with encoding conversion."""
    
    def __init__(self, source_db: str, target_db: str, target_schema: str = "public",
                 logger: Optional[Logger] = None):
        """Initialize batch rewriter."""
        self.sql_rewriter = SQLRewriter(source_db, target_db, target_schema, logger)
        self.encoding_converter = EncodingConverter()
        self.logger = logger or Logger()
    
    def rewrite_files(self, file_mappings: List[Tuple[str, str, str]], 
                     target_encoding: str = 'utf-8') -> List[bool]:
        """
        Rewrite multiple SQL files.
        
        Args:
            file_mappings: List of (source_file, target_file, source_encoding) tuples
            target_encoding: Target encoding for output files
            
        Returns:
            List of success flags for each file
        """
        results = []
        
        for source_file, target_file, source_encoding in file_mappings:
            self.logger.debug(f"Rewriting {source_file} -> {target_file}")
            
            success = self.sql_rewriter.rewrite_sql_file(
                source_file, target_file, source_encoding, target_encoding
            )
            results.append(success)
            
            if success:
                self.logger.debug(f"✓ Successfully rewrote {source_file}")
            else:
                self.logger.error(f"✗ Failed to rewrite {source_file}")
        
        return results
    
    def get_combined_statistics(self) -> Dict[str, any]:
        """Get combined rewrite statistics."""
        rewrite_stats = self.sql_rewriter.get_rewrite_statistics()
        
        return {
            'rewrite_rules_applied': rewrite_stats,
            'total_transformations': sum(rewrite_stats.values()),
            'rules_used': len([k for k, v in rewrite_stats.items() if v > 0])
        }


class PostgreSQLCompatibilityChecker:
    """Checker for PostgreSQL compatibility issues."""
    
    def __init__(self, logger: Optional[Logger] = None):
        """Initialize compatibility checker."""
        self.logger = logger or Logger()
        
        # Define compatibility issues to check
        self.compatibility_checks = [
            {
                'pattern': r'\bCONNECT\s+BY\b',
                'issue': 'Oracle CONNECT BY hierarchical queries not supported',
                'suggestion': 'Use WITH RECURSIVE for hierarchical queries'
            },
            {
                'pattern': r'\bSTART\s+WITH\b',
                'issue': 'Oracle START WITH clause not supported',
                'suggestion': 'Use WITH RECURSIVE for hierarchical queries'
            },
            {
                'pattern': r'\bROWID\b',
                'issue': 'Oracle ROWID not available in PostgreSQL',
                'suggestion': 'Use ctid or add a serial primary key'
            },
            {
                'pattern': r'\bROWNUM\b',
                'issue': 'Oracle ROWNUM not available in PostgreSQL',
                'suggestion': 'Use LIMIT, OFFSET, or ROW_NUMBER() window function'
            },
            {
                'pattern': r'\bDECODE\s*\(',
                'issue': 'Oracle DECODE function not available',
                'suggestion': 'Use CASE WHEN statements'
            },
            {
                'pattern': r'\bNVL2\s*\(',
                'issue': 'Oracle NVL2 function not available',
                'suggestion': 'Use CASE WHEN or nested COALESCE'
            },
            {
                'pattern': r'\bTO_CHAR\s*\(',
                'issue': 'Oracle TO_CHAR may have different behavior',
                'suggestion': 'Review date/number formatting functions'
            }
        ]
    
    def check_compatibility(self, sql_content: str) -> List[Dict[str, any]]:
        """
        Check SQL content for PostgreSQL compatibility issues.
        
        Args:
            sql_content: SQL content to check
            
        Returns:
            List of compatibility issues found
        """
        issues = []
        
        for check in self.compatibility_checks:
            matches = re.finditer(check['pattern'], sql_content, re.IGNORECASE)
            
            for match in matches:
                # Find line number
                line_num = sql_content[:match.start()].count('\n') + 1
                
                issues.append({
                    'line': line_num,
                    'position': match.start(),
                    'matched_text': match.group(0),
                    'issue': check['issue'],
                    'suggestion': check['suggestion']
                })
        
        return issues
    
    def generate_compatibility_report(self, issues: List[Dict[str, any]]) -> str:
        """Generate a compatibility report."""
        if not issues:
            return "No compatibility issues found."
        
        report = f"PostgreSQL Compatibility Issues Found: {len(issues)}\n"
        report += "=" * 50 + "\n\n"
        
        for i, issue in enumerate(issues, 1):
            report += f"{i}. Line {issue['line']}: {issue['matched_text']}\n"
            report += f"   Issue: {issue['issue']}\n"
            report += f"   Suggestion: {issue['suggestion']}\n\n"
        
        return report