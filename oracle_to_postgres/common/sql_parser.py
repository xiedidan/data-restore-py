"""
SQL statement parsing utilities for Oracle to PostgreSQL migration tool.
"""

import re
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum


class DataType(Enum):
    """Enumeration of PostgreSQL data types."""
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    TEXT = "TEXT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    TIME = "TIME"
    BOOLEAN = "BOOLEAN"
    UUID = "UUID"
    JSON = "JSON"
    JSONB = "JSONB"
    BYTEA = "BYTEA"


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: DataType
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    default_value: Optional[str] = None
    
    def to_ddl_fragment(self) -> str:
        """Convert column info to DDL fragment."""
        ddl = f'"{self.name}" {self.data_type.value}'
        
        # Add length/precision/scale
        if self.data_type in [DataType.VARCHAR, DataType.CHAR] and self.max_length:
            ddl += f"({self.max_length})"
        elif self.data_type in [DataType.DECIMAL, DataType.NUMERIC]:
            if self.precision and self.scale is not None:
                ddl += f"({self.precision},{self.scale})"
            elif self.precision:
                ddl += f"({self.precision})"
        
        # Add constraints
        if not self.nullable:
            ddl += " NOT NULL"
        
        if self.default_value:
            ddl += f" DEFAULT {self.default_value}"
        
        return ddl


@dataclass
class InsertStatement:
    """Parsed INSERT statement information."""
    table_name: str
    columns: List[str]
    values: List[Tuple[Any, ...]]
    original_statement: str
    
    def get_sample_values(self, max_samples: int = 5) -> List[Tuple[Any, ...]]:
        """Get sample values for analysis."""
        return self.values[:max_samples]


class SQLParser:
    """Parser for Oracle SQL INSERT statements."""
    
    # Regex patterns for parsing INSERT statements
    INSERT_PATTERN = re.compile(
        r'INSERT\s+INTO\s+(?:(\w+)\.)?(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)',
        re.IGNORECASE | re.DOTALL
    )
    
    INSERT_SIMPLE_PATTERN = re.compile(
        r'INSERT\s+INTO\s+(?:(\w+)\.)?(\w+)\s+VALUES\s*\(([^)]+)\)',
        re.IGNORECASE | re.DOTALL
    )
    
    # Pattern for quoted strings (handles escaped quotes)
    QUOTED_STRING_PATTERN = re.compile(r"'(?:[^'\\]|\\.)*'")
    
    def __init__(self):
        """Initialize SQL parser."""
        self.type_inference = DataTypeInference()
    
    def parse_insert_statements(self, sql_content: str, encoding: str = 'utf-8') -> List[InsertStatement]:
        """
        Parse INSERT statements from SQL content.
        
        Args:
            sql_content: SQL file content
            encoding: File encoding (for proper string handling)
            
        Returns:
            List of parsed InsertStatement objects
        """
        statements = []
        
        # Split content into individual statements
        sql_statements = self._split_sql_statements(sql_content)
        
        for statement in sql_statements:
            parsed = self._parse_single_insert(statement.strip())
            if parsed:
                statements.append(parsed)
        
        return statements
    
    def _split_sql_statements(self, content: str) -> List[str]:
        """Split SQL content into individual statements."""
        # Simple split by semicolon (could be improved for complex cases)
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
    
    def _parse_single_insert(self, statement: str) -> Optional[InsertStatement]:
        """Parse a single INSERT statement."""
        # Try pattern with column list
        match = self.INSERT_PATTERN.match(statement)
        if match:
            schema, table, columns_str, values_str = match.groups()
            table_name = table
            columns = [col.strip().strip('"\'') for col in columns_str.split(',')]
            values = self._parse_values(values_str)
            
            return InsertStatement(
                table_name=table_name,
                columns=columns,
                values=[values] if values else [],
                original_statement=statement
            )
        
        # Try simple pattern without column list
        match = self.INSERT_SIMPLE_PATTERN.match(statement)
        if match:
            schema, table, values_str = match.groups()
            table_name = table
            values = self._parse_values(values_str)
            
            return InsertStatement(
                table_name=table_name,
                columns=[],  # No column names available
                values=[values] if values else [],
                original_statement=statement
            )
        
        return None
    
    def _parse_values(self, values_str: str) -> Optional[Tuple[Any, ...]]:
        """Parse VALUES clause into tuple of values."""
        try:
            # Split by comma, but respect quoted strings
            values = []
            current_value = ""
            in_string = False
            escape_next = False
            paren_depth = 0
            
            for char in values_str:
                if escape_next:
                    current_value += char
                    escape_next = False
                    continue
                
                if char == '\\':
                    escape_next = True
                    current_value += char
                    continue
                
                if char == "'" and not escape_next:
                    in_string = not in_string
                
                if not in_string:
                    if char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
                    elif char == ',' and paren_depth == 0:
                        values.append(self._parse_single_value(current_value.strip()))
                        current_value = ""
                        continue
                
                current_value += char
            
            # Add the last value
            if current_value.strip():
                values.append(self._parse_single_value(current_value.strip()))
            
            return tuple(values)
        
        except Exception:
            return None
    
    def _parse_single_value(self, value_str: str) -> Any:
        """Parse a single value from string representation."""
        value_str = value_str.strip()
        
        # NULL values
        if value_str.upper() in ('NULL', 'NONE'):
            return None
        
        # String values (quoted)
        if value_str.startswith("'") and value_str.endswith("'"):
            # Remove quotes and handle escaped quotes
            return value_str[1:-1].replace("''", "'").replace("\\'", "'")
        
        # Numeric values
        try:
            # Try integer first
            if '.' not in value_str and 'e' not in value_str.lower():
                return int(value_str)
            else:
                return float(value_str)
        except ValueError:
            pass
        
        # Boolean values
        if value_str.upper() in ('TRUE', 'FALSE', 'T', 'F', '1', '0'):
            return value_str.upper() in ('TRUE', 'T', '1')
        
        # Date/timestamp patterns (basic detection)
        if self._looks_like_date(value_str):
            return value_str  # Keep as string for now
        
        # Default to string
        return value_str
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if value looks like a date/timestamp."""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value):
                return True
        
        return False
    
    def analyze_table_structure(self, statements: List[InsertStatement]) -> Dict[str, List[ColumnInfo]]:
        """
        Analyze INSERT statements to infer table structure.
        
        Args:
            statements: List of parsed INSERT statements
            
        Returns:
            Dictionary mapping table names to column information
        """
        table_structures = {}
        
        # Group statements by table
        tables = {}
        for stmt in statements:
            if stmt.table_name not in tables:
                tables[stmt.table_name] = []
            tables[stmt.table_name].append(stmt)
        
        # Analyze each table
        for table_name, table_statements in tables.items():
            columns = self._analyze_table_columns(table_statements)
            table_structures[table_name] = columns
        
        return table_structures
    
    def _analyze_table_columns(self, statements: List[InsertStatement]) -> List[ColumnInfo]:
        """Analyze columns for a specific table."""
        # Collect all column information
        column_data = {}
        
        for stmt in statements:
            if not stmt.columns:
                # Skip statements without column names
                continue
            
            for i, col_name in enumerate(stmt.columns):
                if col_name not in column_data:
                    column_data[col_name] = {
                        'values': [],
                        'position': i
                    }
                
                # Collect values for this column
                for value_tuple in stmt.values:
                    if i < len(value_tuple):
                        column_data[col_name]['values'].append(value_tuple[i])
        
        # Infer column types and constraints
        columns = []
        for col_name, data in column_data.items():
            column_info = self.type_inference.infer_column_type(
                col_name, data['values']
            )
            columns.append(column_info)
        
        # Sort by position to maintain column order
        columns.sort(key=lambda c: column_data[c.name]['position'])
        
        return columns


class DataTypeInference:
    """Utility for inferring PostgreSQL data types from sample values."""
    
    def infer_column_type(self, column_name: str, values: List[Any]) -> ColumnInfo:
        """
        Infer column type from sample values.
        
        Args:
            column_name: Name of the column
            values: List of sample values
            
        Returns:
            ColumnInfo with inferred type and constraints
        """
        # Filter out None values for type analysis
        non_null_values = [v for v in values if v is not None]
        nullable = len(non_null_values) < len(values)
        
        if not non_null_values:
            # All values are NULL, default to TEXT
            return ColumnInfo(
                name=column_name,
                data_type=DataType.TEXT,
                nullable=True
            )
        
        # Analyze value types
        type_counts = {}
        max_length = 0
        max_precision = 0
        max_scale = 0
        
        for value in non_null_values:
            value_type = self._classify_value_type(value)
            type_counts[value_type] = type_counts.get(value_type, 0) + 1
            
            # Track string lengths
            if isinstance(value, str):
                max_length = max(max_length, len(value))
            
            # Track numeric precision/scale
            if isinstance(value, (int, float)):
                precision, scale = self._get_numeric_precision_scale(value)
                max_precision = max(max_precision, precision)
                max_scale = max(max_scale, scale)
        
        # Determine the most appropriate type
        data_type = self._select_best_type(type_counts, max_length, max_precision, max_scale)
        
        # Create column info
        column_info = ColumnInfo(
            name=column_name,
            data_type=data_type,
            nullable=nullable
        )
        
        # Set type-specific parameters
        if data_type in [DataType.VARCHAR, DataType.CHAR]:
            # Add some buffer to max length
            column_info.max_length = min(max_length + 50, 65535) if max_length > 0 else 255
        elif data_type in [DataType.DECIMAL, DataType.NUMERIC]:
            column_info.precision = max_precision
            column_info.scale = max_scale
        
        return column_info
    
    def _classify_value_type(self, value: Any) -> str:
        """Classify a single value's type."""
        if value is None:
            return 'null'
        
        if isinstance(value, bool):
            return 'boolean'
        
        if isinstance(value, int):
            if -32768 <= value <= 32767:
                return 'smallint'
            elif -2147483648 <= value <= 2147483647:
                return 'integer'
            else:
                return 'bigint'
        
        if isinstance(value, float):
            return 'decimal'
        
        if isinstance(value, str):
            # Check for specific string patterns
            if self._looks_like_uuid(value):
                return 'uuid'
            elif self._looks_like_json(value):
                return 'json'
            elif self._looks_like_date(value):
                return 'date'
            elif self._looks_like_timestamp(value):
                return 'timestamp'
            elif self._looks_like_boolean_string(value):
                return 'boolean'
            else:
                return 'text'
        
        return 'text'  # Default fallback
    
    def _looks_like_uuid(self, value: str) -> bool:
        """Check if string looks like a UUID."""
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, value, re.IGNORECASE))
    
    def _looks_like_json(self, value: str) -> bool:
        """Check if string looks like JSON."""
        value = value.strip()
        return (value.startswith('{') and value.endswith('}')) or \
               (value.startswith('[') and value.endswith(']'))
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if string looks like a date."""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value):
                return True
        
        return False
    
    def _looks_like_timestamp(self, value: str) -> bool:
        """Check if string looks like a timestamp."""
        timestamp_patterns = [
            r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',    # ISO format
        ]
        
        for pattern in timestamp_patterns:
            if re.match(pattern, value):
                return True
        
        return False
    
    def _looks_like_boolean_string(self, value: str) -> bool:
        """Check if string represents a boolean value."""
        return value.upper() in ('TRUE', 'FALSE', 'T', 'F', 'YES', 'NO', 'Y', 'N')
    
    def _get_numeric_precision_scale(self, value: Any) -> Tuple[int, int]:
        """Get precision and scale for numeric value."""
        if isinstance(value, int):
            return len(str(abs(value))), 0
        
        if isinstance(value, float):
            str_value = str(value)
            if '.' in str_value:
                integer_part, decimal_part = str_value.split('.')
                return len(integer_part) + len(decimal_part), len(decimal_part)
            else:
                return len(str_value), 0
        
        return 10, 2  # Default
    
    def _select_best_type(self, type_counts: Dict[str, int], 
                         max_length: int, max_precision: int, max_scale: int) -> DataType:
        """Select the best PostgreSQL data type based on analysis."""
        if not type_counts:
            return DataType.TEXT
        
        # Get the most common type
        most_common_type = max(type_counts.keys(), key=lambda k: type_counts[k])
        
        # Type mapping
        type_mapping = {
            'boolean': DataType.BOOLEAN,
            'smallint': DataType.SMALLINT,
            'integer': DataType.INTEGER,
            'bigint': DataType.BIGINT,
            'decimal': DataType.DECIMAL,
            'uuid': DataType.UUID,
            'json': DataType.JSONB,
            'date': DataType.DATE,
            'timestamp': DataType.TIMESTAMP,
            'text': DataType.VARCHAR if max_length <= 255 else DataType.TEXT
        }
        
        return type_mapping.get(most_common_type, DataType.TEXT)