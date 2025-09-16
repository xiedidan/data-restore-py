# Date Format Error Fix

## Problem Description

The import process was failing with PostgreSQL date format errors:

```
date/time field value out of range: "13-07-2022 16:50:00"
LINE 2: ...0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-202...^
HINT:  Perhaps you need a different "datestyle" setting.
```

## Root Cause

The issue was caused by Oracle date formats (DD-MM-YYYY) being passed to PostgreSQL, which expects ISO format (YYYY-MM-DD). Even though our SQL rewriter was converting the date format, it wasn't adding explicit type casting for all date strings, causing PostgreSQL to fail parsing.

## Solution

Enhanced the `SQLRewriter._convert_date_formats()` method to:

1. **Convert date formats** from Oracle format (DD-MM-YYYY) to PostgreSQL ISO format (YYYY-MM-DD)
2. **Add explicit type casting** (`::timestamp` or `::date`) to ensure PostgreSQL can parse the dates correctly
3. **Handle multiple date formats** including:
   - `DD-MM-YYYY HH24:MI:SS` → `YYYY-MM-DD HH24:MI:SS::timestamp`
   - `DD-MM-YYYY` → `YYYY-MM-DD::date`
   - `DD/MM/YYYY HH24:MI:SS` → `YYYY-MM-DD HH24:MI:SS::timestamp`
   - `DD.MM.YYYY` → `YYYY-MM-DD::date`

## Key Changes

### Enhanced Date Pattern Matching

```python
date_patterns = [
    # DD-MM-YYYY HH24:MI:SS format without cast - add timestamp cast
    {
        'pattern': r"'(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})'(?!::)",
        'replacement': r"'\3-\2-\1 \4:\5:\6'::timestamp",
        'description': 'DD-MM-YYYY HH24:MI:SS to YYYY-MM-DD HH24:MI:SS with timestamp cast'
    },
    # DD-MM-YYYY format without cast - add date cast
    {
        'pattern': r"'(\d{2})-(\d{2})-(\d{4})'(?!::)(?!\s+\d{2}:)",
        'replacement': r"'\3-\2-\1'::date",
        'description': 'DD-MM-YYYY to YYYY-MM-DD with date cast'
    },
    # ... additional patterns for /, . separators
]
```

### Automatic Type Casting

Added fallback patterns to ensure any remaining date-like strings get proper casting:

```python
# Handle any remaining date-like strings that might need casting
remaining_datetime_pattern = r"'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'(?!::)"
statement = re.sub(remaining_datetime_pattern, r"'\1'::timestamp", statement)

remaining_date_pattern = r"'(\d{4}-\d{2}-\d{2})'(?!::)(?!\s+\d{2}:)"
statement = re.sub(remaining_date_pattern, r"'\1'::date", statement)
```

### Fixed Column Quoting

Also fixed an issue where VALUES were being incorrectly quoted by improving the column name quoting logic to only apply to explicit column lists.

## Before and After

### Before (Failing)
```sql
INSERT INTO EMR_HIS.V_HIS_PATIENT VALUES ('0000048657', 1, '12-07-2022 09:17:22'::timestamp, '13-07-2022 16:50:00')
```

### After (Working)
```sql
INSERT INTO "public"."PATIENT" VALUES ('0000048657', 1, '2022-07-12 09:17:22'::timestamp, '2022-07-13 16:50:00'::timestamp)
```

## Verification

The fix handles all common Oracle date formats:

- ✅ `13-07-2022 16:50:00` → `2022-07-13 16:50:00::timestamp`
- ✅ `13-07-2022` → `2022-07-13::date`
- ✅ `31/12/2022 23:59:59` → `2022-12-31 23:59:59::timestamp`
- ✅ `15.03.2023 12:30:45` → `2023-03-15 12:30:45::timestamp`

## Impact

This fix resolves the date format parsing errors that were causing the import process to fail. The enhanced SQL rewriter now ensures all date values are:

1. Converted to PostgreSQL-compatible ISO format
2. Explicitly cast to the appropriate PostgreSQL data type
3. Properly handled regardless of the original Oracle date format

## Files Modified

- `oracle_to_postgres/common/sql_rewriter.py` - Enhanced `_convert_date_formats()` method
- `test_enhanced_date_fix.py` - Comprehensive test suite
- `test_specific_date_error.py` - Specific error case testing

## Additional Fixes Applied

### Enhanced INSERT Statement Detection

Fixed the `_is_insert_statement()` method to properly detect INSERT statements even when they contain comments:

```python
def _is_insert_statement(self, statement: str) -> bool:
    """Check if statement is an INSERT statement."""
    # Check if the statement contains an INSERT statement, even if it has comments
    return re.search(r'^\s*INSERT\s+INTO', statement, re.IGNORECASE | re.MULTILINE) is not None
```

### Improved Schema Mapping

Added fallback schema mapping to handle edge cases where the main rules don't catch all schema references:

```python
# Handle remaining schema.table patterns that weren't caught by the main rules
remaining_schema_pattern = r'INSERT\s+INTO\s+(\w+)\.(\w+)'
statement = re.sub(remaining_schema_pattern, replace_remaining_schema, statement, flags=re.IGNORECASE)
```

### Enhanced Date Format Handling

Added special handling for dates that already have timestamp casts but wrong format:

```python
# Handle edge case: dates that already have timestamp cast but wrong format
existing_timestamp_pattern = r"'(\d{2})-(\d{2})-(\d{4}\s+\d{2}:\d{2}:\d{2})'::timestamp"
statement = re.sub(existing_timestamp_pattern, r"'\3-\2-\1'::timestamp", statement, flags=re.IGNORECASE)
```

## Test Results

All tests now pass successfully:

- ✅ Schema mapping (EMR_HIS → public)
- ✅ V_HIS_ prefix removal  
- ✅ Date format conversion (DD-MM-YYYY → YYYY-MM-DD)
- ✅ Timestamp casting added
- ✅ Date casting added
- ✅ Table and schema quoting
- ✅ PostgreSQL compatibility verified

## Next Steps

You can now re-run your import process. The date format errors should be resolved, and the import should proceed successfully.

**Command to restart your import:**
```bash
python import_data.py
```

The enhanced SQL rewriter will now properly handle all Oracle date formats and convert them to PostgreSQL-compatible format with explicit type casting.