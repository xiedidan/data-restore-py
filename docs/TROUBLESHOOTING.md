# Oracle to PostgreSQL Migration - Troubleshooting Guide

This guide helps you diagnose and resolve common issues encountered during Oracle to PostgreSQL migration.

## Table of Contents

1. [Common Error Messages](#common-error-messages)
2. [Configuration Issues](#configuration-issues)
3. [Database Connection Problems](#database-connection-problems)
4. [API Integration Issues](#api-integration-issues)
5. [File Processing Errors](#file-processing-errors)
6. [Performance Issues](#performance-issues)
7. [Data Import Problems](#data-import-problems)
8. [Debugging Techniques](#debugging-techniques)
9. [Recovery Procedures](#recovery-procedures)

## Common Error Messages

### "Configuration file not found"

**Error Message:**
```
FileNotFoundError: Configuration file not found: config.yaml
```

**Cause:** The configuration file doesn't exist or path is incorrect.

**Solution:**
```bash
# Check if file exists
ls -la config.yaml

# Create from template if missing
cp config.yaml.template config.yaml

# Use absolute path if needed
python analyze_sql.py --config /full/path/to/config.yaml
```

### "DeepSeek API key is required"

**Error Message:**
```
ValueError: Configuration validation failed:
- DeepSeek API key is required
```

**Cause:** Missing or empty DeepSeek API key in configuration.

**Solution:**
```yaml
# In config.yaml, ensure API key is set
deepseek:
  api_key: "sk-your-actual-api-key-here"  # Not empty!
```

**Alternative:** Use environment variable
```bash
export DEEPSEEK_API_KEY="sk-your-api-key"
python analyze_sql.py --deepseek-api-key "$DEEPSEEK_API_KEY"
```

### "PostgreSQL database name is required"

**Error Message:**
```
ValueError: Configuration validation failed:
- PostgreSQL database name is required
```

**Cause:** Missing database name in configuration.

**Solution:**
```yaml
postgresql:
  database: "your_actual_database_name"  # Must be specified
```

### "Source directory does not exist"

**Error Message:**
```
ValueError: Configuration validation failed:
- Source directory does not exist: /path/to/dumps
```

**Cause:** The specified source directory doesn't exist.

**Solution:**
```bash
# Check directory exists
ls -la /path/to/dumps

# Create directory if needed
mkdir -p /path/to/dumps

# Update config with correct path
source_directory: "/correct/path/to/dumps"
```

## Configuration Issues

### Invalid YAML Syntax

**Symptoms:**
- Script crashes on startup
- "YAML parsing error" messages

**Diagnosis:**
```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

**Common YAML Issues:**
```yaml
# WRONG: Missing quotes around special characters
password: my@password!

# CORRECT: Quote special characters
password: "my@password!"

# WRONG: Inconsistent indentation
deepseek:
  api_key: "key"
   timeout: 30  # Wrong indentation

# CORRECT: Consistent indentation (2 spaces)
deepseek:
  api_key: "key"
  timeout: 30
```

### Environment Variable Issues

**Problem:** Environment variables not being resolved.

**Solution:**
```bash
# Check environment variables
echo $POSTGRES_PASSWORD

# Set if missing
export POSTGRES_PASSWORD="your_password"

# Use in config.yaml
postgresql:
  password: "${POSTGRES_PASSWORD}"
```

## Database Connection Problems

### Connection Refused

**Error Message:**
```
psycopg2.OperationalError: could not connect to server: Connection refused
```

**Diagnosis Steps:**
```bash
# 1. Check if PostgreSQL is running
sudo systemctl status postgresql

# 2. Check if port is open
netstat -ln | grep 5432

# 3. Test connection manually
psql -h localhost -p 5432 -U postgres -d mydb
```

**Solutions:**
```bash
# Start PostgreSQL if not running
sudo systemctl start postgresql

# Check PostgreSQL configuration
sudo nano /etc/postgresql/*/main/postgresql.conf
# Ensure: listen_addresses = '*'

sudo nano /etc/postgresql/*/main/pg_hba.conf
# Add: host all all 0.0.0.0/0 md5

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### Authentication Failed

**Error Message:**
```
psycopg2.OperationalError: FATAL: password authentication failed for user "postgres"
```

**Solutions:**
```bash
# 1. Reset PostgreSQL password
sudo -u postgres psql
ALTER USER postgres PASSWORD 'newpassword';
\q

# 2. Update config.yaml
postgresql:
  username: "postgres"
  password: "newpassword"

# 3. Use .pgpass file for security
echo "localhost:5432:*:postgres:newpassword" >> ~/.pgpass
chmod 600 ~/.pgpass
```

### Database Does Not Exist

**Error Message:**
```
psycopg2.OperationalError: FATAL: database "mydb" does not exist
```

**Solution:**
```bash
# Create the database
createdb mydb

# Or using SQL
sudo -u postgres psql
CREATE DATABASE mydb;
\q
```

## API Integration Issues

### DeepSeek API Timeout

**Error Message:**
```
requests.exceptions.Timeout: HTTPSConnectionPool: Read timed out
```

**Solutions:**
```yaml
# Increase timeout in config.yaml
deepseek:
  timeout: 120  # Increase from 30 to 120 seconds
  max_retries: 5  # More retries
```

### API Rate Limiting

**Error Message:**
```
HTTP 429: Too Many Requests
```

**Solutions:**
```yaml
# Reduce parallel processing
performance:
  max_workers: 1  # Process one file at a time

# Add delays between API calls
deepseek:
  timeout: 60
  max_retries: 10
```

### Invalid API Response

**Error Message:**
```
ValueError: Invalid DDL response from DeepSeek API
```

**Diagnosis:**
```bash
# Enable debug logging to see API responses
python analyze_sql.py --config config.yaml --log-level DEBUG
```

**Solutions:**
1. Check API key validity
2. Verify sample data quality
3. Increase sample lines for better context

## File Processing Errors

### Encoding Detection Failures

**Error Message:**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte 0x... in position ...
```

**Solutions:**
```yaml
# Increase sample size for better detection
sample_lines: 500

# Force specific encoding if known
target_encoding: "gbk"  # or "latin1", "cp1252"
```

**Manual Encoding Detection:**
```bash
# Use file command
file -i your_file.sql

# Use chardet
pip install chardet
chardetect your_file.sql

# Use iconv to convert
iconv -f gbk -t utf-8 input.sql > output.sql
```

### Large File Memory Issues

**Error Message:**
```
MemoryError: Unable to allocate array
```

**Solutions:**
```yaml
# Reduce memory usage
performance:
  batch_size: 100  # Smaller batches
  memory_limit_mb: 512  # Lower limit
  max_workers: 1  # Single worker

# Process files individually
sample_lines: 50  # Fewer samples
```

### File Permission Errors

**Error Message:**
```
PermissionError: [Errno 13] Permission denied: '/path/to/file.sql'
```

**Solutions:**
```bash
# Fix file permissions
chmod 644 /path/to/oracle/dumps/*.sql

# Fix directory permissions
chmod 755 /path/to/oracle/dumps/

# Run with appropriate user
sudo -u postgres python analyze_sql.py --config config.yaml
```

## Performance Issues

### Slow Processing

**Symptoms:**
- Scripts take much longer than expected
- High CPU or memory usage
- Database connections timing out

**Diagnosis:**
```bash
# Monitor system resources
top -p $(pgrep -f python)
iostat -x 1
free -h

# Check database activity
psql -d mydb -c "SELECT * FROM pg_stat_activity;"
```

**Solutions:**
```yaml
# Optimize performance settings
performance:
  max_workers: 4  # Adjust based on CPU cores
  batch_size: 1000  # Balance between speed and memory
  memory_limit_mb: 2048  # Increase if you have RAM

# Optimize PostgreSQL
postgresql:
  # Use connection pooling if available
  host: "pgbouncer_host"
  port: 6432
```

### Database Lock Issues

**Error Message:**
```
psycopg2.errors.LockNotAvailable: could not obtain lock on relation "table_name"
```

**Solutions:**
```bash
# Check for blocking queries
psql -d mydb -c "SELECT * FROM pg_locks WHERE NOT granted;"

# Kill blocking processes if safe
psql -d mydb -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction';"
```

## Data Import Problems

### Duplicate Key Violations

**Error Message:**
```
psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint
```

**Solutions:**
```sql
-- Drop constraints before import
ALTER TABLE table_name DROP CONSTRAINT constraint_name;

-- Re-add after import
ALTER TABLE table_name ADD CONSTRAINT constraint_name UNIQUE (column_name);
```

### Data Type Mismatches

**Error Message:**
```
psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type integer
```

**Solutions:**
1. Review generated DDL files
2. Manually adjust data types
3. Clean source data

```sql
-- Example DDL adjustment
-- Change from:
CREATE TABLE users (id INTEGER, ...);
-- To:
CREATE TABLE users (id BIGINT, ...);
```

### Foreign Key Violations

**Error Message:**
```
psycopg2.errors.ForeignKeyViolation: insert or update on table violates foreign key constraint
```

**Solutions:**
```sql
-- Disable foreign key checks during import
SET session_replication_role = replica;

-- Import data

-- Re-enable foreign key checks
SET session_replication_role = DEFAULT;
```

## Debugging Techniques

### Enable Debug Logging

```yaml
# In config.yaml
logging:
  level: "DEBUG"
  file: "./debug.log"
```

```bash
# Run with debug output
python analyze_sql.py --config config.yaml --log-level DEBUG 2>&1 | tee debug_output.log
```

### Isolate Problems

```bash
# Test with single file
mkdir test_single
cp problematic_file.sql test_single/
python analyze_sql.py --source-directory test_single --config config.yaml

# Test database connection only
python -c "
from oracle_to_postgres.common.database import DatabaseManager
from oracle_to_postgres.common.config import Config
config = Config.from_file('config.yaml')
db = DatabaseManager(config.postgresql)
db.connect()
print('Connection successful!')
"
```

### Validate Configuration

```bash
# Test configuration loading
python -c "
from oracle_to_postgres.common.config import Config
config = Config.from_file('config.yaml')
config.validate()
print('Configuration valid!')
"
```

### Check API Connectivity

```bash
# Test DeepSeek API
curl -X POST "https://api.deepseek.com/v1/chat/completions" \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"model": "deepseek-coder", "messages": [{"role": "user", "content": "test"}]}'
```

## Recovery Procedures

### Partial Migration Recovery

If migration fails partway through:

```bash
# 1. Check what was completed
ls ddl/  # See which DDL files exist
psql -d mydb -c "\dt"  # See which tables exist

# 2. Resume from where it left off
# Edit source directory to only include unprocessed files
mkdir remaining_files
mv unprocessed_*.sql remaining_files/

# 3. Update config and continue
python analyze_sql.py --source-directory remaining_files --config config.yaml
```

### Database Recovery

If you need to start over:

```sql
-- Drop all migrated tables
DROP SCHEMA IF EXISTS migrated CASCADE;
CREATE SCHEMA migrated;

-- Or drop specific tables
SELECT 'DROP TABLE IF EXISTS ' || tablename || ' CASCADE;' 
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename LIKE 'oracle_%';
```

### Clean Restart

```bash
# Remove all generated files
rm -rf ddl/ reports/
mkdir -p ddl reports

# Clear log files
> migration.log

# Start fresh
python analyze_sql.py --config config.yaml
```

## Getting Help

### Log Analysis

```bash
# Find errors in logs
grep -i error migration.log

# Find warnings
grep -i warning migration.log

# Get last 100 lines
tail -100 migration.log

# Follow log in real-time
tail -f migration.log
```

### System Information

```bash
# Collect system info for support
echo "=== System Information ===" > support_info.txt
uname -a >> support_info.txt
python --version >> support_info.txt
psql --version >> support_info.txt
echo "=== Configuration ===" >> support_info.txt
cat config.yaml >> support_info.txt
echo "=== Recent Errors ===" >> support_info.txt
grep -i error migration.log | tail -20 >> support_info.txt
```

### Performance Monitoring

```bash
# Monitor during migration
#!/bin/bash
while true; do
    echo "$(date): $(ps aux | grep python | grep -v grep | wc -l) Python processes"
    echo "$(date): $(psql -d mydb -t -c "SELECT count(*) FROM pg_stat_activity WHERE state='active';")" active connections"
    sleep 30
done > monitoring.log &
```

## Prevention Tips

1. **Always test with small datasets first**
2. **Backup databases before migration**
3. **Monitor system resources during migration**
4. **Use version control for configuration files**
5. **Document any manual changes made**
6. **Set up proper logging from the start**
7. **Plan for rollback procedures**
8. **Test recovery procedures before production migration**