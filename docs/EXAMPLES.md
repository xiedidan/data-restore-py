# Oracle to PostgreSQL Migration - Examples and Use Cases

This document provides practical examples and common use cases for the Oracle to PostgreSQL migration tool.

## Table of Contents

1. [Basic Migration Example](#basic-migration-example)
2. [Large Dataset Migration](#large-dataset-migration)
3. [Development Environment Setup](#development-environment-setup)
4. [Production Migration](#production-migration)
5. [Troubleshooting Common Issues](#troubleshooting-common-issues)
6. [Configuration Examples](#configuration-examples)
7. [Advanced Use Cases](#advanced-use-cases)

## Basic Migration Example

### Scenario
Migrating a small Oracle database (< 1GB) with 10 tables to PostgreSQL for development purposes.

### Configuration (config-basic.yaml)

```yaml
# Basic migration configuration
source_directory: "./oracle_dumps"
ddl_directory: "./ddl"
sample_lines: 100
target_encoding: "utf-8"

deepseek:
  api_key: "sk-your-api-key-here"
  base_url: "https://api.deepseek.com"
  timeout: 30
  max_retries: 3

postgresql:
  host: "localhost"
  port: 5432
  database: "dev_database"
  schema: "public"
  username: "postgres"
  password: "postgres"

performance:
  max_workers: 2
  batch_size: 500
  memory_limit_mb: 512

logging:
  level: "INFO"
  file: "./migration.log"
```

### Step-by-Step Execution

```bash
# 1. Prepare the environment
mkdir -p oracle_dumps ddl reports
cp config.yaml.template config-basic.yaml
# Edit config-basic.yaml with your settings

# 2. Place Oracle SQL files in oracle_dumps directory
ls oracle_dumps/
# users.sql  orders.sql  products.sql  categories.sql

# 3. Analyze SQL files
python analyze_sql.py --config config-basic.yaml
# Expected output: 4 DDL files generated

# 4. Create PostgreSQL tables
python create_tables.py --config config-basic.yaml --drop-existing
# Expected output: 4 tables created

# 5. Import data
python import_data.py --config config-basic.yaml
# Expected output: Data imported successfully

# 6. Verify results
psql -d dev_database -c "SELECT tablename, n_tup_ins FROM pg_stat_user_tables;"
```

### Expected Results

- Processing time: 5-10 minutes
- Memory usage: < 500MB
- Success rate: > 95%

## Large Dataset Migration

### Scenario
Migrating a large Oracle database (50GB+) with 100+ tables to PostgreSQL for production use.

### Configuration (config-large.yaml)

```yaml
# Large dataset migration configuration
source_directory: "/data/oracle_dumps"
ddl_directory: "/data/ddl"
sample_lines: 500  # More samples for better analysis
target_encoding: "utf-8"

deepseek:
  api_key: "sk-your-production-api-key"
  base_url: "https://api.deepseek.com"
  timeout: 60  # Longer timeout for complex tables
  max_retries: 5  # More retries for reliability

postgresql:
  host: "prod-postgres.example.com"
  port: 5432
  database: "production_db"
  schema: "migrated"
  username: "migration_user"
  password: "secure_password"

performance:
  max_workers: 8  # More workers for faster processing
  batch_size: 2000  # Larger batches for efficiency
  memory_limit_mb: 4096  # More memory for large files

logging:
  level: "INFO"
  file: "/var/log/migration.log"
```

### Optimized Execution Strategy

```bash
# 1. Pre-migration checks
python analyze_sql.py --config config-large.yaml --sample-lines 1000
# Review analysis report for potential issues

# 2. Create tables in batches (if needed)
python create_tables.py --config config-large.yaml --drop-existing

# 3. Import data with monitoring
nohup python import_data.py --config config-large.yaml > import.log 2>&1 &

# 4. Monitor progress
tail -f /var/log/migration.log
tail -f import.log

# 5. Verify critical tables first
psql -d production_db -c "SELECT schemaname, tablename, n_tup_ins FROM pg_stat_user_tables WHERE schemaname='migrated' ORDER BY n_tup_ins DESC LIMIT 10;"
```

### Performance Expectations

- Processing time: 4-8 hours
- Memory usage: 2-4GB
- Throughput: 1000-5000 records/second

## Development Environment Setup

### Scenario
Setting up a development environment with sample data for testing applications.

### Configuration (config-dev.yaml)

```yaml
# Development environment configuration
source_directory: "./sample_data"
ddl_directory: "./dev_ddl"
sample_lines: 50  # Smaller samples for faster processing
target_encoding: "utf-8"

deepseek:
  api_key: "sk-dev-api-key"
  base_url: "https://api.deepseek.com"
  timeout: 15  # Shorter timeout for dev
  max_retries: 2

postgresql:
  host: "localhost"
  port: 5432
  database: "app_dev"
  schema: "public"
  username: "developer"
  password: "devpass"

performance:
  max_workers: 1  # Single worker to avoid overwhelming dev machine
  batch_size: 100  # Small batches for quick feedback
  memory_limit_mb: 256

logging:
  level: "DEBUG"  # Verbose logging for development
  file: "./dev_migration.log"
```

### Quick Setup Script

```bash
#!/bin/bash
# dev_setup.sh - Quick development environment setup

set -e

echo "Setting up development migration environment..."

# Create directories
mkdir -p sample_data dev_ddl reports

# Create sample configuration
cp config.yaml.template config-dev.yaml
# Edit config-dev.yaml as needed

# Create development database
createdb app_dev

# Run migration
echo "Analyzing sample data..."
python analyze_sql.py --config config-dev.yaml

echo "Creating tables..."
python create_tables.py --config config-dev.yaml --drop-existing

echo "Importing data..."
python import_data.py --config config-dev.yaml

echo "Development environment ready!"
echo "Connect to: postgresql://developer@localhost:5432/app_dev"
```

## Production Migration

### Scenario
Mission-critical production migration with zero-downtime requirements.

### Pre-Migration Checklist

```bash
# 1. Backup existing PostgreSQL database
pg_dump production_db > backup_$(date +%Y%m%d_%H%M%S).sql

# 2. Test migration with subset of data
python analyze_sql.py --config config-prod.yaml --sample-lines 1000

# 3. Validate DDL files manually
ls ddl/*.sql | head -5 | xargs -I {} psql -d test_db -f {}

# 4. Estimate migration time
python import_data.py --config config-prod.yaml --dry-run

# 5. Schedule maintenance window
# Plan for 2x estimated time + buffer
```

### Production Configuration (config-prod.yaml)

```yaml
# Production migration configuration
source_directory: "/backup/oracle_exports"
ddl_directory: "/migration/ddl"
sample_lines: 1000
target_encoding: "utf-8"

deepseek:
  api_key: "sk-production-key"
  base_url: "https://api.deepseek.com"
  timeout: 120
  max_retries: 10

postgresql:
  host: "prod-db-cluster.internal"
  port: 5432
  database: "main_production"
  schema: "oracle_migrated"
  username: "migration_service"
  password: "${POSTGRES_PASSWORD}"  # Use environment variable

performance:
  max_workers: 16  # High-performance server
  batch_size: 5000
  memory_limit_mb: 8192

logging:
  level: "INFO"
  file: "/var/log/production_migration.log"
```

### Production Execution

```bash
#!/bin/bash
# production_migration.sh

set -e

# Load environment variables
source /etc/migration/env

# Pre-flight checks
echo "Starting production migration at $(date)"
echo "Checking database connectivity..."
psql -h prod-db-cluster.internal -d main_production -c "SELECT version();"

# Start migration with monitoring
echo "Phase 1: Analyzing Oracle dumps..."
time python analyze_sql.py --config config-prod.yaml

echo "Phase 2: Creating PostgreSQL tables..."
time python create_tables.py --config config-prod.yaml

echo "Phase 3: Importing data..."
time python import_data.py --config config-prod.yaml

# Post-migration validation
echo "Phase 4: Validation..."
python validate_migration.py --config config-prod.yaml

echo "Production migration completed at $(date)"
```

## Configuration Examples

### High-Performance Configuration

```yaml
# For powerful servers with lots of RAM and CPU cores
performance:
  max_workers: 32
  batch_size: 10000
  memory_limit_mb: 16384

postgresql:
  # Use connection pooling
  host: "pgbouncer.internal"
  port: 6432

logging:
  level: "WARNING"  # Reduce log noise
```

### Memory-Constrained Configuration

```yaml
# For servers with limited RAM
performance:
  max_workers: 1
  batch_size: 100
  memory_limit_mb: 128

logging:
  level: "ERROR"  # Minimal logging
```

### Network-Optimized Configuration

```yaml
# For slow or unreliable networks
deepseek:
  timeout: 300  # 5 minutes
  max_retries: 10

postgresql:
  # Use local connection
  host: "localhost"
  
performance:
  max_workers: 2  # Reduce network load
```

## Advanced Use Cases

### Selective Table Migration

```bash
# Migrate only specific tables
mkdir -p selective_migration
cp users.sql orders.sql selective_migration/

# Update config to point to selective_migration directory
python analyze_sql.py --source-directory selective_migration --config config.yaml
```

### Schema Mapping

```yaml
# Migrate to different schema
postgresql:
  schema: "legacy_oracle"  # All tables go to this schema
```

### Encoding Conversion

```yaml
# Handle mixed encodings
target_encoding: "utf-8"  # Convert everything to UTF-8

# For files with known encoding issues
sample_lines: 1000  # More samples for better detection
```

### Parallel Processing by File Size

```bash
# Sort files by size and process largest first
ls -la oracle_dumps/*.sql | sort -k5 -nr > processing_order.txt

# Process in batches
split -l 10 processing_order.txt batch_
for batch in batch_*; do
    python import_data.py --file-list $batch --config config.yaml
done
```

### Custom DDL Modifications

```bash
# After DDL generation, modify files as needed
for ddl in ddl/*.sql; do
    # Add indexes
    echo "CREATE INDEX idx_${table}_created_at ON ${table}(created_at);" >> $ddl
    
    # Add constraints
    echo "ALTER TABLE ${table} ADD CONSTRAINT pk_${table} PRIMARY KEY (id);" >> $ddl
done

# Then create tables
python create_tables.py --config config.yaml
```

### Monitoring and Alerting

```bash
# Monitor migration progress
watch -n 30 'tail -20 /var/log/migration.log'

# Set up alerts for failures
tail -f /var/log/migration.log | grep -i error | while read line; do
    echo "Migration error: $line" | mail -s "Migration Alert" admin@company.com
done
```

### Data Validation

```python
# validate_migration.py - Custom validation script
import psycopg2
import csv

def validate_record_counts():
    """Compare record counts between source and target."""
    with open('reports/analysis_report_*.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row['table_name']
            # Query PostgreSQL for count
            # Compare with expected count
            print(f"Table {table}: validation {'PASS' if counts_match else 'FAIL'}")

if __name__ == "__main__":
    validate_record_counts()
```

## Tips for Different Scenarios

### Small Datasets (< 1GB)
- Use single worker (`max_workers: 1`)
- Small batch size (`batch_size: 100-500`)
- Higher sample lines for better analysis (`sample_lines: 200-500`)

### Medium Datasets (1-10GB)
- Moderate parallelism (`max_workers: 2-4`)
- Medium batch size (`batch_size: 1000-2000`)
- Standard sample lines (`sample_lines: 100-200`)

### Large Datasets (> 10GB)
- High parallelism (`max_workers: 8-16`)
- Large batch size (`batch_size: 2000-5000`)
- Efficient sampling (`sample_lines: 500-1000`)

### Network Considerations
- **Fast network**: Higher parallelism, larger batches
- **Slow network**: Lower parallelism, smaller batches, longer timeouts
- **Unreliable network**: More retries, shorter timeouts, smaller batches