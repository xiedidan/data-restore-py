# Oracle to PostgreSQL Migration Tool - Documentation

Welcome to the comprehensive documentation for the Oracle to PostgreSQL Migration Tool. This documentation will help you successfully migrate your Oracle databases to PostgreSQL.

## 📖 Documentation Overview

### Getting Started
- **[Main README](../README.md)** - Project overview and quick start guide
- **[Usage Guide](USAGE.md)** - Detailed step-by-step instructions
- **[Configuration Template](../config.yaml.template)** - Complete configuration reference

### Practical Guides
- **[Examples & Use Cases](EXAMPLES.md)** - Real-world migration scenarios
- **[Troubleshooting Guide](TROUBLESHOOTING.md)** - Solutions for common problems

## 🎯 Choose Your Path

### First Time User
1. Start with the [Main README](../README.md) for project overview
2. Follow the [Usage Guide](USAGE.md) for detailed instructions
3. Use [Examples](EXAMPLES.md) to find a scenario similar to yours

### Experienced User
1. Copy and customize the [Configuration Template](../config.yaml.template)
2. Refer to [Examples](EXAMPLES.md) for optimization tips
3. Keep [Troubleshooting Guide](TROUBLESHOOTING.md) handy for issues

### Production Deployment
1. Review [Production Migration Example](EXAMPLES.md#production-migration)
2. Follow [Troubleshooting Guide](TROUBLESHOOTING.md) for monitoring
3. Use [Advanced Use Cases](EXAMPLES.md#advanced-use-cases) for optimization

## 🔍 Quick Reference

### Essential Commands

```bash
# Basic migration workflow
python analyze_sql.py --config config.yaml
python create_tables.py --config config.yaml
python import_data.py --config config.yaml

# Debug mode
python analyze_sql.py --config config.yaml --log-level DEBUG

# Production mode with monitoring
nohup python import_data.py --config config.yaml > import.log 2>&1 &
```

### Key Configuration Sections

```yaml
# Required settings
source_directory: "/path/to/oracle/dumps"
deepseek:
  api_key: "your-api-key"
postgresql:
  database: "target_db"
  username: "user"
  password: "pass"

# Performance tuning
performance:
  max_workers: 4
  batch_size: 1000
  memory_limit_mb: 1024
```

### Common File Locations

| File/Directory | Purpose |
|----------------|---------|
| `config.yaml` | Main configuration file |
| `ddl/` | Generated PostgreSQL DDL files |
| `reports/` | Analysis and execution reports |
| `migration.log` | Detailed execution log |

## 📋 Migration Checklist

### Pre-Migration
- [ ] Install Python 3.8+ and dependencies
- [ ] Obtain DeepSeek API key
- [ ] Set up PostgreSQL database
- [ ] Prepare Oracle SQL dump files
- [ ] Create and test configuration file

### During Migration
- [ ] Run analysis script and review DDL files
- [ ] Create tables and verify structure
- [ ] Import data with monitoring
- [ ] Validate record counts and data integrity

### Post-Migration
- [ ] Compare source and target record counts
- [ ] Test application connectivity
- [ ] Set up indexes and constraints
- [ ] Plan for ongoing maintenance

## 🆘 Getting Help

### Self-Service Resources
1. **Search the [Troubleshooting Guide](TROUBLESHOOTING.md)** for your specific error
2. **Check [Examples](EXAMPLES.md)** for similar use cases
3. **Enable debug logging** with `--log-level DEBUG`
4. **Review log files** in `migration.log`

### Diagnostic Information
When seeking help, please provide:
- Configuration file (with sensitive data removed)
- Error messages from logs
- System information (OS, Python version, PostgreSQL version)
- Sample of problematic SQL files (if applicable)

### Common Support Scenarios

| Scenario | First Check | Documentation |
|----------|-------------|---------------|
| Script won't start | Configuration syntax | [Usage Guide](USAGE.md#configuration) |
| API errors | DeepSeek API key and quota | [Troubleshooting](TROUBLESHOOTING.md#api-integration-issues) |
| Database errors | PostgreSQL connection | [Troubleshooting](TROUBLESHOOTING.md#database-connection-problems) |
| Performance issues | System resources | [Examples](EXAMPLES.md#performance-expectations) |
| Data import failures | File encoding and permissions | [Troubleshooting](TROUBLESHOOTING.md#file-processing-errors) |

## 📚 Additional Resources

### External Documentation
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [DeepSeek API Documentation](https://platform.deepseek.com/docs)
- [Python psycopg2 Documentation](https://www.psycopg.org/docs/)

### Best Practices
- Always test migrations on a subset of data first
- Backup your PostgreSQL database before importing
- Monitor system resources during large migrations
- Use version control for configuration files
- Document any manual schema modifications

## 🔄 Documentation Updates

This documentation is maintained alongside the codebase. If you find errors or have suggestions for improvements, please:

1. Check if the issue is already covered in existing documentation
2. Verify the information against the current codebase
3. Submit feedback with specific suggestions for improvement

---

**Need immediate help?** Start with the [Troubleshooting Guide](TROUBLESHOOTING.md) or enable debug logging to get more detailed error information.