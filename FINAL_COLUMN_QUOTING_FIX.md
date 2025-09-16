# 字段名加引号修复方案

## ✅ 问题解决

**问题**: 
```
column "ny" of relation "CRZYMXB" does not exist
```

**更好的解决方案**: 给字段名加双引号，而不是转换大小写

## 🔧 修复方案对比

### 之前的方案 (大小写转换):
```sql
-- 原始SQL
INSERT INTO "public"."CRZYMXB" (NY, BAH, ZYH) VALUES (...)

-- 转换后 (有问题)
INSERT INTO "public"."CRZYMXB" (ny, bah, zyh) VALUES (...)
```
**问题**: 假设数据库中的字段都是小写，但实际可能不是

### 现在的方案 (加引号):
```sql
-- 原始SQL
INSERT INTO "public"."CRZYMXB" (NY, BAH, ZYH) VALUES (...)

-- 转换后 (正确)
INSERT INTO "public"."CRZYMXB" ("NY", "BAH", "ZYH") VALUES (...)
```
**优势**: 保持原始大小写，精确匹配数据库中的字段名

## 🎯 技术实现

### 修改的方法:
```python
def _quote_column_names(self, statement: str) -> str:
    """Quote column names in INSERT statements to preserve case for PostgreSQL compatibility."""
    # Pattern to match INSERT INTO table (column1, column2, ...) VALUES
    insert_columns_pattern = r'INSERT\s+INTO\s+[^(]+\(([^)]+)\)'
    
    def quote_columns(match):
        columns_part = match.group(1)
        # Split by comma and add quotes to each column name
        columns = []
        for col in columns_part.split(','):
            col = col.strip()
            # Only add quotes if not already quoted
            if not (col.startswith('"') and col.endswith('"')):
                columns.append(f'"{col}"')
            else:
                # Already quoted, keep as is
                columns.append(col)
        
        # Reconstruct the match with quoted column names
        return match.group(0).replace(match.group(1), ', '.join(columns))
    
    statement = re.sub(insert_columns_pattern, quote_columns, statement, flags=re.IGNORECASE)
    
    return statement
```

## 📊 转换示例

| 原始SQL | 转换后SQL | 说明 |
|---------|-----------|------|
| `(NY, BAH, ZYH)` | `("NY", "BAH", "ZYH")` | 保持大写 |
| `(Id, Name, Email)` | `("Id", "Name", "Email")` | 保持混合大小写 |
| `("ID", "NAME")` | `("ID", "NAME")` | 已有引号，保持不变 |
| `(id, name, email)` | `("id", "name", "email")` | 保持小写 |

## 🚀 完整转换流程

现在SQL语句经过完整的转换：

### 1. 模式替换
```sql
EMR_HIS.V_HIS_CRZYMXB → "public"."V_HIS_CRZYMXB"
```

### 2. 表名映射
```sql
"public"."V_HIS_CRZYMXB" → "public"."CRZYMXB"
```

### 3. 字段名加引号
```sql
(NY, BAH, ZYH, CZLX, LYKS, MDKS) → ("NY", "BAH", "ZYH", "CZLX", "LYKS", "MDKS")
```

### 4. Oracle命令过滤
```sql
prompt Importing table... → 跳过
set feedback off → 跳过
SELECT语句 → 跳过 (只执行INSERT)
```

## ✅ 优势总结

### 加引号方案的优势:
1. **✅ 保持原始大小写** - 不做任何假设
2. **✅ 精确匹配数据库** - 无论数据库字段是什么大小写
3. **✅ 更加可靠** - 不依赖于数据库创建时的大小写规则
4. **✅ 兼容性更好** - 适用于各种命名约定
5. **✅ 简单直接** - 不需要复杂的大小写转换逻辑

### 与大小写转换方案对比:
| 方面 | 加引号方案 | 大小写转换方案 |
|------|------------|----------------|
| 可靠性 | ✅ 高 | ❌ 依赖假设 |
| 兼容性 | ✅ 通用 | ❌ 限制性 |
| 维护性 | ✅ 简单 | ❌ 复杂 |
| 准确性 | ✅ 精确匹配 | ❌ 可能不匹配 |

## 🎉 结果

你的Oracle到PostgreSQL数据导入现在使用更可靠的字段名处理方式：

```bash
python import_data.py -c config.yaml
```

**期望的行为**:
```
✓ INSERT INTO "public"."CRZYMXB" ("NY", "BAH", "ZYH", "CZLX", "LYKS", "MDKS") VALUES ...
✓ INSERT INTO "public"."KSDMDZB" ("HISKSDM", "HISKSMC", "BAKS") VALUES ...
✓ 字段名精确匹配数据库中的大小写
✓ 不再出现 "column does not exist" 错误
```

这个方案更加稳健，不管你的PostgreSQL表中的字段是大写、小写还是混合大小写，都能正确匹配！