# Requirements Document

## Introduction

本功能实现Oracle SQL dump文件批量迁移到PostgreSQL数据库的完整工具链。该工具需要处理大型SQL文件（可达几十GB），自动检测文件编码，通过AI生成DDL语句，并支持并行数据导入以提高迁移效率。

## Requirements

### Requirement 1

**User Story:** 作为数据库管理员，我希望能够分析SQL dump文件的编码和结构，以便为PostgreSQL生成正确的DDL语句

#### Acceptance Criteria

1. WHEN 用户指定包含Oracle SQL dump文件的目录 THEN 系统 SHALL 扫描该目录下的所有.sql文件
2. WHEN 系统处理每个SQL文件 THEN 系统 SHALL 读取前N行数据（N可配置，默认100行）
3. WHEN 系统读取文件内容 THEN 系统 SHALL 自动检测文件的字符编码格式
4. WHEN 系统检测到文件编码 THEN 系统 SHALL 将文件名、编码信息记录到CSV格式的报告文件中
5. WHEN 系统分析INSERT语句 THEN 系统 SHALL 调用DeepSeek API分析数据结构并生成PostgreSQL兼容的DDL语句
6. WHEN 系统生成DDL语句 THEN 系统 SHALL 将DDL保存到ddl目录中，文件名格式为create_表名.sql
7. IF DeepSeek API调用失败 THEN 系统 SHALL 记录错误信息并继续处理其他文件

### Requirement 2

**User Story:** 作为数据库管理员，我希望能够批量执行DDL语句在PostgreSQL中创建表结构，以便为数据导入做准备

#### Acceptance Criteria

1. WHEN 用户执行建表命令 THEN 系统 SHALL 读取ddl目录中的所有DDL文件
2. WHEN 系统连接PostgreSQL THEN 系统 SHALL 允许用户指定目标数据库名称和schema名称
3. WHEN 用户选择删除已有表选项 THEN 系统 SHALL 在创建新表前执行DROP TABLE IF EXISTS语句
4. WHEN 系统执行DDL语句 THEN 系统 SHALL 按照依赖关系正确排序表的创建顺序
5. WHEN DDL执行失败 THEN 系统 SHALL 记录详细错误信息并继续处理其他表
6. WHEN 所有DDL执行完成 THEN 系统 SHALL 生成建表结果报告

### Requirement 3

**User Story:** 作为数据库管理员，我希望能够高效地将Oracle SQL dump文件中的数据导入到PostgreSQL，以便完成数据迁移

#### Acceptance Criteria

1. WHEN 用户执行数据导入命令 THEN 系统 SHALL 读取原始SQL文件并识别其中的INSERT语句
2. WHEN 系统处理INSERT语句 THEN 系统 SHALL 将Oracle数据库名称替换为目标PostgreSQL数据库名称
3. WHEN 系统读取SQL文件 THEN 系统 SHALL 根据任务一中记录的编码信息正确解码文件内容
4. WHEN 系统转换编码 THEN 系统 SHALL 将文件内容转换为UTF-8编码（或用户指定的目标编码）
5. WHEN 系统执行数据导入 THEN 系统 SHALL 支持多线程并行处理以提高大文件导入速度
6. WHEN 系统处理大型文件 THEN 系统 SHALL 支持分批处理INSERT语句以避免内存溢出
7. WHEN 数据导入出现错误 THEN 系统 SHALL 记录失败的INSERT语句和错误信息
8. WHEN 数据导入完成 THEN 系统 SHALL 生成导入结果统计报告

### Requirement 4

**User Story:** 作为用户，我希望有独立的Python脚本来执行各个迁移任务，以便灵活控制迁移过程

#### Acceptance Criteria

1. WHEN 用户运行脚本 THEN 系统 SHALL 提供三个独立的Python脚本分别对应三个主要任务
2. WHEN 用户配置参数 THEN 系统 SHALL 支持通过命令行参数和配置文件设置所有可配置选项
3. WHEN 系统执行任务 THEN 系统 SHALL 提供详细的进度显示和日志输出
4. WHEN 用户需要查看结果 THEN 系统 SHALL 生成详细的执行报告和统计信息
5. WHEN 系统遇到错误 THEN 系统 SHALL 提供清晰的错误信息和异常处理