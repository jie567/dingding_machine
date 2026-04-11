# 快速开始指南

## 1. 环境准备

### 1.1 安装依赖

```bash
pip install oracledb asyncpg mysql-connector-python pandas pydantic sqlparse
```

### 1.2 配置环境变量

创建 `.env` 文件：

```bash
# Oracle 数据库密码
ORACLE_PASSWORD=AIR_SJ_JY1

# PostgreSQL 密码（如果有）
POSTGRES_PASSWORD=your_postgres_password

# Doris 密码（如果有）
DORIS_PASSWORD=your_doris_password
```

### 1.3 配置数据源

编辑 `resources/config/datasources.yaml` 文件：

```yaml
data_sources:
  oracle_main:
    type: oracle
    host: 192.168.1.73
    port: 1521
    service_name: ORCL
    user: AIR_SJ_JY
    password: ${ORACLE_PASSWORD}
    pool_min: 2
    pool_max: 10
    timeout: 30
    default: true
```

## 2. 快速测试

### 2.1 测试 Oracle 连接

```python
import asyncio
from src.datasource import DataSourceRegistry

async def test_oracle():
    # 加载配置
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    
    # 获取数据源
    ds = registry.get('oracle_main')
    
    # 测试连接
    is_connected = await ds.test_connection()
    print(f"连接状态: {'成功' if is_connected else '失败'}")
    
    # 执行查询
    result = await ds.query("SELECT * FROM user_tables WHERE rownum <= 5")
    print(f"查询到 {result.row_count} 行数据")
    print(result.data)

asyncio.run(test_oracle())
```

### 2.2 运行测试文件

```bash
python tests/test_datasource.py
```

## 3. 基本使用

### 3.1 查询数据

```python
from src.datasource import DataSourceRegistry

async def query_data():
    registry = DataSourceRegistry.from_yaml('config/datasources.yaml')
    ds = registry.get()
    
    # 简单查询
    result = await ds.query("SELECT SYSDATE FROM DUAL")
    print(result.data)
    
    # 参数化查询
    result = await ds.query(
        "SELECT * FROM users WHERE created_at >= :start_date",
        {"start_date": "2024-01-01"}
    )
    print(result.data)

asyncio.run(query_data())
```

### 3.2 获取表结构

```python
async def get_schema():
    registry = DataSourceRegistry.from_yaml('config/datasources.yaml')
    ds = registry.get()
    
    schema = await ds.get_schema("TB_FOC_T1011D")
    
    print(f"表名: {schema.table_name}")
    print(f"列数: {len(schema.columns)}")
    
    for col in schema.columns:
        print(f"  - {col['name']}: {col['type']}")

asyncio.run(get_schema())
```

### 3.3 健康检查

```python
async def health_check():
    registry = DataSourceRegistry.from_yaml('config/datasources.yaml')
    
    # 检查所有数据源
    status = await registry.health_check()
    
    for name, is_healthy in status.items():
        print(f"{name}: {'正常' if is_healthy else '异常'}")

asyncio.run(health_check())
```

## 4. 与现有系统集成

### 4.1 替换 oracle_connect.py

**旧代码**：
```python
from src.util.oracle_connect import OracleDataConn

conn = OracleDataConn()
df = conn.query_as_df("SELECT * FROM table")
```

**新代码**：
```python
from src.datasource import DataSourceRegistry

async def query():
    registry = DataSourceRegistry.from_yaml('config/datasources.yaml')
    ds = registry.get()
    
    result = await ds.query("SELECT * FROM table")
    df = result.data
    return df
```

### 4.2 在 Task 中使用

```python
from src.task.Task import Task, task_config
from src.datasource import DataSourceRegistry

@task_config(name="测试任务", task_type="file")
class TestTask(Task):
    async def execute_task(self, conn, **kwargs):
        # 使用新的数据源抽象层
        registry = DataSourceRegistry.get_instance()
        ds = registry.get()
        
        result = await ds.query("SELECT * FROM table")
        self.file_list.append(result.data)
```

## 5. 常见问题

### Q1: 如何处理连接超时？

**A**: 在配置中设置 `timeout` 参数：

```yaml
oracle_main:
  timeout: 30  # 30秒超时
```

### Q2: 如何处理连接池耗尽？

**A**: 调整连接池大小：

```yaml
oracle_main:
  pool_min: 2
  pool_max: 20  # 增加最大连接数
```

### Q3: 如何调试 SQL 查询？

**A**: 查看 `QueryResult` 对象的 `query_sql` 字段：

```python
result = await ds.query("SELECT * FROM table")
print(f"执行的 SQL: {result.query_sql}")
print(f"执行时间: {result.execution_time}秒")
```

### Q4: 如何处理 SQL 注入？

**A**: 使用参数化查询：

```python
# 安全的方式
result = await ds.query(
    "SELECT * FROM users WHERE id = :id",
    {"id": user_input}
)

# 不安全的方式（会被拒绝）
result = await ds.query(f"SELECT * FROM users WHERE id = {user_input}")
```

## 6. 下一步

1. ✅ 测试 Oracle 连接
2. 📝 根据实际数据库调整配置
3. 🚀 开始第二阶段开发（Agent 核心能力）

## 7. 获取帮助

- 查看详细文档：`resources/docs/development_plan.md`
- 查看使用示例：`resources/docs/datasource_usage.md`
- 运行测试：`python tests/test_datasource.py`
