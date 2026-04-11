# 数据源抽象层使用指南

## 快速开始

### 1. 安装依赖

```bash
pip install oracledb asyncpg mysql-connector-python pandas pydantic sqlparse
```

### 2. 配置数据源

编辑 `resources/config/datasources.yaml` 文件：

```yaml
data_sources:
  oracle_main:
    type: oracle
    host: 192.168.1.73
    port: 1521
    service_name: ORCL
    user: AIR_SJ_JY
    password: ${ORACLE_PASSWORD}  # 从环境变量读取
    pool_min: 2
    pool_max: 10
    default: true
```

### 3. 使用示例

#### 方式一：从配置文件加载

```python
import asyncio
from src.datasource import DataSourceRegistry

async def main():
    # 从配置文件加载数据源
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    
    # 获取默认数据源
    ds = registry.get()
    
    # 执行查询
    result = await ds.query("SELECT * FROM user_tables WHERE rownum <= 5")
    
    print(f"查询到 {result.row_count} 行数据")
    print(f"执行时间: {result.execution_time:.2f}秒")
    print(result.data)

asyncio.run(main())
```

#### 方式二：手动注册数据源

```python
import asyncio
from src.datasource import DataSourceRegistry
from src.datasource.adapters import OracleConfig

async def main():
    registry = DataSourceRegistry()
    
    # 创建配置
    config = OracleConfig(
        name="my_oracle",
        host="192.168.1.73",
        port=1521,
        service_name="ORCL",
        user="AIR_SJ_JY",
        password="your_password",
        pool_min=2,
        pool_max=10,
        default=True
    )
    
    # 注册数据源
    ds = registry.register(config)
    
    # 执行查询
    result = await ds.query("SELECT SYSDATE FROM DUAL")
    print(result.data)

asyncio.run(main())
```

### 4. 使用上下文管理器

```python
async def with_context_manager():
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    ds = registry.get()
    
    async with ds:
        # 自动连接和断开
        result = await ds.query("SELECT 1 FROM DUAL")
        print(result.data)
```

### 5. 多数据源切换

```python
async def multi_datasource():
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    
    # 查询 Oracle
    oracle_ds = registry.get("oracle_main")
    oracle_result = await oracle_ds.query("SELECT * FROM user_tables")
    
    # 查询 PostgreSQL
    pg_ds = registry.get("postgres_report")
    pg_result = await pg_ds.query("SELECT * FROM information_schema.tables")
    
    # 查询 Doris
    doris_ds = registry.get("doris_analytics")
    doris_result = await doris_ds.query("SELECT * FROM information_schema.tables")
```

### 6. 获取表结构

```python
async def get_table_info():
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    ds = registry.get()
    
    # 获取表结构
    schema = await ds.get_schema("TB_FOC_T1011D")
    
    print(f"表名: {schema.table_name}")
    print(f"列数: {len(schema.columns)}")
    print(f"主键: {schema.primary_keys}")
    
    for col in schema.columns:
        print(f"  - {col['name']}: {col['type']}")
```

### 7. 健康检查

```python
async def health_check():
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    
    # 检查所有数据源
    health_status = await registry.health_check()
    
    for name, status in health_status.items():
        print(f"{name}: {'正常' if status else '异常'}")
```

## 高级功能

### SQL 安全检查

所有查询都会自动进行 SQL 注入检查：

```python
# 安全的查询
result = await ds.query("SELECT * FROM users WHERE id = :id", {"id": 123})

# 危险的查询（会被拒绝）
try:
    await ds.query("SELECT * FROM users; DROP TABLE users; --")
except SQLInjectionRiskException as e:
    print(f"检测到 SQL 注入风险: {e}")
```

### 结果集限制

```python
# 限制返回 100 行
result = await ds.query("SELECT * FROM large_table", limit=100)
```

### 参数化查询

```python
# Oracle 使用 :param 格式
result = await ds.query(
    "SELECT * FROM users WHERE created_at >= :start_date",
    {"start_date": "2024-01-01"}
)

# PostgreSQL 使用 $1, $2 格式
result = await ds.query(
    "SELECT * FROM users WHERE created_at >= $1",
    ("2024-01-01",)
)
```

## 与现有系统集成

### 替换现有 oracle_connect.py

**旧代码：**
```python
from src.util.oracle_connect import OracleDataConn

conn = OracleDataConn()
df = conn.query_as_df("SELECT * FROM table")
```

**新代码：**
```python
from src.datasource import DataSourceRegistry

registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
ds = registry.get()

result = await ds.query("SELECT * FROM table")
df = result.data
```

## 环境变量配置

在 `.env` 文件或系统环境变量中设置：

```bash
export ORACLE_PASSWORD="your_oracle_password"
export POSTGRES_PASSWORD="your_postgres_password"
export DORIS_PASSWORD="your_doris_password"
```

## 下一步

1. 编写单元测试
2. 集成到现有任务系统
3. 实现 Agent 工具调用
4. 添加缓存层

## 常见问题

**Q: 如何处理连接超时？**
A: 在配置中设置 `timeout` 参数，默认 30 秒。

**Q: 如何处理连接池耗尽？**
A: 调整 `pool_min` 和 `pool_max` 参数，确保连接池大小合适。

**Q: 如何调试 SQL 查询？**
A: `QueryResult` 对象包含 `query_sql` 字段，可以查看实际执行的 SQL。
