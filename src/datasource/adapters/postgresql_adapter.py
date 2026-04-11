"""
PostgreSQL 数据源适配器

使用 asyncpg 实现异步 PostgreSQL 连接
"""
import asyncpg
from typing import Any, Dict, List, Optional, Union
from pandas import DataFrame
from pydantic import Field

from ..base import DataSource, DataSourceConfig, QueryResult, TableSchema, DataSourceMetadata
from ..exceptions import ConnectionException, QueryException
from ..dialect import DialectHelper


class PostgreSQLConfig(DataSourceConfig):
    """PostgreSQL 数据源配置"""
    type: str = "postgresql"
    host: str
    port: int = 5432
    database: str
    user: str
    password: str
    
    pool_min: int = Field(2, description="连接池最小连接数")
    pool_max: int = Field(10, description="连接池最大连接数")
    ssl_mode: str = Field("prefer", description="SSL 模式")


class PostgreSQLAdapter(DataSource):
    """PostgreSQL 数据源适配器"""
    
    def __init__(self, config: PostgreSQLConfig):
        """
        初始化 PostgreSQL 适配器
        
        Args:
            config: PostgreSQL 配置
        """
        super().__init__(config)
        self.config: PostgreSQLConfig = config
        self._pool: Optional[asyncpg.Pool] = None
        self._dialect = DialectHelper("postgresql")
    
    async def connect(self) -> bool:
        """
        建立 PostgreSQL 连接池
        
        Returns:
            bool: 连接是否成功
            
        Raises:
            ConnectionException: 连接失败时抛出
        """
        try:
            self._pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=self.config.pool_min,
                max_size=self.config.pool_max,
                ssl=self.config.ssl_mode
            )
            
            self._is_connected = True
            return True
            
        except Exception as e:
            raise ConnectionException(
                f"PostgreSQL 连接失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    async def disconnect(self) -> None:
        """关闭 PostgreSQL 连接池"""
        if self._pool:
            await self._pool.close()
            self._is_connected = False
    
    async def query(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None,
        limit: Optional[int] = None
    ) -> QueryResult:
        """
        执行 PostgreSQL 查询
        
        Args:
            sql: SQL 查询语句
            params: 查询参数
            limit: 结果集大小限制
            
        Returns:
            QueryResult: 查询结果
            
        Raises:
            QueryException: 查询失败时抛出
        """
        if not self._is_connected:
            await self.connect()
        
        self._dialect.validate_sql(sql)
        
        if limit:
            sql = self._dialect.add_limit(sql, limit)
        
        start_time = self._measure_time()
        
        try:
            async with self._pool.acquire() as conn:
                if params:
                    if isinstance(params, dict):
                        rows = await conn.fetch(sql, *params.values())
                    else:
                        rows = await conn.fetch(sql, *params)
                else:
                    rows = await conn.fetch(sql)
                
                if rows:
                    df = DataFrame([dict(row) for row in rows])
                else:
                    df = DataFrame()
                
                execution_time = self._measure_time() - start_time
                
                return QueryResult(
                    data=df,
                    row_count=len(df),
                    columns=list(df.columns) if not df.empty else [],
                    execution_time=execution_time,
                    source_name=self.config.name,
                    query_sql=sql
                )
                
        except Exception as e:
            raise QueryException(
                f"PostgreSQL 查询失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    async def execute(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None
    ) -> int:
        """
        执行 PostgreSQL 写入操作
        
        Args:
            sql: SQL 语句
            params: 参数
            
        Returns:
            int: 影响的行数
            
        Raises:
            QueryException: 执行失败时抛出
        """
        if not self._is_connected:
            await self.connect()
        
        try:
            async with self._pool.acquire() as conn:
                if params:
                    if isinstance(params, dict):
                        result = await conn.execute(sql, *params.values())
                    else:
                        result = await conn.execute(sql, *params)
                else:
                    result = await conn.execute(sql)
                
                parts = result.split()
                return int(parts[-1]) if len(parts) > 1 else 0
                
        except Exception as e:
            raise QueryException(
                f"PostgreSQL 执行失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    async def test_connection(self) -> bool:
        """
        测试 PostgreSQL 连接
        
        Returns:
            bool: 连接是否正常
        """
        try:
            result = await self.query("SELECT 1")
            return len(result.data) > 0
        except Exception:
            return False
    
    async def get_schema(self, table_name: str) -> TableSchema:
        """
        获取 PostgreSQL 表结构
        
        Args:
            table_name: 表名
            
        Returns:
            TableSchema: 表结构信息
        """
        sql = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
        """
        
        result = await self.query(sql, (table_name,))
        
        columns = []
        for _, row in result.data.iterrows():
            col_type = row["data_type"]
            if row["character_maximum_length"]:
                col_type += f"({row['character_maximum_length']})"
            elif row["numeric_precision"]:
                col_type += f"({row['numeric_precision']})"
            
            columns.append({
                "name": row["column_name"],
                "type": col_type,
                "nullable": row["is_nullable"] == "YES",
                "default": row["column_default"],
                "comment": None
            })
        
        pk_sql = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass AND i.indisprimary
        """
        pk_result = await self.query(pk_sql, (table_name,))
        primary_keys = pk_result.data["attname"].tolist() if not pk_result.data.empty else []
        
        return TableSchema(
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys
        )
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        获取 PostgreSQL 表列表
        
        Args:
            schema: Schema 名称
            
        Returns:
            List[str]: 表名列表
        """
        schema = schema or "public"
        sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = await self.query(sql, (schema,))
        return result.data["table_name"].tolist()
    
    async def describe(self) -> DataSourceMetadata:
        """
        获取 PostgreSQL 数据源描述
        
        Returns:
            DataSourceMetadata: 数据源元数据
        """
        is_connected = await self.test_connection()
        
        version_result = await self.query("SELECT version()")
        version = version_result.data.iloc[0]["version"] if not version_result.data.empty else None
        
        db_result = await self.query("""
            SELECT datname FROM pg_database 
            WHERE datistemplate = false
        """)
        databases = db_result.data["datname"].tolist()
        
        return DataSourceMetadata(
            source_name=self.config.name,
            source_type="postgresql",
            version=version,
            databases=databases,
            connection_status="connected" if is_connected else "disconnected"
        )
