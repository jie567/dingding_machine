"""
Doris 数据源适配器

Doris 使用 MySQL 协议，基于 mysql-connector-python 实现
"""
import mysql.connector
from mysql.connector import pooling
from typing import Any, Dict, List, Optional, Union
from pandas import DataFrame
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pydantic import Field

from ..base import DataSource, DataSourceConfig, QueryResult, TableSchema, DataSourceMetadata
from ..exceptions import ConnectionException, QueryException
from ..dialect import DialectHelper


class DorisConfig(DataSourceConfig):
    """Doris 数据源配置"""
    type: str = "doris"
    host: str
    port: int = 9030
    database: str
    user: str
    password: str
    
    pool_name: str = "doris_pool"
    pool_size: int = Field(10, description="连接池大小")


class DorisAdapter(DataSource):
    """Doris 数据源适配器"""
    
    def __init__(self, config: DorisConfig):
        super().__init__(config)
        self.config: DorisConfig = config
        self._pool: Optional[pooling.MySQLConnectionPool] = None
        self._executor = ThreadPoolExecutor(max_workers=config.pool_size)
        self._dialect = DialectHelper("doris")
    
    async def connect(self) -> bool:
        try:
            self._pool = pooling.MySQLConnectionPool(
                pool_name=self.config.pool_name,
                pool_size=self.config.pool_size,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                charset='utf8mb4',
                autocommit=True
            )
            
            self._is_connected = True
            return True
            
        except mysql.connector.Error as e:
            raise ConnectionException(
                f"Doris 连接失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    async def disconnect(self) -> None:
        self._is_connected = False
    
    async def query(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None,
        limit: Optional[int] = None
    ) -> QueryResult:
        if not self._is_connected:
            await self.connect()
        
        self._dialect.validate_sql(sql)
        
        if limit:
            sql = self._dialect.add_limit(sql, limit)
        
        start_time = self._measure_time()
        
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self._executor,
                self._execute_query_sync,
                sql,
                params
            )
            
            execution_time = self._measure_time() - start_time
            
            return QueryResult(
                data=result,
                row_count=len(result),
                columns=list(result.columns),
                execution_time=execution_time,
                source_name=self.config.name,
                query_sql=sql
            )
            
        except mysql.connector.Error as e:
            raise QueryException(
                f"Doris 查询失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    def _execute_query_sync(self, sql: str, params: Optional[Union[Dict[str, Any], tuple]]) -> DataFrame:
        conn = self._pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            data = cursor.fetchall()
            
            if data:
                df = DataFrame(data)
            else:
                df = DataFrame()
            
            return df
            
        finally:
            cursor.close()
            conn.close()
    
    async def execute(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None
    ) -> int:
        if not self._is_connected:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            affected_rows = await loop.run_in_executor(
                self._executor,
                self._execute_write_sync,
                sql,
                params
            )
            return affected_rows
            
        except mysql.connector.Error as e:
            raise QueryException(
                f"Doris 执行失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    def _execute_write_sync(self, sql: str, params: Optional[Union[Dict[str, Any], tuple]]) -> int:
        conn = self._pool.get_connection()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            affected_rows = cursor.rowcount
            return affected_rows
            
        finally:
            cursor.close()
            conn.close()
    
    async def test_connection(self) -> bool:
        try:
            result = await self.query("SELECT 1")
            return len(result.data) > 0
        except Exception:
            return False
    
    async def get_schema(self, table_name: str) -> TableSchema:
        sql = """
        SELECT 
            COLUMN_NAME,
            COLUMN_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COLUMN_COMMENT
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        
        result = await self.query(sql, (self.config.database, table_name))
        
        columns = []
        for _, row in result.data.iterrows():
            columns.append({
                "name": row["COLUMN_NAME"],
                "type": row["COLUMN_TYPE"],
                "nullable": row["IS_NULLABLE"] == "YES",
                "default": row["COLUMN_DEFAULT"],
                "comment": row["COLUMN_COMMENT"]
            })
        
        return TableSchema(
            table_name=table_name,
            columns=columns
        )
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        db = schema or self.config.database
        sql = """
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        result = await self.query(sql, (db,))
        return result.data["TABLE_NAME"].tolist()
    
    async def describe(self) -> DataSourceMetadata:
        is_connected = await self.test_connection()
        
        version_result = await self.query("SELECT VERSION()")
        version = version_result.data.iloc[0]["VERSION()"] if not version_result.data.empty else None
        
        db_result = await self.query("SHOW DATABASES")
        databases = db_result.data["Database"].tolist()
        
        return DataSourceMetadata(
            source_name=self.config.name,
            source_type="doris",
            version=version,
            databases=databases,
            connection_status="connected" if is_connected else "disconnected"
        )
