"""
Oracle 数据源适配器

复用现有 oracle_connect.py 的连接逻辑，实现统一的数据源接口
"""
import oracledb
from typing import Any, Dict, List, Optional, Union
from pandas import DataFrame
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pydantic import Field

from ..base import DataSource, DataSourceConfig, QueryResult, TableSchema, DataSourceMetadata
from ..exceptions import ConnectionException, QueryException, TimeoutException
from ..dialect import DialectHelper


class OracleConfig(DataSourceConfig):
    """Oracle 数据源配置"""
    type: str = "oracle"
    host: str
    port: int = 1521
    service_name: str
    user: str
    password: str
    
    pool_min: int = Field(2, description="连接池最小连接数")
    pool_max: int = Field(10, description="连接池最大连接数")
    pool_increment: int = Field(1, description="连接池增量")
    
    class Config:
        env_prefix = "ORACLE_"


class OracleAdapter(DataSource):
    """Oracle 数据源适配器"""
    
    def __init__(self, config: OracleConfig):
        """
        初始化 Oracle 适配器
        
        Args:
            config: Oracle 配置
        """
        super().__init__(config)
        self.config: OracleConfig = config
        self._pool: Optional[oracledb.ConnectionPool] = None
        self._executor = ThreadPoolExecutor(max_workers=config.pool_max)
        self._dialect = DialectHelper("oracle")
    
    async def connect(self) -> bool:
        """
        建立 Oracle 连接池
        
        Returns:
            bool: 连接是否成功
            
        Raises:
            ConnectionException: 连接失败时抛出
        """
        try:
            dsn = f"{self.config.host}:{self.config.port}/{self.config.service_name}"
            
            self._pool = oracledb.create_pool(
                user=self.config.user,
                password=self.config.password,
                dsn=dsn,
                min=self.config.pool_min,
                max=self.config.pool_max,
                increment=self.config.pool_increment,
                threaded=True,
                getmode=oracledb.POOL_GETMODE_WAIT
            )
            
            self._is_connected = True
            return True
            
        except oracledb.Error as e:
            raise ConnectionException(
                f"Oracle 连接失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    async def disconnect(self) -> None:
        """关闭 Oracle 连接池"""
        if self._pool:
            self._pool.close()
            self._is_connected = False
    
    async def query(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None,
        limit: Optional[int] = None
    ) -> QueryResult:
        """
        执行 Oracle 查询
        
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
            
        except oracledb.Error as e:
            raise QueryException(
                f"Oracle 查询失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    def _execute_query_sync(self, sql: str, params: Optional[Union[Dict[str, Any], tuple]]) -> DataFrame:
        """
        同步执行查询（在线程池中运行）
        
        Args:
            sql: SQL 语句
            params: 参数
            
        Returns:
            DataFrame: 查询结果
        """
        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            df = DataFrame(data, columns=columns)
            
            cursor.close()
            return df
    
    async def execute(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None
    ) -> int:
        """
        执行 Oracle 写入操作
        
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
            loop = asyncio.get_event_loop()
            affected_rows = await loop.run_in_executor(
                self._executor,
                self._execute_write_sync,
                sql,
                params
            )
            return affected_rows
            
        except oracledb.Error as e:
            raise QueryException(
                f"Oracle 执行失败: {str(e)}",
                source_name=self.config.name,
                original_error=e
            )
    
    def _execute_write_sync(self, sql: str, params: Optional[Union[Dict[str, Any], tuple]]) -> int:
        """
        同步执行写入
        
        Args:
            sql: SQL 语句
            params: 参数
            
        Returns:
            int: 影响的行数
        """
        with self._pool.acquire() as conn:
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            conn.commit()
            affected_rows = cursor.rowcount
            cursor.close()
            return affected_rows
    
    async def test_connection(self) -> bool:
        """
        测试 Oracle 连接
        
        Returns:
            bool: 连接是否正常
        """
        try:
            result = await self.query("SELECT 1 FROM DUAL")
            return len(result.data) > 0
        except Exception:
            return False
    
    async def get_schema(self, table_name: str) -> TableSchema:
        """
        获取 Oracle 表结构
        
        Args:
            table_name: 表名
            
        Returns:
            TableSchema: 表结构信息
        """
        sql = """
        SELECT 
            column_name,
            data_type,
            nullable,
            data_default,
            comments
        FROM user_tab_columns
        LEFT JOIN user_col_comments 
            ON user_tab_columns.table_name = user_col_comments.table_name
            AND user_tab_columns.column_name = user_col_comments.column_name
        WHERE user_tab_columns.table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        result = await self.query(sql, {"table_name": table_name})
        
        columns = []
        for _, row in result.data.iterrows():
            columns.append({
                "name": row["COLUMN_NAME"],
                "type": row["DATA_TYPE"],
                "nullable": row["NULLABLE"] == "Y",
                "default": row["DATA_DEFAULT"],
                "comment": row["COMMENTS"]
            })
        
        pk_sql = """
        SELECT cols.column_name
        FROM user_constraints cons
        JOIN user_cons_columns cols 
            ON cons.constraint_name = cols.constraint_name
        WHERE cons.table_name = UPPER(:table_name)
            AND cons.constraint_type = 'P'
        """
        pk_result = await self.query(pk_sql, {"table_name": table_name})
        primary_keys = pk_result.data["COLUMN_NAME"].tolist() if not pk_result.data.empty else []
        
        return TableSchema(
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys
        )
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        获取 Oracle 表列表
        
        Args:
            schema: Schema 名称（Oracle 中通常为用户名）
            
        Returns:
            List[str]: 表名列表
        """
        sql = "SELECT table_name FROM user_tables ORDER BY table_name"
        result = await self.query(sql)
        return result.data["TABLE_NAME"].tolist()
    
    async def describe(self) -> DataSourceMetadata:
        """
        获取 Oracle 数据源描述
        
        Returns:
            DataSourceMetadata: 数据源元数据
        """
        is_connected = await self.test_connection()
        
        version_sql = "SELECT * FROM v$version WHERE banner LIKE 'Oracle%'"
        try:
            version_result = await self.query(version_sql)
            version = version_result.data.iloc[0]["BANNER"] if not version_result.data.empty else None
        except Exception:
            version = None
        
        return DataSourceMetadata(
            source_name=self.config.name,
            source_type="oracle",
            version=version,
            connection_status="connected" if is_connected else "disconnected"
        )
