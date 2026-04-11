"""
数据源抽象基类

定义了所有数据源必须实现的统一接口
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from pandas import DataFrame
from pydantic import BaseModel, Field
from datetime import datetime
import time


class QueryResult(BaseModel):
    """统一查询结果模型"""
    data: DataFrame = Field(..., description="查询结果数据")
    row_count: int = Field(..., description="返回行数")
    columns: List[str] = Field(..., description="列名列表")
    execution_time: float = Field(..., description="执行耗时(秒)")
    source_name: str = Field(..., description="数据源名称")
    query_sql: Optional[str] = Field(None, description="执行的 SQL")
    
    class Config:
        arbitrary_types_allowed = True


class DataSourceMetadata(BaseModel):
    """数据源元数据"""
    source_name: str
    source_type: str
    version: Optional[str] = None
    databases: Optional[List[str]] = None
    schemas: Optional[List[str]] = None
    connection_status: str = "unknown"
    last_check_time: Optional[datetime] = None


class TableSchema(BaseModel):
    """表结构信息"""
    table_name: str
    columns: List[Dict[str, Any]]
    primary_keys: Optional[List[str]] = None
    row_count: Optional[int] = None


class DataSourceConfig(BaseModel):
    """数据源配置基类"""
    name: str = Field(..., description="数据源名称")
    type: str = Field(..., description="数据源类型")
    timeout: int = Field(30, description="查询超时时间(秒)")
    retry_times: int = Field(3, description="重试次数")
    retry_delay: float = Field(1.0, description="重试延迟(秒)")
    default: bool = Field(False, description="是否为默认数据源")


class DataSource(ABC):
    """数据源抽象基类"""
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化数据源
        
        Args:
            config: 数据源配置
        """
        self.config = config
        self._connection = None
        self._is_connected = False
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        建立数据源连接
        
        Returns:
            bool: 连接是否成功
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """断开数据源连接"""
        pass
    
    @abstractmethod
    async def query(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None,
        limit: Optional[int] = None
    ) -> QueryResult:
        """
        执行查询并返回标准化结果
        
        Args:
            sql: SQL 查询语句
            params: 查询参数（参数化查询）
            limit: 结果集大小限制
            
        Returns:
            QueryResult: 统一格式的查询结果
            
        Raises:
            DataSourceException: 查询失败时抛出
        """
        pass
    
    @abstractmethod
    async def execute(
        self, 
        sql: str, 
        params: Optional[Union[Dict[str, Any], tuple]] = None
    ) -> int:
        """
        执行写入/更新/删除操作
        
        Args:
            sql: SQL 语句
            params: 参数
            
        Returns:
            int: 影响的行数
        """
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """
        测试连接是否正常
        
        Returns:
            bool: 连接是否正常
        """
        pass
    
    @abstractmethod
    async def get_schema(self, table_name: str) -> TableSchema:
        """
        获取表结构元数据
        
        Args:
            table_name: 表名
            
        Returns:
            TableSchema: 表结构信息
        """
        pass
    
    @abstractmethod
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        列出可用的表/集合
        
        Args:
            schema: Schema 名称（可选）
            
        Returns:
            List[str]: 表名列表
        """
        pass
    
    @abstractmethod
    async def describe(self) -> DataSourceMetadata:
        """
        返回数据源描述信息
        
        Returns:
            DataSourceMetadata: 数据源元数据
        """
        pass
    
    async def __aenter__(self):
        """支持异步上下文管理器"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时自动断开连接"""
        await self.disconnect()
    
    def _measure_time(self) -> float:
        """测量执行时间"""
        return time.time()
