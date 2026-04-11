"""
数据源异常体系

定义了数据源操作中可能出现的各种异常类型
"""
from typing import Optional


class DataSourceException(Exception):
    """数据源异常基类"""
    
    def __init__(
        self, 
        message: str, 
        source_name: Optional[str] = None, 
        original_error: Optional[Exception] = None
    ):
        """
        初始化数据源异常
        
        Args:
            message: 异常消息
            source_name: 数据源名称
            original_error: 原始异常
        """
        self.message = message
        self.source_name = source_name
        self.original_error = original_error
        super().__init__(self.message)
    
    def __str__(self):
        parts = [self.message]
        if self.source_name:
            parts.insert(0, f"[{self.source_name}]")
        if self.original_error:
            parts.append(f"原始错误: {str(self.original_error)}")
        return " ".join(parts)


class ConnectionException(DataSourceException):
    """连接异常"""
    pass


class QueryException(DataSourceException):
    """查询异常"""
    pass


class TimeoutException(DataSourceException):
    """超时异常"""
    pass


class AuthenticationException(DataSourceException):
    """认证异常"""
    pass


class SchemaNotFoundException(DataSourceException):
    """Schema 未找到异常"""
    pass


class SQLInjectionRiskException(DataSourceException):
    """SQL 注入风险异常"""
    pass


class DataSourceNotFoundException(DataSourceException):
    """数据源未找到异常"""
    pass


class ConfigurationException(DataSourceException):
    """配置异常"""
    pass
