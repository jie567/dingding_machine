"""
数据源抽象层

提供统一的数据源访问接口
"""
from .base import DataSource, DataSourceConfig, QueryResult, TableSchema, DataSourceMetadata
from .exceptions import (
    DataSourceException,
    ConnectionException,
    QueryException,
    TimeoutException,
    AuthenticationException,
    SchemaNotFoundException,
    SQLInjectionRiskException,
    DataSourceNotFoundException,
    ConfigurationException
)
from .registry import DataSourceRegistry
from .dialect import DialectHelper

__all__ = [
    "DataSource",
    "DataSourceConfig",
    "QueryResult",
    "TableSchema",
    "DataSourceMetadata",
    "DataSourceRegistry",
    "DialectHelper",
    "DataSourceException",
    "ConnectionException",
    "QueryException",
    "TimeoutException",
    "AuthenticationException",
    "SchemaNotFoundException",
    "SQLInjectionRiskException",
    "DataSourceNotFoundException",
    "ConfigurationException",
]
