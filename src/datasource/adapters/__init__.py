"""
数据源适配器模块
"""
from .oracle_adapter import OracleAdapter, OracleConfig
from .postgresql_adapter import PostgreSQLAdapter, PostgreSQLConfig
from .doris_adapter import DorisAdapter, DorisConfig

__all__ = [
    "OracleAdapter",
    "OracleConfig",
    "PostgreSQLAdapter",
    "PostgreSQLConfig",
    "DorisAdapter",
    "DorisConfig",
]
