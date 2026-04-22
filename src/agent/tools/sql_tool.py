"""
SQL 查询工具

提供数据库查询功能，支持多数据源和参数化查询。
是 Agent 与数据库交互的核心工具。
"""

from typing import Any, Dict, Optional
import json
import time

from .base import BaseTool, ToolResult
from ...datasource import DataSourceRegistry


class SQLQueryTool(BaseTool):
    """
    SQL 查询工具
    
    执行 SQL 查询并返回结果，支持：
    - 多数据源切换
    - 参数化查询（防止 SQL 注入）
    - 结果集大小限制
    - 自动格式化为 JSON
    
    使用示例：
        tool = SQLQueryTool()
        result = await tool.run(
            sql="SELECT * FROM users WHERE age > :min_age",
            params={"min_age": 18},
            data_source="oracle_main",
            limit=100
        )
    """
    
    @property
    def name(self) -> str:
        """工具名称"""
        return "sql_query"
    
    @property
    def description(self) -> str:
        """工具描述"""
        return (
            "执行 SQL 查询并返回结果。"
            "支持多数据源和参数化查询。"
            "用于从数据库获取数据。"
        )
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """参数定义"""
        return {
            "sql": {
                "type": "string",
                "description": "SQL 查询语句",
                "required": True
            },
            "params": {
                "type": "dict",
                "description": "查询参数（用于参数化查询）",
                "required": False
            },
            "data_source": {
                "type": "string",
                "description": "数据源名称（可选，默认使用主数据源）",
                "required": False
            },
            "limit": {
                "type": "integer",
                "description": "结果集大小限制（默认 1000）",
                "required": False
            }
        }
    
    async def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        data_source: Optional[str] = None,
        limit: int = 1000,
        **kwargs
    ) -> ToolResult:
        """
        执行 SQL 查询
        
        Args:
            sql: SQL 查询语句
            params: 查询参数（用于参数化查询，防止 SQL 注入）
            data_source: 数据源名称（可选）
            limit: 结果集大小限制
            **kwargs: 额外参数
            
        Returns:
            ToolResult: 查询结果
        """
        try:
            # 获取数据源注册中心
            registry = DataSourceRegistry.get_instance()
            
            # 获取数据源
            ds = registry.get(data_source)
            
            print(f"🔍 执行 SQL 查询 [数据源: {ds.config.name}]")
            print(f"   SQL: {sql[:100]}{'...' if len(sql) > 100 else ''}")
            
            # 执行查询
            query_result = await ds.query(sql, params=params, limit=limit)
            
            # 格式化结果
            result_data = {
                "row_count": query_result.row_count,
                "columns": query_result.columns,
                "execution_time": query_result.execution_time,
                "data": query_result.data.to_dict(orient='records')
            }
            
            # 生成摘要信息
            message = (
                f"查询成功，返回 {query_result.row_count} 行数据，"
                f"耗时 {query_result.execution_time:.2f} 秒"
            )
            
            return ToolResult(
                success=True,
                data=result_data,
                message=message
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                error=str(e),
                message=f"SQL 查询失败: {str(e)}"
            )


class SchemaExplorerTool(BaseTool):
    """
    数据库结构探索工具
    
    获取数据库的表结构和元数据信息，帮助 Agent 了解数据库结构。
    """
    
    @property
    def name(self) -> str:
        """工具名称"""
        return "schema_explorer"
    
    @property
    def description(self) -> str:
        """工具描述"""
        return (
            "探索数据库结构，获取表列表和表结构信息。"
            "用于了解数据库中有哪些表以及表的结构。"
        )
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """参数定义"""
        return {
            "action": {
                "type": "string",
                "description": "操作类型: 'list_tables' 或 'get_schema'",
                "required": True
            },
            "table_name": {
                "type": "string",
                "description": "表名（当 action='get_schema' 时需要）",
                "required": False
            },
            "data_source": {
                "type": "string",
                "description": "数据源名称（可选）",
                "required": False
            }
        }
    
    async def execute(
        self,
        action: str,
        table_name: Optional[str] = None,
        data_source: Optional[str] = None,
        **kwargs
    ) -> ToolResult:
        """
        执行结构探索
        
        Args:
            action: 操作类型 ('list_tables' 或 'get_schema')
            table_name: 表名
            data_source: 数据源名称
            **kwargs: 额外参数
            
        Returns:
            ToolResult: 探索结果
        """
        try:
            registry = DataSourceRegistry.get_instance()
            ds = registry.get(data_source)
            
            if action == "list_tables":
                # 获取表列表
                tables = await ds.get_tables()
                
                return ToolResult(
                    success=True,
                    data={"tables": tables},
                    message=f"找到 {len(tables)} 个表"
                )
                
            elif action == "get_schema":
                # 获取表结构
                if not table_name:
                    return ToolResult(
                        success=False,
                        error="缺少 table_name 参数",
                        message="获取表结构需要提供表名"
                    )
                
                schema = await ds.get_schema(table_name)
                
                # 格式化表结构信息
                schema_info = {
                    "table_name": schema.table_name,
                    "columns": [
                        {
                            "name": col["name"],
                            "type": col["type"],
                            "nullable": col.get("nullable", True),
                            "comment": col.get("comment", "")
                        }
                        for col in schema.columns
                    ],
                    "primary_keys": schema.primary_keys or []
                }
                
                return ToolResult(
                    success=True,
                    data=schema_info,
                    message=f"表 '{table_name}' 有 {len(schema.columns)} 个字段"
                )
                
            else:
                return ToolResult(
                    success=False,
                    error=f"不支持的操作: {action}",
                    message="支持的操作: list_tables, get_schema"
                )
                
        except Exception as e:
            return ToolResult(
                success=False,
                error=str(e),
                message=f"结构探索失败: {str(e)}"
            )
