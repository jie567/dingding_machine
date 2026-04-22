"""
Agent 工具基类模块

定义了所有 Agent 工具的基础接口和通用功能。
工具是 Agent 与外部世界交互的接口，包括数据库查询、数据分析、消息发送等。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class ToolResult:
    """
    工具执行结果
    
    统一封装工具的执行结果，包含成功状态、数据、消息和错误信息。
    
    Attributes:
        success: 是否执行成功
        data: 执行结果数据（可以是任意类型）
        message: 执行结果消息（用于展示给用户）
        error: 错误信息（失败时填充）
        execution_time: 执行耗时（秒）
        tool_name: 工具名称
    """
    success: bool
    data: Any = None
    message: str = ""
    error: Optional[str] = None
    execution_time: float = 0.0
    tool_name: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典格式
        
        Returns:
            Dict[str, Any]: 字典格式的结果
        """
        return {
            "success": self.success,
            "data": self.data,
            "message": self.message,
            "error": self.error,
            "execution_time": self.execution_time,
            "tool_name": self.tool_name
        }
    
    def to_json(self) -> str:
        """
        转换为 JSON 字符串
        
        Returns:
            str: JSON 格式的结果
        """
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)
    
    def __str__(self) -> str:
        """字符串表示"""
        if self.success:
            return f"[{self.tool_name}] 成功: {self.message}"
        else:
            return f"[{self.tool_name}] 失败: {self.error}"


class BaseTool(ABC):
    """
    Agent 工具抽象基类
    
    所有 Agent 工具必须继承此类，实现统一的接口。
    工具是 Agent 调用外部功能的标准方式。
    
    使用示例：
        class SQLQueryTool(BaseTool):
            @property
            def name(self) -> str:
                return "sql_query"
            
            @property
            def description(self) -> str:
                return "执行 SQL 查询"
            
            async def execute(self, sql: str, **kwargs) -> ToolResult:
                # 执行查询逻辑
                return ToolResult(success=True, data=result)
    """
    
    def __init__(self):
        """初始化工具"""
        self._execution_count = 0
        self._success_count = 0
        self._error_count = 0
        self._total_execution_time = 0.0
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        工具名称
        
        Returns:
            str: 工具的唯一标识名称
        """
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """
        工具描述
        
        Returns:
            str: 工具的功能描述，用于 Agent 理解工具用途
        """
        pass
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """
        工具参数定义
        
        Returns:
            Dict[str, Any]: 参数定义，用于 Agent 了解如何调用工具
        """
        return {}
    
    @abstractmethod
    async def execute(self, **kwargs) -> ToolResult:
        """
        执行工具
        
        Args:
            **kwargs: 工具参数
            
        Returns:
            ToolResult: 执行结果
        """
        pass
    
    async def run(self, **kwargs) -> ToolResult:
        """
        运行工具（带统计和错误处理）
        
        这是外部调用的入口，会自动记录执行统计信息。
        
        Args:
            **kwargs: 工具参数
            
        Returns:
            ToolResult: 执行结果
        """
        import time
        
        start_time = time.time()
        self._execution_count += 1
        
        try:
            # 执行工具
            result = await self.execute(**kwargs)
            
            # 更新统计
            execution_time = time.time() - start_time
            self._total_execution_time += execution_time
            
            if result.success:
                self._success_count += 1
            else:
                self._error_count += 1
            
            # 填充工具名称和执行时间
            result.tool_name = self.name
            result.execution_time = execution_time
            
            return result
            
        except Exception as e:
            # 执行异常
            execution_time = time.time() - start_time
            self._total_execution_time += execution_time
            self._error_count += 1
            
            return ToolResult(
                success=False,
                error=str(e),
                message=f"工具执行异常: {str(e)}",
                execution_time=execution_time,
                tool_name=self.name
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取工具执行统计
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        avg_time = (
            self._total_execution_time / self._execution_count 
            if self._execution_count > 0 else 0
        )
        
        return {
            "tool_name": self.name,
            "execution_count": self._execution_count,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "success_rate": (
                self._success_count / self._execution_count 
                if self._execution_count > 0 else 0
            ),
            "average_execution_time": avg_time,
            "total_execution_time": self._total_execution_time
        }
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"{self.name}: {self.description}"


class ToolRegistry:
    """
    工具注册中心
    
    管理所有 Agent 工具的注册、查找和调用。
    支持动态注册和注销工具。
    
    使用示例：
        registry = ToolRegistry()
        registry.register(SQLQueryTool())
        registry.register(DataAnalysisTool())
        
        # 获取工具
        tool = registry.get("sql_query")
        
        # 执行工具
        result = await tool.run(sql="SELECT * FROM users")
    """
    
    def __init__(self):
        """初始化工具注册中心"""
        # 存储所有注册的工具
        self._tools: Dict[str, BaseTool] = {}
    
    def register(self, tool: BaseTool) -> None:
        """
        注册工具
        
        Args:
            tool: 工具实例
            
        Raises:
            ValueError: 工具名称已存在时抛出
        """
        if tool.name in self._tools:
            raise ValueError(f"工具 '{tool.name}' 已存在")
        
        self._tools[tool.name] = tool
        print(f"🔧 已注册工具: {tool.name}")
    
    def unregister(self, name: str) -> None:
        """
        注销工具
        
        Args:
            name: 工具名称
        """
        if name in self._tools:
            del self._tools[name]
            print(f"🗑️ 已注销工具: {name}")
    
    def get(self, name: str) -> BaseTool:
        """
        获取工具
        
        Args:
            name: 工具名称
            
        Returns:
            BaseTool: 工具实例
            
        Raises:
            KeyError: 工具不存在时抛出
        """
        if name not in self._tools:
            raise KeyError(f"工具 '{name}' 不存在")
        
        return self._tools[name]
    
    def list_tools(self) -> List[str]:
        """
        列出所有已注册的工具名称
        
        Returns:
            List[str]: 工具名称列表
        """
        return list(self._tools.keys())
    
    def get_tool_descriptions(self) -> str:
        """
        获取所有工具的描述信息
        
        Returns:
            str: 工具描述文本，用于 Agent 了解可用工具
        """
        descriptions = []
        
        for name, tool in self._tools.items():
            desc = f"- {name}: {tool.description}"
            
            # 添加参数信息
            if tool.parameters:
                params_str = ", ".join(
                    f"{k} ({v.get('type', 'any')})" 
                    for k, v in tool.parameters.items()
                )
                desc += f" [参数: {params_str}]"
            
            descriptions.append(desc)
        
        return "\n".join(descriptions)
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有工具的统计信息
        
        Returns:
            Dict[str, Dict[str, Any]]: 工具名称 -> 统计信息
        """
        return {
            name: tool.get_stats() 
            for name, tool in self._tools.items()
        }
    
    async def execute(self, name: str, **kwargs) -> ToolResult:
        """
        执行指定工具
        
        Args:
            name: 工具名称
            **kwargs: 工具参数
            
        Returns:
            ToolResult: 执行结果
        """
        tool = self.get(name)
        return await tool.run(**kwargs)


# 全局工具注册中心实例
tool_registry = ToolRegistry()


def get_tool_registry() -> ToolRegistry:
    """
    获取全局工具注册中心
    
    Returns:
        ToolRegistry: 全局工具注册中心实例
    """
    return tool_registry
