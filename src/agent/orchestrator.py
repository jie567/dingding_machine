"""
Agent 编排器模块

核心组件，负责协调 Agent 的各个模块：
1. 接收用户输入
2. 识别意图
3. 规划任务
4. 调用工具
5. 生成回复

是 Agent 的大脑，决定如何响应用户的请求。
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import json

from .llm_router import LLMRouter, get_llm_router
from .config_loader import get_initialized_llm_router
from .conversation.manager import ConversationManager, get_conversation_manager
from .conversation.intent import IntentRecognizer, IntentType
from .tools.base import ToolRegistry, get_tool_registry
from .tools.sql_tool import SQLQueryTool, SchemaExplorerTool
from .tools.analysis_tool import DataAnalysisTool
from .tools.excel_tool import ExcelGeneratorTool, ExcelMultiSheetTool


@dataclass
class AgentResponse:
    """
    Agent 响应结果
    
    Attributes:
        success: 是否成功
        message: 回复消息（展示给用户）
        data: 附加数据
        actions: 执行的操作列表
        error: 错误信息
    """
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    actions: List[str] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "actions": self.actions or [],
            "error": self.error
        }


class AgentOrchestrator:
    """
    Agent 编排器
    
    协调 Agent 的各个组件，处理用户请求：
    1. 管理对话上下文
    2. 识别用户意图
    3. 规划执行步骤
    4. 调用工具执行
    5. 生成自然语言回复
    
    使用示例：
        orchestrator = AgentOrchestrator()
        
        # 处理用户消息
        response = await orchestrator.process_message(
            user_input="查一下昨天的航线收益",
            chat_id="chat_xxx",
            user_id="user_xxx"
        )
        
        print(response.message)
    """
    
    def __init__(
        self,
        llm_router: Optional[LLMRouter] = None,
        conversation_manager: Optional[ConversationManager] = None,
        tool_registry: Optional[ToolRegistry] = None
    ):
        """
        初始化 Agent 编排器
        
        Args:
            llm_router: LLM 路由器（可选）
            conversation_manager: 对话管理器（可选）
            tool_registry: 工具注册中心（可选）
        """
        # LLM 路由器（使用配置文件初始化）
        self._llm_router = llm_router or get_initialized_llm_router()
        
        # 对话管理器
        self._conversation_manager = conversation_manager or get_conversation_manager()
        
        # 工具注册中心
        self._tool_registry = tool_registry or get_tool_registry()
        
        # 意图识别器
        self._intent_recognizer = IntentRecognizer()
        
        # 初始化工具
        self._init_tools()
    
    def _init_tools(self):
        """初始化并注册默认工具"""
        # 注册 SQL 查询工具
        if "sql_query" not in self._tool_registry.list_tools():
            self._tool_registry.register(SQLQueryTool())
        
        # 注册结构探索工具
        if "schema_explorer" not in self._tool_registry.list_tools():
            self._tool_registry.register(SchemaExplorerTool())
        
        # 注册数据分析工具
        if "data_analysis" not in self._tool_registry.list_tools():
            self._tool_registry.register(DataAnalysisTool())
        
        # 注册 Excel 生成工具
        if "excel_generator" not in self._tool_registry.list_tools():
            self._tool_registry.register(ExcelGeneratorTool())
        
        # 注册多 Sheet Excel 工具
        if "excel_multi_sheet" not in self._tool_registry.list_tools():
            self._tool_registry.register(ExcelMultiSheetTool())
        
        print(f"🔧 已注册 {len(self._tool_registry.list_tools())} 个工具")
    
    async def process_message(
        self,
        user_input: str,
        chat_id: str,
        user_id: str,
        session_id: Optional[str] = None
    ) -> AgentResponse:
        """
        处理用户消息（主入口）
        
        完整的处理流程：
        1. 获取或创建会话
        2. 记录用户消息
        3. 识别意图
        4. 根据意图执行相应处理
        5. 记录助手回复
        6. 返回响应
        
        Args:
            user_input: 用户输入文本
            chat_id: 钉钉群聊 ID
            user_id: 用户 ID
            session_id: 会话 ID（可选）
            
        Returns:
            AgentResponse: 处理结果
        """
        print(f"\n{'='*50}")
        print(f"📝 用户输入: {user_input}")
        print(f"👤 用户: {user_id} | 💬 群聊: {chat_id}")
        print(f"{'='*50}")
        
        try:
            # 1. 获取或创建会话
            if session_id:
                session = self._conversation_manager.get_session(session_id)
                if not session:
                    session = self._conversation_manager.create_session(chat_id, user_id)
            else:
                session = self._conversation_manager.get_or_create_session(chat_id, user_id)
            
            # 2. 记录用户消息
            self._conversation_manager.add_message(
                session.session_id,
                "user",
                user_input
            )
            
            # 3. 识别意图
            intent_result = self._intent_recognizer.recognize(user_input)
            print(f"🎯 {intent_result.message}")
            
            # 4. 根据意图处理
            if intent_result.intent == IntentType.DATA_QUERY:
                response = await self._handle_data_query(session, user_input, intent_result)
                
            elif intent_result.intent == IntentType.DATA_ANALYSIS:
                response = await self._handle_data_analysis(session, user_input, intent_result)
                
            elif intent_result.intent == IntentType.REPORT_GENERATION:
                response = await self._handle_report_generation(session, user_input, intent_result)
                
            elif intent_result.intent == IntentType.TASK_MANAGEMENT:
                response = await self._handle_task_management(session, user_input, intent_result)
                
            elif intent_result.intent == IntentType.HELP:
                response = await self._handle_help(session, user_input)
                
            elif intent_result.intent == IntentType.GREETING:
                response = await self._handle_greeting(session, user_input)
                
            elif intent_result.intent == IntentType.CLARIFICATION:
                response = await self._handle_clarification(session, user_input, intent_result)
                
            else:
                # 未知意图，使用 LLM 生成回复
                response = await self._handle_unknown(session, user_input)
            
            # 5. 记录助手回复
            if response.success:
                self._conversation_manager.add_message(
                    session.session_id,
                    "assistant",
                    response.message,
                    metadata={"actions": response.actions}
                )
            
            print(f"✅ 处理完成")
            return response
            
        except Exception as e:
            error_msg = f"处理消息时出错: {str(e)}"
            print(f"❌ {error_msg}")
            
            return AgentResponse(
                success=False,
                message="抱歉，处理您的请求时出现了错误。请稍后重试。",
                error=str(e)
            )
    
    async def _handle_data_query(
        self,
        session,
        user_input: str,
        intent_result
    ) -> AgentResponse:
        """
        处理数据查询意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            intent_result: 意图识别结果
            
        Returns:
            AgentResponse: 响应结果
        """
        print("🔍 处理数据查询...")
        
        try:
            # 使用 LLM 生成 SQL
            sql = await self._generate_sql(user_input, session)
            
            if not sql:
                return AgentResponse(
                    success=False,
                    message="无法理解您的查询需求，请尝试更具体的描述。",
                    actions=["尝试生成 SQL 失败"]
                )
            
            print(f"📝 生成 SQL: {sql}")
            
            # 执行 SQL 查询
            query_result = await self._tool_registry.execute(
                "sql_query",
                sql=sql,
                limit=100
            )
            
            if not query_result.success:
                return AgentResponse(
                    success=False,
                    message=f"查询失败: {query_result.message}",
                    error=query_result.error,
                    actions=["执行 SQL 查询"]
                )
            
            # 保存查询结果到会话上下文
            session.context.add_query_result(query_result.data)
            
            # 格式化结果
            result_data = query_result.data
            row_count = result_data.get("row_count", 0)
            
            # 生成回复
            if row_count == 0:
                message = "查询完成，但未找到匹配的数据。"
            else:
                # 构建数据摘要
                columns = result_data.get("columns", [])
                data_preview = result_data.get("data", [])[:5]  # 前 5 条
                
                message = f"查询完成！共找到 {row_count} 条数据。\n\n"
                message += f"字段: {', '.join(columns)}\n\n"
                
                if data_preview:
                    message += "数据预览:\n"
                    for i, row in enumerate(data_preview, 1):
                        row_str = ", ".join(f"{k}={v}" for k, v in row.items())
                        message += f"{i}. {row_str}\n"
                
                if row_count > 5:
                    message += f"\n... 还有 {row_count - 5} 条数据"
            
            return AgentResponse(
                success=True,
                message=message,
                data=result_data,
                actions=["生成 SQL", "执行查询", "格式化结果"]
            )
            
        except Exception as e:
            return AgentResponse(
                success=False,
                message=f"数据查询失败: {str(e)}",
                error=str(e),
                actions=["尝试数据查询"]
            )
    
    async def _handle_data_analysis(
        self,
        session,
        user_input: str,
        intent_result
    ) -> AgentResponse:
        """
        处理数据分析意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            intent_result: 意图识别结果
            
        Returns:
            AgentResponse: 响应结果
        """
        print("📊 处理数据分析...")
        
        # 检查是否有之前的查询结果
        if not session.context.query_results:
            return AgentResponse(
                success=False,
                message="请先进行数据查询，然后再进行分析。",
                actions=["检查查询历史"]
            )
        
        try:
            # 获取最后一次查询结果
            last_result = session.context.query_results[-1]
            data_json = json.dumps(last_result.get("data", []))
            
            # 确定分析类型
            analysis_type = "summary"  # 默认统计摘要
            if "趋势" in user_input or "变化" in user_input:
                analysis_type = "trend"
            elif "对比" in user_input or "比较" in user_input:
                analysis_type = "compare"
            elif "相关" in user_input:
                analysis_type = "correlation"
            elif "分组" in user_input:
                analysis_type = "groupby"
            
            # 执行分析
            analysis_result = await self._tool_registry.execute(
                "data_analysis",
                data_json=data_json,
                analysis_type=analysis_type
            )
            
            if not analysis_result.success:
                return AgentResponse(
                    success=False,
                    message=f"分析失败: {analysis_result.message}",
                    error=analysis_result.error
                )
            
            # 保存分析结果
            session.context.add_analysis_result(analysis_result.data)
            
            # 生成回复
            message = f"📊 分析完成！\n\n{analysis_result.message}"
            
            # 添加详细结果
            if analysis_result.data:
                message += "\n\n详细结果:\n"
                message += json.dumps(
                    analysis_result.data, 
                    ensure_ascii=False, 
                    indent=2
                )[:500]  # 限制长度
            
            return AgentResponse(
                success=True,
                message=message,
                data=analysis_result.data,
                actions=["执行数据分析"]
            )
            
        except Exception as e:
            return AgentResponse(
                success=False,
                message=f"数据分析失败: {str(e)}",
                error=str(e)
            )
    
    async def _handle_report_generation(
        self,
        session,
        user_input: str,
        intent_result
    ) -> AgentResponse:
        """
        处理报表生成意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            intent_result: 意图识别结果
            
        Returns:
            AgentResponse: 响应结果
        """
        print("📄 处理报表生成...")
        
        # 检查是否有查询结果
        if not session.context.query_results:
            return AgentResponse(
                success=False,
                message="请先生成数据查询，然后再导出报表。",
                actions=["检查查询历史"]
            )
        
        try:
            # 获取最后一次查询结果
            last_result = session.context.query_results[-1]
            data_json = json.dumps(last_result.get("data", []))
            
            # 生成 Excel
            excel_result = await self._tool_registry.execute(
                "excel_generator",
                data_json=data_json,
                sheet_name="报表数据"
            )
            
            if not excel_result.success:
                return AgentResponse(
                    success=False,
                    message=f"报表生成失败: {excel_result.message}",
                    error=excel_result.error
                )
            
            # 获取文件信息
            file_path = excel_result.data.get("file_path", "")
            file_size = excel_result.data.get("file_size_kb", 0)
            row_count = excel_result.data.get("row_count", 0)
            
            message = (
                f"📊 Excel 报表生成成功！\n\n"
                f"文件: {file_path}\n"
                f"大小: {file_size:.1f} KB\n"
                f"数据: {row_count} 行"
            )
            
            return AgentResponse(
                success=True,
                message=message,
                data=excel_result.data,
                actions=["生成 Excel 报表"]
            )
            
        except Exception as e:
            return AgentResponse(
                success=False,
                message=f"报表生成失败: {str(e)}",
                error=str(e)
            )
    
    async def _handle_task_management(
        self,
        session,
        user_input: str,
        intent_result
    ) -> AgentResponse:
        """
        处理任务管理意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            intent_result: 意图识别结果
            
        Returns:
            AgentResponse: 响应结果
        """
        print("⏰ 处理任务管理...")
        
        # TODO: 实现定时任务管理
        return AgentResponse(
            success=True,
            message="任务管理功能正在开发中，敬请期待！",
            actions=["任务管理（待实现）"]
        )
    
    async def _handle_help(
        self,
        session,
        user_input: str
    ) -> AgentResponse:
        """
        处理帮助意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            
        Returns:
            AgentResponse: 响应结果
        """
        help_message = """
🤖 **我是 Dingding Machine 智能助手**

我可以帮您：

📊 **数据查询**
- "查一下昨天的航线收益"
- "统计本月 CPA 利润"
- "显示最近 7 天的航班数据"

📈 **数据分析**
- "分析一下收益趋势"
- "对比本月和上月的利润"
- "计算相关性分析"

📄 **报表生成**
- "生成 Excel 报表"
- "导出查询结果"

⏰ **定时任务**（开发中）
- "每天早上 9 点发送日报"

💡 **使用提示**
- 尽量使用具体的描述
- 可以指定时间范围（今天、昨天、本周、本月等）
- 支持多轮对话，可以基于上次查询继续分析

有什么可以帮您的吗？
        """.strip()
        
        return AgentResponse(
            success=True,
            message=help_message,
            actions=["提供帮助信息"]
        )
    
    async def _handle_greeting(
        self,
        session,
        user_input: str
    ) -> AgentResponse:
        """
        处理问候意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            
        Returns:
            AgentResponse: 响应结果
        """
        greetings = [
            "您好！我是 Dingding Machine 智能助手，有什么可以帮您的吗？",
            "你好！我可以帮您查询数据、分析数据、生成报表，需要做什么？",
            "您好！有什么数据相关的问题需要我帮忙吗？"
        ]
        
        import random
        message = random.choice(greetings)
        
        return AgentResponse(
            success=True,
            message=message,
            actions=["问候回复"]
        )
    
    async def _handle_clarification(
        self,
        session,
        user_input: str,
        intent_result
    ) -> AgentResponse:
        """
        处理需要澄清的意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            intent_result: 意图识别结果
            
        Returns:
            AgentResponse: 响应结果
        """
        message = (
            "抱歉，我不太理解您的需求。\n\n"
            "您可以尝试:\n"
            "- 更具体地描述您想查询的数据\n"
            "- 使用关键词如'查询'、'分析'、'生成报表'\n"
            "- 输入'帮助'查看使用指南"
        )
        
        return AgentResponse(
            success=True,
            message=message,
            actions=["请求澄清"]
        )
    
    async def _handle_unknown(
        self,
        session,
        user_input: str
    ) -> AgentResponse:
        """
        处理未知意图
        
        Args:
            session: 会话对象
            user_input: 用户输入
            
        Returns:
            AgentResponse: 响应结果
        """
        # 尝试使用 LLM 生成回复
        try:
            messages = [
                {"role": "system", "content": "你是一个数据助手，帮助用户查询和分析数据。"},
                {"role": "user", "content": user_input}
            ]
            
            # 添加历史上下文
            history = session.get_messages_for_llm(5)
            if history:
                messages = history + messages
            
            response = await self._llm_router.chat(
                messages,
                temperature=0.7
            )
            
            return AgentResponse(
                success=True,
                message=response,
                actions=["LLM 生成回复"]
            )
            
        except Exception as e:
            return AgentResponse(
                success=False,
                message="抱歉，我无法理解您的请求。请尝试输入'帮助'查看使用指南。",
                error=str(e)
            )
    
    async def _generate_sql(
        self,
        user_input: str,
        session
    ) -> Optional[str]:
        """
        使用 LLM 生成 SQL
        
        Args:
            user_input: 用户输入
            session: 会话对象
            
        Returns:
            Optional[str]: 生成的 SQL，失败返回 None
        """
        try:
            # 构建提示
            system_prompt = """你是一个 SQL 专家。根据用户的自然语言描述，生成对应的 Oracle SQL 查询语句。

要求：
1. 只返回 SQL 语句，不要返回解释
2. 使用标准的 Oracle SQL 语法
3. 表名和字段名使用大写
4. 时间条件使用 TO_DATE 函数
5. 限制返回行数（使用 ROWNUM）

常用表：
- TB_FOC_T1011D: 航班运营数据
- TB_CPA_PROFIT: CPA 利润数据

返回格式：
```sql
SELECT ... FROM ... WHERE ...
```
"""
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"请生成 SQL: {user_input}"}
            ]
            
            # 调用 LLM
            response = await self._llm_router.chat(
                messages,
                temperature=0.3,
                max_tokens=500
            )
            
            # 提取 SQL
            sql = self._extract_sql(response)
            
            return sql
            
        except Exception as e:
            print(f"生成 SQL 失败: {e}")
            return None
    
    def _extract_sql(self, text: str) -> Optional[str]:
        """
        从文本中提取 SQL
        
        Args:
            text: 包含 SQL 的文本
            
        Returns:
            Optional[str]: 提取的 SQL
        """
        import re
        
        # 尝试提取 ```sql ... ``` 格式的 SQL
        sql_match = re.search(r'```sql\s*(.*?)\s*```', text, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()
        
        # 尝试提取 ``` ... ``` 格式的 SQL
        sql_match = re.search(r'```\s*(SELECT.*?)\s*```', text, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()
        
        # 尝试直接提取 SELECT 语句
        sql_match = re.search(r'(SELECT\s+.*?)(?:;|$)', text, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()
        
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取编排器统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {
            "conversation_stats": self._conversation_manager.get_stats(),
            "tool_stats": self._tool_registry.get_all_stats(),
            "registered_tools": self._tool_registry.list_tools()
        }


# 全局编排器实例
_orchestrator_instance: Optional[AgentOrchestrator] = None


def get_orchestrator() -> AgentOrchestrator:
    """
    获取全局编排器实例
    
    Returns:
        AgentOrchestrator: 全局编排器实例
    """
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = AgentOrchestrator()
    return _orchestrator_instance
