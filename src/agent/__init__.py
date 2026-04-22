"""
Agent 核心模块

提供智能 Data Agent 的核心功能，包括：
- LLM 路由和管理
- 对话管理
- 意图识别
- 工具调用
- 任务编排
"""

from .llm_router import (
    LLMRouter,
    LLMProvider,
    LLMProviderConfig,
    LLMProviderType,
    LLMHealthStatus,
    LLMStatus,
    QwenProvider,
    DeepSeekProvider,
    LocalProvider,
    get_llm_router
)

from .orchestrator import (
    AgentOrchestrator,
    AgentResponse,
    get_orchestrator
)

from .conversation.manager import (
    ConversationManager,
    ConversationSession,
    ConversationContext,
    Message,
    get_conversation_manager
)

from .conversation.intent import (
    IntentRecognizer,
    IntentType,
    IntentRecognitionResult
)

from .tools.base import (
    BaseTool,
    ToolResult,
    ToolRegistry,
    get_tool_registry
)

__all__ = [
    # LLM 路由
    "LLMRouter",
    "LLMProvider",
    "LLMProviderConfig",
    "LLMProviderType",
    "LLMHealthStatus",
    "LLMStatus",
    "QwenProvider",
    "DeepSeekProvider",
    "LocalProvider",
    "get_llm_router",
    
    # 编排器
    "AgentOrchestrator",
    "AgentResponse",
    "get_orchestrator",
    
    # 对话管理
    "ConversationManager",
    "ConversationSession",
    "ConversationContext",
    "Message",
    "get_conversation_manager",
    
    # 意图识别
    "IntentRecognizer",
    "IntentType",
    "IntentRecognitionResult",
    
    # 工具
    "BaseTool",
    "ToolResult",
    "ToolRegistry",
    "get_tool_registry"
]
