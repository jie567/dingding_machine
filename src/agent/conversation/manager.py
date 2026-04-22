"""
对话管理器模块

管理多轮对话的上下文、会话状态和历史记录。
支持会话创建、消息管理、上下文维护等功能。
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import uuid
import json


@dataclass
class Message:
    """
    对话消息
    
    封装单条消息的信息，包括角色、内容和时间戳。
    
    Attributes:
        role: 消息角色 (user/assistant/system)
        content: 消息内容
        timestamp: 消息时间戳
        metadata: 额外元数据（如工具调用信息）
    """
    role: str  # user, assistant, system
    content: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "role": self.role,
            "content": self.content,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """从字典创建"""
        return cls(
            role=data["role"],
            content=data["content"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metadata=data.get("metadata", {})
        )


@dataclass
class ConversationContext:
    """
    对话上下文
    
    存储对话过程中的临时数据和状态。
    
    Attributes:
        query_intent: 当前查询意图
        data_source: 当前使用的数据源
        query_results: 查询结果缓存
        analysis_results: 分析结果缓存
        pending_tasks: 待处理任务
        custom_data: 自定义数据
    """
    query_intent: Optional[str] = None
    data_source: Optional[str] = None
    query_results: List[Dict[str, Any]] = field(default_factory=list)
    analysis_results: List[Dict[str, Any]] = field(default_factory=list)
    pending_tasks: List[str] = field(default_factory=list)
    custom_data: Dict[str, Any] = field(default_factory=dict)
    
    def add_query_result(self, result: Dict[str, Any]):
        """添加查询结果"""
        self.query_results.append(result)
    
    def add_analysis_result(self, result: Dict[str, Any]):
        """添加分析结果"""
        self.analysis_results.append(result)
    
    def clear(self):
        """清空上下文"""
        self.query_intent = None
        self.data_source = None
        self.query_results.clear()
        self.analysis_results.clear()
        self.pending_tasks.clear()
        self.custom_data.clear()


@dataclass
class ConversationSession:
    """
    对话会话
    
    封装一个完整的对话会话，包含会话信息、消息历史和上下文。
    
    Attributes:
        session_id: 会话唯一标识
        chat_id: 钉钉群聊 ID
        user_id: 用户 ID
        messages: 消息历史
        context: 对话上下文
        created_at: 创建时间
        updated_at: 最后更新时间
        is_active: 是否活跃
    """
    session_id: str
    chat_id: str
    user_id: str
    messages: List[Message] = field(default_factory=list)
    context: ConversationContext = field(default_factory=ConversationContext)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    is_active: bool = True
    
    def add_message(self, role: str, content: str, metadata: Optional[Dict] = None):
        """
        添加消息
        
        Args:
            role: 消息角色
            content: 消息内容
            metadata: 元数据
        """
        message = Message(
            role=role,
            content=content,
            metadata=metadata or {}
        )
        self.messages.append(message)
        self.updated_at = datetime.now()
    
    def get_recent_messages(self, count: int = 10) -> List[Message]:
        """
        获取最近的消息
        
        Args:
            count: 消息数量
            
        Returns:
            List[Message]: 最近的消息列表
        """
        return self.messages[-count:] if len(self.messages) > count else self.messages
    
    def get_messages_for_llm(self, count: int = 10) -> List[Dict[str, str]]:
        """
        获取用于 LLM 的消息格式
        
        Args:
            count: 消息数量
            
        Returns:
            List[Dict[str, str]]: LLM 格式的消息列表
        """
        recent_messages = self.get_recent_messages(count)
        return [
            {"role": msg.role, "content": msg.content}
            for msg in recent_messages
        ]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "session_id": self.session_id,
            "chat_id": self.chat_id,
            "user_id": self.user_id,
            "messages": [msg.to_dict() for msg in self.messages],
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "is_active": self.is_active,
            "message_count": len(self.messages)
        }


class ConversationManager:
    """
    对话管理器
    
    管理所有对话会话，提供会话创建、查找、维护和清理功能。
    
    使用示例：
        manager = ConversationManager()
        
        # 创建会话
        session = manager.create_session(chat_id="xxx", user_id="yyy")
        
        # 添加消息
        session.add_message("user", "查一下昨天的航线收益")
        
        # 获取会话
        session = manager.get_session(session.session_id)
        
        # 获取历史消息
        messages = session.get_messages_for_llm()
    """
    
    def __init__(self, max_history: int = 20, session_timeout: int = 3600):
        """
        初始化对话管理器
        
        Args:
            max_history: 最大历史消息数
            session_timeout: 会话超时时间（秒）
        """
        # 存储所有会话
        self._sessions: Dict[str, ConversationSession] = {}
        
        # 按 chat_id 索引会话
        self._chat_sessions: Dict[str, List[str]] = {}
        
        # 最大历史消息数
        self._max_history = max_history
        
        # 会话超时时间
        self._session_timeout = session_timeout
    
    def create_session(self, chat_id: str, user_id: str) -> ConversationSession:
        """
        创建新会话
        
        Args:
            chat_id: 钉钉群聊 ID
            user_id: 用户 ID
            
        Returns:
            ConversationSession: 新创建的会话
        """
        # 生成唯一会话 ID
        session_id = str(uuid.uuid4())
        
        # 创建会话
        session = ConversationSession(
            session_id=session_id,
            chat_id=chat_id,
            user_id=user_id
        )
        
        # 存储会话
        self._sessions[session_id] = session
        
        # 更新索引
        if chat_id not in self._chat_sessions:
            self._chat_sessions[chat_id] = []
        self._chat_sessions[chat_id].append(session_id)
        
        print(f"🆕 创建新会话: {session_id[:8]}... (chat: {chat_id})")
        
        return session
    
    def get_session(self, session_id: str) -> Optional[ConversationSession]:
        """
        获取会话
        
        Args:
            session_id: 会话 ID
            
        Returns:
            Optional[ConversationSession]: 会话对象，不存在返回 None
        """
        return self._sessions.get(session_id)
    
    def get_or_create_session(
        self, 
        chat_id: str, 
        user_id: str,
        create_new: bool = False
    ) -> ConversationSession:
        """
        获取或创建会话
        
        如果存在活跃的会话，返回该会话；否则创建新会话。
        
        Args:
            chat_id: 钉钉群聊 ID
            user_id: 用户 ID
            create_new: 是否强制创建新会话
            
        Returns:
            ConversationSession: 会话对象
        """
        if not create_new and chat_id in self._chat_sessions:
            # 查找该群聊的活跃会话
            for session_id in reversed(self._chat_sessions[chat_id]):
                session = self._sessions.get(session_id)
                if session and session.is_active:
                    # 检查会话是否超时
                    if self._is_session_valid(session):
                        print(f"📌 复用会话: {session_id[:8]}...")
                        return session
                    else:
                        # 会话超时，标记为不活跃
                        session.is_active = False
        
        # 创建新会话
        return self.create_session(chat_id, user_id)
    
    def _is_session_valid(self, session: ConversationSession) -> bool:
        """
        检查会话是否有效（未超时）
        
        Args:
            session: 会话对象
            
        Returns:
            bool: 是否有效
        """
        elapsed = (datetime.now() - session.updated_at).total_seconds()
        return elapsed < self._session_timeout
    
    def add_message(
        self, 
        session_id: str, 
        role: str, 
        content: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """
        添加消息到会话
        
        Args:
            session_id: 会话 ID
            role: 消息角色
            content: 消息内容
            metadata: 元数据
            
        Returns:
            bool: 是否添加成功
        """
        session = self._sessions.get(session_id)
        if not session:
            return False
        
        # 添加消息
        session.add_message(role, content, metadata)
        
        # 限制历史消息数量
        if len(session.messages) > self._max_history:
            # 保留系统消息和最近的消息
            system_messages = [m for m in session.messages if m.role == "system"]
            other_messages = [m for m in session.messages if m.role != "system"]
            
            # 保留最近的 max_history - len(system_messages) 条非系统消息
            keep_count = self._max_history - len(system_messages)
            other_messages = other_messages[-keep_count:] if keep_count > 0 else []
            
            session.messages = system_messages + other_messages
        
        return True
    
    def close_session(self, session_id: str) -> bool:
        """
        关闭会话
        
        Args:
            session_id: 会话 ID
            
        Returns:
            bool: 是否关闭成功
        """
        session = self._sessions.get(session_id)
        if session:
            session.is_active = False
            print(f"🔒 关闭会话: {session_id[:8]}...")
            return True
        return False
    
    def cleanup_expired_sessions(self) -> int:
        """
        清理过期会话
        
        Returns:
            int: 清理的会话数量
        """
        expired_count = 0
        expired_sessions = []
        
        for session_id, session in self._sessions.items():
            if not session.is_active or not self._is_session_valid(session):
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            del self._sessions[session_id]
            expired_count += 1
            
            # 更新索引
            for chat_id, session_ids in self._chat_sessions.items():
                if session_id in session_ids:
                    session_ids.remove(session_id)
        
        if expired_count > 0:
            print(f"🧹 清理了 {expired_count} 个过期会话")
        
        return expired_count
    
    def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        获取会话信息
        
        Args:
            session_id: 会话 ID
            
        Returns:
            Optional[Dict[str, Any]]: 会话信息
        """
        session = self._sessions.get(session_id)
        if session:
            return session.to_dict()
        return None
    
    def list_sessions(self, chat_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        列出会话
        
        Args:
            chat_id: 群聊 ID（可选，列出所有会话）
            
        Returns:
            List[Dict[str, Any]]: 会话信息列表
        """
        if chat_id:
            session_ids = self._chat_sessions.get(chat_id, [])
        else:
            session_ids = list(self._sessions.keys())
        
        return [
            self._sessions[sid].to_dict()
            for sid in session_ids
            if sid in self._sessions
        ]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        active_sessions = sum(1 for s in self._sessions.values() if s.is_active)
        total_messages = sum(len(s.messages) for s in self._sessions.values())
        
        return {
            "total_sessions": len(self._sessions),
            "active_sessions": active_sessions,
            "total_messages": total_messages,
            "avg_messages_per_session": (
                total_messages / len(self._sessions) 
                if self._sessions else 0
            )
        }


# 全局对话管理器实例
_conversation_manager: Optional[ConversationManager] = None


def get_conversation_manager() -> ConversationManager:
    """
    获取全局对话管理器
    
    Returns:
        ConversationManager: 全局对话管理器实例
    """
    global _conversation_manager
    if _conversation_manager is None:
        _conversation_manager = ConversationManager()
    return _conversation_manager
