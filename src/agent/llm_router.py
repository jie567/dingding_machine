"""
LLM Router 模块

负责管理多个 LLM 提供商，实现智能路由、负载均衡和降级策略。
支持通义千问、DeepSeek 和本地模型，确保系统的高可用性。
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
import random
import asyncio
from abc import ABC, abstractmethod


class LLMProviderType(Enum):
    """LLM 提供商类型枚举"""
    QWEN = "qwen"           # 通义千问（阿里云）
    DEEPSEEK = "deepseek"   # DeepSeek
    LOCAL = "local"         # 本地模型（Ollama）


class LLMStatus(Enum):
    """LLM 服务状态枚举"""
    HEALTHY = "healthy"         # 健康
    DEGRADED = "degraded"       # 降级（响应慢）
    UNAVAILABLE = "unavailable" # 不可用


@dataclass
class LLMProviderConfig:
    """
    LLM 提供商配置
    
    Attributes:
        name: 提供商名称
        provider_type: 提供商类型
        api_key: API 密钥
        api_base: API 基础地址
        model: 模型名称
        timeout: 请求超时时间（秒）
        max_retries: 最大重试次数
        priority: 优先级（数字越小优先级越高）
        weight: 权重（用于负载均衡）
        enabled: 是否启用
    """
    name: str
    provider_type: LLMProviderType
    api_key: str
    api_base: str
    model: str
    timeout: float = 30.0
    max_retries: int = 3
    priority: int = 1
    weight: float = 1.0
    enabled: bool = True


@dataclass
class LLMHealthStatus:
    """
    LLM 健康状态
    
    Attributes:
        provider_name: 提供商名称
        status: 服务状态
        last_check_time: 最后检查时间
        avg_response_time: 平均响应时间（秒）
        error_count: 错误计数
        success_count: 成功计数
        last_error: 最后错误信息
    """
    provider_name: str
    status: LLMStatus = LLMStatus.HEALTHY
    last_check_time: float = field(default_factory=time.time)
    avg_response_time: float = 0.0
    error_count: int = 0
    success_count: int = 0
    last_error: Optional[str] = None
    
    @property
    def success_rate(self) -> float:
        """计算成功率"""
        total = self.success_count + self.error_count
        if total == 0:
            return 1.0
        return self.success_count / total
    
    @property
    def is_available(self) -> bool:
        """检查是否可用"""
        return self.status != LLMStatus.UNAVAILABLE


class LLMProvider(ABC):
    """
    LLM 提供商抽象基类
    
    所有 LLM 提供商必须实现此接口，确保统一调用方式。
    """
    
    def __init__(self, config: LLMProviderConfig):
        """
        初始化 LLM 提供商
        
        Args:
            config: 提供商配置
        """
        self.config = config
        self.health = LLMHealthStatus(provider_name=config.name)
    
    @abstractmethod
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        发送聊天请求
        
        Args:
            messages: 消息列表，格式为 [{"role": "user", "content": "..."}]
            **kwargs: 额外参数（temperature, max_tokens 等）
            
        Returns:
            str: LLM 的回复内容
            
        Raises:
            Exception: 请求失败时抛出
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """
        健康检查
        
        Returns:
            bool: 服务是否正常
        """
        pass
    
    def record_success(self, response_time: float):
        """
        记录成功请求
        
        Args:
            response_time: 响应时间（秒）
        """
        self.health.success_count += 1
        self.health.last_check_time = time.time()
        
        # 更新平均响应时间（指数移动平均）
        alpha = 0.3
        self.health.avg_response_time = (
            alpha * response_time + 
            (1 - alpha) * self.health.avg_response_time
        )
        
        # 如果响应时间超过阈值，标记为降级
        if self.health.avg_response_time > self.config.timeout * 0.8:
            self.health.status = LLMStatus.DEGRADED
        else:
            self.health.status = LLMStatus.HEALTHY
    
    def record_error(self, error_message: str):
        """
        记录失败请求
        
        Args:
            error_message: 错误信息
        """
        self.health.error_count += 1
        self.health.last_error = error_message
        self.health.last_check_time = time.time()
        
        # 如果错误率过高，标记为不可用
        if self.health.success_rate < 0.5 and self.health.error_count > 3:
            self.health.status = LLMStatus.UNAVAILABLE


class QwenProvider(LLMProvider):
    """
    通义千问提供商
    
    阿里云通义千问大模型，适合中文场景和数据分析任务。
    """
    
    def __init__(self, config: LLMProviderConfig):
        """
        初始化通义千问提供商
        
        Args:
            config: 提供商配置
        """
        super().__init__(config)
        self._client = None
    
    async def _get_client(self):
        """
        获取或创建 HTTP 客户端（懒加载）
        
        Returns:
            httpx.AsyncClient: 异步 HTTP 客户端
        """
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(
                base_url=self.config.api_base,
                timeout=self.config.timeout,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json"
                }
            )
        return self._client
    
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        发送聊天请求到通义千问
        
        Args:
            messages: 消息列表
            **kwargs: 额外参数
            
        Returns:
            str: 模型回复内容
        """
        start_time = time.time()
        
        try:
            client = await self._get_client()
            
            # 构建请求体
            payload = {
                "model": self.config.model,
                "messages": messages,
                "temperature": kwargs.get("temperature", 0.7),
                "max_tokens": kwargs.get("max_tokens", 2000)
            }
            
            # 发送请求
            response = await client.post("/chat/completions", json=payload)
            response.raise_for_status()
            
            # 解析响应
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            # 记录成功
            self.record_success(time.time() - start_time)
            
            return content
            
        except Exception as e:
            # 记录错误
            self.record_error(str(e))
            raise Exception(f"通义千问请求失败: {str(e)}")
    
    async def health_check(self) -> bool:
        """
        检查通义千问服务状态
        
        Returns:
            bool: 服务是否正常
        """
        try:
            # 发送一个简单的测试请求
            await self.chat([{"role": "user", "content": "你好"}], max_tokens=10)
            return True
        except Exception:
            return False


class DeepSeekProvider(LLMProvider):
    """
    DeepSeek 提供商
    
    DeepSeek 大模型，推理能力强，性价比高。
    """
    
    def __init__(self, config: LLMProviderConfig):
        """
        初始化 DeepSeek 提供商
        
        Args:
            config: 提供商配置
        """
        super().__init__(config)
        self._client = None
    
    async def _get_client(self):
        """获取或创建 HTTP 客户端"""
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(
                base_url=self.config.api_base,
                timeout=self.config.timeout,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json"
                }
            )
        return self._client
    
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        发送聊天请求到 DeepSeek
        
        Args:
            messages: 消息列表
            **kwargs: 额外参数
            
        Returns:
            str: 模型回复内容
        """
        start_time = time.time()
        
        try:
            client = await self._get_client()
            
            payload = {
                "model": self.config.model,
                "messages": messages,
                "temperature": kwargs.get("temperature", 0.7),
                "max_tokens": kwargs.get("max_tokens", 2000)
            }
            
            response = await client.post("/chat/completions", json=payload)
            response.raise_for_status()
            
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            self.record_success(time.time() - start_time)
            
            return content
            
        except Exception as e:
            self.record_error(str(e))
            raise Exception(f"DeepSeek 请求失败: {str(e)}")
    
    async def health_check(self) -> bool:
        """检查 DeepSeek 服务状态"""
        try:
            await self.chat([{"role": "user", "content": "Hello"}], max_tokens=10)
            return True
        except Exception:
            return False


class LocalProvider(LLMProvider):
    """
    本地模型提供商（Ollama）
    
    通过 Ollama 运行本地模型，用于离线场景和敏感数据处理。
    """
    
    def __init__(self, config: LLMProviderConfig):
        """
        初始化本地模型提供商
        
        Args:
            config: 提供商配置
        """
        super().__init__(config)
        self._client = None
    
    async def _get_client(self):
        """获取或创建 HTTP 客户端"""
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(
                base_url=self.config.api_base,
                timeout=self.config.timeout
            )
        return self._client
    
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        发送聊天请求到本地模型
        
        Args:
            messages: 消息列表
            **kwargs: 额外参数
            
        Returns:
            str: 模型回复内容
        """
        start_time = time.time()
        
        try:
            client = await self._get_client()
            
            # Ollama API 格式
            payload = {
                "model": self.config.model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": kwargs.get("temperature", 0.7),
                    "num_predict": kwargs.get("max_tokens", 2000)
                }
            }
            
            response = await client.post("/api/chat", json=payload)
            response.raise_for_status()
            
            data = response.json()
            content = data["message"]["content"]
            
            self.record_success(time.time() - start_time)
            
            return content
            
        except Exception as e:
            self.record_error(str(e))
            raise Exception(f"本地模型请求失败: {str(e)}")
    
    async def health_check(self) -> bool:
        """检查本地模型服务状态"""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.config.api_base}/api/tags")
                return response.status_code == 200
        except Exception:
            return False


class LLMRouter:
    """
    LLM 路由器
    
    核心组件，负责：
    1. 管理多个 LLM 提供商
    2. 实现智能路由策略（优先级、权重、健康状态）
    3. 自动降级和故障转移
    4. 负载均衡
    
    使用示例：
        router = LLMRouter()
        router.register_provider(QwenConfig(...))
        router.register_provider(DeepSeekConfig(...))
        
        # 自动选择最佳提供商
        response = await router.chat(messages)
        
        # 指定提供商
        response = await router.chat(messages, provider="deepseek")
    """
    
    def __init__(self):
        """初始化 LLM 路由器"""
        # 存储所有注册的提供商
        self._providers: Dict[str, LLMProvider] = {}
        
        # 存储提供商配置
        self._configs: Dict[str, LLMProviderConfig] = {}
        
        # 健康检查间隔（秒）
        self._health_check_interval = 60
        
        # 是否启用自动健康检查
        self._auto_health_check = True
        
        # 健康检查任务
        self._health_check_task = None
    
    def register_provider(self, config: LLMProviderConfig) -> None:
        """
        注册 LLM 提供商
        
        Args:
            config: 提供商配置
            
        Raises:
            ValueError: 提供商名称已存在时抛出
        """
        if config.name in self._providers:
            raise ValueError(f"提供商 '{config.name}' 已存在")
        
        # 根据类型创建对应的提供商实例
        if config.provider_type == LLMProviderType.QWEN:
            provider = QwenProvider(config)
        elif config.provider_type == LLMProviderType.DEEPSEEK:
            provider = DeepSeekProvider(config)
        elif config.provider_type == LLMProviderType.LOCAL:
            provider = LocalProvider(config)
        else:
            raise ValueError(f"不支持的提供商类型: {config.provider_type}")
        
        self._providers[config.name] = provider
        self._configs[config.name] = config
        
        print(f"✅ 已注册 LLM 提供商: {config.name} ({config.provider_type.value})")
    
    def unregister_provider(self, name: str) -> None:
        """
        注销 LLM 提供商
        
        Args:
            name: 提供商名称
        """
        if name in self._providers:
            del self._providers[name]
            del self._configs[name]
            print(f"🗑️ 已注销 LLM 提供商: {name}")
    
    def get_provider(self, name: Optional[str] = None) -> LLMProvider:
        """
        获取 LLM 提供商
        
        如果未指定名称，自动选择最佳提供商（基于优先级、权重和健康状态）。
        
        Args:
            name: 提供商名称（可选）
            
        Returns:
            LLMProvider: 提供商实例
            
        Raises:
            Exception: 没有可用的提供商时抛出
        """
        if name:
            # 指定了提供商名称
            if name not in self._providers:
                raise ValueError(f"提供商 '{name}' 不存在")
            
            provider = self._providers[name]
            if not provider.health.is_available:
                raise Exception(f"提供商 '{name}' 当前不可用")
            
            return provider
        
        # 自动选择最佳提供商
        available_providers = [
            (name, p) for name, p in self._providers.items()
            if p.health.is_available and self._configs[name].enabled
        ]
        
        if not available_providers:
            raise Exception("没有可用的 LLM 提供商")
        
        # 按优先级排序（数字小的优先级高）
        available_providers.sort(
            key=lambda x: (
                self._configs[x[0]].priority,
                -x[1].health.success_rate,  # 成功率高的优先
                x[1].health.avg_response_time  # 响应时间短的优先
            )
        )
        
        # 如果有多个同优先级的提供商，按权重随机选择
        best_priority = self._configs[available_providers[0][0]].priority
        candidates = [
            (name, p) for name, p in available_providers
            if self._configs[name].priority == best_priority
        ]
        
        if len(candidates) == 1:
            return candidates[0][1]
        
        # 按权重随机选择
        weights = [self._configs[name].weight for name, _ in candidates]
        total_weight = sum(weights)
        probabilities = [w / total_weight for w in weights]
        
        selected_idx = random.choices(
            range(len(candidates)), 
            weights=probabilities,
            k=1
        )[0]
        
        selected_name, selected_provider = candidates[selected_idx]
        print(f"🎯 自动选择 LLM 提供商: {selected_name}")
        
        return selected_provider
    
    async def chat(
        self, 
        messages: List[Dict[str, str]], 
        provider: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        发送聊天请求（自动路由）
        
        如果指定了提供商，直接使用；否则自动选择最佳提供商。
        如果请求失败，会自动尝试其他提供商（故障转移）。
        
        Args:
            messages: 消息列表
            provider: 指定提供商名称（可选）
            **kwargs: 额外参数（temperature, max_tokens 等）
            
        Returns:
            str: LLM 的回复内容
            
        Raises:
            Exception: 所有提供商都失败时抛出
        """
        # 获取提供商列表（优先使用指定的，否则自动选择）
        if provider:
            providers_to_try = [provider]
        else:
            # 按优先级排序的所有可用提供商
            providers_to_try = [
                name for name, p in self._providers.items()
                if p.health.is_available and self._configs[name].enabled
            ]
            providers_to_try.sort(
                key=lambda x: self._configs[x].priority
            )
        
        # 依次尝试每个提供商
        last_error = None
        for provider_name in providers_to_try:
            try:
                provider = self._providers[provider_name]
                
                print(f"🔄 尝试使用 '{provider_name}' 处理请求...")
                
                response = await provider.chat(messages, **kwargs)
                
                print(f"✅ '{provider_name}' 请求成功")
                
                return response
                
            except Exception as e:
                last_error = e
                print(f"❌ '{provider_name}' 请求失败: {str(e)}")
                
                # 记录错误
                if provider_name in self._providers:
                    self._providers[provider_name].record_error(str(e))
                
                # 继续尝试下一个提供商
                continue
        
        # 所有提供商都失败了
        raise Exception(f"所有 LLM 提供商都不可用。最后错误: {str(last_error)}")
    
    async def health_check(self, provider_name: Optional[str] = None) -> Dict[str, bool]:
        """
        健康检查
        
        Args:
            provider_name: 指定提供商名称（可选，检查所有）
            
        Returns:
            Dict[str, bool]: 提供商名称 -> 健康状态
        """
        if provider_name:
            providers = {provider_name: self._providers.get(provider_name)}
        else:
            providers = self._providers
        
        results = {}
        for name, provider in providers.items():
            try:
                is_healthy = await provider.health_check()
                results[name] = is_healthy
                
                # 更新健康状态
                if is_healthy:
                    provider.health.status = LLMStatus.HEALTHY
                else:
                    provider.health.status = LLMStatus.UNAVAILABLE
                    
            except Exception as e:
                results[name] = False
                provider.health.status = LLMStatus.UNAVAILABLE
                provider.health.last_error = str(e)
        
        return results
    
    def get_health_status(self) -> Dict[str, LLMHealthStatus]:
        """
        获取所有提供商的健康状态
        
        Returns:
            Dict[str, LLMHealthStatus]: 提供商名称 -> 健康状态
        """
        return {
            name: provider.health 
            for name, provider in self._providers.items()
        }
    
    async def start_auto_health_check(self):
        """启动自动健康检查（后台任务）"""
        if self._health_check_task is not None:
            return
        
        self._auto_health_check = True
        
        async def _check_loop():
            while self._auto_health_check:
                try:
                    await self.health_check()
                    await asyncio.sleep(self._health_check_interval)
                except Exception as e:
                    print(f"健康检查出错: {e}")
                    await asyncio.sleep(10)
        
        self._health_check_task = asyncio.create_task(_check_loop())
        print("🩺 自动健康检查已启动")
    
    async def stop_auto_health_check(self):
        """停止自动健康检查"""
        self._auto_health_check = False
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
        print("🛑 自动健康检查已停止")
    
    def list_providers(self) -> List[str]:
        """
        列出所有已注册的提供商
        
        Returns:
            List[str]: 提供商名称列表
        """
        return list(self._providers.keys())
    
    @classmethod
    def from_config(cls, config_path: str) -> "LLMRouter":
        """
        从配置文件创建 LLM 路由器
        
        Args:
            config_path: 配置文件路径（YAML 格式）
            
        Returns:
            LLMRouter: 配置好的路由器实例
        """
        import yaml
        
        router = cls()
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        llm_configs = config_data.get("llm_providers", {})
        
        for name, provider_config in llm_configs.items():
            provider_type = LLMProviderType(provider_config.pop("type"))
            
            config = LLMProviderConfig(
                name=name,
                provider_type=provider_type,
                **provider_config
            )
            
            router.register_provider(config)
        
        return router


# 全局路由器实例（单例模式）
_router_instance: Optional[LLMRouter] = None


def get_llm_router() -> LLMRouter:
    """
    获取全局 LLM 路由器实例
    
    Returns:
        LLMRouter: 全局路由器实例
    """
    global _router_instance
    if _router_instance is None:
        _router_instance = LLMRouter()
    return _router_instance
