"""
Agent 配置加载器

从 config.yaml 加载 LLM 配置，并初始化 LLM Router。
"""

import yaml
import os
from typing import Dict, Any, Optional

from .llm_router import LLMRouter, LLMProviderConfig, LLMProviderType


def load_llm_config(config_path: str = "src/config.yaml") -> Dict[str, Any]:
    """
    从 YAML 文件加载 LLM 配置
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Dict[str, Any]: LLM 配置字典
    """
    # 如果路径是相对的，基于项目根目录
    if not os.path.isabs(config_path):
        # 获取项目根目录（当前工作目录）
        project_root = os.getcwd()
        config_path = os.path.join(project_root, config_path)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config.get("llm_providers", {})


def init_llm_router_from_config(config_path: str = "src/config.yaml") -> LLMRouter:
    """
    从配置文件初始化 LLM Router
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        LLMRouter: 配置好的 LLM Router
    """
    router = LLMRouter()
    
    # 加载配置
    llm_config = load_llm_config(config_path)
    
    if not llm_config:
        print("⚠️  未找到 LLM 配置，请检查 config.yaml")
        return router
    
    # 注册主提供商（通义千问）
    if "primary" in llm_config:
        primary = llm_config["primary"]
        config = LLMProviderConfig(
            name=primary.get("name", "qwen"),
            provider_type=LLMProviderType.QWEN,
            api_key=primary["api_key"],
            api_base=primary["api_base"],
            model=primary["model"],
            timeout=30,
            max_retries=3,
            priority=1,  # 主提供商优先级最高
            weight=1.0,
            enabled=True
        )
        router.register_provider(config)
    
    # 注册备用提供商（DeepSeek）
    if "secondary" in llm_config:
        secondary = llm_config["secondary"]
        config = LLMProviderConfig(
            name=secondary.get("name", "deepseek"),
            provider_type=LLMProviderType.DEEPSEEK,
            api_key=secondary["api_key"],
            api_base=secondary["api_base"],
            model=secondary["model"],
            timeout=30,
            max_retries=3,
            priority=2,  # 备用提供商优先级较低
            weight=0.5,
            enabled=True
        )
        router.register_provider(config)
    
    print(f"✅ LLM Router 初始化完成，注册了 {len(router.list_providers())} 个提供商")
    
    return router


# 全局 LLM Router 实例（懒加载）
_llm_router_instance: Optional[LLMRouter] = None


def get_initialized_llm_router() -> LLMRouter:
    """
    获取已初始化的 LLM Router（单例）
    
    Returns:
        LLMRouter: 已配置的 LLM Router
    """
    global _llm_router_instance
    if _llm_router_instance is None:
        _llm_router_instance = init_llm_router_from_config()
    return _llm_router_instance
