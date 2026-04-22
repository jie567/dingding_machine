"""
数据源注册中心

管理所有数据源的注册、查找和健康检查
"""
from typing import Dict, List, Optional, Type
import yaml
import os

from .base import DataSource, DataSourceConfig
from .adapters.oracle_adapter import OracleAdapter, OracleConfig
from .adapters.postgresql_adapter import PostgreSQLAdapter, PostgreSQLConfig
from .adapters.doris_adapter import DorisAdapter, DorisConfig
from .exceptions import DataSourceException, DataSourceNotFoundException


class DataSourceRegistry:
    """数据源注册中心"""
    
    _adapters: Dict[str, Type[DataSource]] = {
        "oracle": OracleAdapter,
        "postgresql": PostgreSQLAdapter,
        "doris": DorisAdapter,
    }
    
    _configs: Dict[str, Type[DataSourceConfig]] = {
        "oracle": OracleConfig,
        "postgresql": PostgreSQLConfig,
        "doris": DorisConfig,
    }
    
    _instance: Optional["DataSourceRegistry"] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._data_sources = {}
            cls._instance._default_source = None
        return cls._instance
    
    def register(self, config: DataSourceConfig) -> DataSource:
        """
        注册数据源
        
        Args:
            config: 数据源配置
            
        Returns:
            DataSource: 数据源实例
        """
        source_type = config.type
        
        if source_type not in self._adapters:
            raise DataSourceException(f"不支持的数据源类型: {source_type}")
        
        adapter_class = self._adapters[source_type]
        config_class = self._configs[source_type]
        
        if not isinstance(config, config_class):
            config = config_class(**config.dict())
        
        adapter = adapter_class(config)
        
        self._data_sources[config.name] = adapter
        
        if config.default or self._default_source is None:
            self._default_source = config.name
        
        return adapter
    
    def unregister(self, name: str) -> None:
        """注销数据源"""
        if name in self._data_sources:
            del self._data_sources[name]
            
            if self._default_source == name:
                self._default_source = None
    
    def get(self, name: Optional[str] = None) -> DataSource:
        """
        获取数据源
        
        Args:
            name: 数据源名称，如果为 None 则返回默认数据源
            
        Returns:
            DataSource: 数据源实例
        """
        if name is None:
            name = self._default_source
        
        if name is None:
            raise DataSourceException("未指定数据源且没有默认数据源")
        
        if name not in self._data_sources:
            raise DataSourceNotFoundException(f"数据源不存在: {name}")
        
        return self._data_sources[name]
    
    def list_sources(self) -> List[str]:
        """列出所有已注册的数据源"""
        return list(self._data_sources.keys())
    
    def get_by_type(self, source_type: str) -> List[DataSource]:
        """按类型获取数据源"""
        return [
            ds for ds in self._data_sources.values()
            if ds.config.type == source_type
        ]
    
    async def health_check(self, name: Optional[str] = None) -> Dict[str, bool]:
        """
        健康检查
        
        Args:
            name: 指定数据源名称，如果为 None 则检查所有
            
        Returns:
            Dict[str, bool]: 数据源名称 -> 健康状态
        """
        if name:
            sources = {name: self._data_sources.get(name)}
        else:
            sources = self._data_sources
        
        results = {}
        for source_name, source in sources.items():
            try:
                results[source_name] = await source.test_connection()
            except Exception:
                results[source_name] = False
        
        return results
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "DataSourceRegistry":
        """
        从 YAML 文件加载数据源配置
        
        Args:
            yaml_path: YAML 配置文件路径
            
        Returns:
            DataSourceRegistry: 注册中心实例
        """
        registry = cls()
        
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        data_sources = config_data.get("data_sources", {})
        
        for name, source_config in data_sources.items():
            source_config = cls._resolve_env_vars(source_config)
            source_config["name"] = name
            
            source_type = source_config.get("type")
            config_class = cls._configs.get(source_type)
            
            if config_class:
                config = config_class(**source_config)
                registry.register(config)
        
        return registry
    
    @staticmethod
    def _resolve_env_vars(config: dict) -> dict:
        """解析环境变量引用"""
        import re
        
        def resolve_value(value):
            if isinstance(value, str):
                pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
                
                def replace_env(match):
                    var_name = match.group(1)
                    default_value = match.group(2)
                    return os.getenv(var_name, default_value or "")
                
                return re.sub(pattern, replace_env, value)
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(item) for item in value]
            else:
                return value
        
        return resolve_value(config)
    
    @classmethod
    def get_instance(cls) -> "DataSourceRegistry":
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
