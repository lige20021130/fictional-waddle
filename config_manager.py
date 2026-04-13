# config_manager.py - 统一配置管理模块
"""
纳米酶文献提取系统 - 统一配置管理

功能：
1. 集中管理所有配置项
2. 支持热重载
3. 单例模式确保配置全局一致
4. 配置验证和默认值

使用方法：
    from config_manager import ConfigManager
    
    config = ConfigManager.get_instance()
    llm_config = config.llm
    pipeline_config = config.pipeline
"""

import os
import yaml
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class LLMConfig:
    """文本提取 LLM 配置"""
    base_url: str
    api_key: str
    model: str
    max_retries: int = 3
    temperature: float = 0.1
    max_tokens: int = 4096
    timeout: int = 120
    
    def validate(self) -> bool:
        """验证配置有效性"""
        if not self.base_url:
            return False
        if not self.api_key or self.api_key in ['your-deepseek-api-key', 'your-key', '']:
            return False
        if not self.model:
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'base_url': self.base_url,
            'api_key': self.api_key,
            'model': self.model,
            'max_retries': self.max_retries,
            'temperature': self.temperature,
            'max_tokens': self.max_tokens,
            'timeout': self.timeout
        }


@dataclass
class VLMConfig:
    """图像分析 VLM 配置"""
    base_url: str
    api_key: str
    model: str
    max_retries: int = 3
    max_tokens: int = 2048
    timeout: int = 180
    
    def validate(self) -> bool:
        """验证配置有效性"""
        if not self.base_url:
            return False
        if not self.api_key or self.api_key in ['your-openai-api-key', 'your-key', '']:
            return False
        if not self.model:
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'base_url': self.base_url,
            'api_key': self.api_key,
            'model': self.model,
            'max_retries': self.max_retries,
            'max_tokens': self.max_tokens,
            'timeout': self.timeout
        }


@dataclass
class PipelineConfig:
    """管道配置"""
    chunk_batch_size: int = 5
    vlm_batch_size: int = 2
    confidence_threshold: float = 0.7
    results_dir: Path = field(default_factory=lambda: Path("./extraction_results"))
    rulebook_path: Path = field(default_factory=lambda: Path("./rulebook.json"))
    cache_dir: Path = field(default_factory=lambda: Path("./cache"))
    task_queue_path: Path = field(default_factory=lambda: Path("./task_queue.json"))
    enable_cache: bool = True
    enable_rag: bool = False
    rag_top_k: int = 10
    
    def __post_init__(self):
        """后处理：确保路径类型正确"""
        if isinstance(self.results_dir, str):
            self.results_dir = Path(self.results_dir)
        if isinstance(self.rulebook_path, str):
            self.rulebook_path = Path(self.rulebook_path)
        if isinstance(self.cache_dir, str):
            self.cache_dir = Path(self.cache_dir)
        if isinstance(self.task_queue_path, str):
            self.task_queue_path = Path(self.task_queue_path)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'chunk_batch_size': self.chunk_batch_size,
            'vlm_batch_size': self.vlm_batch_size,
            'confidence_threshold': self.confidence_threshold,
            'results_dir': str(self.results_dir),
            'rulebook_path': str(self.rulebook_path),
            'cache_dir': str(self.cache_dir),
            'task_queue_path': str(self.task_queue_path),
            'enable_cache': self.enable_cache,
            'enable_rag': self.enable_rag,
            'rag_top_k': self.rag_top_k
        }


@dataclass
class FieldDefinition:
    """字段定义（支持从配置文件扩展）"""
    name: str
    type: str
    unit: Optional[str] = None
    required: bool = False
    default: Any = None
    validation_pattern: Optional[str] = None
    description: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FieldDefinition':
        return cls(
            name=data['name'],
            type=data['type'],
            unit=data.get('unit'),
            required=data.get('required', False),
            default=data.get('default'),
            validation_pattern=data.get('validation_pattern'),
            description=data.get('description')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'type': self.type,
            'unit': self.unit,
            'required': self.required,
            'default': self.default,
            'validation_pattern': self.validation_pattern,
            'description': self.description
        }


@dataclass
class RateLimitConfig:
    """速率限制配置"""
    requests_per_minute: int = 60
    requests_per_second: float = 10.0
    base_delay: float = 1.0
    max_delay: float = 60.0
    retry_on_429: bool = True
    respect_retry_after: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CacheConfig:
    """缓存配置"""
    enabled: bool = True
    dir: str = "./cache"
    max_age_days: int = 7
    max_size_mb: int = 500
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass  
class QueueConfig:
    """队列配置"""
    enabled: bool = True
    max_workers: int = 3
    task_timeout: int = 3600
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ConfigManager:
    """
    配置管理器（单例模式）
    
    确保整个应用程序使用一致的配置
    """
    _instance: Optional['ConfigManager'] = None
    
    def __new__(cls, config_path: str = "config.yaml"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: str = "config.yaml"):
        if self._initialized:
            return
        
        self.config_path = Path(config_path)
        self._initialized = True
        self._last_modified = None
        
        # 加载配置
        self.llm: Optional[LLMConfig] = None
        self.vlm: Optional[VLMConfig] = None
        self.pipeline: Optional[PipelineConfig] = None
        self.field_definitions: list[FieldDefinition] = []
        self.rate_limit: RateLimitConfig = RateLimitConfig()
        self.cache: CacheConfig = CacheConfig()
        self.queue: QueueConfig = QueueConfig()
        
        self._load()
    
    def _load(self) -> None:
        """加载配置文件"""
        if not self.config_path.exists():
            logger.warning(f"配置文件不存在: {self.config_path}，使用默认配置")
            self._use_defaults()
            return
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data:
                logger.warning("配置文件为空，使用默认配置")
                self._use_defaults()
                return
            
            # 加载 LLM 配置
            llm_data = data.get('text_llm', {})
            self.llm = LLMConfig(
                base_url=llm_data.get('base_url', ''),
                api_key=llm_data.get('api_key', ''),
                model=llm_data.get('model', 'glm-4'),
                max_retries=llm_data.get('max_retries', 3),
                temperature=llm_data.get('temperature', 0.1),
                max_tokens=llm_data.get('text_max_tokens', 4096),
                timeout=llm_data.get('timeout', 120)
            )
            
            # 加载 VLM 配置
            vlm_data = data.get('vision_vlm', {})
            self.vlm = VLMConfig(
                base_url=vlm_data.get('base_url', ''),
                api_key=vlm_data.get('api_key', ''),
                model=vlm_data.get('model', ''),
                max_retries=vlm_data.get('max_retries', 3),
                max_tokens=vlm_data.get('vision_max_tokens', 2048),
                timeout=vlm_data.get('timeout', 180)
            )
            
            # 加载管道配置
            self.pipeline = PipelineConfig(
                chunk_batch_size=data.get('chunk_batch_size', 5),
                vlm_batch_size=data.get('vlm_batch_size', 2),
                confidence_threshold=data.get('confidence_threshold', 0.7),
                results_dir=Path(data.get('results_dir', './extraction_results')),
                rulebook_path=Path(data.get('rulebook_path', './rulebook.json'))
            )
            
            # 加载速率限制配置
            rate_limit_data = data.get('rate_limit', {})
            self.rate_limit = RateLimitConfig(
                requests_per_minute=rate_limit_data.get('requests_per_minute', 60),
                requests_per_second=rate_limit_data.get('requests_per_second', 10.0),
                base_delay=rate_limit_data.get('base_delay', 1.0),
                max_delay=rate_limit_data.get('max_delay', 60.0),
                retry_on_429=rate_limit_data.get('retry_on_429', True),
                respect_retry_after=rate_limit_data.get('respect_retry_after', True)
            )
            
            # 加载缓存配置
            cache_data = data.get('cache', {})
            self.cache = CacheConfig(
                enabled=cache_data.get('enabled', True),
                dir=cache_data.get('dir', './cache'),
                max_age_days=cache_data.get('max_age_days', 7),
                max_size_mb=cache_data.get('max_size_mb', 500)
            )
            
            # 加载队列配置
            queue_data = data.get('queue', {})
            self.queue = QueueConfig(
                enabled=queue_data.get('enabled', True),
                max_workers=queue_data.get('max_workers', 3),
                task_timeout=queue_data.get('task_timeout', 3600)
            )
            
            # 加载字段定义（支持从配置文件扩展）
            field_defs_data = data.get('field_definitions', [])
            self.field_definitions = [
                FieldDefinition.from_dict(f) for f in field_defs_data
            ]
            
            # 如果没有从配置文件加载字段定义，使用默认值
            if not self.field_definitions:
                self._load_default_field_definitions()
            
            # 记录文件修改时间
            self._last_modified = self.config_path.stat().st_mtime
            
            logger.info(f"配置加载成功: LLM={self.llm.model}, VLM={self.vlm.model}")
            
        except Exception as e:
            logger.error(f"配置加载失败: {e}，使用默认配置")
            self._use_defaults()
    
    def _use_defaults(self) -> None:
        """使用默认配置"""
        self.llm = LLMConfig(
            base_url="https://open.bigmodel.cn/api/paas/v4/",
            api_key="",
            model="glm-4"
        )
        self.vlm = VLMConfig(
            base_url="https://api-inference.modelscope.cn/v1",
            api_key="",
            model=""
        )
        self.pipeline = PipelineConfig()
        self.rate_limit = RateLimitConfig()
        self.cache = CacheConfig()
        self.queue = QueueConfig()
        self._load_default_field_definitions()
    
    def _load_default_field_definitions(self) -> None:
        """加载默认字段定义"""
        self.field_definitions = [
            FieldDefinition(name="material", type="string", description="纳米酶材料名称"),
            FieldDefinition(name="metal_center", type="string", description="金属中心"),
            FieldDefinition(name="coordination", type="string", description="配位环境"),
            FieldDefinition(name="enzyme_type", type="string", 
                          description="酶活性类型",
                          validation_pattern="peroxidase-like|oxidase-like|catalase-like"),
            FieldDefinition(name="Km", type="float", unit="mM", description="米氏常数"),
            FieldDefinition(name="Vmax", type="float", unit="mM/s", description="最大反应速率"),
            FieldDefinition(name="pH_opt", type="float", description="最佳pH"),
            FieldDefinition(name="T_opt", type="float", unit="°C", description="最佳温度"),
            FieldDefinition(name="characterization", type="list", description="表征手段"),
            FieldDefinition(name="table_data", type="string", description="表格数据"),
        ]
    
    def reload(self) -> bool:
        """
        重新加载配置
        
        Returns:
            是否重新加载成功
        """
        if not self.config_path.exists():
            logger.warning("配置文件不存在，无法重载")
            return False
        
        current_mtime = self.config_path.stat().st_mtime
        if self._last_modified == current_mtime:
            logger.debug("配置文件未变化，跳过重载")
            return False
        
        logger.info("检测到配置文件变化，重新加载...")
        self._load()
        return True
    
    def get_config_hash(self) -> str:
        """获取配置哈希值（用于缓存键）"""
        import hashlib
        
        config_str = ""
        if self.llm:
            config_str += self.llm.model
        if self.vlm:
            config_str += self.vlm.model
        config_str += str(self.pipeline.chunk_batch_size)
        config_str += str(self.pipeline.vlm_batch_size)
        
        return hashlib.md5(config_str.encode()).hexdigest()[:16]
    
    def validate(self) -> Dict[str, bool]:
        """
        验证所有配置
        
        Returns:
            验证结果字典
        """
        results = {
            'llm': self.llm.validate() if self.llm else False,
            'vlm': self.vlm.validate() if self.vlm else False,
            'pipeline': self.pipeline is not None
        }
        results['all'] = all(results.values())
        return results
    
    def get_status_report(self) -> Dict[str, Any]:
        """
        获取配置状态报告
        
        Returns:
            状态报告字典
        """
        validation = self.validate()
        
        return {
            'config_path': str(self.config_path),
            'loaded_at': datetime.now().isoformat(),
            'llm': {
                'model': self.llm.model if self.llm else None,
                'url': self.llm.base_url if self.llm else None,
                'configured': bool(self.llm and self.llm.api_key),
                'valid': validation['llm']
            },
            'vlm': {
                'model': self.vlm.model if self.vlm else None,
                'url': self.vlm.base_url if self.vlm else None,
                'configured': bool(self.vlm and self.vlm.api_key),
                'valid': validation['vlm']
            },
            'pipeline': {
                'batch_size': self.pipeline.chunk_batch_size if self.pipeline else None,
                'results_dir': str(self.pipeline.results_dir) if self.pipeline else None,
                'cache_enabled': self.pipeline.enable_cache if self.pipeline else False
            },
            'validation': validation
        }
    
    @classmethod
    def get_instance(cls, config_path: str = "config.yaml") -> 'ConfigManager':
        """获取配置管理器单例"""
        if cls._instance is None:
            cls._instance = cls(config_path)
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（主要用于测试）"""
        cls._instance = None


def get_config() -> ConfigManager:
    """快捷函数：获取配置管理器"""
    return ConfigManager.get_instance()


def reload_config() -> bool:
    """快捷函数：重新加载配置"""
    config = ConfigManager.get_instance()
    return config.reload()


# 导出便捷访问的属性
def get_llm_config() -> Optional[LLMConfig]:
    return get_config().llm


def get_vlm_config() -> Optional[VLMConfig]:
    return get_config().vlm


def get_pipeline_config() -> PipelineConfig:
    return get_config().pipeline


def get_field_definitions() -> list[FieldDefinition]:
    return get_config().field_definitions
