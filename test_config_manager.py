# test_config_manager.py - 配置管理器单元测试
"""
配置管理器单元测试
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

# 测试配置管理器的各种功能
class TestConfigManager:
    """配置管理器测试"""
    
    def test_llm_config_validation(self):
        """测试 LLM 配置验证"""
        from config_manager import LLMConfig
        
        # 有效配置
        valid_config = LLMConfig(
            base_url="https://api.example.com",
            api_key="test-key",
            model="test-model"
        )
        assert valid_config.validate() == True
        
        # 无 API key
        invalid_config = LLMConfig(
            base_url="https://api.example.com",
            api_key="",
            model="test-model"
        )
        assert invalid_config.validate() == False
        
        # placeholder key
        placeholder_config = LLMConfig(
            base_url="https://api.example.com",
            api_key="your-deepseek-api-key",
            model="test-model"
        )
        assert placeholder_config.validate() == False
    
    def test_vlm_config_validation(self):
        """测试 VLM 配置验证"""
        from config_manager import VLMConfig
        
        valid_config = VLMConfig(
            base_url="https://api.example.com",
            api_key="test-key",
            model="test-vlm"
        )
        assert valid_config.validate() == True
    
    def test_pipeline_config_defaults(self):
        """测试管道配置默认值"""
        from config_manager import PipelineConfig
        
        config = PipelineConfig()
        
        assert config.chunk_batch_size == 5
        assert config.vlm_batch_size == 2
        assert config.confidence_threshold == 0.7
        assert config.enable_cache == True
        assert isinstance(config.results_dir, Path)
    
    def test_config_singleton(self):
        """测试单例模式"""
        from config_manager import ConfigManager
        
        # 创建临时配置文件
        config_data = """
text_llm:
  base_url: "https://api.example.com"
  api_key: "test-key"
  model: "test-model"

vision_vlm:
  base_url: "https://api.example.com"
  api_key: "test-key"
  model: "test-vlm"
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_data)
            config_path = f.name
        
        try:
            # 重置单例
            ConfigManager.reset_instance()
            
            # 第一次获取
            config1 = ConfigManager(config_path)
            
            # 第二次获取（应该返回相同实例）
            config2 = ConfigManager(config_path)
            
            assert config1 is config2
            
        finally:
            Path(config_path).unlink(missing_ok=True)
            ConfigManager.reset_instance()
    
    def test_config_hash(self):
        """测试配置哈希生成"""
        from config_manager import ConfigManager
        
        ConfigManager.reset_instance()
        config = ConfigManager()
        
        # 添加 mock llm 和 vlm
        config.llm = MagicMock()
        config.llm.model = "test-model"
        config.vlm = MagicMock()
        config.vlm.model = "test-vlm"
        config.pipeline = MagicMock()
        config.pipeline.chunk_batch_size = 5
        config.pipeline.vlm_batch_size = 2
        
        hash1 = config.get_config_hash()
        hash2 = config.get_config_hash()
        
        # 相同配置应生成相同哈希
        assert hash1 == hash2
        assert len(hash1) == 16  # MD5 前16字符
    
    def test_status_report(self):
        """测试状态报告"""
        from config_manager import ConfigManager, LLMConfig, VLMConfig, PipelineConfig
        
        ConfigManager.reset_instance()
        config = ConfigManager()
        
        # 设置测试数据
        config.llm = LLMConfig(
            base_url="https://api.example.com",
            api_key="test-key",
            model="test-model"
        )
        config.vlm = VLMConfig(
            base_url="https://api.example.com",
            api_key="test-key",
            model="test-vlm"
        )
        config.pipeline = PipelineConfig()
        
        report = config.get_status_report()
        
        assert 'llm' in report
        assert 'vlm' in report
        assert 'pipeline' in report
        assert 'validation' in report
        assert report['llm']['configured'] == True
        assert report['vlm']['configured'] == True


class TestFieldDefinitions:
    """字段定义测试"""
    
    def test_field_definition_creation(self):
        """测试字段定义创建"""
        from config_manager import FieldDefinition
        
        field = FieldDefinition(
            name="test_field",
            type="string",
            unit="test",
            required=True
        )
        
        assert field.name == "test_field"
        assert field.type == "string"
        assert field.unit == "test"
        assert field.required == True
    
    def test_field_definition_from_dict(self):
        """测试从字典创建字段定义"""
        from config_manager import FieldDefinition
        
        data = {
            'name': 'Km',
            'type': 'float',
            'unit': 'mM',
            'required': True
        }
        
        field = FieldDefinition.from_dict(data)
        
        assert field.name == 'Km'
        assert field.type == 'float'
        assert field.unit == 'mM'
        assert field.required == True
    
    def test_default_field_definitions(self):
        """测试默认字段定义"""
        from config_manager import ConfigManager
        
        ConfigManager.reset_instance()
        config = ConfigManager()
        
        fields = config.field_definitions
        
        assert len(fields) > 0
        
        # 检查核心字段存在
        field_names = [f.name for f in fields]
        assert 'material' in field_names
        assert 'enzyme_type' in field_names
        assert 'Km' in field_names
        assert 'Vmax' in field_names
