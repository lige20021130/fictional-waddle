# test_cache_manager.py - 缓存管理器单元测试
"""
缓存管理器单元测试
"""

import pytest
import json
import time
from pathlib import Path


class TestCacheManager:
    """缓存管理器测试"""
    
    def test_cache_initialization(self, temp_dir):
        """测试缓存初始化"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        assert cache.cache_dir.exists()
        assert len(cache._index) == 0
    
    def test_cache_set_and_get(self, temp_dir):
        """测试缓存设置和获取"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        pdf_path = "test.pdf"
        config_hash = "abc123"
        result = {"material": "Fe3O4", "Km": 0.32}
        
        # 设置缓存
        cache_key = cache.set(pdf_path, config_hash, result)
        assert cache_key is not None
        assert len(cache_key) == 16
        
        # 获取缓存
        cached = cache.get(pdf_path, config_hash, check_file_change=False)
        assert cached is not None
        assert cached['material'] == 'Fe3O4'
        assert cached['Km'] == 0.32
    
    def test_cache_miss(self, temp_dir):
        """测试缓存未命中"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        result = cache.get("nonexistent.pdf", "hash", check_file_change=False)
        assert result is None
    
    def test_cache_invalidate(self, temp_dir):
        """测试缓存失效"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        pdf_path = "test.pdf"
        cache.set(pdf_path, "hash1", {"data": "test1"})
        cache.set(pdf_path, "hash2", {"data": "test2"})
        
        # 失效所有
        count = cache.invalidate(pdf_path)
        assert count == 2
        
        # 验证已失效
        assert cache.get(pdf_path, "hash1", check_file_change=False) is None
    
    def test_cache_statistics(self, temp_dir):
        """测试缓存统计"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        # 添加缓存
        cache.set("test1.pdf", "hash1", {"data": "test1"})
        cache.set("test2.pdf", "hash2", {"data": "test2"})
        
        stats = cache.get_statistics()
        
        assert stats['total_entries'] == 2
        # 验证统计包含必要字段
        assert 'total_size_mb' in stats
    
    def test_cache_clear_all(self, temp_dir):
        """测试清空所有缓存"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        cache.set("test.pdf", "hash", {"data": "test"})
        count = cache.clear_all()
        
        assert count == 1
        assert len(cache._index) == 0
    
    def test_cache_entry_access_tracking(self, temp_dir):
        """测试访问跟踪"""
        from cache_manager import CacheManager
        
        cache = CacheManager(cache_dir=str(temp_dir))
        
        cache.set("test.pdf", "hash", {"data": "test"})
        
        # 第一次访问
        cache.get("test.pdf", "hash", check_file_change=False)
        
        # 第二次访问
        cache.get("test.pdf", "hash", check_file_change=False)
        
        entry = list(cache._index.values())[0]
        assert entry.access_count == 3  # set + 2 get


class TestCacheEntry:
    """缓存条目测试"""
    
    def test_cache_entry_creation(self):
        """测试缓存条目创建"""
        from cache_manager import CacheEntry
        
        entry = CacheEntry(
            pdf_path="test.pdf",
            config_hash="hash123",
            result={"data": "test"},
            created_at="2024-01-01T00:00:00",
            accessed_at="2024-01-01T00:00:00"
        )
        
        assert entry.pdf_path == "test.pdf"
        assert entry.access_count == 0
    
    def test_cache_entry_update_access(self):
        """测试更新访问"""
        from cache_manager import CacheEntry
        
        entry = CacheEntry(
            pdf_path="test.pdf",
            config_hash="hash123",
            result={"data": "test"},
            created_at="2024-01-01T00:00:00",
            accessed_at="2024-01-01T00:00:00"
        )
        
        initial_count = entry.access_count
        entry.update_access()
        
        assert entry.access_count == initial_count + 1
    
    def test_cache_entry_serialization(self):
        """测试序列化/反序列化"""
        from cache_manager import CacheEntry
        
        original = CacheEntry(
            pdf_path="test.pdf",
            config_hash="hash123",
            result={"data": "test", "number": 42},
            created_at="2024-01-01T00:00:00",
            accessed_at="2024-01-01T00:00:00",
            access_count=5,
            file_hash="file123",
            size_bytes=1000
        )
        
        # 序列化
        data = original.to_dict()
        assert isinstance(data, dict)
        assert data['pdf_path'] == "test.pdf"
        assert data['access_count'] == 5
        
        # 反序列化
        restored = CacheEntry.from_dict(data)
        assert restored.pdf_path == original.pdf_path
        assert restored.access_count == original.access_count
        assert restored.result == original.result
