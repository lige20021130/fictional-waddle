# cache_manager.py - 结果缓存管理模块
"""
纳米酶文献提取系统 - 结果缓存管理

功能：
1. 基于PDF内容和配置生成缓存键
2. 缓存提取结果避免重复API调用
3. 支持缓存过期和清理
4. 线程安全

使用方法：
    from cache_manager import CacheManager
    
    cache = CacheManager()
    cached_result = cache.get(pdf_path, config_hash)
    if cached_result:
        return cached_result
    # ... 处理后保存
    cache.set(pdf_path, config_hash, result)
"""

import json
import hashlib
import logging
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
import threading
import os

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """缓存条目"""
    pdf_path: str
    config_hash: str
    result: Dict[str, Any]
    created_at: str
    accessed_at: str
    access_count: int = 0
    file_hash: Optional[str] = None  # PDF文件的MD5哈希
    size_bytes: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CacheEntry':
        # 移除可能不存在的字段（向后兼容）
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)
    
    def update_access(self):
        """更新访问记录"""
        self.accessed_at = datetime.now().isoformat()
        self.access_count += 1


class CacheManager:
    """
    缓存管理器
    
    特性：
    - 线程安全
    - 支持过期清理
    - 基于文件内容和配置生成缓存键
    - 记录访问统计
    """
    
    def __init__(
        self, 
        cache_dir: Optional[str] = None,
        max_age_days: int = 7,
        max_cache_size_mb: int = 500,
        enable_stats: bool = True
    ):
        """
        初始化缓存管理器
        
        Args:
            cache_dir: 缓存目录，默认 ./cache
            max_age_days: 缓存最大保存天数
            max_cache_size_mb: 缓存最大占用空间(MB)
            enable_stats: 是否启用访问统计
        """
        self.cache_dir = Path(cache_dir) if cache_dir else Path("./cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_age = timedelta(days=max_age_days)
        self.max_size_bytes = max_cache_size_mb * 1024 * 1024
        self.enable_stats = enable_stats
        
        self._lock = threading.Lock()
        self._index_file = self.cache_dir / "cache_index.json"
        self._index: Dict[str, CacheEntry] = {}
        
        self._load_index()
        
        logger.info(f"缓存管理器初始化: dir={self.cache_dir}, max_age={max_age_days}天")
    
    def _load_index(self) -> None:
        """加载缓存索引"""
        if not self._index_file.exists():
            return
        
        try:
            with open(self._index_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for key, entry_data in data.items():
                try:
                    self._index[key] = CacheEntry.from_dict(entry_data)
                except Exception as e:
                    logger.warning(f"跳过无效缓存条目 {key}: {e}")
                    
            logger.info(f"已加载 {len(self._index)} 个缓存条目")
        except Exception as e:
            logger.error(f"加载缓存索引失败: {e}")
            self._index = {}
    
    def _save_index(self) -> None:
        """保存缓存索引"""
        try:
            data = {key: entry.to_dict() for key, entry in self._index.items()}
            with open(self._index_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存缓存索引失败: {e}")
    
    def _generate_cache_key(self, pdf_path: str, config_hash: str, file_hash: Optional[str] = None) -> str:
        """
        生成缓存键
        
        Args:
            pdf_path: PDF文件路径
            config_hash: 配置哈希
            file_hash: 文件内容哈希（可选，用于检测文件变化）
            
        Returns:
            缓存键
        """
        if file_hash:
            content = f"{pdf_path}:{config_hash}:{file_hash}"
        else:
            # 如果没有文件哈希，使用文件修改时间作为次优方案
            try:
                mtime = os.path.getmtime(pdf_path)
                content = f"{pdf_path}:{config_hash}:{mtime}"
            except:
                content = f"{pdf_path}:{config_hash}"
        
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _calculate_file_hash(self, file_path: str) -> Optional[str]:
        """
        计算文件MD5哈希
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件哈希或None
        """
        try:
            hasher = hashlib.md5()
            with open(file_path, 'rb') as f:
                # 只读取前64KB + 后64KB，用于快速检测大文件变化
                chunk1 = f.read(65536)
                hasher.update(chunk1)
                
                f.seek(0, 2)  # 跳到末尾
                file_size = f.tell()
                
                if file_size > 131072:
                    f.seek(-65536, 2)  # 读取最后64KB
                    chunk2 = f.read(65536)
                    hasher.update(chunk2)
                else:
                    hasher.update(chunk1)
                    
            return hasher.hexdigest()
        except Exception as e:
            logger.warning(f"计算文件哈希失败 {file_path}: {e}")
            return None
    
    def get(self, pdf_path: str, config_hash: str, check_file_change: bool = True) -> Optional[Dict[str, Any]]:
        """
        获取缓存结果
        
        Args:
            pdf_path: PDF文件路径
            config_hash: 配置哈希
            check_file_change: 是否检查文件变化
            
        Returns:
            缓存的结果或None
        """
        with self._lock:
            file_hash = None
            if check_file_change:
                file_hash = self._calculate_file_hash(pdf_path)
            
            cache_key = self._generate_cache_key(pdf_path, config_hash, file_hash)
            
            # 尝试精确匹配
            if cache_key in self._index:
                entry = self._index[cache_key]
                entry.update_access()
                self._save_index()
                logger.info(f"缓存命中: {pdf_path}")
                return entry.result
            
            # 如果检查了文件变化，尝试无文件哈希的匹配
            if file_hash:
                alt_key = self._generate_cache_key(pdf_path, config_hash, None)
                if alt_key in self._index:
                    entry = self._index[alt_key]
                    # 检查文件是否真的没变化
                    if entry.file_hash == file_hash:
                        entry.update_access()
                        self._save_index()
                        logger.info(f"缓存命中(无文件检查): {pdf_path}")
                        return entry.result
            
            logger.debug(f"缓存未命中: {pdf_path}")
            return None
    
    def set(
        self, 
        pdf_path: str, 
        config_hash: str, 
        result: Dict[str, Any],
        check_file_change: bool = True
    ) -> str:
        """
        保存结果到缓存
        
        Args:
            pdf_path: PDF文件路径
            config_hash: 配置哈希
            result: 要缓存的结果
            check_file_change: 是否检查文件变化
            
        Returns:
            缓存键
        """
        with self._lock:
            file_hash = None
            if check_file_change:
                file_hash = self._calculate_file_hash(pdf_path)
            
            cache_key = self._generate_cache_key(pdf_path, config_hash, file_hash)
            
            entry = CacheEntry(
                pdf_path=pdf_path,
                config_hash=config_hash,
                result=result,
                created_at=datetime.now().isoformat(),
                accessed_at=datetime.now().isoformat(),
                access_count=1,
                file_hash=file_hash,
                size_bytes=len(json.dumps(result, ensure_ascii=False).encode())
            )
            
            self._index[cache_key] = entry
            self._save_index()
            
            logger.info(f"缓存已保存: {pdf_path} -> {cache_key}")
            return cache_key
    
    def invalidate(self, pdf_path: str, config_hash: Optional[str] = None) -> int:
        """
        使缓存失效
        
        Args:
            pdf_path: PDF文件路径
            config_hash: 可选的配置哈希，不指定则删除所有匹配项
            
        Returns:
            删除的缓存数量
        """
        with self._lock:
            to_remove = []
            for key, entry in self._index.items():
                if entry.pdf_path == pdf_path:
                    if config_hash is None or entry.config_hash == config_hash:
                        to_remove.append(key)
            
            for key in to_remove:
                del self._index[key]
            
            if to_remove:
                self._save_index()
                logger.info(f"已使 {len(to_remove)} 个缓存失效: {pdf_path}")
            
            return len(to_remove)
    
    def clean_expired(self) -> int:
        """
        清理过期缓存
        
        Returns:
            删除的缓存数量
        """
        with self._lock:
            now = datetime.now()
            to_remove = []
            
            for key, entry in self._index.items():
                created = datetime.fromisoformat(entry.created_at)
                if now - created > self.max_age:
                    to_remove.append(key)
            
            for key in to_remove:
                del self._index[key]
            
            if to_remove:
                self._save_index()
                logger.info(f"已清理 {len(to_remove)} 个过期缓存")
            
            return len(to_remove)
    
    def clean_by_size(self, target_size_mb: int = None) -> int:
        """
        按访问频率清理缓存以达到目标大小
        
        Args:
            target_size_mb: 目标大小(MB)，默认50%最大限制
            
        Returns:
            删除的缓存数量
        """
        with self._lock:
            if target_size_mb is None:
                target_size_mb = self.max_size_bytes // (2 * 1024 * 1024)
            target_bytes = target_size_mb * 1024 * 1024
            
            # 按访问频率和访问时间排序
            entries = sorted(
                self._index.items(),
                key=lambda x: (x[1].access_count, x[1].accessed_at)
            )
            
            current_size = sum(e.size_bytes for e in self._index.values())
            removed = 0
            
            for key, entry in entries:
                if current_size <= target_bytes:
                    break
                del self._index[key]
                current_size -= entry.size_bytes
                removed += 1
            
            if removed > 0:
                self._save_index()
                logger.info(f"按大小清理: 删除了 {removed} 个缓存，释放约 {current_size // (1024*1024)}MB")
            
            return removed
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            统计信息字典
        """
        with self._lock:
            if not self._index:
                return {
                    'total_entries': 0,
                    'total_size_mb': 0,
                    'oldest_entry': None,
                    'newest_entry': None,
                    'most_accessed': None,
                    'hits': 0
                }
            
            now = datetime.now()
            total_size = sum(e.size_bytes for e in self._index.values())
            total_hits = sum(e.access_count for e in self._index.values())
            
            entries_sorted = sorted(self._index.values(), key=lambda x: x.created_at)
            
            return {
                'total_entries': len(self._index),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'oldest_entry': entries_sorted[0].created_at if entries_sorted else None,
                'newest_entry': entries_sorted[-1].created_at if entries_sorted else None,
                'most_accessed': max(self._index.values(), key=lambda x: x.access_count).pdf_path if self._index else None,
                'total_hits': total_hits,
                'avg_access': round(total_hits / len(self._index), 2) if self._index else 0,
                'expired_count': sum(
                    1 for e in self._index.values() 
                    if now - datetime.fromisoformat(e.created_at) > self.max_age
                )
            }
    
    def clear_all(self) -> int:
        """
        清空所有缓存
        
        Returns:
            删除的缓存数量
        """
        with self._lock:
            count = len(self._index)
            self._index = {}
            self._save_index()
            logger.info(f"已清空所有缓存 ({count} 条)")
            return count
    
    def list_entries(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        列出缓存条目
        
        Args:
            limit: 返回数量限制
            
        Returns:
            缓存条目列表
        """
        with self._lock:
            entries = sorted(
                self._index.values(),
                key=lambda x: x.accessed_at,
                reverse=True
            )[:limit]
            
            return [
                {
                    'cache_key': key,
                    'pdf_path': e.pdf_path,
                    'created_at': e.created_at,
                    'accessed_at': e.accessed_at,
                    'access_count': e.access_count,
                    'size_kb': round(e.size_bytes / 1024, 1)
                }
                for key, e in zip(self._index.keys(), entries)
                if e in entries
            ]


# 便捷函数
_cache_manager: Optional[CacheManager] = None


def get_cache_manager(
    cache_dir: Optional[str] = None,
    max_age_days: int = 7
) -> CacheManager:
    """获取缓存管理器单例"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager(cache_dir, max_age_days)
    return _cache_manager


def clear_cache() -> int:
    """清空所有缓存"""
    return get_cache_manager().clear_all()


def clean_expired_cache() -> int:
    """清理过期缓存"""
    return get_cache_manager().clean_expired()
