# task_queue.py - 持久化任务队列
"""
纳米酶文献提取系统 - 持久化任务队列

功能：
1. 持久化任务状态到磁盘
2. 支持断点续传
3. 失败重试机制
4. 任务优先级
5. 并发控制

使用方法：
    from task_queue import TaskQueue, TaskStatus, TaskPriority
    
    queue = TaskQueue()
    task_id = queue.add(pdf_path, mid_json_path)
    
    # 处理任务
    task = queue.get_pending()
    if task:
        # ... 处理
        queue.mark_completed(task['id'])
"""

import json
import logging
import threading
import time
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class TaskPriority(Enum):
    """任务优先级"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class Task:
    """任务数据类"""
    id: str
    pdf_path: str
    mid_json_path: str
    status: str = TaskStatus.PENDING.value
    priority: int = TaskPriority.NORMAL.value
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    result_path: Optional[str] = None
    progress: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Task':
        return cls(**data)
    
    def update_status(self, status: TaskStatus, error: Optional[str] = None):
        """更新任务状态"""
        self.status = status.value
        self.updated_at = datetime.now().isoformat()
        
        if status == TaskStatus.PROCESSING and not self.started_at:
            self.started_at = datetime.now().isoformat()
        elif status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            self.completed_at = datetime.now().isoformat()
        
        if error:
            self.error = error
    
    def can_retry(self) -> bool:
        """检查是否可以重试"""
        return self.retry_count < self.max_retries
    
    def increment_retry(self):
        """增加重试计数"""
        self.retry_count += 1
        self.updated_at = datetime.now().isoformat()


class TaskQueue:
    """
    持久化任务队列
    
    特性：
    - 线程安全
    - 持久化到磁盘
    - 支持优先级
    - 自动清理
    - 事件回调
    """
    
    def __init__(
        self,
        queue_file: Optional[str] = None,
        auto_save: bool = True,
        max_retry: int = 3,
        task_timeout: int = 3600,  # 1小时
        cleanup_interval: int = 300  # 5分钟
    ):
        """
        初始化任务队列
        
        Args:
            queue_file: 队列文件路径
            auto_save: 是否自动保存
            max_retry: 默认最大重试次数
            task_timeout: 任务超时时间(秒)
            cleanup_interval: 清理间隔(秒)
        """
        self.queue_file = Path(queue_file) if queue_file else Path("./task_queue.json")
        self.auto_save = auto_save
        self.max_retry = max_retry
        self.task_timeout = task_timeout
        
        self._lock = threading.Lock()
        self._tasks: Dict[str, Task] = {}
        self._callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
        # 加载队列
        self._load()
        
        # 后台清理线程
        self._cleanup_running = True
        self._cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self._cleanup_thread.start()
        
        logger.info(f"任务队列初始化: {self.queue_file}, 加载 {len(self._tasks)} 个任务")
    
    def _load(self) -> None:
        """从文件加载队列"""
        if not self.queue_file.exists():
            return
        
        try:
            with open(self.queue_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for task_data in data.get('tasks', []):
                try:
                    task = Task.from_dict(task_data)
                    self._tasks[task.id] = task
                except Exception as e:
                    logger.warning(f"跳过无效任务: {e}")
            
            logger.info(f"已加载 {len(self._tasks)} 个任务")
        except Exception as e:
            logger.error(f"加载队列失败: {e}")
    
    def _save(self) -> None:
        """保存队列到文件"""
        try:
            data = {
                'tasks': [task.to_dict() for task in self._tasks.values()],
                'saved_at': datetime.now().isoformat()
            }
            
            # 原子写入
            temp_file = self.queue_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            temp_file.replace(self.queue_file)
            
        except Exception as e:
            logger.error(f"保存队列失败: {e}")
    
    def _cleanup_worker(self) -> None:
        """后台清理工作者"""
        while self._cleanup_running:
            time.sleep(300)  # 5分钟
            try:
                self.cleanup_stale()
            except Exception as e:
                logger.error(f"清理失败: {e}")
    
    # ========== 公共 API ==========
    
    def add(
        self,
        pdf_path: str,
        mid_json_path: str,
        priority: TaskPriority = TaskPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        max_retries: Optional[int] = None
    ) -> str:
        """
        添加新任务
        
        Args:
            pdf_path: PDF文件路径
            mid_json_path: 中间JSON路径
            priority: 优先级
            metadata: 额外元数据
            max_retries: 最大重试次数
            
        Returns:
            任务ID
        """
        with self._lock:
            task_id = f"task_{uuid.uuid4().hex[:12]}"
            
            task = Task(
                id=task_id,
                pdf_path=pdf_path,
                mid_json_path=mid_json_path,
                priority=priority.value,
                metadata=metadata or {},
                max_retries=max_retries or self.max_retry
            )
            
            self._tasks[task_id] = task
            
            if self.auto_save:
                self._save()
            
            logger.info(f"添加任务: {task_id} - {pdf_path}")
            self._trigger_callback('added', task)
            
            return task_id
    
    def get(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        with self._lock:
            return self._tasks.get(task_id)
    
    def get_pending(self, limit: int = 10) -> List[Task]:
        """
        获取待处理任务
        
        Args:
            limit: 返回数量限制
            
        Returns:
            按优先级和创建时间排序的任务列表
        """
        with self._lock:
            pending = [
                t for t in self._tasks.values()
                if t.status == TaskStatus.PENDING.value
            ]
            
            # 按优先级和创建时间排序
            pending.sort(key=lambda t: (-t.priority, t.created_at))
            
            return pending[:limit]
    
    def get_retryable(self, limit: int = 5) -> List[Task]:
        """
        获取可重试的任务
        
        Returns:
            可重试的任务列表
        """
        with self._lock:
            retryable = [
                t for t in self._tasks.values()
                if t.status == TaskStatus.FAILED.value and t.can_retry()
            ]
            retryable.sort(key=lambda t: (-t.priority, -t.retry_count))
            return retryable[:limit]
    
    def mark_processing(self, task_id: str) -> bool:
        """
        标记任务为处理中
        
        Returns:
            是否成功
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            
            if task.status not in (TaskStatus.PENDING.value, TaskStatus.FAILED.value):
                logger.warning(f"任务状态不允许开始: {task_id} - {task.status}")
                return False
            
            task.update_status(TaskStatus.PROCESSING)
            
            if self.auto_save:
                self._save()
            
            self._trigger_callback('processing', task)
            return True
    
    def mark_completed(
        self,
        task_id: str,
        result_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        标记任务为完成
        
        Returns:
            是否成功
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            
            task.update_status(TaskStatus.COMPLETED)
            
            if result_path:
                task.result_path = result_path
            if metadata:
                task.metadata.update(metadata)
            
            if self.auto_save:
                self._save()
            
            logger.info(f"任务完成: {task_id}")
            self._trigger_callback('completed', task)
            return True
    
    def mark_failed(
        self,
        task_id: str,
        error: str,
        can_retry: bool = True
    ) -> bool:
        """
        标记任务为失败
        
        Args:
            task_id: 任务ID
            error: 错误信息
            can_retry: 是否可以重试
            
        Returns:
            是否成功
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            
            if can_retry and task.can_retry():
                task.increment_retry()
                task.update_status(TaskStatus.PENDING, error)
                logger.warning(f"任务失败(将重试 {task.retry_count}/{task.max_retries}): {task_id} - {error}")
                self._trigger_callback('retry', task)
            else:
                task.update_status(TaskStatus.FAILED, error)
                logger.error(f"任务失败(不可重试): {task_id} - {error}")
                self._trigger_callback('failed', task)
            
            if self.auto_save:
                self._save()
            
            return True
    
    def mark_cancelled(self, task_id: str) -> bool:
        """取消任务"""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            
            task.update_status(TaskStatus.CANCELLED)
            
            if self.auto_save:
                self._save()
            
            self._trigger_callback('cancelled', task)
            return True
    
    def update_progress(self, task_id: str, progress: float) -> bool:
        """更新任务进度"""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            
            task.progress = min(1.0, max(0.0, progress))
            task.updated_at = datetime.now().isoformat()
            
            if self.auto_save:
                self._save()
            
            self._trigger_callback('progress', task)
            return True
    
    def remove(self, task_id: str) -> bool:
        """删除任务"""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                if self.auto_save:
                    self._save()
                return True
            return False
    
    def clear_completed(self) -> int:
        """清除已完成的任务"""
        with self._lock:
            to_remove = [
                tid for tid, t in self._tasks.items()
                if t.status in (TaskStatus.COMPLETED.value, TaskStatus.CANCELLED.value)
            ]
            
            for tid in to_remove:
                del self._tasks[tid]
            
            if to_remove and self.auto_save:
                self._save()
            
            logger.info(f"已清除 {len(to_remove)} 个任务")
            return len(to_remove)
    
    def cleanup_stale(self) -> int:
        """
        清理超时任务
        
        Returns:
            清理数量
        """
        with self._lock:
            now = datetime.now()
            threshold = timedelta(seconds=self.task_timeout)
            to_remove = []
            
            for task in self._tasks.values():
                if task.status == TaskStatus.PROCESSING.value and task.started_at:
                    started = datetime.fromisoformat(task.started_at)
                    if now - started > threshold:
                        task.update_status(
                            TaskStatus.FAILED,
                            f"任务超时 (>{self.task_timeout}秒)"
                        )
                        to_remove.append(task.id)
            
            if to_remove:
                logger.warning(f"清理 {len(to_remove)} 个超时任务")
                if self.auto_save:
                    self._save()
            
            return len(to_remove)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            stats = defaultdict(int)
            for task in self._tasks.values():
                stats[task.status] += 1
            
            pending = [t for t in self._tasks.values() 
                      if t.status == TaskStatus.PENDING.value]
            
            return {
                'total': len(self._tasks),
                'pending': stats[TaskStatus.PENDING.value],
                'processing': stats[TaskStatus.PROCESSING.value],
                'completed': stats[TaskStatus.COMPLETED.value],
                'failed': stats[TaskStatus.FAILED.value],
                'cancelled': stats[TaskStatus.CANCELLED.value],
                'can_retry': sum(1 for t in self._tasks.values() 
                               if t.can_retry()),
                'highest_priority': max((t.priority for t in pending), default=0)
            }
    
    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        列出任务
        
        Args:
            status: 按状态过滤
            limit: 返回数量
            
        Returns:
            任务列表
        """
        with self._lock:
            tasks = list(self._tasks.values())
            
            if status:
                tasks = [t for t in tasks if t.status == status.value]
            
            tasks.sort(key=lambda t: t.created_at, reverse=True)
            
            return [t.to_dict() for t in tasks[:limit]]
    
    def register_callback(self, event: str, callback: Callable[[Task], None]):
        """
        注册事件回调
        
        Args:
            event: 事件类型 (added, processing, completed, failed, retry, cancelled, progress)
            callback: 回调函数
        """
        self._callbacks[event].append(callback)
    
    def _trigger_callback(self, event: str, task: Task):
        """触发回调"""
        for callback in self._callbacks.get(event, []):
            try:
                callback(task)
            except Exception as e:
                logger.error(f"回调执行失败 ({event}): {e}")
    
    def stop(self):
        """停止队列（清理资源）"""
        self._cleanup_running = False
        if self.auto_save:
            self._save()
        logger.info("任务队列已停止")


# 便捷函数
_task_queue: Optional[TaskQueue] = None


def get_task_queue(queue_file: Optional[str] = None) -> TaskQueue:
    """获取任务队列单例"""
    global _task_queue
    if _task_queue is None:
        _task_queue = TaskQueue(queue_file)
    return _task_queue


def add_extraction_task(
    pdf_path: str,
    mid_json_path: str,
    priority: TaskPriority = TaskPriority.NORMAL
) -> str:
    """快捷函数：添加提取任务"""
    return get_task_queue().add(pdf_path, mid_json_path, priority)
