# test_task_queue.py - 任务队列单元测试
"""
任务队列单元测试
"""

import pytest
import time
from pathlib import Path


class TestTaskQueue:
    """任务队列测试"""
    
    def test_queue_initialization(self, temp_dir):
        """测试队列初始化"""
        from task_queue import TaskQueue, TaskStatus
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        assert len(queue._tasks) == 0
    
    def test_add_task(self, temp_dir):
        """测试添加任务"""
        from task_queue import TaskQueue, TaskStatus
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        task_id = queue.add("test.pdf", "mid_task.json")
        
        assert task_id is not None
        assert task_id.startswith("task_")
        
        # 验证任务存在
        task = queue.get(task_id)
        assert task is not None
        assert task.pdf_path == "test.pdf"
        assert task.status == TaskStatus.PENDING.value
    
    def test_get_pending_tasks(self, temp_dir):
        """测试获取待处理任务"""
        from task_queue import TaskQueue, TaskPriority
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        # 添加多个任务
        queue.add("test1.pdf", "mid1.json", priority=TaskPriority.LOW)
        queue.add("test2.pdf", "mid2.json", priority=TaskPriority.HIGH)
        queue.add("test3.pdf", "mid3.json", priority=TaskPriority.NORMAL)
        
        pending = queue.get_pending(limit=10)
        
        assert len(pending) == 3
        # 验证按优先级排序
        assert pending[0].priority >= pending[1].priority
    
    def test_mark_processing(self, temp_dir):
        """测试标记处理中"""
        from task_queue import TaskQueue, TaskStatus
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        task_id = queue.add("test.pdf", "mid_task.json")
        
        success = queue.mark_processing(task_id)
        
        assert success == True
        
        task = queue.get(task_id)
        assert task.status == TaskStatus.PROCESSING.value
        assert task.started_at is not None
    
    def test_mark_completed(self, temp_dir):
        """测试标记完成"""
        from task_queue import TaskQueue, TaskStatus
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        task_id = queue.add("test.pdf", "mid_task.json")
        
        queue.mark_processing(task_id)
        success = queue.mark_completed(task_id, result_path="output.json")
        
        assert success == True
        
        task = queue.get(task_id)
        assert task.status == TaskStatus.COMPLETED.value
        assert task.result_path == "output.json"
        assert task.completed_at is not None
    
    def test_mark_failed_with_retry(self, temp_dir):
        """测试标记失败（可重试）"""
        from task_queue import TaskQueue, TaskStatus
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"), max_retry=3)
        
        task_id = queue.add("test.pdf", "mid_task.json")
        
        # 第一次失败
        queue.mark_failed(task_id, "Network error")
        
        task = queue.get(task_id)
        assert task.status == TaskStatus.PENDING.value  # 自动重试
        assert task.retry_count == 1
        assert task.error == "Network error"
    
    def test_mark_failed_no_retry(self, temp_dir):
        """测试标记失败（不可重试）"""
        from task_queue import TaskQueue, TaskStatus
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"), max_retry=1)
        
        task_id = queue.add("test.pdf", "mid_task.json")
        
        # 第一次失败（可重试）
        queue.mark_failed(task_id, "Fatal error")
        
        # 第二次失败（不可重试）
        queue.mark_failed(task_id, "Another error", can_retry=False)
        
        task = queue.get(task_id)
        assert task.status == TaskStatus.FAILED.value
        assert task.retry_count == 1
    
    def test_update_progress(self, temp_dir):
        """测试更新进度"""
        from task_queue import TaskQueue
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        task_id = queue.add("test.pdf", "mid_task.json")
        
        success = queue.update_progress(task_id, 0.5)
        
        assert success == True
        
        task = queue.get(task_id)
        assert task.progress == 0.5
    
    def test_clear_completed(self, temp_dir):
        """测试清除已完成任务"""
        from task_queue import TaskQueue
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        # 添加并完成一些任务
        task_id1 = queue.add("test1.pdf", "mid1.json")
        task_id2 = queue.add("test2.pdf", "mid2.json")
        
        queue.mark_processing(task_id1)
        queue.mark_completed(task_id1)
        queue.mark_processing(task_id2)
        queue.mark_completed(task_id2)
        
        # 清除已完成
        count = queue.clear_completed()
        
        assert count == 2
        assert queue.get(task_id1) is None
    
    def test_statistics(self, temp_dir):
        """测试统计信息"""
        from task_queue import TaskQueue
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        # 添加各种状态的任务
        queue.add("test1.pdf", "mid1.json")
        task_id2 = queue.add("test2.pdf", "mid2.json")
        task_id3 = queue.add("test3.pdf", "mid3.json")
        
        queue.mark_processing(task_id2)
        queue.mark_processing(task_id3)
        
        stats = queue.get_statistics()
        
        assert stats['total'] == 3
        assert stats['pending'] == 1
        assert stats['processing'] == 2
        assert stats['completed'] == 0
    
    def test_callback(self, temp_dir):
        """测试回调机制"""
        from task_queue import TaskQueue
        
        queue = TaskQueue(queue_file=str(temp_dir / "test_queue.json"))
        
        events = []
        
        def callback(task):
            events.append((task.id, task.status))
        
        queue.register_callback('completed', callback)
        
        task_id = queue.add("test.pdf", "mid_task.json")
        queue.mark_processing(task_id)
        queue.mark_completed(task_id)
        
        assert len(events) == 1
        assert events[0][1] == 'completed'


class TestTask:
    """任务数据类测试"""
    
    def test_task_creation(self):
        """测试任务创建"""
        from task_queue import Task
        
        task = Task(
            id="test_123",
            pdf_path="test.pdf",
            mid_json_path="mid.json"
        )
        
        assert task.id == "test_123"
        assert task.pdf_path == "test.pdf"
        assert task.retry_count == 0
    
    def test_can_retry(self):
        """测试可重试检查"""
        from task_queue import Task, TaskStatus
        
        task = Task(
            id="test_123",
            pdf_path="test.pdf",
            mid_json_path="mid.json",
            max_retries=3
        )
        
        assert task.can_retry() == True
        
        task.retry_count = 3
        assert task.can_retry() == False
    
    def test_update_status(self):
        """测试状态更新"""
        from task_queue import Task, TaskStatus
        
        task = Task(
            id="test_123",
            pdf_path="test.pdf",
            mid_json_path="mid.json"
        )
        
        task.update_status(TaskStatus.PROCESSING)
        
        assert task.status == TaskStatus.PROCESSING.value
        assert task.started_at is not None
        
        task.update_status(TaskStatus.COMPLETED)
        
        assert task.status == TaskStatus.COMPLETED.value
        assert task.completed_at is not None
