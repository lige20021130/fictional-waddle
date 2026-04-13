# base_extractor.py - 提取器基类
"""
纳米酶文献提取系统 - 提取器基类

功能：
1. 定义 LLM/VLM 提取器的公共接口
2. 实现通用批量处理逻辑
3. 统一错误处理和重试机制
4. 进度回调支持

使用方法：
    from base_extractor import BaseExtractor, LLMBasedExtractor, VLMBasedExtractor
    
    class MyExtractor(LLMBasedExtractor):
        async def _extract_single(self, chunk: str, prompt: str) -> Dict:
            # 实现具体的提取逻辑
            pass
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar('T')  # 输入类型
R = TypeVar('R')  # 输出类型


class ExtractionStatus(Enum):
    """提取状态"""
    SUCCESS = "success"
    PARTIAL = "partial"  # 部分成功
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


@dataclass
class ExtractionResult:
    """单个提取结果"""
    status: ExtractionStatus
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    source: Optional[str] = None
    processing_time: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    @property
    def is_success(self) -> bool:
        return self.status == ExtractionStatus.SUCCESS
    
    @property
    def is_partial(self) -> bool:
        return self.status == ExtractionStatus.PARTIAL
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status.value,
            'data': self.data,
            'error': self.error,
            'retry_count': self.retry_count,
            'source': self.source,
            'processing_time': self.processing_time,
            'timestamp': self.timestamp
        }


@dataclass
class BatchExtractionResult:
    """批量提取结果"""
    total: int
    successful: int
    partial: int
    failed: int
    results: List[ExtractionResult]
    total_time: float
    start_time: str
    end_time: str = field(default_factory=lambda: datetime.now().isoformat())
    
    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.successful / self.total
    
    @property
    def effective_rate(self) -> float:
        """有效率（成功+部分成功）"""
        if self.total == 0:
            return 0.0
        return (self.successful + self.partial) / self.total
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total': self.total,
            'successful': self.successful,
            'partial': self.partial,
            'failed': self.failed,
            'success_rate': round(self.success_rate, 3),
            'effective_rate': round(self.effective_rate, 3),
            'total_time': round(self.total_time, 2),
            'start_time': self.start_time,
            'end_time': self.end_time,
            'results': [r.to_dict() for r in self.results]
        }
    
    def get_successful_results(self) -> List[Dict[str, Any]]:
        """获取所有成功的结果"""
        return [r.data for r in self.results if r.is_success and r.data]
    
    def get_failed_results(self) -> List[ExtractionResult]:
        """获取所有失败的结果"""
        return [r for r in self.results if r.status == ExtractionStatus.FAILED]
    
    def get_partial_results(self) -> List[ExtractionResult]:
        """获取所有部分成功的结果"""
        return [r for r in self.results if r.is_partial]


class BaseExtractor(ABC, Generic[T, R]):
    """
    提取器基类
    
    提供通用的批量处理、重试、日志等功能
    """
    
    def __init__(
        self,
        client,
        batch_size: int = 5,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """
        初始化提取器
        
        Args:
            client: API客户端
            batch_size: 并发批处理大小
            max_retries: 最大重试次数
            retry_delay: 重试延迟(秒)
            progress_callback: 进度回调函数 (message, current, total)
        """
        self.client = client
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.progress_callback = progress_callback
        
        # 统计
        self._stats = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_retries': 0
        }
    
    @abstractmethod
    async def _extract_single(self, item: T, **kwargs) -> ExtractionResult:
        """
        提取单个项目（子类必须实现）
        
        Args:
            item: 输入项目
            **kwargs: 额外参数
            
        Returns:
            ExtractionResult
        """
        pass
    
    @abstractmethod
    def _prepare_items(self, items: List[T], **kwargs) -> List[T]:
        """
        预处理项目列表（可选实现）
        
        Args:
            items: 原始项目列表
            **kwargs: 额外参数
            
        Returns:
            处理后的项目列表
        """
        return items
    
    async def _retry_with_backoff(
        self, 
        item: T, 
        extract_func,
        **kwargs
    ) -> ExtractionResult:
        """
        带指数退避的重试机制
        
        Args:
            item: 输入项目
            extract_func: 提取函数
            **kwargs: 额外参数
            
        Returns:
            ExtractionResult
        """
        last_error = None
        result = ExtractionResult(status=ExtractionStatus.FAILED)
        
        for attempt in range(self.max_retries):
            try:
                result = await extract_func(item, **kwargs)
                if result.is_success:
                    return result
                
                # 部分成功也返回
                if result.is_partial:
                    return result
                    
            except Exception as e:
                last_error = str(e)
                result = ExtractionResult(
                    status=ExtractionStatus.RETRYING,
                    error=last_error
                )
            
            # 指数退避
            if attempt < self.max_retries - 1:
                delay = self.retry_delay * (2 ** attempt)
                logger.warning(f"重试 {attempt + 1}/{self.max_retries}, 等待 {delay}s: {last_error}")
                await asyncio.sleep(delay)
        
        # 所有重试都失败
        return ExtractionResult(
            status=ExtractionStatus.FAILED,
            error=last_error or "未知错误",
            retry_count=self.max_retries
        )
    
    async def extract_batch(
        self, 
        items: List[T],
        **kwargs
    ) -> BatchExtractionResult:
        """
        批量提取
        
        Args:
            items: 项目列表
            **kwargs: 额外参数
            
        Returns:
            BatchExtractionResult
        """
        start_time = datetime.now()
        total = len(items)
        
        logger.info(f"开始批量提取: {total} 个项目, 批处理大小: {self.batch_size}")
        
        # 预处理
        processed_items = self._prepare_items(items, **kwargs)
        
        semaphore = asyncio.Semaphore(self.batch_size)
        results: List[ExtractionResult] = []
        counter = 0
        counter_lock = asyncio.Lock()
        
        async def bounded_process(item: T, idx: int) -> ExtractionResult:
            nonlocal counter
            async with semaphore:
                start = time.time()
                
                try:
                    result = await self._retry_with_backoff(
                        item, 
                        self._extract_single,
                        **kwargs
                    )
                    result.source = f"item_{idx}"
                    result.processing_time = time.time() - start
                    
                except Exception as e:
                    result = ExtractionResult(
                        status=ExtractionStatus.FAILED,
                        error=str(e),
                        source=f"item_{idx}",
                        processing_time=time.time() - start
                    )
                
                # 更新计数器
                async with counter_lock:
                    counter += 1
                    current = counter
                
                # 进度回调
                if self.progress_callback and current % max(1, total // 10) == 0:
                    msg = f"处理进度: {current}/{total}"
                    try:
                        self.progress_callback(msg, current, total)
                    except Exception as e:
                        logger.warning(f"进度回调失败: {e}")
                
                return result
        
        # 创建任务
        tasks = [bounded_process(item, i) for i, item in enumerate(processed_items)]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        for i, r in enumerate(raw_results):
            if isinstance(r, Exception):
                results.append(ExtractionResult(
                    status=ExtractionStatus.FAILED,
                    error=str(r),
                    source=f"item_{i}"
                ))
            else:
                results.append(r)
        
        # 统计
        successful = sum(1 for r in results if r.is_success)
        partial = sum(1 for r in results if r.is_partial)
        failed = sum(1 for r in results if r.status == ExtractionStatus.FAILED)
        
        total_time = (datetime.now() - start_time).total_seconds()
        
        batch_result = BatchExtractionResult(
            total=total,
            successful=successful,
            partial=partial,
            failed=failed,
            results=results,
            total_time=total_time,
            start_time=start_time.isoformat()
        )
        
        logger.info(
            f"批量提取完成: 成功={successful}/{total}, "
            f"部分成功={partial}, 失败={failed}, "
            f"耗时={total_time:.1f}s"
        )
        
        return batch_result
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            **self._stats,
            'success_rate': (
                self._stats['total_successful'] / self._stats['total_processed']
                if self._stats['total_processed'] > 0 else 0
            )
        }
    
    def reset_statistics(self):
        """重置统计信息"""
        self._stats = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_retries': 0
        }


class LLMBasedExtractor(BaseExtractor[str, Dict[str, Any]]):
    """
    LLM文本提取器基类
    
    专门处理文本块的提取
    """
    
    def _prepare_items(self, items: List[str], **kwargs) -> List[str]:
        """预处理文本块"""
        # 过滤空文本
        return [item for item in items if item and item.strip()]
    
    async def _extract_single(self, chunk: str, **kwargs) -> ExtractionResult:
        """
        提取单个文本块（子类实现）
        
        Args:
            chunk: 文本块
            **kwargs: 需要 prompt_template
            
        Returns:
            ExtractionResult
        """
        prompt_template = kwargs.get('prompt_template', '')
        if not prompt_template:
            return ExtractionResult(
                status=ExtractionStatus.FAILED,
                error="缺少 prompt_template"
            )
        
        try:
            user_prompt = prompt_template.replace("{{text}}", chunk)
            messages = [{"role": "user", "content": user_prompt}]
            response = await self.client.chat_completion_text(messages)
            
            if not response:
                return ExtractionResult(
                    status=ExtractionStatus.FAILED,
                    error="API返回空响应"
                )
            
            # 解析响应（子类可能需要覆盖）
            import json
            try:
                data = json.loads(response)
                return ExtractionResult(
                    status=ExtractionStatus.SUCCESS,
                    data=data
                )
            except json.JSONDecodeError:
                return ExtractionResult(
                    status=ExtractionStatus.PARTIAL,
                    data={'raw_response': response},
                    error="JSON解析失败，返回原始响应"
                )
                
        except Exception as e:
            return ExtractionResult(
                status=ExtractionStatus.FAILED,
                error=str(e)
            )


class VLMBasedExtractor(BaseExtractor[Dict[str, Any], Dict[str, Any]]):
    """
    VLM图像提取器基类
    
    专门处理图像的提取
    """
    
    def _prepare_items(self, items: List[Dict[str, Any]], **kwargs) -> List[Dict[str, Any]]:
        """预处理图像任务"""
        # 过滤无效任务
        return [
            item for item in items 
            if item.get('image_path') and item.get('image_path') != '未知'
        ]
    
    async def _extract_single(self, task: Dict[str, Any], **kwargs) -> ExtractionResult:
        """
        提取单个图像（子类实现）
        
        Args:
            task: 任务字典，包含 image_path, caption 等
            
        Returns:
            ExtractionResult
        """
        from pathlib import Path
        
        image_path = task.get('image_path', '')
        caption = task.get('caption', '')
        
        if not image_path or not Path(image_path).exists():
            return ExtractionResult(
                status=ExtractionStatus.FAILED,
                error=f"图片不存在: {image_path}"
            )
        
        try:
            import base64
            
            # 编码图片
            with open(image_path, 'rb') as f:
                b64_data = base64.b64encode(f.read()).decode('utf-8')
            
            # 构建消息
            prompt = kwargs.get('vision_prompt', 
                "请分析这张图片并提取相关信息")
            
            if caption:
                prompt = f"图片标注：{caption}\n\n{prompt}"
            
            messages = [{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_data}"}}
                ]
            }]
            
            response = await self.client.chat_completion_vision(messages)
            
            if not response:
                return ExtractionResult(
                    status=ExtractionStatus.FAILED,
                    error="API返回空响应"
                )
            
            # 解析响应
            import json
            try:
                data = json.loads(response)
                # 添加来源信息
                data['_source'] = task
                return ExtractionResult(
                    status=ExtractionStatus.SUCCESS,
                    data=data
                )
            except json.JSONDecodeError:
                return ExtractionResult(
                    status=ExtractionStatus.PARTIAL,
                    data={'raw_response': response, '_source': task},
                    error="JSON解析失败"
                )
                
        except Exception as e:
            return ExtractionResult(
                status=ExtractionStatus.FAILED,
                error=str(e)
            )


# 便捷函数
def create_progress_callback(
    logger_fn, 
    step: int = 5
) -> Callable[[str, int, int], None]:
    """
    创建进度回调函数
    
    Args:
        logger_fn: 日志函数
        step: 每隔多少报告一次
        
    Returns:
        回调函数
    """
    def callback(msg: str, current: int, total: int):
        if current % step == 0 or current == total:
            logger_fn(f"{msg} ({current}/{total})")
    return callback
