# api_client.py - 支持速率限制处理的API客户端
"""
纳米酶文献提取系统 - API客户端

增强功能：
1. 支持速率限制处理（429错误自动重试）
2. 指数退避重试机制
3. 请求限流（令牌桶算法）
4. 并发控制
"""

import asyncio
import logging
import time
import json
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import threading

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """速率限制配置"""
    requests_per_minute: int = 60  # 每分钟请求数
    requests_per_second: int = 10   # 每秒请求数
    max_retries: int = 5            # 最大重试次数
    base_delay: float = 1.0        # 基础延迟(秒)
    max_delay: float = 60.0       # 最大延迟(秒)
    retry_on_429: bool = True     # 遇到429是否重试
    respect_retry_after: bool = True  # 是否遵守Retry-After头


class TokenBucket:
    """令牌桶算法实现"""
    
    def __init__(self, rate: float, capacity: float):
        self.rate = rate  # 每秒补充的令牌数
        self.capacity = capacity  # 桶容量
        self.tokens = capacity
        self.last_update = time.time()
        self._lock = threading.Lock()
    
    def consume(self, tokens: float = 1.0) -> float:
        """
        尝试消费令牌
        
        Returns:
            需要等待的秒数，如果可以立即消费则返回0
        """
        with self._lock:
            now = time.time()
            # 补充令牌
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            else:
                # 需要等待的时间
                wait_time = (tokens - self.tokens) / self.rate
                return wait_time
    
    async def async_consume(self, tokens: float = 1.0) -> float:
        """异步版本的令牌消费"""
        wait_time = self.consume(tokens)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        return wait_time


class APIClient:
    """
    API客户端
    
    支持：
    - LLM文本补全
    - VLM图像分析
    - 速率限制处理
    - 自动重试
    """
    
    def __init__(
        self,
        llm_base_url: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        llm_model: Optional[str] = None,
        vlm_base_url: Optional[str] = None,
        vlm_api_key: Optional[str] = None,
        vlm_model: Optional[str] = None,
        rate_limit_config: Optional[RateLimitConfig] = None
    ):
        # 加载配置
        self._load_config()
        
        # LLM配置
        self.llm_base_url = llm_base_url or self.config.get('text_llm', {}).get('base_url')
        self.llm_api_key = llm_api_key or self.config.get('text_llm', {}).get('api_key')
        self.llm_model = llm_model or self.config.get('text_llm', {}).get('model', 'glm-4')
        
        # VLM配置
        self.vlm_base_url = vlm_base_url or self.config.get('vision_vlm', {}).get('base_url')
        self.vlm_api_key = vlm_api_key or self.config.get('vision_vlm', {}).get('api_key')
        self.vlm_model = vlm_model or self.config.get('vision_vlm', {}).get('model', 'kimi-k2.5')
        
        # 速率限制配置
        self.rate_config = rate_limit_config or RateLimitConfig(
            requests_per_minute=self.config.get('requests_per_minute', 60),
            requests_per_second=self.config.get('requests_per_second', 2),  # 降低默认值: 10->2
            max_retries=self.config.get('max_retries', 5)
        )
        
        # 令牌桶 - 更保守的设置
        self.llm_bucket = TokenBucket(
            rate=min(self.rate_config.requests_per_second, 2),  # 最多2个/秒
            capacity=min(self.rate_config.requests_per_second, 2)  # 容量=速率
        )
        self.vlm_bucket = TokenBucket(
            rate=min(self.rate_config.requests_per_second, 1),  # VLM更慢: 1个/秒
            capacity=min(self.rate_config.requests_per_second, 1)
        )
        
        # 统计信息
        self._stats = {
            'llm_requests': 0,
            'vlm_requests': 0,
            'llm_retries': 0,
            'vlm_retries': 0,
            'rate_limited': 0
        }
        
        # HTTP会话
        self._session: Optional[aiohttp.ClientSession] = None
        
        logger.info(f"API客户端初始化: LLM={self.llm_model}, VLM={self.vlm_model}")
    
    def _load_config(self):
        """加载配置文件"""
        try:
            import yaml
            from pathlib import Path
            
            config_path = Path("config.yaml")
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f) or {}
            else:
                self.config = {}
        except Exception as e:
            logger.warning(f"配置文件加载失败: {e}")
            self.config = {}
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=300),
            headers={'Content-Type': 'application/json'}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self._session:
            await self._session.close()
    
    async def _make_request(
        self,
        url: str,
        api_key: str,
        data: Dict,
        model: str,
        bucket: TokenBucket,
        max_tokens: int = 4096,
        timeout: int = 120
    ) -> Dict:
        """
        发起API请求（带速率限制和重试）
        
        Args:
            url: API地址
            api_key: API密钥
            data: 请求数据
            model: 模型名称
            bucket: 令牌桶
            max_tokens: 最大token数
            timeout: 超时时间(秒)
            
        Returns:
            API响应
        """
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        # 构建请求
        request_data = {
            'model': model,
            **data
        }
        
        last_error = None
        retry_count = 0
        
        for attempt in range(self.rate_config.max_retries):
            try:
                # 等待令牌
                await bucket.async_consume(1.0)
                
                async with self._session.post(
                    url,
                    headers=headers,
                    json=request_data,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    status = response.status
                    
                    if status == 200:
                        result = await response.json()
                        return result
                    
                    elif status == 429:
                        # 速率限制
                        self._stats['rate_limited'] += 1
                        
                        if not self.rate_config.retry_on_429:
                            raise Exception(f"API速率限制 (429): {response.reason}")
                        
                        # 获取Retry-After头
                        retry_after = None
                        if self.rate_config.respect_retry_after:
                            retry_after = response.headers.get('Retry-After')
                        
                        if retry_after:
                            wait_time = int(retry_after)
                        else:
                            # 指数退避
                            wait_time = min(
                                self.rate_config.base_delay * (2 ** attempt),
                                self.rate_config.max_delay
)
                        
                        logger.warning(
                            f"API速率限制触发，等待 {wait_time:.1f}秒 "
                            f"(尝试 {attempt + 1}/{self.rate_config.max_retries})"
                        )
                        
                        await asyncio.sleep(wait_time)
                        retry_count += 1
                        continue
                    
                    elif status == 401:
                        raise Exception("API认证失败，请检查API密钥")
                    
                    elif status == 500:
                        # 服务器错误，重试
                        wait_time = self.rate_config.base_delay * (2 ** attempt)
                        logger.warning(f"API服务器错误 (500)，等待 {wait_time:.1f}秒后重试")
                        await asyncio.sleep(wait_time)
                        retry_count += 1
                        continue
                    
                    else:
                        # 其他错误
                        error_text = await response.text()
                        raise Exception(f"API错误 ({status}): {error_text[:200]}")
                        
            except asyncio.TimeoutError:
                last_error = f"请求超时 (>{timeout}秒)"
                wait_time = self.rate_config.base_delay * (2 ** attempt)
                logger.warning(f"{last_error}，等待 {wait_time:.1f}秒后重试")
                await asyncio.sleep(wait_time)
                retry_count += 1
                continue
                
            except aiohttp.ClientError as e:
                last_error = str(e)
                wait_time = self.rate_config.base_delay * (2 ** attempt)
                logger.warning(f"请求失败: {e}，等待 {wait_time:.1f}秒后重试")
                await asyncio.sleep(wait_time)
                retry_count += 1
                continue
        
        # 所有重试都失败
        raise Exception(
            f"API请求失败，已重试 {retry_count} 次: {last_error}"
        )
    
    async def chat_completion_text(
        self,
        messages: List[Dict],
        temperature: float = 0.1,
        max_tokens: int = 4096
    ) -> str:
        """
        LLM文本补全
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            生成的文本
        """
        url = f"{self.llm_base_url.rstrip('/')}/chat/completions"
        
        data = {
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens
        }
        
        result = await self._make_request(
            url=url,
            api_key=self.llm_api_key,
            data=data,
            model=self.llm_model,
            bucket=self.llm_bucket,
            max_tokens=max_tokens
        )
        
        self._stats['llm_requests'] += 1
        
        # 解析响应
        try:
            return result['choices'][0]['message']['content']
        except (KeyError, IndexError) as e:
            raise Exception(f"解析LLM响应失败: {e}, 原始响应: {result}")
    
    async def chat_completion_vision(
        self,
        messages: List[Dict],
        temperature: float = 0.1,
        max_tokens: int = 2048
    ) -> str:
        """
        VLM图像分析
        
        Args:
            messages: 包含图像的消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            生成的文本
        """
        url = f"{self.vlm_base_url.rstrip('/')}/chat/completions"
        
        data = {
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens
        }
        
        result = await self._make_request(
            url=url,
            api_key=self.vlm_api_key,
            data=data,
            model=self.vlm_model,
            bucket=self.vlm_bucket,
            max_tokens=max_tokens,
            timeout=180  # VLM超时更长
        )
        
        self._stats['vlm_requests'] += 1
        
        # 解析响应
        try:
            return result['choices'][0]['message']['content']
        except (KeyError, IndexError) as e:
            raise Exception(f"解析VLM响应失败: {e}, 原始响应: {result}")
    
    async def test_connection(self, model_type: str = 'text') -> Dict:
        """
        测试API连接
        
        Args:
            model_type: 'text' 或 'vision'
            
        Returns:
            测试结果
        """
        try:
            if model_type == 'text':
                result = await self.chat_completion_text(
                    messages=[{"role": "user", "content": "Hi"}],
                    max_tokens=10
                )
                return {'success': True, 'message': f'连接成功: {result[:50]}...'}
            else:
                result = await self.chat_completion_vision(
                    messages=[{"role": "user", "content": "Hi"}],
                    max_tokens=10
                )
                return {'success': True, 'message': f'连接成功: {result[:50]}...'}
        except Exception as e:
            return {'success': False, 'message': str(e)}
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        total_requests = self._stats['llm_requests'] + self._stats['vlm_requests']
        total_retries = self._stats['llm_retries'] + self._stats['vlm_retries']
        
        return {
            **self._stats,
            'total_requests': total_requests,
            'total_retries': total_retries,
            'retry_rate': round(total_retries / total_requests, 3) if total_requests else 0
        }
    
    def reset_statistics(self):
        """重置统计信息"""
        self._stats = {
            'llm_requests': 0,
            'vlm_requests': 0,
            'llm_retries': 0,
            'vlm_retries': 0,
            'rate_limited': 0
        }


# ========== 便捷函数 ==========

_async_client: Optional[APIClient] = None


async def get_async_client() -> APIClient:
    """获取异步客户端单例"""
    global _async_client
    if _async_client is None:
        _async_client = APIClient()
        await _async_client.__aenter__()
    return _async_client


async def close_async_client():
    """关闭异步客户端"""
    global _async_client
    if _async_client:
        await _async_client.__aexit__(None, None, None)
        _async_client = None
