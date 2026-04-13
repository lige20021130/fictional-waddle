# api_client.py
import sys
import asyncio
import aiohttp
import yaml
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RateLimitState:
    """单个 API 供应商的速率限制状态"""
    limit: Optional[int] = None          # 时间窗口内的总配额
    remaining: Optional[int] = None      # 当前剩余配额
    reset_at: Optional[float] = None     # 配额重置时间戳（秒）
    
    # 动态调整参数
    min_interval: float = 0.1            # 最小请求间隔（秒）
    max_interval: float = 5.0            # 最大请求间隔（秒）
    backoff_factor: float = 1.5          # 减速因子
    recovery_factor: float = 0.9         # 恢复因子
    
    # 内部状态
    last_request_time: float = 0.0
    consecutive_success: int = 0
    consecutive_429: int = 0
    
    def update_from_headers(self, headers: Dict[str, str]):
        """从响应头更新限流状态（支持多种常见命名）"""
        # 尝试多种可能的头部命名
        limit_keys = ['X-RateLimit-Limit', 'RateLimit-Limit', 'X-Ratelimit-Limit']
        remaining_keys = ['X-RateLimit-Remaining', 'RateLimit-Remaining', 'X-Ratelimit-Remaining']
        reset_keys = ['X-RateLimit-Reset', 'RateLimit-Reset', 'X-Ratelimit-Reset']
        
        for key in limit_keys:
            if key in headers:
                try:
                    self.limit = int(headers[key])
                    break
                except ValueError:
                    pass
        
        for key in remaining_keys:
            if key in headers:
                try:
                    self.remaining = int(headers[key])
                    break
                except ValueError:
                    pass
        
        for key in reset_keys:
            if key in headers:
                try:
                    self.reset_at = float(headers[key])
                    break
                except ValueError:
                    pass

    def get_wait_time(self) -> float:
        """计算建议的等待时间（基于剩余配额和重置时间）"""
        now = time.time()
        wait = self.min_interval
        
        # 如果有剩余配额信息
        if self.remaining is not None and self.limit is not None and self.reset_at is not None:
            if self.remaining <= 0 and self.reset_at > now:
                # 配额耗尽，等待到重置时间
                wait = max(wait, self.reset_at - now + 0.5)
            elif self.limit > 0:
                # 按比例调整：剩余越少，间隔越大
                ratio = self.remaining / self.limit
                if ratio < 0.2:
                    wait = max(wait, self.min_interval * 5)
                elif ratio < 0.5:
                    wait = max(wait, self.min_interval * 2)
        
        # 根据历史 429 次数增加等待
        if self.consecutive_429 > 0:
            wait = min(wait * (self.backoff_factor ** self.consecutive_429), self.max_interval)
        
        # 根据成功次数减少等待（恢复）
        if self.consecutive_success > 10:
            wait = max(wait * self.recovery_factor, self.min_interval)
        
        return wait

    def record_success(self, headers: Dict[str, str]):
        """记录一次成功的请求"""
        self.update_from_headers(headers)
        self.last_request_time = time.time()
        self.consecutive_success += 1
        self.consecutive_429 = 0

    def record_429(self, retry_after: Optional[int] = None):
        """记录一次 429 响应"""
        self.consecutive_429 += 1
        self.consecutive_success = 0
        if retry_after:
            # 强制等待 Retry-After 指定的秒数
            self.reset_at = time.time() + retry_after
            self.remaining = 0


class APIClient:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            self._create_default_config()
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 为文本 LLM 和视觉 VLM 分别维护限流状态
        self.text_rate_state = RateLimitState()
        self.vision_rate_state = RateLimitState()
        
        # 全局限流锁（确保请求串行，避免并发竞争）
        self._request_lock = asyncio.Lock()

    def _create_default_config(self):
        # ... 保持原有默认配置生成逻辑不变 ...
        pass

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def chat_completion_text(self, messages: List[Dict]) -> str:
        """调用文本 LLM"""
        return await self._chat_completion(
            self.config['text_llm'],
            messages,
            self.config.get('text_max_tokens', 4096),
            {"type": "json_object"},
            self.text_rate_state
        )

    async def chat_completion_vision(self, messages: List[Dict]) -> str:
        """调用视觉 VLM"""
        return await self._chat_completion(
            self.config['vision_vlm'],
            messages,
            self.config.get('vision_max_tokens', 2048),
            {"type": "json_object"},
            self.vision_rate_state
        )

    async def test_connection(self, llm_type: str = "text") -> dict:
        """测试API连通性
        
        Args:
            llm_type: 'text' 或 'vision'
            
        Returns:
            dict: {'success': bool, 'message': str, 'response_time': float}
        """
        import time
        
        llm_cfg = self.config['text_llm'] if llm_type == 'text' else self.config['vision_vlm']
        url = f"{llm_cfg['base_url'].rstrip('/')}/chat/completions"
        headers = {"Authorization": f"Bearer {llm_cfg['api_key']}"}
        
        # 简单测试消息
        payload = {
            "model": llm_cfg['model'],
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 10
        }
        
        start_time = time.time()
        try:
            async with self.session.post(url, json=payload, headers=headers, timeout=10) as resp:
                response_time = time.time() - start_time
                
                if resp.status == 200:
                    return {
                        'success': True,
                        'message': f"连接成功 (响应时间: {response_time:.2f}s)",
                        'response_time': response_time
                    }
                else:
                    error_text = await resp.text()
                    return {
                        'success': False,
                        'message': f"API错误 {resp.status}: {error_text[:200]}",
                        'response_time': response_time
                    }
        except asyncio.TimeoutError:
            return {
                'success': False,
                'message': f"连接超时 (>{response_time:.2f}s)",
                'response_time': response_time
            }
        except Exception as e:
            response_time = time.time() - start_time
            return {
                'success': False,
                'message': f"连接失败: {str(e)}",
                'response_time': response_time
            }

    async def _chat_completion(
        self,
        llm_cfg: Dict,
        messages: List[Dict],
        max_tokens: int,
        response_format: Optional[Dict],
        rate_state: RateLimitState
    ) -> str:
        url = f"{llm_cfg['base_url'].rstrip('/')}/chat/completions"
        headers = {"Authorization": f"Bearer {llm_cfg['api_key']}"}
        payload = {
            "model": llm_cfg['model'],
            "messages": messages,
            "temperature": self.config.get('temperature', 0.1),
            "max_tokens": max_tokens,
        }
        if response_format:
            payload['response_format'] = response_format

        max_retries = self.config.get('max_retries', 3)
        
        for attempt in range(max_retries):
            # 获取建议等待时间并执行等待（全局限流）
            async with self._request_lock:
                wait_time = rate_state.get_wait_time()
                now = time.time()
                time_since_last = now - rate_state.last_request_time
                if time_since_last < wait_time:
                    await asyncio.sleep(wait_time - time_since_last)
            
            try:
                async with self.session.post(url, json=payload, headers=headers) as resp:
                    # 解析响应头
                    resp_headers = {k: v for k, v in resp.headers.items()}
                    
                    if resp.status == 200:
                        data = await resp.json()
                        content = data['choices'][0]['message']['content']
                        if not content:
                            logger.warning("API 返回空内容")
                            raise Exception("Empty response")
                        # 记录成功
                        async with self._request_lock:
                            rate_state.record_success(resp_headers)
                        return content
                    
                    elif resp.status == 429:
                        # 处理速率限制
                        retry_after = resp_headers.get('Retry-After')
                        if retry_after:
                            try:
                                wait = int(retry_after)
                            except ValueError:
                                wait = 5
                        else:
                            wait = 5
                        
                        logger.warning(f"速率限制 (429)，等待 {wait} 秒...")
                        async with self._request_lock:
                            rate_state.record_429(wait)
                        
                        # 等待 Retry-After 时间后重试
                        await asyncio.sleep(wait)
                        continue
                    
                    else:
                        text = await resp.text()
                        raise Exception(f"API error {resp.status}: {text[:200]}")
            
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        
        return ""