# llm_extractor.py - 增强版：JSON容错、单独提取方法
import json
import asyncio
import logging
import re
from typing import Dict, List, Optional, Any
from api_client_v2 import APIClient

logger = logging.getLogger(__name__)


class JSONFixer:
    """
    JSON 格式修复器

    尝试修复常见的 LLM 输出格式问题
    """

    @staticmethod
    def fix_common_issues(text: str) -> Optional[Dict]:
        """
        修复常见 JSON 格式问题

        修复策略：
        1. 移除 markdown 代码块标记
        2. 处理截断的 JSON
        3. 处理多余逗号
        4. 处理单引号
        5. 处理未闭合的括号
        """
        # 移除 markdown 代码块
        text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'^```\s*$', '', text, flags=re.MULTILINE)

        # 移除前导/尾随空白
        text = text.strip()

        # 尝试多种修复策略
        strategies = [
            JSONFixer._fix_single_quotes,
            JSONFixer._fix_trailing_comma,
            JSONFixer._fix_unquoted_keys,
            JSONFixer._fix_truncated_json,
            JSONFixer._fix_control_characters,
        ]

        for strategy in strategies:
            text = strategy(text)

        # 尝试解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # 尝试更激进的修复
            text = JSONFixer._aggressive_fix(text)
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return None

    @staticmethod
    def _fix_single_quotes(text: str) -> str:
        """将单引号替换为双引号（处理简单情况）"""
        # 只替换 property names 和字符串值中的单引号
        result = []
        i = 0
        in_string = False
        current_quote = None

        while i < len(text):
            char = text[i]

            if not in_string and char in ('"', "'"):
                in_string = True
                current_quote = char
                result.append('"')
            elif in_string and char == current_quote:
                # 检查是否是转义的引号
                if i > 0 and text[i-1] == '\\':
                    result.append(char)
                else:
                    in_string = False
                    result.append('"')
            elif in_string and char == "'" and current_quote == "'":
                result.append('"')
            else:
                result.append(char)

            i += 1

        return ''.join(result)

    @staticmethod
    def _fix_trailing_comma(text: str) -> str:
        """移除尾随逗号"""
        # 移除对象末尾的逗号
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        return text

    @staticmethod
    def _fix_unquoted_keys(text: str) -> str:
        """修复未加引号的键（简单情况）"""
        # 匹配未引用的键
        def replace_key(match):
            key = match.group(1)
            return f'"{key}"'

        # 只处理键名是简单字母数字组合的情况
        text = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', replace_key, text)
        return text

    @staticmethod
    def _fix_truncated_json(text: str) -> Optional[str]:
        """尝试修复截断的 JSON"""
        # 找到最后一个完整的对象/数组
        stack = []
        in_string = False
        escape_next = False

        for i, char in enumerate(text):
            if escape_next:
                escape_next = False
                continue

            if char == '\\':
                escape_next = True
                continue

            if char == '"' and not escape_next:
                in_string = not in_string
                continue

            if in_string:
                continue

            if char in '{[':
                stack.append(char)
            elif char == '}':
                if stack and stack[-1] == '{':
                    stack.pop()
            elif char == ']':
                if stack and stack[-1] == '[':
                    stack.pop()

        # 如果栈不为空，尝试闭合
        if stack:
            closes = {'{': '}', '[': ']'}
            result = text
            for opener in reversed(stack):
                result += closes[opener]
            return result

        return text

    @staticmethod
    def _fix_control_characters(text: str) -> str:
        """移除控制字符"""
        # 移除常见的控制字符，保留换行和制表符
        text = re.sub(r'[\x00-\x09\x0b\x0c\x0e-\x1f\x7f]', '', text)
        return text

    @staticmethod
    def _aggressive_fix(text: str) -> str:
        """
        激进的 JSON 修复

        尝试提取有效的 JSON 部分
        """
        # 尝试找到 JSON 对象的边界
        # 首先找到第一个 { 或 [
        first_brace = text.find('{')
        first_bracket = text.find('[')

        start = 0
        if first_brace != -1 and (first_bracket == -1 or first_brace < first_bracket):
            start = first_brace
        elif first_bracket != -1:
            start = first_bracket

        if start > 0:
            text = text[start:]

        # 尝试闭合 JSON
        text = JSONFixer._fix_truncated_json(text) or text

        # 移除尾部的非 JSON 内容
        # 找到最后一个 } 或 ]
        last_brace = text.rfind('}')
        last_bracket = text.rfind(']')

        end = len(text)
        if last_brace != -1 and (last_bracket == -1 or last_brace > last_bracket):
            end = last_brace + 1
        elif last_bracket != -1:
            end = last_bracket + 1

        text = text[:end]

        return text


class LLMExtractor:
    """
    增强版 LLM 提取器

    新增功能：
    1. JSON 容错处理
    2. 单独提取方法（支持重试）
    3. 更好的错误处理
    """

    def __init__(self, client: APIClient, batch_size: int = 5):
        self.client = client
        self.batch_size = batch_size
        self.json_fixer = JSONFixer()

    async def extract_single_chunk(
        self,
        chunk: str,
        prompt_template: str
    ) -> Optional[Dict]:
        """
        提取单个文本块

        Args:
            chunk: 文本块内容
            prompt_template: 提示模板

        Returns:
            提取的字典结果，失败返回 None
        """
        try:
            user_prompt = prompt_template.replace("{{text}}", chunk)
            messages = [{"role": "user", "content": user_prompt}]
            response = await self.client.chat_completion_text(messages)

            if not response:
                logger.warning("API 返回空响应")
                return None

            # 尝试解析 JSON
            result = self._parse_json_response(response)
            if result:
                return result

            logger.warning(f"JSON 解析失败: {response[:100]}...")
            return None

        except Exception as e:
            logger.error(f"提取单个文本块失败: {e}")
            raise

    async def extract_all_chunks(
        self,
        chunks: List[str],
        prompt_template: str
    ) -> List[Dict]:
        """
        批量提取文本块（原有方法保持兼容）

        Args:
            chunks: 文本块列表
            prompt_template: 提示模板

        Returns:
            成功提取的结果列表
        """
        logger.info(f"开始处理 {len(chunks)} 个文本块, 批处理大小: {self.batch_size}")
        semaphore = asyncio.Semaphore(self.batch_size)
        processed = 0

        async def bounded(chunk: str):
            nonlocal processed
            async with semaphore:
                try:
                    result = await self.extract_single_chunk(chunk, prompt_template)
                    processed += 1
                    if processed % 5 == 0 or processed == len(chunks):
                        logger.info(f"LLM 进度: {processed}/{len(chunks)} 个文本块")
                    return result
                except Exception as e:
                    logger.error(f"文本块处理异常: {e}")
                    return None

        tasks = [bounded(c) for c in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.error(f"文本块 {i+1} 处理失败: {r}")
            elif r:
                valid.append(r)

        logger.info(f"LLM 提取完成: 成功 {len(valid)}/{len(chunks)} 个文本块")
        return valid

    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """
        解析 JSON 响应，尝试修复格式问题

        Args:
            response: API 返回的原始文本

        Returns:
            解析后的字典，失败返回 None
        """
        if not response:
            return None

        # 去除首尾空白
        response = response.strip()

        # 方法1：直接解析
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass

        # 方法2：使用 JSONFixer 修复
        fixed = self.json_fixer.fix_common_issues(response)
        if fixed:
            return fixed

        # 方法3：尝试提取 JSON 部分
        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                fixed = self.json_fixer.fix_common_issues(json_match.group(0))
                if fixed:
                    return fixed

        return None

    def validate_result(self, result: Dict) -> bool:
        """
        验证提取结果的有效性

        Args:
            result: 提取结果字典

        Returns:
            是否有效
        """
        if not isinstance(result, dict):
            return False

        # 检查是否包含至少一个有效字段
        valid_fields = 0
        for field_def in [
            {"name": "material", "type": "string"},
            {"name": "metal_center", "type": "string"},
            {"name": "enzyme_type", "type": "string"},
            {"name": "Km", "type": "float"},
            {"name": "Vmax", "type": "float"},
        ]:
            field_name = field_def['name']
            if field_name in result and result[field_name] is not None:
                if field_def['type'] == 'float':
                    try:
                        float(result[field_name])
                        valid_fields += 1
                    except (ValueError, TypeError):
                        pass
                else:
                    if str(result[field_name]).strip():
                        valid_fields += 1

        return valid_fields > 0


# 保持向后兼容的别名
LegacyLLMExtractor = LLMExtractor
