# vlm_extractor.py - 增强版：统一日志、进度回调
import base64
import json
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Callable
from api_client_v2 import APIClient

logger = logging.getLogger(__name__)

VISION_PROMPT = """请分析这张来自纳米酶文献的图表。

{{caption}}

提取以下信息：
1. 图表类型（TEM、XRD、Lineweaver-Burk图等）
2. 若为酶动力学图，读取Km和Vmax数值
3. 若为TEM图，估计颗粒尺寸(nm)
4. 其他关键数值

输出JSON格式：
{
    "chart_type": "string",
    "extracted_values": {
        "Km": {"value": float或null, "unit": "mM"},
        "Vmax": {"value": float或null, "unit": "mM/s"},
        "particle_size": {"value": float或null, "unit": "nm"}
    },
    "observations": "string"
}"""

class VLMExtractor:
    def __init__(self, client: APIClient, batch_size: int = 2):
        self.client = client
        self.batch_size = batch_size

    def _encode_image(self, image_path: str) -> str:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    async def _extract_from_image(self, image_path: str, caption: str = "") -> Dict:
        if not Path(image_path).exists():
            logger.warning(f"图片不存在: {image_path}")
            return {"error": "file_not_found"}

        b64 = self._encode_image(image_path)
        prompt = VISION_PROMPT.replace("{{caption}}", f"图片标注：{caption}" if caption else "")

        messages = [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}}
            ]
        }]

        response = await self.client.chat_completion_vision(messages)
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return {"error": "json_parse_failed", "raw": response[:200]}

    async def extract_all_images(self, vlm_tasks: List[Dict]) -> List[Dict]:
        logger.info(f"开始处理 {len(vlm_tasks)} 个图像任务, 批处理大小: {self.batch_size}")
        semaphore = asyncio.Semaphore(self.batch_size)
        processed = 0

        async def bounded(task):
            nonlocal processed
            async with semaphore:
                try:
                    image_path = task.get('image_path', '未知')
                    logger.debug(f"处理图像: {Path(image_path).name}")
                    result = await self._extract_from_image(task['image_path'], task.get('caption', ''))
                    processed += 1
                    if processed % 3 == 0 or processed == len(vlm_tasks):
                        logger.info(f"VLM 进度: {processed}/{len(vlm_tasks)} 张图片")
                except Exception as e:
                    logger.error(f"VLM 任务 {processed} 失败 ({image_path}): {e}")
                    result = {"error": str(e)}
                # 返回元组而非直接修改result
                return (result, task)

        tasks = [bounded(t) for t in vlm_tasks]
        # 收集元组结果
        raw_results = await asyncio.gather(*tasks)
        
        # 后处理: 将 _source 添加到结果字典中
        final_results = []
        for res, src_task in raw_results:
            res['_source'] = src_task
            final_results.append(res)
        
        # 统计
        error_count = sum(1 for r in final_results if 'error' in r)
        logger.info(f"VLM 提取完成: 成功 {len(final_results) - error_count}/{len(vlm_tasks)} 张图片")
        if error_count > 0:
            logger.warning(f"其中 {error_count} 个图像任务出现错误")
        
        return final_results