# extraction_pipeline.py - 增强版：集成新模块、统一日志
"""
纳米酶文献提取系统 - 提取管道

增强功能：
1. 集成配置管理模块
2. 集成缓存管理
3. 集成任务队列
4. 统一日志系统
5. 更好的错误处理和进度报告
"""

import asyncio
import json
import sys
import logging
from pathlib import Path
from typing import Optional, Callable, Dict, Any
from datetime import datetime

# 尝试导入新模块
try:
    from config_manager import ConfigManager, get_config
    from cache_manager import CacheManager, get_cache_manager
    from task_queue import TaskQueue, TaskStatus, get_task_queue
    from logging_setup import setup_logging, get_logger
    CONFIG_MANAGER_AVAILABLE = True
except ImportError as e:
    CONFIG_MANAGER_AVAILABLE = False
    get_logger = lambda x: logging.getLogger(x)

# 导入原有模块
try:
    import yaml
    from api_client import APIClient
    from llm_extractor import LLMExtractor
    from vlm_extractor import VLMExtractor
    from result_integrator import ResultIntegrator, FIELD_DEFS
    from rule_learner import RuleLearner
    MODULES_AVAILABLE = True
except ImportError as e:
    MODULES_AVAILABLE = False

logger = logging.getLogger(__name__)


class ExtractionPipeline:
    """
    增强版提取管道
    
    支持：
    - 配置管理
    - 结果缓存
    - 任务队列
    - 进度回调
    - 错误恢复
    """
    
    def __init__(
        self,
        config_path: str = "config.yaml",
        output_dir: Optional[str] = None,
        enable_cache: bool = True,
        enable_queue: bool = False,
        use_new_modules: bool = True
    ):
        """
        初始化提取管道
        
        Args:
            config_path: 配置文件路径
            output_dir: 输出目录
            enable_cache: 是否启用缓存
            enable_queue: 是否启用任务队列
            use_new_modules: 是否使用新模块（配置管理、缓存等）
        """
        self._setup_logging()
        
        # 加载配置
        if use_new_modules and CONFIG_MANAGER_AVAILABLE:
            self.config = ConfigManager.get_instance(config_path)
            self.output_dir = Path(output_dir) if output_dir else self.config.pipeline.results_dir
            self.enable_cache = enable_cache and self.config.pipeline.enable_cache
            self.confidence_threshold = self.config.pipeline.confidence_threshold
            self.rulebook_path = self.config.pipeline.rulebook_path
            
            # 初始化缓存管理器
            if self.enable_cache:
                self.cache_manager = get_cache_manager(
                    str(self.config.pipeline.cache_dir),
                    max_age_days=7
                )
            else:
                self.cache_manager = None
            
            # 初始化任务队列
            if enable_queue:
                self.task_queue = get_task_queue(str(self.config.pipeline.task_queue_path))
            else:
                self.task_queue = None
                
        else:
            # 使用原有配置加载方式
            self._load_legacy_config(config_path)
            self.enable_cache = False
            self.cache_manager = None
            self.task_queue = None
        
        # 创建输出目录
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化组件
        self.integrator = ResultIntegrator(self.confidence_threshold)
        self.rule_learner = RuleLearner(str(self.rulebook_path))
        
        logger.info(f"提取管道初始化完成: output_dir={self.output_dir}")
        if self.enable_cache:
            logger.info("缓存功能已启用")
        if self.task_queue:
            logger.info("任务队列已启用")
    
    def _setup_logging(self):
        """设置日志"""
        if not logging.getLogger().handlers:
            setup_logging(level=logging.INFO, detailed=False)
    
    def _load_legacy_config(self, config_path: str):
        """加载原有格式的配置（向后兼容）"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        self.output_dir = Path(config.get('results_dir', './extraction_results'))
        self.confidence_threshold = config.get('confidence_threshold', 0.7)
        self.rulebook_path = Path(config.get('rulebook_path', './rulebook.json'))
    
    async def process_mid_json(
        self,
        mid_json_path: str,
        progress_callback: Optional[Callable[[str, Optional[int]], None]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        处理中间任务JSON
        
        Args:
            mid_json_path: 中间任务JSON路径
            progress_callback: 进度回调 (message, percent)
            use_cache: 是否使用缓存
            
        Returns:
            提取结果
        """
        mid_json_path = Path(mid_json_path)
        
        # 检查缓存
        if use_cache and self.enable_cache and self.cache_manager:
            try:
                config_hash = ""
                if CONFIG_MANAGER_AVAILABLE:
                    config_hash = self.config.get_config_hash()
                
                cached = self.cache_manager.get(
                    str(mid_json_path),
                    config_hash,
                    check_file_change=True
                )
                if cached:
                    logger.info("使用缓存结果")
                    if progress_callback:
                        progress_callback("使用缓存结果", 100)
                    return cached
            except Exception as e:
                logger.warning(f"缓存检查失败: {e}")
        
        # 加载中间任务
        with open(mid_json_path, 'r', encoding='utf-8') as f:
            mid = json.load(f)
        
        chunks = mid['llm_task']['chunks']
        prompt_template = mid['llm_task']['prompt_template']
        vlm_tasks = mid.get('vlm_tasks', [])
        metadata = mid.get('metadata', {})
        
        try:
            async with APIClient() as client:
                # LLM 提取阶段
                if progress_callback:
                    progress_callback(f"开始 LLM 提取 ({len(chunks)} 个文本块)...", 15)
                logger.info(f"LLM 提取: {len(chunks)} 个文本块")
                
                llm = LLMExtractor(client, self._get_batch_size('chunk'))
                llm_results = await llm.extract_all_chunks(chunks, prompt_template)
                logger.info(f"LLM 提取完成: {len(llm_results)}/{len(chunks)} 个文本块")
                
                # VLM 提取阶段
                vlm_results = []
                if vlm_tasks:
                    if progress_callback:
                        progress_callback(f"开始 VLM 提取 ({len(vlm_tasks)} 张图片)...", 50)
                    logger.info(f"VLM 提取: {len(vlm_tasks)} 张图片")
                    
                    vlm = VLMExtractor(client, self._get_batch_size('vlm'))
                    vlm_results = await vlm.extract_all_images(vlm_tasks)
                    logger.info(f"VLM 提取完成: {len(vlm_results)}/{len(vlm_tasks)} 张图片")
                else:
                    logger.info("无图像任务，跳过 VLM 提取")
        
        except RuntimeError as e:
            logger.error(f"API 调用失败: {e}")
            raise
        
        # 结果整合
        if progress_callback:
            progress_callback("整合结果...", 85)
        
        result = self.integrator.integrate(llm_results, vlm_results)
        result['metadata'].update(metadata)
        result['metadata']['processed_at'] = datetime.now().isoformat()
        
        # 保存结果
        out_path = self.output_dir / f"{mid_json_path.stem}_extracted.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"结果已保存至: {out_path}")
        
        # 更新缓存
        if self.enable_cache and self.cache_manager:
            try:
                config_hash = ""
                if CONFIG_MANAGER_AVAILABLE:
                    config_hash = self.config.get_config_hash()
                self.cache_manager.set(str(mid_json_path), config_hash, result)
            except Exception as e:
                logger.warning(f"缓存保存失败: {e}")
        
        if progress_callback:
            progress_callback("提取完成", 100)
        
        return result
    
    def _get_batch_size(self, type_name: str) -> int:
        """获取批处理大小"""
        if CONFIG_MANAGER_AVAILABLE and hasattr(self, 'config'):
            if type_name == 'chunk':
                return self.config.pipeline.chunk_batch_size
            elif type_name == 'vlm':
                return self.config.pipeline.vlm_batch_size
        return 5  # 默认值
    
    def process_mid_json_sync(
        self,
        mid_json_path: str,
        progress_callback: Optional[Callable[[str, Optional[int]], None]] = None,
        use_cache: bool = True
    ) -> str:
        """
        同步执行提取，返回结果 JSON 文件路径
        
        Args:
            mid_json_path: 中间任务JSON路径
            progress_callback: 进度回调
            use_cache: 是否使用缓存
            
        Returns:
            结果JSON文件路径
        """
        result = asyncio.run(self.process_mid_json(
            mid_json_path,
            progress_callback,
            use_cache
        ))
        return str(self.output_dir / f"{Path(mid_json_path).stem}_extracted.json")
    
    def run_feedback(self, mid_json_path: str, corrections: Dict[str, Any]) -> None:
        """
        处理人工反馈
        
        Args:
            mid_json_path: 中间任务JSON路径
            corrections: 修正数据
        """
        for field, new_val in corrections.items():
            self.rule_learner.learn_from_correction(field, None, new_val)
        logger.info(f"已记录 {len(corrections)} 条反馈")
    
    def invalidate_cache(self, mid_json_path: str) -> None:
        """使缓存失效"""
        if self.cache_manager:
            self.cache_manager.invalidate(mid_json_path)
            logger.info(f"缓存已失效: {mid_json_path}")
    
    def clear_cache(self) -> int:
        """清空所有缓存"""
        if self.cache_manager:
            return self.cache_manager.clear_all()
        return 0
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            'output_dir': str(self.output_dir),
            'confidence_threshold': self.confidence_threshold,
        }
        
        if self.cache_manager:
            stats['cache'] = self.cache_manager.get_statistics()
        
        if self.task_queue:
            stats['queue'] = self.task_queue.get_statistics()
        
        return stats


class BatchExtractionPipeline(ExtractionPipeline):
    """
    批量提取管道
    
    继承自 ExtractionPipeline，支持批量处理多个文件
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_results = []
    
    async def process_batch(
        self,
        mid_json_paths: list,
        progress_callback: Optional[Callable[[str, int, int, Optional[int]], None]] = None,
        stop_on_error: bool = False
    ) -> list:
        """
        批量处理多个文件
        
        Args:
            mid_json_paths: 中间任务JSON路径列表
            progress_callback: 进度回调 (message, current, total, percent)
            stop_on_error: 遇到错误是否停止
            
        Returns:
            结果列表
        """
        total = len(mid_json_paths)
        results = []
        
        for i, mid_json_path in enumerate(mid_json_paths):
            try:
                if progress_callback:
                    progress_callback(f"处理 {i+1}/{total}: {Path(mid_json_path).name}", i+1, total, None)
                
                result = await self.process_mid_json(mid_json_path)
                results.append({
                    'path': mid_json_path,
                    'success': True,
                    'result': result
                })
                
            except Exception as e:
                logger.error(f"处理失败 {mid_json_path}: {e}")
                results.append({
                    'path': mid_json_path,
                    'success': False,
                    'error': str(e)
                })
                
                if stop_on_error:
                    break
        
        self.batch_results = results
        return results
    
    def get_batch_summary(self) -> Dict[str, Any]:
        """获取批量处理摘要"""
        if not self.batch_results:
            return {'total': 0, 'successful': 0, 'failed': 0}
        
        return {
            'total': len(self.batch_results),
            'successful': sum(1 for r in self.batch_results if r.get('success')),
            'failed': sum(1 for r in self.batch_results if not r.get('success')),
            'results': self.batch_results
        }


async def main():
    """命令行入口"""
    if len(sys.argv) < 2:
        print("用法: python extraction_pipeline.py <mid_task.json> [--no-cache]")
        sys.exit(1)
    
    mid_json_path = sys.argv[1]
    use_cache = '--no-cache' not in sys.argv
    
    pipeline = ExtractionPipeline()
    
    def progress_callback(msg, percent):
        print(f"[进度 {percent}%] {msg}")
    
    result = await pipeline.process_mid_json(
        mid_json_path,
        progress_callback=progress_callback,
        use_cache=use_cache
    )
    
    # 显示需要审核的字段
    needs_review = {k: v for k, v in result['fields'].items() if v.get('needs_review')}
    if needs_review:
        print("\n⚠️ 以下字段置信度较低，建议人工确认：")
        for field, info in needs_review.items():
            print(f"  {field}: {info['value']} (置信度: {info['confidence']:.2f})")
        
        if input("\n是否输入修正值？(y/n): ").lower() == 'y':
            corrections = {}
            for field in needs_review:
                new_val = input(f"{field} 的正确值 (回车跳过): ").strip()
                if new_val:
                    field_def = next((f for f in FIELD_DEFS if f['name'] == field), None)
                    if field_def and field_def['type'] == 'float':
                        try:
                            new_val = float(new_val)
                        except:
                            pass
                    corrections[field] = new_val
            if corrections:
                pipeline.run_feedback(mid_json_path, corrections)
                print("反馈已记录，规则库已更新。")


if __name__ == "__main__":
    if MODULES_AVAILABLE:
        asyncio.run(main())
    else:
        print("错误: 缺少必要的依赖模块")
        sys.exit(1)
