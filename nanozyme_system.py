# nanozyme_system.py - 纳米酶文献提取系统 (整合版)
"""
纳米酶文献提取系统 - 整合版

整合了以下新模块：
1. 配置管理 (config_manager)
2. API客户端增强 (api_client_v2 - 支持速率限制)
3. 结果缓存 (cache_manager)
4. 任务队列 (task_queue)
5. Pydantic数据模型 (nanozyme_models)
6. 统一日志 (logging_setup)

使用方法：
    from nanozyme_system import NanozymeSystem
    
    system = NanozymeSystem()
    system.setup()
    
    # 或使用命令行：
    # python nanozyme_system.py --config config.yaml
"""

import asyncio
import json
import sys
import logging
import argparse
from pathlib import Path
from typing import Optional, Callable, Dict, Any
from datetime import datetime

# 导入新模块
from config_manager import ConfigManager, get_config
from cache_manager import CacheManager, get_cache_manager
from task_queue import TaskQueue, get_task_queue, TaskPriority
from logging_setup import setup_logging, get_logger, GUILogHandler
from nanozyme_models import ExtractionOutput, validate_extraction_result, ValidationReport

# 导入原有模块
try:
    import yaml
    from api_client_v2 import APIClient
    from llm_extractor import LLMExtractor
    from vlm_extractor import VLMExtractor
    from result_integrator import ResultIntegrator
    from rule_learner import RuleLearner
    MODULES_OK = True
except ImportError as e:
    MODULES_OK = False
    print(f"警告: 缺少模块 {e}")


class NanozymeSystem:
    """
    纳米酶文献提取系统 - 整合版
    
    提供统一的接口来管理整个提取流程
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化系统
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.logger = None
        self.config_manager: Optional[ConfigManager] = None
        self.cache_manager: Optional[CacheManager] = None
        self.task_queue: Optional[TaskQueue] = None
        self.api_client: Optional[APIClient] = None
        
        # 组件
        self.llm_extractor = None
        self.vlm_extractor = None
        self.integrator = None
        self.rule_learner = None
    
    def setup(
        self,
        log_level: int = logging.INFO,
        log_file: Optional[str] = None,
        enable_cache: bool = True,
        enable_queue: bool = True,
        gui_callback: Optional[Callable] = None
    ) -> bool:
        """
        设置系统
        
        Args:
            log_level: 日志级别
            log_file: 日志文件路径
            enable_cache: 是否启用缓存
            enable_queue: 是否启用任务队列
            gui_callback: GUI日志回调
            
        Returns:
            是否设置成功
        """
        # 设置日志
        setup_logging(
            level=log_level,
            log_file=log_file,
            gui_callback=gui_callback
        )
        self.logger = get_logger(__name__)
        
        self.logger.info("=" * 60)
        self.logger.info("纳米酶文献提取系统 (整合版) 启动")
        self.logger.info("=" * 60)
        
        # 加载配置
        self.config_manager = ConfigManager.get_instance(self.config_path)
        
        # 验证配置
        validation = self.config_manager.validate()
        if not validation['llm']:
            self.logger.error("LLM配置无效，请检查config.yaml")
            return False
        if not validation['vlm']:
            self.logger.warning("VLM配置无效，图像提取将不可用")
        
        # 初始化缓存
        if enable_cache:
            self.cache_manager = get_cache_manager(
                str(self.config_manager.pipeline.cache_dir),
                max_age_days=7
            )
            self.logger.info(f"缓存已启用: {self.config_manager.pipeline.cache_dir}")
        
        # 初始化任务队列
        if enable_queue:
            self.task_queue = get_task_queue(
                str(self.config_manager.pipeline.task_queue_path)
            )
            self.logger.info("任务队列已启用")
        
        # 初始化结果整合器
        self.integrator = ResultIntegrator(
            self.config_manager.pipeline.confidence_threshold
        )
        
        # 初始化规则学习器
        self.rule_learner = RuleLearner(
            str(self.config_manager.pipeline.rulebook_path)
        )
        
        self.logger.info("系统设置完成")
        self.logger.info(f"LLM: {self.config_manager.llm.model}")
        self.logger.info(f"VLM: {self.config_manager.vlm.model}")
        
        return True
    
    async def _create_api_client(self) -> APIClient:
        """创建API客户端"""
        return APIClient(
            llm_base_url=self.config_manager.llm.base_url,
            llm_api_key=self.config_manager.llm.api_key,
            llm_model=self.config_manager.llm.model,
            vlm_base_url=self.config_manager.vlm.base_url,
            vlm_api_key=self.config_manager.vlm.api_key,
            vlm_model=self.config_manager.vlm.model
        )
    
    async def extract(
        self,
        mid_json_path: str,
        progress_callback: Optional[Callable[[str, Optional[int]], None]] = None,
        use_cache: bool = True,
        add_to_queue: bool = True
    ) -> Dict[str, Any]:
        """
        执行提取
        
        Args:
            mid_json_path: 中间任务JSON路径
            progress_callback: 进度回调
            use_cache: 是否使用缓存
            add_to_queue: 是否添加到任务队列
            
        Returns:
            提取结果
        """
        mid_path = Path(mid_json_path)
        
        # 检查缓存
        if use_cache and self.cache_manager:
            config_hash = self.config_manager.get_config_hash()
            cached = self.cache_manager.get(
                str(mid_path),
                config_hash,
                check_file_change=True
            )
            if cached:
                self.logger.info("使用缓存结果")
                if progress_callback:
                    progress_callback("使用缓存结果", 100)
                return cached
        
        # 添加到任务队列
        task_id = None
        if add_to_queue and self.task_queue:
            task_id = self.task_queue.add(
                str(mid_path),
                str(mid_path),
                metadata={'source': 'direct'}
            )
            self.task_queue.mark_processing(task_id)
        
        try:
            # 加载中间任务
            with open(mid_path, 'r', encoding='utf-8') as f:
                mid_data = json.load(f)
            
            chunks = mid_data['llm_task']['chunks']
            prompt_template = mid_data['llm_task']['prompt_template']
            vlm_tasks = mid_data.get('vlm_tasks', [])
            metadata = mid_data.get('metadata', {})
            
            # 创建API客户端
            async with await self._create_api_client() as client:
                # LLM提取
                if progress_callback:
                    progress_callback(f"LLM提取 ({len(chunks)} 个文本块)...", 15)
                
                self.llm_extractor = LLMExtractor(
                    client,
                    self.config_manager.pipeline.chunk_batch_size
                )
                llm_results = await self.llm_extractor.extract_all_chunks(
                    chunks, prompt_template
                )
                self.logger.info(f"LLM完成: {len(llm_results)}/{len(chunks)}")
                
                # VLM提取
                vlm_results = []
                if vlm_tasks:
                    if progress_callback:
                        progress_callback(f"VLM提取 ({len(vlm_tasks)} 张图片)...", 50)
                    
                    self.vlm_extractor = VLMExtractor(
                        client,
                        self.config_manager.pipeline.vlm_batch_size
                    )
                    vlm_results = await self.vlm_extractor.extract_all_images(vlm_tasks)
                    self.logger.info(f"VLM完成: {len(vlm_results)}/{len(vlm_tasks)}")
            
            # 整合结果
            if progress_callback:
                progress_callback("整合结果...", 85)
            
            result = self.integrator.integrate(llm_results, vlm_results)
            result['metadata'].update(metadata)
            result['metadata']['processed_at'] = datetime.now().isoformat()
            
            # 验证结果
            is_valid, validation_report = validate_extraction_result(result)
            if not is_valid:
                self.logger.warning(f"结果验证问题:\n{validation_report.summary()}")
            
            # 保存结果
            output_dir = self.config_manager.pipeline.results_dir
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{mid_path.stem}_extracted.json"
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"结果已保存: {output_path}")
            
            # 更新缓存
            if use_cache and self.cache_manager:
                config_hash = self.config_manager.get_config_hash()
                self.cache_manager.set(str(mid_path), config_hash, result)
            
            # 更新任务队列
            if task_id and self.task_queue:
                self.task_queue.mark_completed(
                    task_id,
                    result_path=str(output_path)
                )
            
            if progress_callback:
                progress_callback("完成", 100)
            
            return result
            
        except Exception as e:
            self.logger.error(f"提取失败: {e}")
            
            if task_id and self.task_queue:
                self.task_queue.mark_failed(task_id, str(e))
            
            raise
    
    def extract_sync(
        self,
        mid_json_path: str,
        progress_callback: Optional[Callable[[str, Optional[int]], None]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """同步提取"""
        return asyncio.run(self.extract(
            mid_json_path,
            progress_callback,
            use_cache
        ))
    
    def run_feedback(self, corrections: Dict[str, Any]) -> None:
        """
        处理人工反馈
        
        Args:
            corrections: 修正数据 {字段名: 正确值}
        """
        for field, new_val in corrections.items():
            self.rule_learner.learn_from_correction(field, None, new_val)
        
        self.logger.info(f"已记录 {len(corrections)} 条反馈")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取系统统计"""
        stats = {
            'timestamp': datetime.now().isoformat()
        }
        
        if self.config_manager:
            stats['config'] = self.config_manager.get_status_report()
        
        if self.cache_manager:
            stats['cache'] = self.cache_manager.get_statistics()
        
        if self.task_queue:
            stats['queue'] = self.task_queue.get_statistics()
        
        return stats
    
    def clear_cache(self) -> int:
        """清空缓存"""
        if self.cache_manager:
            return self.cache_manager.clear_all()
        return 0
    
    def cleanup(self) -> None:
        """清理资源"""
        if self.task_queue:
            self.task_queue.stop()


# ========== 命令行入口 ==========

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='纳米酶文献提取系统')
    parser.add_argument('mid_json', nargs='?', help='中间任务JSON文件路径')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径')
    parser.add_argument('--no-cache', action='store_true', help='禁用缓存')
    parser.add_argument('--no-queue', action='store_true', help='禁用任务队列')
    parser.add_argument('--debug', action='store_true', help='启用调试日志')
    
    args = parser.parse_args()
    
    if not args.mid_json:
        parser.print_help()
        print("\n示例:")
        print("  python nanozyme_system.py sample_mid_task.json")
        print("  python nanozyme_system.py sample.json --debug")
        return
    
    # 创建系统
    system = NanozymeSystem(config_path=args.config)
    
    # 设置
    log_level = logging.DEBUG if args.debug else logging.INFO
    success = system.setup(
        log_level=log_level,
        enable_cache=not args.no_cache,
        enable_queue=not args.no_queue
    )
    
    if not success:
        print("系统设置失败，请检查配置")
        return
    
    # 定义进度回调
    def progress(msg, percent):
        if percent:
            print(f"[{percent:3d}%] {msg}")
        else:
            print(f"       {msg}")
    
    # 执行提取
    try:
        result = system.extract_sync(
            args.mid_json,
            progress_callback=progress,
            use_cache=not args.no_cache
        )
        
        # 显示结果摘要
        print("\n" + "=" * 60)
        print("提取完成!")
        print("=" * 60)
        
        needs_review = {
            k: v for k, v in result.get('fields', {}).items()
            if v.get('needs_review')
        }
        
        if needs_review:
            print(f"\n⚠️  {len(needs_review)} 个字段需要人工审核:")
            for field, info in needs_review.items():
                print(f"  - {field}: {info.get('value')} (置信度: {info.get('confidence', 0):.2f})")
        else:
            print("\n✓ 所有字段提取成功!")
        
        # 显示统计
        stats = system.get_statistics()
        if 'cache' in stats:
            cache = stats['cache']
            print(f"\n缓存统计: {cache.get('total_entries', 0)} 条")
        
        # 询问是否记录反馈
        if needs_review:
            print("\n是否输入修正值? (输入字段名=正确值，回车跳过)")
            corrections = {}
            for field in needs_review.keys():
                val = input(f"  {field}: ").strip()
                if val:
                    corrections[field] = val
            
            if corrections:
                system.run_feedback(corrections)
                print("反馈已记录")
        
    except Exception as e:
        print(f"\n错误: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
    finally:
        system.cleanup()


if __name__ == "__main__":
    if MODULES_OK:
        main()
    else:
        print("错误: 缺少必要的依赖模块，请运行: pip install -r requirements.txt")
