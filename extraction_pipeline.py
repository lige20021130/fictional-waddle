# extraction_pipeline.py
import asyncio
import json
import sys
import logging
from pathlib import Path
import yaml
from api_client import APIClient
from llm_extractor import LLMExtractor
from vlm_extractor import VLMExtractor
from result_integrator import ResultIntegrator, FIELD_DEFS
from rule_learner import RuleLearner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExtractionPipeline:
    def __init__(self, config_path: str = "config.yaml", output_dir: str = None):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        # 如果指定了输出目录,使用它;否则使用配置文件中的
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = Path(self.config['results_dir'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.integrator = ResultIntegrator(self.config['confidence_threshold'])
        self.rule_learner = RuleLearner(self.config['rulebook_path'])

    async def process_mid_json(self, mid_json_path: str):
        with open(mid_json_path, 'r', encoding='utf-8') as f:
            mid = json.load(f)

        chunks = mid['llm_task']['chunks']
        prompt_template = mid['llm_task']['prompt_template']
        vlm_tasks = mid.get('vlm_tasks', [])
        metadata = mid.get('metadata', {})

        try:
            async with APIClient() as client:
                llm_extractor = LLMExtractor(client, self.config.get('chunk_batch_size', 5))
                llm_results = await llm_extractor.extract_all_chunks(chunks, prompt_template)
                logger.info(f"LLM处理了 {len(llm_results)} 个文本块")

                vlm_results = []
                if vlm_tasks:
                    vlm_extractor = VLMExtractor(client, self.config.get('vlm_batch_size', 2))
                    vlm_results = await vlm_extractor.extract_all_images(vlm_tasks)
                    logger.info(f"VLM处理了 {len(vlm_results)} 张图片")
        except RuntimeError as e:
            logger.error(str(e))
            sys.exit(1)

        result = self.integrator.integrate(llm_results, vlm_results)
        result['metadata'].update(metadata)

        out_path = self.output_dir / f"{Path(mid_json_path).stem}_extracted.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logger.info(f"提取完成，结果保存至 {out_path}")
        return result

    def run_feedback(self, mid_json_path: str, corrections):
        for field, new_val in corrections.items():
            self.rule_learner.learn_from_correction(field, None, new_val)
        logger.info("反馈已记录")

    def process_mid_json_sync(self, mid_json_path: str, progress_callback=None) -> str:
        """同步执行提取，返回结果 JSON 文件路径"""
        return asyncio.run(self._process_with_progress(mid_json_path, progress_callback))

    async def _process_with_progress(self, mid_json_path: str, progress_callback=None) -> str:
        """带进度回调的异步处理"""
        try:
            if progress_callback:
                progress_callback("读取 mid_task.json...", 5)
            logger.info("=" * 60)
            logger.info("开始执行大模型提取流程")
            logger.info("=" * 60)
            
            with open(mid_json_path, 'r', encoding='utf-8') as f:
                mid = json.load(f)
            
            chunks = mid['llm_task']['chunks']
            prompt_template = mid['llm_task']['prompt_template']
            vlm_tasks = mid.get('vlm_tasks', [])
            metadata = mid.get('metadata', {})
            
            logger.info(f"加载配置文件: {len(chunks)} 个文本块, {len(vlm_tasks)} 个图像任务")

            async with APIClient() as client:
                # LLM 提取阶段
                if progress_callback:
                    progress_callback(f"开始 LLM 提取 ({len(chunks)} 个文本块)...", 15)
                logger.info("-" * 60)
                logger.info("阶段 1: LLM 文本提取开始")
                logger.info("-" * 60)
                logger.info(f"文本块数量: {len(chunks)}")
                logger.info(f"批处理大小: {self.config.get('chunk_batch_size', 5)}")
                logger.info(f"API配置: {self.config['text_llm']['model']} @ {self.config['text_llm']['base_url']}")
                
                llm = LLMExtractor(client, self.config.get('chunk_batch_size', 5))
                logger.info("开始调用 LLM API...")
                llm_results = await llm.extract_all_chunks(chunks, prompt_template)
                
                logger.info(f"LLM 提取完成, 成功处理 {len(llm_results)}/{len(chunks)} 个文本块")
                if len(llm_results) < len(chunks):
                    logger.warning(f"警告: {len(chunks) - len(llm_results)} 个文本块提取失败")

                # VLM 提取阶段
                vlm_results = []
                if vlm_tasks:
                    if progress_callback:
                        progress_callback(f"开始 VLM 提取 ({len(vlm_tasks)} 张图片)...", 50)
                    logger.info("-" * 60)
                    logger.info("阶段 2: VLM 图像提取开始")
                    logger.info("-" * 60)
                    logger.info(f"图像任务数量: {len(vlm_tasks)}")
                    logger.info(f"批处理大小: {self.config.get('vlm_batch_size', 2)}")
                    logger.info(f"API配置: {self.config['vision_vlm']['model']} @ {self.config['vision_vlm']['base_url']}")
                    
                    vlm = VLMExtractor(client, self.config.get('vlm_batch_size', 2))
                    logger.info("开始调用 VLM API...")
                    vlm_results = await vlm.extract_all_images(vlm_tasks)
                    
                    logger.info(f"VLM 提取完成, 成功处理 {len(vlm_results)}/{len(vlm_tasks)} 张图片")
                    
                    # 检查是否有错误
                    error_count = sum(1 for r in vlm_results if 'error' in r)
                    if error_count > 0:
                        logger.warning(f"警告: {error_count} 个图像任务出现错误")
                else:
                    logger.info("阶段 2: 无图像任务,跳过 VLM 提取")

                # 结果整合阶段
                if progress_callback:
                    progress_callback("整合结果...", 85)
                logger.info("-" * 60)
                logger.info("阶段 3: 结果整合开始")
                logger.info("-" * 60)
                
                result = self.integrator.integrate(llm_results, vlm_results)
                result['metadata'].update(metadata)
                
                # 统计信息
                fields_count = len(result.get('fields', {}))
                low_confidence = sum(1 for f in result.get('fields', {}).values() if f.get('needs_review', False))
                logger.info(f"整合完成, 共提取 {fields_count} 个字段")
                if low_confidence > 0:
                    logger.warning(f"其中 {low_confidence} 个字段置信度较低,建议人工审核")

                # 保存结果
                out_path = self.output_dir / f"{Path(mid_json_path).stem}_extracted.json"
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)

                logger.info(f"结果已保存至: {out_path}")
                logger.info("=" * 60)
                logger.info("大模型提取流程全部完成")
                logger.info("=" * 60)
                
                if progress_callback:
                    progress_callback("提取完成", 100)
                return str(out_path)
                
        except RuntimeError as e:
            logger.error("=" * 60)
            logger.error(f"提取流程失败: {str(e)}")
            logger.error("=" * 60)
            import traceback
            logger.error(f"详细堆栈:\n{traceback.format_exc()}")
            raise
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"提取流程发生未知错误: {str(e)}")
            logger.error("=" * 60)
            import traceback
            logger.error(f"详细堆栈:\n{traceback.format_exc()}")
            raise

async def main():
    if len(sys.argv) < 2:
        print("用法: python extraction_pipeline.py <mid_task.json>")
        sys.exit(1)

    pipeline = ExtractionPipeline()
    result = await pipeline.process_mid_json(sys.argv[1])

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
                pipeline.run_feedback(sys.argv[1], corrections)
                print("反馈已记录，规则库已更新。")

if __name__ == "__main__":
    asyncio.run(main())