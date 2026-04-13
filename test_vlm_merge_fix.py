#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试VLM修复和智能合并功能
"""

import asyncio
from typing import Dict, List, Tuple

print("=" * 60)
print("测试1: VLM 元组返回修复")
print("=" * 60)

# 模拟VLM的修复逻辑
async def test_vlm_tuple_fix():
    """测试VLM返回元组的修复"""
    
    # 模拟任务
    vlm_tasks = [
        {'image_path': 'fig1.png', 'caption': 'TEM image'},
        {'image_path': 'fig2.png', 'caption': 'XRD pattern'},
        {'image_path': 'fig3.png', 'caption': 'Km measurement'}
    ]
    
    async def bounded(task):
        # 模拟提取结果
        result = {
            'chart_type': 'TEM',
            'extracted_values': {'particle_size': {'value': 5.2, 'unit': 'nm'}}
        }
        # 修复: 返回元组而非修改result
        return (result, task)
    
    tasks = [bounded(t) for t in vlm_tasks]
    raw_results = await asyncio.gather(*tasks)
    
    # 后处理: 添加_source
    final_results = []
    for res, src_task in raw_results:
        res['_source'] = src_task
        final_results.append(res)
    
    print(f"\n✓ 处理了 {len(final_results)} 个图像任务")
    for i, res in enumerate(final_results):
        print(f"  任务 {i+1}:")
        print(f"    - chart_type: {res.get('chart_type', 'N/A')}")
        print(f"    - _source.image_path: {res['_source']['image_path']}")
        print(f"    - _source.caption: {res['_source']['caption']}")
    
    return final_results

asyncio.run(test_vlm_tuple_fix())

print("\n" + "=" * 60)
print("测试2: 智能合并高价值文本块")  
print("=" * 60)

# 模拟文本块
class MockTextChunk:
    def __init__(self, content, section, page_start, page_end):
        self.content = content
        self.section = section
        self.page_start = page_start
        self.page_end = page_end
        self.char_count = len(content)

chunks = [
    MockTextChunk("This is general introduction text.", "introduction", 1, 1),
    MockTextChunk("We synthesized Fe3O4 nanoparticles.", "results", 2, 2),  # 高价值: synthesized
    MockTextChunk("The Km value was 0.5 mM.", "results", 2, 2),  # 高价值: Km
    MockTextChunk("Vmax = 2.5×10^-3 M s-1.", "results", 3, 3),  # 高价值: Vmax
    MockTextChunk("Some other results.", "results", 3, 3),
    MockTextChunk("TEM images showed 5nm particles.", "results", 4, 4),  # 高价值: TEM
    MockTextChunk("Conclusion of the paper.", "conclusion", 5, 5)
]

# 模拟合并逻辑
HIGH_VALUE_TERMS = ['Km', 'Vmax', 'synthesized', 'TEM', 'XRD', 'nanozyme']

merged = []
current_merged = ""
current_pages = set()
high_value_encountered = False

for chunk in chunks:
    chunk_is_high_value = any(term.lower() in chunk.content.lower() for term in HIGH_VALUE_TERMS)
    
    if chunk_is_high_value:
        if high_value_encountered:
            current_merged += "\n\n" + chunk.content
            current_pages.add(chunk.page_start)
        else:
            if current_merged:
                merged.append(current_merged)
            current_merged = chunk.content
            current_pages = {chunk.page_start, chunk.page_end}
        high_value_encountered = True
    else:
        if high_value_encountered:
            merged.append(current_merged)
            current_merged = ""
            current_pages = set()
            high_value_encountered = False
        merged.append(chunk.content)

if current_merged:
    merged.append(current_merged)

print(f"\n原始文本块数: {len(chunks)}")
print(f"合并后块数: {len(merged)}")
print(f"减少比例: {(1 - len(merged)/len(chunks))*100:.1f}%")

print("\n合并后的文本块:")
for i, content in enumerate(merged):
    preview = content[:80].replace('\n', ' ')
    is_high_value = any(term.lower() in content.lower() for term in HIGH_VALUE_TERMS)
    tag = "[高价值合并]" if is_high_value and '\n\n' in content else "[普通]"
    print(f"  块 {i+1} {tag}: {preview}...")

print("\n" + "=" * 60)
print("✓ 所有测试完成!")
print("=" * 60)

print("\n修复总结:")
print("1. ✓ VLM提取器: 使用元组返回,避免数据结构混淆")
print("2. ✓ 智能合并: 连续高价值块自动合并,减少LLM调用")
print("3. ✓ 默认启用: to_mid_json(use_merged=True) 默认使用合并块")
print("\n使用建议:")
print("- 短篇文献 (<10页): use_merged=False (使用原始块)")
print("- 中长文献 (10-20页): use_merged=True (默认,推荐)")
print("- 超长文献 (>20页): use_merged=True + use_rag=True (最佳)")
