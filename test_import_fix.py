#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试模块导入修复
"""

import sys
import os

print("=" * 60)
print("测试模块导入路径修复")
print("=" * 60)

# 测试1: 检查当前工作目录
print("\n[1] 当前工作目录:")
print(f"    os.getcwd() = {os.getcwd()}")

# 测试2: 检查脚本所在目录
print("\n[2] 脚本所在目录:")
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f"    __file__ = {__file__}")
print(f"    script_dir = {script_dir}")

# 测试3: 检查sys.path
print("\n[3] sys.path内容:")
for i, path in enumerate(sys.path[:5], 1):
    print(f"    {i}. {path}")

# 测试4: 添加脚本目录到sys.path
print("\n[4] 添加脚本目录到sys.path:")
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
    print(f"    ✓ 已添加: {script_dir}")
else:
    print(f"    ✓ 已存在: {script_dir}")

# 测试5: 尝试导入模块
print("\n[5] 尝试导入extraction_pipeline:")
try:
    import extraction_pipeline
    from extraction_pipeline import ExtractionPipeline
    print("    ✓ 成功导入 ExtractionPipeline")
    print(f"    模块位置: {extraction_pipeline.__file__}")
except ImportError as e:
    print(f"    ✗ 导入失败: {e}")

# 测试6: 尝试导入其他模块
print("\n[6] 尝试导入其他模块:")
modules_to_test = [
    ('nanozyme_preprocessor_midjson', 'NanozymePreprocessor'),
    ('api_client', 'APIClient'),
    ('llm_extractor', 'LLMExtractor'),
    ('vlm_extractor', 'VLMExtractor'),
]

for module_name, class_name in modules_to_test:
    try:
        module = __import__(module_name, fromlist=[class_name])
        cls = getattr(module, class_name)
        print(f"    ✓ {module_name}.{class_name}")
    except Exception as e:
        print(f"    ✗ {module_name}.{class_name}: {e}")

print("\n" + "=" * 60)
print("诊断建议")
print("=" * 60)

print("""
如果导入失败,可能的原因:
1. 文件不存在: 检查 extraction_pipeline.py 是否在 d:\\ocr 目录
2. 权限问题: 确保文件可读
3. 语法错误: 运行 python -m py_compile extraction_pipeline.py
4. 依赖缺失: 检查 requirements.txt 是否已安装

修复方案:
- GUI启动时会自动添加脚本目录到sys.path
- 确保从 d:\\ocr 目录启动GUI
- 使用 start_basic_gui.bat 启动(推荐)
""")

print("=" * 60)
print("✓ 测试完成!")
print("=" * 60)
