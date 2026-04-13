#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速测试API连通性功能
"""

import asyncio

async def test_api_connection():
    """测试API连通性"""
    from api_client import APIClient
    
    print("=" * 60)
    print("测试 API 连通性")
    print("=" * 60)
    
    async with APIClient() as client:
        # 测试文本LLM
        print("\n[1] 测试文本 LLM...")
        result_text = await client.test_connection('text')
        print(f"    成功: {result_text['success']}")
        print(f"    消息: {result_text['message']}")
        print(f"    响应时间: {result_text['response_time']:.2f}s")
        
        # 测试视觉VLM
        print("\n[2] 测试视觉 VLM...")
        result_vision = await client.test_connection('vision')
        print(f"    成功: {result_vision['success']}")
        print(f"    消息: {result_vision['message']}")
        print(f"    响应时间: {result_vision['response_time']:.2f}s")
    
    print("\n" + "=" * 60)
    if result_text['success'] and result_vision['success']:
        print("✓ 两个API都连接成功!")
    else:
        print("✗ 部分或全部API连接失败,请检查config.yaml配置")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_api_connection())
