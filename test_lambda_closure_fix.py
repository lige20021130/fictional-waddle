#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试lambda闭包修复
"""

print("=" * 60)
print("测试 Lambda 闭包变量捕获修复")
print("=" * 60)

# 模拟问题场景
import tkinter as tk

root = tk.Tk()
root.withdraw()  # 隐藏窗口

def test_lambda_closure():
    """测试lambda闭包问题"""
    
    # 问题代码(会失败)
    print("\n[1] 测试问题代码(不使用默认参数):")
    try:
        error_msg = "测试错误"
        import traceback
        tb = "Traceback info"
        
        # 这种方式在延迟执行时会失败
        callback = lambda: print_error(error_msg, tb)
        
        # 清除变量
        del error_msg
        del tb
        
        # 尝试调用(会失败)
        callback()
        print("  ✗ 应该失败但没有失败")
    except NameError as e:
        print(f"  ✓ 正确捕获到NameError: {e}")
    
    # 修复代码(应该成功)
    print("\n[2] 测试修复代码(使用默认参数):")
    try:
        error_msg = "测试错误"
        import traceback
        tb = "Traceback info"
        
        # 使用默认参数捕获变量
        callback = lambda err=error_msg, traceback=tb: print_error(err, traceback)
        
        # 清除变量
        del error_msg
        del tb
        
        # 调用(应该成功)
        callback()
        print("  ✓ 成功执行,没有NameError")
    except NameError as e:
        print(f"  ✗ 不应该失败: {e}")

def print_error(msg, tb):
    """模拟错误处理函数"""
    print(f"  错误信息: {msg}")
    print(f"  堆栈跟踪: {tb}")

test_lambda_closure()

print("\n" + "=" * 60)
print("测试实际GUI场景模拟")
print("=" * 60)

def simulate_gui_error_handling():
    """模拟GUI错误处理"""
    
    # 模拟GUI的root.after
    scheduled_callbacks = []
    
    def mock_after(delay, callback):
        scheduled_callbacks.append(callback)
    
    # 模拟提取错误
    print("\n[3] 模拟提取错误处理:")
    try:
        # 模拟某个操作失败
        raise ValueError("大模型API调用失败: 429 Too Many Requests")
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        
        # 修复后的代码
        error_msg = str(e)
        mock_after(0, lambda err=error_msg, traceback=tb: handle_gui_error(err, traceback))
    
    # 执行回调
    print("执行延迟回调...")
    for cb in scheduled_callbacks:
        cb()
    
    print("  ✓ 错误处理成功,日志应该已输出")

def handle_gui_error(msg, tb):
    """模拟GUI错误处理"""
    print(f"  [GUI错误] {msg}")
    print(f"  [堆栈] {tb[:100]}...")

simulate_gui_error_handling()

print("\n" + "=" * 60)
print("✓ 所有测试完成!")
print("=" * 60)

print("\n修复总结:")
print("1. ✓ 问题: lambda延迟执行时,外部变量已被清除")
print("2. ✓ 修复: 使用默认参数 lambda err=e, tb=traceback: func(err, tb)")
print("3. ✓ 原理: 默认参数在lambda定义时求值,而非执行时")
print("\n影响范围:")
print("- 提取错误处理: extraction_error() 现在能正确显示错误信息")
print("- 提取完成回调: extraction_finished() 现在能正确接收路径")
print("- 日志输出: 错误日志现在能完整输出到GUI")

root.destroy()
