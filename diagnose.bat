@echo off
title 环境诊断工具
echo ========================================
echo 纳米酶OCR系统 - 环境诊断
echo ========================================
echo.

cd /d %~dp0

echo [1/4] 检查Python环境...
python --version
if errorlevel 1 (
    echo ✗ Python未找到
    pause
    exit /b 1
)
echo ✓ Python环境正常
echo.

echo [2/4] 检查依赖模块...
python -c "import aiohttp; print('  aiohttp:', aiohttp.__version__)" 2>nul
if errorlevel 1 (
    echo ✗ aiohttp未安装
    echo 请运行: pip install -r requirements.txt
    pause
    exit /b 1
)
echo ✓ aiohttp正常

python -c "import yaml; print('  pyyaml:', yaml.__version__)" 2>nul
if errorlevel 1 (
    echo ✗ pyyaml未安装
    echo 请运行: pip install -r requirements.txt
    pause
    exit /b 1
)
echo ✓ pyyaml正常
echo.

echo [3/4] 检查配置文件...
if exist "config.yaml" (
    echo ✓ config.yaml存在
) else (
    echo ✗ config.yaml不存在
    pause
    exit /b 1
)
echo.

echo [4/4] 测试API连通性...
python test_api_connection.py
if errorlevel 1 (
    echo.
    echo ✗ API连接测试失败
    echo 请检查config.yaml中的API配置
    pause
    exit /b 1
)

echo.
echo ========================================
echo ✓ 所有检查通过! 系统可以正常使用
echo ========================================
pause
