@echo off
title Nanozyme PDF Converter (Basic)
echo Starting, please wait...

cd /d %~dp0
if not exist "venv\Scripts\activate.bat" (
    echo Error: Virtual environment not found. Please run: python -m venv venv
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
if errorlevel 1 (
    echo Failed to activate virtual environment
    pause
    exit /b 1
)

python pdf_basic_gui.py
if errorlevel 1 (
    echo Program exited with error
    pause
    exit /b 1
)

pause
