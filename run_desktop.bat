@echo off
chcp 65001 >nul
title Dual-use - Windows 双模网络测速与分流延迟诊断终端
echo ====================================================================
echo   Dual-use · Windows 桌面端双模测速软件启动器
echo ====================================================================
echo.

where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [提示] 未检测到 Python 环境，正在使用默认浏览器直接打开交互式界面...
    start "" "%~dp0netpulse_speedtest.html"
    exit /b 0
)

echo [1/2] 正在检查依赖环境 (pywebview / httpx)...
python -c "import webview, httpx" >nul 2>nul
if %errorlevel% neq 0 (
    echo [安装] 首次运行正在为您自动安装轻量桌面容器依赖...
    pip install -r "%~dp0requirements.txt" -i https://pypi.tuna.tsinghua.edu.cn/simple
)

echo [2/2] 正在启动 Dual-use 桌面客户端...
start "" pythonw "%~dp0main.py"
if %errorlevel% neq 0 (
    python "%~dp0main.py"
)
