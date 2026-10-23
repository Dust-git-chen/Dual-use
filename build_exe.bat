@echo off
chcp 65001 >nul
title Dual-use - 一键打包独立 Windows EXE 工具 v3.0
echo ====================================================================
echo   Dual-use · 正在使用 PyInstaller 打包独立单文件 EXE (v3.0 千兆修复版)
echo ====================================================================
echo.

cd /d "%~dp0"

echo [1/3] 检查依赖...
python -c "import webview" 2>nul
if errorlevel 1 (
    echo     正在安装 pywebview...
    pip install "pywebview>=5.0" -i https://pypi.tuna.tsinghua.edu.cn/simple
)
python -c "import PyInstaller" 2>nul
if errorlevel 1 (
    echo     正在安装 pyinstaller...
    pip install "pyinstaller>=6.0.0" -i https://pypi.tuna.tsinghua.edu.cn/simple
)

echo [2/3] 清理旧的构建产物...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist Dual-use.spec del /q Dual-use_SpeedTest.spec

echo [3/3] 开始打包...
pyinstaller --noconfirm --onefile --windowed --icon "icon_src\aeropulse.ico" --add-data "icon_src\aeropulse.ico;icon_src" --add-data "icon_src\aeropulse_favicon.png;icon_src" ^
  --name "Dual-use" ^
  --add-data "netpulse_speedtest.html;." ^
  --hidden-import webview.platforms.edgechromium ^
  --hidden-import webview.platforms.winforms ^
  --hidden-import clr ^
  main.py

if errorlevel 1 (
    echo.
    echo [失败] 打包出错，请检查上方日志。
    pause
    exit /b 1
)

echo.
echo ====================================================================
echo   打包完成！独立可执行文件位于: dist\Dual-use.exe
echo ====================================================================
dir dist\Dual-use.exe
pause
