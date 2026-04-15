@echo off
title Meta Ads - Testador de Criativos
echo Iniciando servidor...
echo.
echo Para parar, feche esta janela ou pressione Ctrl+C
echo.
cd /d "%~dp0"
start "" "chrome" "http://localhost:5000"
python app.py
pause
