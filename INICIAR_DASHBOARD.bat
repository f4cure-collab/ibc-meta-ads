@echo off
echo ============================================================
echo   IBC Dashboard de Performance - Meta Ads
echo   Abrindo em http://localhost:5001
echo   Login: ibcadmin / ibcadmin
echo ============================================================
echo.
cd /d "%~dp0"
start http://localhost:5001
python dashboard_app.py
pause
