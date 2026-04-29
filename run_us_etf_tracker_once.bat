@echo off
setlocal
cd /d "%~dp0"
if not exist ".\output" mkdir ".\output"
call .\run_us_etf_tracker_silent.bat
type ".\output\us_etf_tracker_last_run.log"
echo.
echo Result log: %CD%\output\us_etf_tracker_last_run.log
echo Dashboard : %CD%\output\us_etf_weight_dashboard.html
echo Pages HTML: %CD%\docs\index.html
echo.
pause
endlocal
