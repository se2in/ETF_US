@echo off
setlocal
cd /d "%~dp0"
if not exist ".\output" mkdir ".\output"
call :main > ".\output\us_etf_tracker_last_run.log" 2>&1
set "EXITCODE=%ERRORLEVEL%"
endlocal & exit /b %EXITCODE%

:main
echo [%date% %time%] Starting US ETF tracker update
python .\us_etf_tracker.py update --config .\us_etf_config.example.json
if errorlevel 1 (
  echo Update step failed.
  exit /b 1
)

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo Git repository not available. Skipping GitHub sync.
  exit /b 0
)

git add docs\index.html docs\.nojekyll
git diff --cached --quiet -- docs/index.html docs/.nojekyll
if not errorlevel 1 (
  echo No GitHub Pages changes detected.
  exit /b 0
)

for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd HH:mm 'KST'\")"`) do set "STAMP=%%i"
if not defined STAMP set "STAMP=%date% %time%"

git commit -m "Update ETF dashboard %STAMP%"
if errorlevel 1 (
  echo Git commit failed.
  exit /b 1
)

git push origin main
if errorlevel 1 (
  echo Git push failed.
  exit /b 1
)

echo GitHub sync completed.
exit /b 0
