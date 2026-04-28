@echo off
setlocal

set "ROOT=%~dp0"
set "RUNNER=%ROOT%run_us_etf_tracker_silent.bat"
set "TASK_0940=ETF US Tracker 0940"
set "TASK_1500=ETF US Tracker 1500"
set "TASK_2200=ETF US Tracker 2200"

if not exist "%RUNNER%" (
  echo Runner not found: %RUNNER%
  pause
  exit /b 1
)

schtasks /Create /F /TN "%TASK_0940%" /SC DAILY /ST 09:40 /TR "\"%RUNNER%\""
if errorlevel 1 (
  echo Failed to register %TASK_0940%
  pause
  exit /b 1
)

schtasks /Create /F /TN "%TASK_1500%" /SC DAILY /ST 15:00 /TR "\"%RUNNER%\""
if errorlevel 1 (
  echo Failed to register %TASK_1500%
  pause
  exit /b 1
)

schtasks /Create /F /TN "%TASK_2200%" /SC DAILY /ST 22:00 /TR "\"%RUNNER%\""
if errorlevel 1 (
  echo Failed to register %TASK_2200%
  pause
  exit /b 1
)

echo Registered tasks:
echo - %TASK_0940% at 09:40
echo - %TASK_1500% at 15:00
echo - %TASK_2200% at 22:00
echo.
echo Runner   : %RUNNER%
echo Log file : %ROOT%output\us_etf_tracker_last_run.log
echo Dashboard: %ROOT%output\us_etf_weight_dashboard.html
echo.
pause
endlocal
