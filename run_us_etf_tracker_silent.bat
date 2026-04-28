@echo off
setlocal
cd /d "%~dp0"
if not exist ".\output" mkdir ".\output"
python .\us_etf_tracker.py update --config .\us_etf_config.example.json > ".\output\us_etf_tracker_last_run.log" 2>&1
endlocal
