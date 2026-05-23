@echo off
REM EP Scan Morning — Local launcher for Windows Task Scheduler
REM Runs the BMO earnings scan locally for diagnostic purposes.
REM Does NOT commit or push — the dashboard is updated by ep-scan-morning.yml
REM in GitHub Actions (5:45 AM PT). Output is routed to scripts\local_runs\
REM (gitignored) via --out-dir so the tracked docs\data\*.json files are never
REM touched by local runs.

cd /d C:\Users\kuantumk\repos\theme_dashboard

set PYTHONPATH=.
C:\Users\kuantumk\AppData\Local\Programs\Python\Python312\python.exe src\reporting\ep_scan_morning.py --out-dir scripts\local_runs --no-discord
if errorlevel 1 (
    echo EP scan failed at %date% %time% >> scripts\local_runs\ep_scan_morning_local.log
    exit /b 1
)

echo Scan completed at %date% %time% >> scripts\local_runs\ep_scan_morning_local.log
