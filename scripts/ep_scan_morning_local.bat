@echo off
REM EP Scan Morning — Local launcher for Windows Task Scheduler
REM Runs the BMO earnings scan locally for diagnostic purposes.
REM Does NOT commit or push — the dashboard is updated by ep-scan-morning.yml
REM in GitHub Actions (5:45 AM PT). The local JSON write is left in the working
REM tree; refresh it with `git checkout -- docs\data\ep_scan_morning.json` if needed.

cd /d C:\Users\kuantumk\repos\theme_dashboard

set PYTHONPATH=.
C:\Users\kuantumk\AppData\Local\Programs\Python\Python312\python.exe src\reporting\ep_scan_morning.py
if errorlevel 1 (
    echo EP scan failed at %date% %time% >> scripts\ep_scan_morning_local.log
    exit /b 1
)

echo Scan completed at %date% %time% >> scripts\ep_scan_morning_local.log
