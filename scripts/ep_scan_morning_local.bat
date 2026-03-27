@echo off
REM EP Scan Morning — Local launcher for Windows Task Scheduler
REM Runs the BMO earnings scan and pushes results to GitHub.
REM GitHub Actions workflow remains as a backup (will no-op if JSON unchanged).

cd /d C:\Users\kuantumk\repos\theme_dashboard

set PYTHONPATH=.
C:\Users\kuantumk\AppData\Local\Programs\Python\Python312\python.exe src\reporting\ep_scan_morning.py
if errorlevel 1 (
    echo EP scan failed at %date% %time% >> scripts\ep_scan_morning_local.log
    exit /b 1
)

REM Commit and push results so dashboard stays updated
git add docs\data\ep_scan_morning.json
git diff --staged --quiet && (
    echo No changes to commit at %date% %time% >> scripts\ep_scan_morning_local.log
    exit /b 0
)
git commit -m "EP scan morning %date:~-4%-%date:~4,2%-%date:~7,2% (local)"
git pull --rebase origin main
git push origin main
echo Scan completed and pushed at %date% %time% >> scripts\ep_scan_morning_local.log
