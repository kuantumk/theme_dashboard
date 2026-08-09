@echo off
REM ============================================================
REM  Tape Pressure Dashboard - local launcher
REM
REM  Starts the poll loop and a loopback web server, then opens
REM  the dashboard in your browser. Ctrl-C in this window stops it.
REM
REM  Requires TRADINGVIEW_SESSIONID (and ideally
REM  TRADINGVIEW_SESSION_SIGN) in .env - see .env.example.
REM  Without them the app still runs, on the delayed feed.
REM
REM  State is written to scripts\local_runs\ (gitignored), never
REM  to docs\data\ or data\, so a local session never dirties the
REM  working tree.
REM ============================================================

REM %~dp0 is this script's directory; the repo is its parent.
cd /d "%~dp0.."

set PYTHONPATH=.

uv run python -m src.bidask.server --out-dir scripts\local_runs --port 8787
if errorlevel 1 (
    echo.
    echo Dashboard exited with an error. Common causes:
    echo   - uv not installed or not on PATH
    echo   - port 8787 already in use  ^(pass --port to change^)
    echo   - .env missing TRADINGVIEW_SESSIONID
    echo.
    pause
    exit /b 1
)
