@echo off
REM ============================================================
REM  Tape Pressure Dashboard - local launcher
REM
REM  Starts the poll loop and a loopback web server, then opens
REM  the dashboard in your browser. Ctrl-C in this window stops it.
REM
REM  Requires TRADINGVIEW_SESSIONID and TRADINGVIEW_SESSION_SIGN in
REM  .env - see .env.example. These are NOT optional for the equity
REM  tab: US equity bid/ask comes from TradingView's quote websocket
REM  (the screener API has no such field for US stocks), and that
REM  socket needs the session cookie to mint its auth token. Without
REM  them the crypto tab still works and equity classifies nothing.
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
