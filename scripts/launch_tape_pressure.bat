@echo off
REM ============================================================
REM  Tape Pressure Dashboard - local launcher
REM
REM  Starts the poll loop and a loopback web server, then opens
REM  the dashboard in your browser. Ctrl-C in this window stops it.
REM
REM  Requires TRADINGVIEW_SESSIONID and TRADINGVIEW_SESSION_SIGN in
REM  .env - see .env.example. These are NOT optional for the equity
REM  tab: relative volume is its only admission path, and the
REM  baselines behind it come from TradingView's chart websocket,
REM  which needs the session cookie to mint its auth token. Without
REM  them the warm-up fails and BOTH boards stay EMPTY - neither
REM  degrades to a partial board, because relative volume is the
REM  only admission path on either tab.
REM
REM  Launch before the open if you can. The warm-up downloads one
REM  bar series per ticker and takes a couple of minutes over the
REM  full universe; the board gates nothing until it lands.
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
