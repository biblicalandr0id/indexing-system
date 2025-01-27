@echo off
echo Starting Indexing System...

:: Set Python environment variables
set PYTHONUNBUFFERED=1
set PYTHONDONTWRITEBYTECODE=1
set PROMETHEUS_MULTIPROC_DIR=%TEMP%

:: Check if Python is installed
where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Python is not installed or not in PATH
    exit /b 1
)

:: Check if Poetry is installed
where poetry >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Installing Poetry...
    curl -sSL https://install.python-poetry.org | python -
)

:: Install dependencies if not already installed
echo Installing dependencies...
poetry install

:: Start Redis (required for caching)
echo Starting Redis...
start "" redis-server

:: Start ZooKeeper (required for distributed locking)
echo Starting ZooKeeper...
start "" zkServer

:: Wait for services to start
timeout /t 5

:: Start Prometheus for metrics
echo Starting Prometheus...
start "" prometheus --config.file=prometheus.yml

:: Start the indexing system
echo Starting main application...
poetry run python -m indexing_system.modern-indexing-ui

echo.
echo To stop the system, press Ctrl+C
echo.

:: Wait for user input before closing
pause
