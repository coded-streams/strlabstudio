@echo off
REM ─────────────────────────────────────────────────────────────────────────────
REM FlinkSQL Studio — Windows CMD setup script
REM Usage: setup.bat [gateway_host] [gateway_port] [studio_port]
REM ─────────────────────────────────────────────────────────────────────────────

SET IMAGE=codedstreams/flinksql-studio:latest
SET CONTAINER=flinksql-studio
SET GATEWAY_HOST=%1
SET GATEWAY_PORT=%2
SET STUDIO_PORT=%3

IF "%GATEWAY_HOST%"=="" SET GATEWAY_HOST=localhost
IF "%GATEWAY_PORT%"=="" SET GATEWAY_PORT=8083
IF "%STUDIO_PORT%"==""  SET STUDIO_PORT=3030

echo.
echo  FlinkSQL Studio - Windows Setup
echo  ================================
echo.

docker info > nul 2>&1
IF ERRORLEVEL 1 (
    echo  [ERROR] Docker is not running. Start Docker Desktop first.
    pause & exit /b 1
)

echo  [OK] Docker is running
echo  [*] Pulling %IMAGE% ...

docker rm -f %CONTAINER% > nul 2>&1
docker pull %IMAGE%
IF ERRORLEVEL 1 ( echo  [ERROR] Pull failed & pause & exit /b 1 )

echo  [*] Starting on port %STUDIO_PORT% ...
docker run -d ^
    --name %CONTAINER% ^
    --restart unless-stopped ^
    -p "%STUDIO_PORT%:80" ^
    -e "FLINK_GATEWAY_HOST=%GATEWAY_HOST%" ^
    -e "FLINK_GATEWAY_PORT=%GATEWAY_PORT%" ^
    %IMAGE%

IF ERRORLEVEL 1 ( echo  [ERROR] Failed to start & pause & exit /b 1 )

echo.
echo  FlinkSQL Studio is running!
echo  Open:    http://localhost:%STUDIO_PORT%
echo  Gateway: %GATEWAY_HOST%:%GATEWAY_PORT%
echo.
echo  Stop:  docker stop %CONTAINER%
echo  Logs:  docker logs -f %CONTAINER%
echo.

start http://localhost:%STUDIO_PORT%
pause
