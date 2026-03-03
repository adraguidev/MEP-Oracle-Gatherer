@echo off
REM ============================================================
REM MEP Oracle Gatherer - Launcher para Windows
REM Integratel Peru - Stefanini Group
REM
REM Ejecuta el script Python directamente si no se tiene el .exe
REM Requiere: Python 3.6+ y sqlplus en PATH
REM ============================================================

title MEP Oracle Gatherer -- Stefanini Group

REM Check if .exe exists in same directory
if exist "%~dp0MEP_Oracle_Gatherer.exe" (
    "%~dp0MEP_Oracle_Gatherer.exe" %*
    goto :eof
)

REM Fallback: run Python script directly
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo  [ERROR] No se encontro Python ni MEP_Oracle_Gatherer.exe
    echo.
    echo  Opciones:
    echo    1^) Usar el ejecutable: MEP_Oracle_Gatherer.exe
    echo    2^) Instalar Python 3.6+: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

python "%~dp0mep_oracle_launcher.py" %*
pause
