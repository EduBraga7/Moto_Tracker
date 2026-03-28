@echo off
setlocal
cd /d "%~dp0"

echo == Moto Tracker backend ==
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-backend.ps1"

if errorlevel 1 (
  echo.
  echo Falha ao iniciar o backend. Confira as mensagens acima.
  pause
)
