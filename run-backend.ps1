$ErrorActionPreference = 'Stop'

Write-Host '== Moto Tracker backend ==' -ForegroundColor Cyan

if (-not (Test-Path '.\.venv\Scripts\python.exe')) {
    Write-Host 'Criando ambiente virtual...' -ForegroundColor Yellow
    python -m venv .venv
}

Write-Host 'Ativando ambiente virtual...' -ForegroundColor Yellow
. .\.venv\Scripts\Activate.ps1

Write-Host 'Atualizando pip e instalando dependencias...' -ForegroundColor Yellow
python -m pip install --upgrade pip
pip install -r requirements.txt

if (-not (Test-Path '.env') -and (Test-Path '.env.example')) {
    Copy-Item '.env.example' '.env'
    Write-Host 'Arquivo .env criado a partir de .env.example.' -ForegroundColor Green
    Write-Host 'Ajuste FIREBASE_KEY no .env antes de continuar.' -ForegroundColor Yellow
}

$env:FLASK_APP = 'app.py'
if (-not $env:FLASK_ENV) { $env:FLASK_ENV = 'development' }
if (-not $env:PORT) { $env:PORT = '5000' }

Write-Host "Iniciando Flask em http://127.0.0.1:$env:PORT" -ForegroundColor Green
flask run --debug --port $env:PORT
