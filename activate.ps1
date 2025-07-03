# activate.ps1
Write-Host "Activating Python virtual environment..." -ForegroundColor Green
& ".\venv\Scripts\Activate.ps1"
Write-Host "Virtual environment activated!" -ForegroundColor Green
Write-Host ""
python --version
Write-Host ""
Write-Host "Ready to work! 🐍" -ForegroundColor Cyan