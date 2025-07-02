# Save this as run_streamlit.ps1
param(
    [string]$app = "invoice_app_auto_detect.py"
)

Write-Host "Starting Streamlit app: $app" -ForegroundColor Green

# Set location to project root
Set-Location $PSScriptRoot

# Activate virtual environment
& ".\venv\Scripts\activate.ps1"

# Run streamlit from project root
Write-Host "Running from: $(Get-Location)" -ForegroundColor Cyan
streamlit run "invoice_processing/apps/$app"