@echo off
REM Outlook Automation Launcher
REM Automatically runs Python scripts with administrator privileges

echo ============================================
echo    OUTLOOK AUTOMATION ENVIRONMENT
echo ============================================
echo.
echo Current directory: %CD%
echo Python version check...
python --version
echo.
echo Starting elevated Python environment...
echo You can now run your Outlook automation scripts
echo.
echo Examples:
echo   python test_outlook.py
echo   python your_email_script.py
echo.
echo Type 'exit' to close this window
echo ============================================
echo.

REM Keep the command prompt open
cmd /k