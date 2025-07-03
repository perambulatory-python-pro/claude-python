"""
Outlook Connection Diagnostic Tool
Helps identify why Outlook COM connection is failing
"""

import win32com.client
import sys
import os
import subprocess
import time
from datetime import datetime

def check_outlook_process():
    """Check if Outlook process is running"""
    print("Checking if Outlook is currently running...")
    
    try:
        # Use tasklist to check for Outlook processes
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq OUTLOOK.EXE'], 
                              capture_output=True, text=True)
        
        if 'OUTLOOK.EXE' in result.stdout:
            print("   ✓ Outlook.exe process is running")
            return True
        else:
            print("   ✗ Outlook.exe process is NOT running")
            return False
            
    except Exception as e:
        print(f"   ? Could not check process status: {e}")
        return None

def check_pywin32_installation():
    """Verify pywin32 is properly installed"""
    print("Checking pywin32 installation...")
    
    try:
        import pythoncom
        import win32api
        import win32com.client
        print("   ✓ pywin32 modules imported successfully")
        
        # Check pywin32 version
        try:
            import win32api
            version = win32api.GetFileVersionInfo(win32api.__file__, "\\")
            print(f"   ✓ pywin32 version detected")
        except:
            print("   ? Could not determine pywin32 version")
            
        return True
        
    except ImportError as e:
        print(f"   ❌ pywin32 import failed: {e}")
        return False

def try_different_connection_methods():
    """Try different ways to connect to Outlook"""
    print("Trying different connection methods...")
    
    methods = [
        ("Standard Dispatch", lambda: win32com.client.Dispatch("Outlook.Application")),
        ("DispatchEx", lambda: win32com.client.DispatchEx("Outlook.Application")),
        ("GetActiveObject", lambda: win32com.client.GetActiveObject("Outlook.Application")),
    ]
    
    for method_name, method_func in methods:
        print(f"\n   Trying {method_name}...")
        try:
            outlook = method_func()
            print(f"   ✓ {method_name} SUCCESS!")
            
            # Try to get basic info
            try:
                namespace = outlook.GetNamespace("MAPI")
                print(f"   ✓ Successfully accessed namespace")
                return True
            except Exception as e:
                print(f"   ⚠️ Connected but couldn't access namespace: {e}")
                
        except Exception as e:
            print(f"   ❌ {method_name} failed: {e}")
    
    return False

def check_com_security():
    """Check for COM security issues"""
    print("Checking COM security and permissions...")
    
    # Check if running as administrator
    try:
        import ctypes
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
        if is_admin:
            print("   ✓ Running with administrator privileges")
        else:
            print("   ⚠️ NOT running as administrator (this might help)")
    except:
        print("   ? Could not check administrator status")
    
    # Check for common antivirus/security software interference
    security_processes = ['mcshield.exe', 'avp.exe', 'avgnt.exe', 'MSASCui.exe']
    print("   Checking for security software that might interfere...")
    
    try:
        result = subprocess.run(['tasklist'], capture_output=True, text=True)
        found_security = []
        for process in security_processes:
            if process.lower() in result.stdout.lower():
                found_security.append(process)
        
        if found_security:
            print(f"   ⚠️ Found security software: {', '.join(found_security)}")
            print("   This might be blocking COM access")
        else:
            print("   ✓ No common interfering security software detected")
            
    except:
        print("   ? Could not check for security software")

def suggest_solutions():
    """Provide targeted solutions based on findings"""
    print("\n" + "="*50)
    print("SUGGESTED SOLUTIONS:")
    print("="*50)
    
    print("\n1. IMMEDIATE ACTIONS:")
    print("   a) Close Outlook completely (check Task Manager)")
    print("   b) Run this script as Administrator:")
    print("      - Right-click Command Prompt → 'Run as administrator'")
    print("      - Navigate to your script folder")
    print("      - Run: python outlook_diagnostic.py")
    
    print("\n2. IF STILL FAILING:")
    print("   a) Restart your computer")
    print("   b) Open Outlook manually first, then run the script")
    print("   c) Try with Outlook closed, then run the script")
    
    print("\n3. ALTERNATIVE INSTALLATION:")
    print("   If pywin32 issues persist, try:")
    print("   pip uninstall pywin32")
    print("   pip install --upgrade pywin32")
    print("   python Scripts/pywin32_postinstall.py -install")
    
    print("\n4. ENTERPRISE ENVIRONMENT:")
    print("   Your IT department might have:")
    print("   - Group policies blocking COM access")
    print("   - Antivirus blocking automation")
    print("   - Outlook security settings preventing automation")

def main():
    """Main diagnostic routine"""
    print("OUTLOOK AUTOMATION DIAGNOSTIC TOOL")
    print("="*50)
    print(f"Python version: {sys.version}")
    print(f"Running from: {os.getcwd()}")
    print(f"Diagnostic time: {datetime.now()}")
    print()
    
    # Run all checks
    outlook_running = check_outlook_process()
    print()
    
    pywin32_ok = check_pywin32_installation()
    print()
    
    if pywin32_ok:
        connection_success = try_different_connection_methods()
        print()
        
        check_com_security()
        print()
    else:
        print("❌ Cannot proceed - pywin32 installation issues")
        connection_success = False
    
    # Provide recommendations
    suggest_solutions()
    
    # Final recommendation
    print("\n" + "="*50)
    if connection_success:
        print("✅ SUCCESS! At least one connection method worked.")
        print("You can proceed with Outlook automation.")
    else:
        print("❌ All connection methods failed.")
        print("Follow the suggested solutions above.")
    
    print("\nNext steps:")
    print("1. Try the immediate actions")
    print("2. If still failing, we'll explore alternative approaches")
    print("3. Consider using Microsoft Graph API as backup option")

if __name__ == "__main__":
    main()
