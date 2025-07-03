"""
Test Outlook Connection
Simple script to verify pywin32 can connect to Outlook
"""

import win32com.client
import sys
from datetime import datetime

def test_outlook_connection():
    """Test if we can connect to Outlook and get basic info"""
    
    print("Testing Outlook Connection...")
    print("=" * 40)
    
    try:
        # Step 1: Connect to Outlook application
        print("1. Connecting to Outlook application...")
        outlook = win32com.client.Dispatch("Outlook.Application")
        print("   ✓ Successfully connected to Outlook")
        
        # Step 2: Get the namespace (root of Outlook data)
        print("\n2. Getting Outlook namespace...")
        namespace = outlook.GetNamespace("MAPI")
        print("   ✓ Successfully accessed Outlook namespace")
        
        # Step 3: Get default folders
        print("\n3. Accessing default folders...")
        inbox = namespace.GetDefaultFolder(6)  # 6 = Inbox
        sent_items = namespace.GetDefaultFolder(5)  # 5 = Sent Items
        
        print(f"   ✓ Inbox folder: {inbox.Name}")
        print(f"   ✓ Sent Items folder: {sent_items.Name}")
        
        # Step 4: Get basic statistics
        print("\n4. Getting folder statistics...")
        inbox_count = inbox.Items.Count
        sent_count = sent_items.Items.Count
        
        print(f"   ✓ Inbox contains: {inbox_count} items")
        print(f"   ✓ Sent Items contains: {sent_count} items")
        
        # Step 5: Test creating (but not sending) an email
        print("\n5. Testing email creation...")
        test_mail = outlook.CreateItem(0)  # 0 = Mail item
        test_mail.Subject = "Test Email - Created by Python"
        test_mail.Body = f"This email was created by Python at {datetime.now()}"
        
        print("   ✓ Successfully created test email object")
        print("   (Email created but not sent - this is just a test)")
        
        print("\n" + "=" * 40)
        print("✅ ALL TESTS PASSED!")
        print("Outlook automation is ready to use.")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Make sure Outlook is installed")
        print("2. Make sure Outlook is not blocked by security software")
        print("3. Try running as administrator")
        print("4. Ensure pywin32 is properly installed")
        
        return False

if __name__ == "__main__":
    print("Outlook Automation Test Script")
    print("Testing Python connection to Microsoft Outlook")
    print(f"Python version: {sys.version}")
    print(f"Test run at: {datetime.now()}")
    print()
    
    success = test_outlook_connection()
    
    if success:
        print("\n🎉 You're ready to start automating Outlook with Python!")
    else:
        print("\n⚠️ Please resolve the connection issues before proceeding.")