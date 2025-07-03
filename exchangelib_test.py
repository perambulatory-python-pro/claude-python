from exchangelib import Credentials, Account, DELEGATE
import getpass

def test_basic_auth():
    """
    Test basic authentication - sometimes works even with 2FA
    """
    print("TESTING BASIC AUTHENTICATION")
    print("=" * 40)
    
    email = input("Enter your email address: ")
    print("\nTry using your REGULAR password first (not app password)")
    password = getpass.getpass("Enter your password: ")
    
    try:
        # Create credentials
        credentials = Credentials(email, password)
        
        # Create account connection with specific server
        print("Attempting connection to Exchange Online...")
        account = Account(
            primary_smtp_address=email,
            credentials=credentials,
            autodiscover=True,
            access_type=DELEGATE
        )
        
        print("✅ Basic auth successful!")
        print(f"Account: {account.primary_smtp_address}")
        print(f"Inbox total: {account.inbox.total_count}")
        
        return account
        
    except Exception as e:
        print(f"❌ Basic auth failed: {str(e)}")
        
        if "401" in str(e) or "authentication" in str(e).lower():
            print("\n🔍 Authentication failed. Let's try alternative approaches:")
            print("1. Your organization might require App Passwords")
            print("2. EWS might be disabled")
            print("3. We might need to use Microsoft Graph API instead")
        
        return None

# Test basic auth
if __name__ == "__main__":
    test_basic_auth()