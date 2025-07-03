import msal
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_graph_connection():
    """
    Test Microsoft Graph API connection with user authentication
    """
    print("TESTING MICROSOFT GRAPH API CONNECTION")
    print("=" * 50)
    
    # Get credentials from .env
    client_id = os.getenv('GRAPH_CLIENT_ID')
    tenant_id = os.getenv('GRAPH_TENANT_ID')
    # Note: No client_secret needed for PublicClientApplication
    
    if not client_id or not tenant_id:
        print("❌ Missing GRAPH_CLIENT_ID or GRAPH_TENANT_ID in .env file")
        return False
    
    print(f"Client ID: {client_id}")
    print(f"Tenant ID: {tenant_id}")
    
    try:
        # Use PublicClientApplication for testing
        app = msal.PublicClientApplication(
            client_id=client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        
        # Define the scopes we need
        scopes = ["https://graph.microsoft.com/Mail.Read"]
        
        print("\nAttempting authentication...")
        print("A browser window should open for you to sign in...")
        
        # Use device code flow (works great for testing)
        flow = app.initiate_device_flow(scopes=scopes)
        
        if "user_code" not in flow:
            raise ValueError("Failed to create device flow")
        
        print(flow["message"])
        
        # Wait for user to authenticate
        result = app.acquire_token_by_device_flow(flow)
        
        if "access_token" in result:
            print("✅ Authentication successful!")
            
            # Test API call to get mailbox info
            headers = {
                'Authorization': f"Bearer {result['access_token']}",
                'Content-Type': 'application/json'
            }
            
            print("Testing email access...")
            response = requests.get('https://graph.microsoft.com/v1.0/me/messages?$top=5', headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                email_count = len(data.get('value', []))
                print(f"✅ Email access successful!")
                print(f"Retrieved {email_count} sample emails")
                
                # Show basic info about first email
                if email_count > 0:
                    first_email = data['value'][0]
                    print(f"First email subject: {first_email.get('subject', 'No subject')}")
                    print(f"From: {first_email.get('sender', {}).get('emailAddress', {}).get('address', 'Unknown')}")
                
                return True
            else:
                print(f"❌ Email access failed: {response.status_code}")
                print(f"Error: {response.text}")
                return False
        else:
            print("❌ Authentication failed:")
            print(f"Error: {result.get('error_description', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {str(e)}")
        return False

# Run the test
if __name__ == "__main__":
    test_graph_connection()