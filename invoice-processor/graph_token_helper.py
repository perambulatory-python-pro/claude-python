"""
Helper script to get Microsoft Graph API access token
Save this as get_token.py and run it to get your token
"""

import msal
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration - using your existing .env variable names
CLIENT_ID = os.getenv('GRAPH_CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
TENANT_ID = os.getenv('GRAPH_TENANT_ID')

# Required scope for client credential flow - must use /.default
SCOPES = ['https://graph.microsoft.com/.default']

def get_access_token():
    """
    Get access token using client credentials flow
    """
    if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID]):
        print("Missing Azure credentials in .env file")
        print("Required variables: GRAPH_CLIENT_ID, CLIENT_SECRET, GRAPH_TENANT_ID")
        return None
    
    # Create MSAL app
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    
    # Get token
    result = app.acquire_token_for_client(scopes=SCOPES)
    
    if "access_token" in result:
        print("✅ Successfully obtained access token!")
        print(f"Token: {result['access_token'][:50]}...")
        print(f"Expires in: {result['expires_in']} seconds")
        return result['access_token']
    else:
        print("❌ Failed to obtain token")
        print(f"Error: {result.get('error')}")
        print(f"Description: {result.get('error_description')}")
        return None

if __name__ == "__main__":
    token = get_access_token()
    if token:
        print("\nAdd this token to your .env file:")
        print(f"GRAPH_ACCESS_TOKEN={token}")
