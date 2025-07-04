# test_api_debug.py
"""
Debug TrackTik API authentication
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_auth():
    """Test authentication with detailed output"""
    
    # Get credentials
    base_url = os.getenv('TRACKTIK_BASE_URL')
    client_id = os.getenv('TRACKTIK_CLIENT_ID')
    client_secret = os.getenv('TRACKTIK_CLIENT_SECRET')
    username = os.getenv('TRACKTIK_USERNAME')
    password = os.getenv('TRACKTIK_PASSWORD')
    
    print("=== Credential Check ===")
    print(f"Base URL: {base_url}")
    print(f"Client ID: {client_id[:10]}..." if client_id else "Client ID: NOT SET")
    print(f"Client Secret: {'*' * 8} (length: {len(client_secret) if client_secret else 0})")
    print(f"Username: {username}")
    print(f"Password: {'*' * 8} (length: {len(password) if password else 0})")
    
    # Build auth request
    auth_url = f"{base_url}/rest/oauth2/access_token"
    auth_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    
    print(f"\n=== Authentication Request ===")
    print(f"URL: {auth_url}")
    print(f"Grant Type: {auth_data['grant_type']}")
    
    # Make request
    try:
        response = requests.post(auth_url, data=auth_data)
        
        print(f"\n=== Response ===")
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Success! Got access token")
            print(f"Token (first 20 chars): {data.get('access_token', '')[:20]}...")
            print(f"Expires in: {data.get('expires_in')} seconds")
        else:
            print(f"✗ Failed!")
            print(f"Response Text: {response.text}")
            
            # Try to parse error
            try:
                error_data = response.json()
                print(f"Error: {error_data.get('error', 'Unknown')}")
                print(f"Error Description: {error_data.get('error_description', 'None provided')}")
            except:
                pass
                
    except Exception as e:
        print(f"\n✗ Request failed: {str(e)}")

if __name__ == "__main__":
    test_auth()