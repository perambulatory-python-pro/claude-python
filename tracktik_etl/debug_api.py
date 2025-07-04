# debug_api.py
import os
import requests
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Get credentials
client_id = os.getenv('TRACKTIK_CLIENT_ID')
client_secret = os.getenv('TRACKTIK_CLIENT_SECRET')
username = os.getenv('TRACKTIK_USERNAME')
password = os.getenv('TRACKTIK_PASSWORD')
base_url = os.getenv('TRACKTIK_BASE_URL')

# Authenticate
print("Authenticating...")
auth_data = {
    'client_id': client_id,
    'client_secret': client_secret,
    'username': username,
    'password': password,
    'grant_type': 'password'
}

auth_response = requests.post(f'{base_url}/rest/oauth2/access_token', data=auth_data)
tokens = auth_response.json()
access_token = tokens['access_token']
print("✓ Got access token")

headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json'
}

# Test different parameter combinations
test_cases = [
    {'limit': 1},                    # We know this works
    {'limit': 10},                   # Test higher limit
    {'limit': 100},                  # Test the original limit
    {'limit': 100, 'page': 1},       # Test with page (this probably fails)
    {'limit': 100, 'offset': 0},     # Test with offset instead
]

print("\nTesting different parameter combinations:")
print("-" * 50)

for params in test_cases:
    response = requests.get(f'{base_url}/rest/v1/employees', headers=headers, params=params)
    print(f"Parameters: {params}")
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        # Check if there's pagination info
        if 'meta' in data and 'pagination' in data['meta']:
            pagination = data['meta']['pagination']
            print(f"Results: {pagination.get('count', 'unknown')} of {pagination.get('total', 'unknown')}")
        else:
            print(f"Results: {len(data.get('data', []))}")
    else:
        print(f"Error: {response.text[:100]}...")  # First 100 chars of error
    
    print("-" * 50)