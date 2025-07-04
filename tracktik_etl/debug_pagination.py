# debug_pagination.py
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

from etl.tracktik_client import TrackTikClient

# Create client but add some debugging
client = TrackTikClient()
client.authenticate()

print("Testing pagination with debug output...")
print("=" * 50)

# Manually test the pagination to see what's happening
headers = {
    'Authorization': f'Bearer {client.access_token}',
    'Accept': 'application/json'
}

# Use a very small date range to limit results
params = {
    'startDateTime:gte': '2024-06-01',
    'startDateTime:lte': '2024-06-02',  # Just 2 days
    'limit': 10,  # Small limit
    'offset': 0
}

print(f"\nTesting with params: {params}")

response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params
)

print(f"Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    
    # Show the structure of the response
    print("\nResponse structure:")
    print(f"  Keys: {list(data.keys())}")
    
    if 'data' in data:
        print(f"  Number of records: {len(data['data'])}")
    
    if 'meta' in data:
        print(f"  Meta keys: {list(data.get('meta', {}).keys())}")
        if 'pagination' in data.get('meta', {}):
            pagination = data['meta']['pagination']
            print(f"  Pagination info:")
            for key, value in pagination.items():
                print(f"    {key}: {value}")
    
    # Now let's test what happens with offset 10
    print("\n\nTesting second page (offset=10):")
    params['offset'] = 10
    
    response2 = client.session.get(
        f'{client.base_url}/rest/v1/shifts',
        headers=headers,
        params=params
    )
    
    if response2.status_code == 200:
        data2 = response2.json()
        print(f"  Number of records: {len(data2.get('data', []))}")
        if 'meta' in data2 and 'pagination' in data2['meta']:
            print(f"  Current page: {data2['meta']['pagination'].get('current_page')}")
            print(f"  Total: {data2['meta']['pagination'].get('total')}")