# investigate_api_behavior.py
import os
from dotenv import load_dotenv
import json

load_dotenv()

from etl.tracktik_client import TrackTikClient

client = TrackTikClient()
client.authenticate()

headers = {
    'Authorization': f'Bearer {client.access_token}',
    'Accept': 'application/json'
}

print("Investigating API behavior...")
print("=" * 50)

# Test 1: Check if date filter is actually working
print("\n1. Testing date filter effectiveness:")
params = {
    'startDateTime:gte': '2024-06-01',
    'startDateTime:lte': '2024-06-01',
    'limit': 5  # Just 5 to see the dates
}

response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params
)

if response.status_code == 200:
    data = response.json()
    shifts = data.get('data', [])
    print(f"Requested: 2024-06-01")
    print(f"Got {len(shifts)} shifts:")
    for shift in shifts:
        print(f"  - Start: {shift.get('startDateTime')}, ID: {shift.get('id')}")

# Test 2: Check meta.count vs actual records
print("\n2. Checking meta information:")
params = {
    'startDateTime:gte': '2024-06-01',
    'startDateTime:lte': '2024-06-01',
    'limit': 100,
    'offset': 0
}

response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params
)

if response.status_code == 200:
    data = response.json()
    meta = data.get('meta', {})
    actual_count = len(data.get('data', []))
    
    print(f"Meta information:")
    print(f"  meta.count: {meta.get('count')}")
    print(f"  meta.itemCount: {meta.get('itemCount')}")
    print(f"  meta.limit: {meta.get('limit')}")
    print(f"  meta.offset: {meta.get('offset')}")
    print(f"  Actual records returned: {actual_count}")

# Test 3: Try to get total count using different approach
print("\n3. Testing if we can get total count:")

# Some APIs support a count endpoint or total in first request
# Let's see if itemCount gives us the total
params_page1 = {
    'startDateTime:gte': '2024-06-01',
    'startDateTime:lte': '2024-06-01',
    'limit': 1,  # Minimal request
    'offset': 0
}

response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params_page1
)

if response.status_code == 200:
    data = response.json()
    meta = data.get('meta', {})
    print(f"With limit=1:")
    print(f"  meta.itemCount: {meta.get('itemCount')}")
    print(f"  This might be the total count for the query")

# Test 4: Check for duplicate IDs across pages
print("\n4. Checking for duplicate records across pages:")
seen_ids = set()
duplicates = []

for offset in [0, 100, 200]:
    params = {
        'startDateTime:gte': '2024-06-01',
        'startDateTime:lte': '2024-06-01',
        'limit': 100,
        'offset': offset
    }
    
    response = client.session.get(
        f'{client.base_url}/rest/v1/shifts',
        headers=headers,
        params=params
    )
    
    if response.status_code == 200:
        data = response.json()
        shifts = data.get('data', [])
        
        print(f"\n  Offset {offset}: Got {len(shifts)} records")
        
        for shift in shifts:
            shift_id = shift.get('id')
            if shift_id in seen_ids:
                duplicates.append(shift_id)
            else:
                seen_ids.add(shift_id)
        
        # Show first and last ID in this batch
        if shifts:
            print(f"    First ID: {shifts[0].get('id')}")
            print(f"    Last ID: {shifts[-1].get('id')}")

if duplicates:
    print(f"\n  Found {len(duplicates)} duplicate IDs!")
else:
    print(f"\n  No duplicates found across pages (unique IDs: {len(seen_ids)})")