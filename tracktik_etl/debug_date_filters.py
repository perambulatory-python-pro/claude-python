# debug_date_filters.py
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

from etl.tracktik_client import TrackTikClient

client = TrackTikClient()
client.authenticate()

headers = {
    'Authorization': f'Bearer {client.access_token}',
    'Accept': 'application/json'
}

print("Debugging date filter formats...")
print("=" * 50)

# Test different date/time formats
test_formats = [
    # Basic formats
    ('2024-06-01', '2024-06-01', 'Simple date'),
    ('2024-06-01T00:00:00', '2024-06-01T23:59:59', 'DateTime without timezone'),
    ('2024-06-01T00:00:00Z', '2024-06-01T23:59:59Z', 'DateTime with Z timezone'),
    ('2024-06-01T00:00:00+00:00', '2024-06-01T23:59:59+00:00', 'DateTime with +00:00'),
    ('2024-06-01T00:00:00-04:00', '2024-06-01T23:59:59-04:00', 'DateTime with EST timezone'),
]

for start_fmt, end_fmt, description in test_formats:
    print(f"\n{description}:")
    print(f"  Start: {start_fmt}")
    print(f"  End: {end_fmt}")
    
    params = {
        'startDateTime:gte': start_fmt,
        'startDateTime:lte': end_fmt,
        'limit': 3
    }
    
    response = client.session.get(
        f'{client.base_url}/rest/v1/shifts',
        headers=headers,
        params=params
    )
    
    if response.status_code == 200:
        data = response.json()
        shifts = data.get('data', [])
        meta = data.get('meta', {})
        
        print(f"  Status: 200 OK")
        print(f"  Total count: {meta.get('count')}")
        print(f"  Returned: {len(shifts)} shifts")
        
        if shifts:
            first_date = shifts[0].get('startDateTime')
            print(f"  First shift date: {first_date}")
            
            # Check if the date matches our filter
            if '2024' in first_date:
                print("  ✓ DATE FILTER WORKING!")
            else:
                print("  ✗ Date filter NOT working")
    else:
        print(f"  Status: {response.status_code}")
        print(f"  Error: {response.text[:100]}")

# Test if the filter syntax is wrong
print("\n\nTesting alternative filter syntax:")

# Maybe it uses different parameter names?
alternative_params = [
    {'startDateTime': '2024-06-01', 'limit': 3},  # Without :gte
    {'start': '2024-06-01', 'limit': 3},  # Different field name
    {'startsOn:gte': '2024-06-01', 'limit': 3},  # Using startsOn field
    {'startTimestamp:gte': '1717200000', 'limit': 3},  # Unix timestamp for 2024-06-01
]

for params in alternative_params:
    print(f"\nTrying: {params}")
    
    response = client.session.get(
        f'{client.base_url}/rest/v1/shifts',
        headers=headers,
        params=params
    )
    
    if response.status_code == 200:
        data = response.json()
        shifts = data.get('data', [])
        if shifts:
            print(f"  First shift: {shifts[0].get('startDateTime')}")
    else:
        print(f"  Error: {response.status_code}")

# Let's also check what scopes might help
print("\n\nTesting with scopes instead of date filters:")
scope_params = {
    'scope': 'IS_CURRENT',
    'limit': 5
}

response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=scope_params
)

if response.status_code == 200:
    data = response.json()
    shifts = data.get('data', [])
    print(f"Current shifts (using scope):")
    for shift in shifts:
        print(f"  - {shift.get('startDateTime')} to {shift.get('endDateTime')}")