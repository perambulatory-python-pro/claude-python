# fixed_get_shifts_test.py
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

from etl.tracktik_client import TrackTikClient
client = TrackTikClient()
client.authenticate()

headers = {
    'Authorization': f'Bearer {client.access_token}',
    'Accept': 'application/json'
}

print("Testing shifts with required filters...")
print("=" * 50)

# Test 1: Using a scope filter (this satisfies the "required filter" rule)
print("\n1. Testing with scope filter:")
params = {
    'scope': 'IS_CURRENT',  # Current shifts
    'limit': 5
}
response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params
)
print(f"Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"Found {len(data.get('data', []))} current shifts")

# Test 2: Using date filter (within 1 month range)
print("\n2. Testing with date filter (last 7 days):")
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
params = {
    'startDateTime:gte': start_date.strftime('%Y-%m-%d'),
    'startDateTime:lte': end_date.strftime('%Y-%m-%d'),
    'limit': 5
}
response = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params
)
print(f"Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    shifts = data.get('data', [])
    print(f"Found {len(shifts)} shifts in the last 7 days")
    for shift in shifts[:3]:
        print(f"  - Shift {shift.get('id')}: {shift.get('startDateTime')} to {shift.get('endDateTime')}")

# Test 3: Let's fix the get_shifts method
print("\n3. Testing corrected get_shifts method:")

# Here's the corrected method to add to your tracktik_client.py:
def get_shifts_corrected(client, start_date: str, end_date: str, **kwargs):
    """Get shifts for a date range (corrected version)"""
    # Parse dates to ensure they're within 1 month
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if (end - start).days > 31:
        raise ValueError("Date range cannot exceed 31 days per API requirements")
    
    params = {
        'startDateTime:gte': start_date,
        'startDateTime:lte': end_date,
        'limit': 100,  # Add explicit limit
        **kwargs  # Allow additional filters
    }
    
    # Only add include if specified
    if 'include' in kwargs:
        params['include'] = kwargs['include']
    
    return client.get_paginated_data('/rest/v1/shifts', params)

# Test the corrected method
try:
    shifts = get_shifts_corrected(client, '2024-06-01', '2024-06-15')
    print(f"Success! Found {len(shifts)} shifts")
except Exception as e:
    print(f"Error: {e}")