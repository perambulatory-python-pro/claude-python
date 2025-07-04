# test_between_filter.py
import os
from dotenv import load_dotenv

load_dotenv()

from etl.tracktik_client import TrackTikClient

client = TrackTikClient()
client.authenticate()

headers = {
    'Authorization': f'Bearer {client.access_token}',
    'Accept': 'application/json'
}

print("Testing :between filter for dates...")
print("=" * 50)

# Test the between filter as shown in docs
params = {
    'startDateTime:between': '2024-06-01|2024-06-07',  # One week
    'limit': 10
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
    meta = data.get('meta', {})
    
    print(f"Total matching shifts: {meta.get('count')}")
    print(f"Returned: {len(shifts)} shifts")
    
    if shifts:
        print("\nFirst few shifts:")
        for shift in shifts[:5]:
            print(f"  {shift.get('startDateTime')} to {shift.get('endDateTime')}")
        
        # Check if dates are in our range
        for shift in shifts:
            start = shift.get('startDateTime', '')
            if '2024-06' in start:
                print("\n✓ Date filter is working correctly!")
                break
        else:
            print("\n✗ Still getting wrong dates")

# Also test if we need to be more specific about the end date
print("\n\nTesting with full datetime:")
params2 = {
    'startDateTime:between': '2024-06-01T00:00:00|2024-06-07T23:59:59',
    'limit': 10
}

response2 = client.session.get(
    f'{client.base_url}/rest/v1/shifts',
    headers=headers,
    params=params2
)

if response2.status_code == 200:
    data2 = response2.json()
    print(f"\nWith full datetime: {data2.get('meta', {}).get('count')} total shifts")