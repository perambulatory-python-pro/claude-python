# test_updated_pagination.py
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# We'll need to temporarily update the client or create a test version
from etl.tracktik_client import TrackTikClient
from etl.config import config

class TestTrackTikClient(TrackTikClient):
    """Test client with updated pagination logic"""
    
    def get_paginated_data(self, endpoint, params=None):
        """Updated pagination that works with TrackTik's response structure"""
        if params is None:
            params = {}
            
        all_records = []
        offset = 0
        page_size = 100  # or config.API_PAGE_SIZE
        
        while True:
            current_params = params.copy()
            current_params.update({
                'limit': page_size,
                'offset': offset
            })
            
            print(f"Fetching offset {offset}...")
            
            response = self.session.get(
                f"{self.base_url}{endpoint}",
                headers=self._get_headers(),
                params=current_params
            )
            response.raise_for_status()
            
            data = response.json()
            records_retrieved = len(data.get('data', []))
            all_records.extend(data['data'])
            
            print(f"  Got {records_retrieved} records (total so far: {len(all_records)})")
            
            # Stop conditions
            if records_retrieved == 0:
                print("  No more records - stopping")
                break
                
            if records_retrieved < page_size:
                print("  Last page (less than limit) - stopping")
                break
                
            # Safety limit
            if offset > 1000:
                print("  Safety limit reached - stopping")
                break
                
            offset += page_size
            
        return all_records

# Test it
print("Testing updated pagination logic...")
print("=" * 50)

client = TestTrackTikClient()

# Test 1: Small date range
print("\n1. Testing one day of shifts:")
try:
    shifts = client.get_shifts('2024-06-01', '2024-06-01')
    print(f"Success! Found {len(shifts)} shifts")
    if shifts:
        print(f"First shift: {shifts[0].get('startDateTime')} to {shifts[0].get('endDateTime')}")
except Exception as e:
    print(f"Error: {e}")

# Test 2: Test employees (should have many pages)
print("\n2. Testing employees endpoint:")
try:
    # Just get first 250 to test pagination
    employees = client.get_paginated_data('/rest/v1/employees', {'limit': 50})
    print(f"Success! Found {len(employees)} employees")
except Exception as e:
    print(f"Error: {e}")