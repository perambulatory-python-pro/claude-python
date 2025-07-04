# test_with_small_range.py (Windows-friendly)
import os
from dotenv import load_dotenv

load_dotenv()

from etl.tracktik_client import TrackTikClient

# Temporarily modify the client to add debug output
class DebugTrackTikClient(TrackTikClient):
    def get_paginated_data(self, endpoint: str, params=None):
        """Override with debug output"""
        if params is None:
            params = {}
            
        all_records = []
        offset = 0
        total_records = None
        page_count = 0
        
        print(f"\nDEBUG: Starting pagination for {endpoint}")
        print(f"DEBUG: Initial params: {params}")
        
        while page_count < 5:  # Safety limit: max 5 pages
            current_params = params.copy()
            current_params.update({
                'limit': 10,  # Small limit for testing
                'offset': offset
            })
            
            print(f"\nDEBUG: Page {page_count + 1}, offset={offset}")
            
            response = self.session.get(
                f"{self.base_url}{endpoint}",
                headers=self._get_headers(),
                params=current_params
            )
            response.raise_for_status()
            
            data = response.json()
            records_retrieved = len(data.get('data', []))
            all_records.extend(data['data'])
            
            print(f"DEBUG: Retrieved {records_retrieved} records")
            
            if 'meta' in data and 'pagination' in data['meta']:
                pagination = data['meta']['pagination']
                if total_records is None:
                    total_records = pagination.get('total', 0)
                    print(f"DEBUG: Total records available: {total_records}")
                
                if offset + records_retrieved >= total_records:
                    print("DEBUG: All records retrieved")
                    break
            else:
                print("DEBUG: No pagination meta, assuming done")
                break
                
            offset += 10
            page_count += 1
            
        if page_count >= 5:
            print("DEBUG: Hit safety limit of 5 pages")
            
        return all_records

# Use the debug client
client = DebugTrackTikClient()

try:
    print("Testing with debug client...")
    shifts = client.get_shifts('2024-06-01', '2024-06-01')  # Just one day
    print(f"\nSuccess! Found {len(shifts)} shifts total")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")