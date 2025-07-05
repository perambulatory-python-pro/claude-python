#!/usr/bin/env python3
"""Final shift structure test without includes"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracktik_etl.etl.tracktik_client import TrackTikClient
import json

def test_shifts_final():
    client = TrackTikClient()
    
    print("=== FINAL SHIFT ANALYSIS ===\n")
    
    # Test 1: Get shifts without include parameter
    print("Test 1: Getting shifts WITHOUT include parameter...")
    try:
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2025-05-30|2025-06-12',
                'limit': 5
            }
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('data'):
            for i, shift in enumerate(data['data']):
                print(f"\nShift {i+1}:")
                print(f"  ID: {shift.get('id')}")
                print(f"  Status: {shift.get('status')}")
                print(f"  Account: {shift.get('account')}")
                print(f"  Position: {shift.get('position')}")
                print(f"  Employee: {shift.get('employee')}")
                print(f"  Start: {shift.get('startDateTime')}")
                print(f"  End: {shift.get('endDateTime')}")
                
                if i == 0:
                    print("\nFull structure of first shift:")
                    print(json.dumps(shift, indent=2, default=str))
                    
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 2: Get N California shifts
    print("\n\nTest 2: Getting N California shifts (region 274)...")
    try:
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2025-05-30|2025-06-12',
                'employee.region': 274,
                'limit': 10
            }
        )
        response.raise_for_status()
        data = response.json()
        
        total = data.get('meta', {}).get('count', 0)
        print(f"  Total N California shifts: {total}")
        
        # Collect unique position and employee IDs
        positions = set()
        employees = set()
        
        for shift in data.get('data', []):
            if shift.get('position'):
                positions.add(shift['position'])
            if shift.get('employee'):
                employees.add(shift['employee'])
        
        print(f"  Unique positions in sample: {len(positions)}")
        print(f"  Unique employees in sample: {len(employees)}")
        print(f"  Sample position IDs: {list(positions)[:5]}")
        print(f"  Sample employee IDs: {list(employees)[:5]}")
        
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 3: Check all KAISER regions
    print("\n\nTest 3: Checking shifts for all KAISER regions...")
    kaiser_regions = {
        'N California': 274,
        'S California': 275,
        'Hawaii': 276,
        'Washington': 277,
        'Colorado': 278,
        'Georgia': 279,
        'MidAtlantic': 282,
        'Northwest': 1029
    }
    
    for region_name, region_id in kaiser_regions.items():
        try:
            response = client.session.get(
                f"{client.base_url}/rest/v1/shifts",
                headers=client._get_headers(),
                params={
                    'startDateTime:between': '2025-05-30|2025-06-12',
                    'employee.region': region_id,
                    'limit': 1
                }
            )
            response.raise_for_status()
            data = response.json()
            
            count = data.get('meta', {}).get('count', 0)
            print(f"  {region_name} ({region_id}): {count} shifts")
            
        except Exception as e:
            print(f"  {region_name} ({region_id}): ERROR - {e}")

if __name__ == "__main__":
    test_shifts_final()