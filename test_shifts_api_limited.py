#!/usr/bin/env python3
"""Diagnose shifts API issues with limited queries"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracktik_etl.etl.tracktik_client import TrackTikClient
from datetime import datetime, timedelta
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_shifts_api_limited():
    client = TrackTikClient()
    
    print("=== LIMITED SHIFTS API DIAGNOSTIC ===\n")
    
    # Test 1: Just check if ANY shifts exist with limit=1
    print("Test 1: Checking if shifts exist (limit=1)...")
    
    try:
        # Override the get_shifts to use limit=1
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2025-05-30|2025-06-12',
                'limit': 1,
                'offset': 0
            }
        )
        response.raise_for_status()
        data = response.json()
        
        total_count = data.get('meta', {}).get('count', 0)
        print(f"  Total shifts in period 2025-05-30 to 2025-06-12: {total_count}")
        
        if data.get('data'):
            shift = data['data'][0]
            print(f"  Sample shift:")
            print(f"    ID: {shift.get('id')}")
            print(f"    Start: {shift.get('startDateTime')}")
            print(f"    Status: {shift.get('status')}")
            print(f"    Account: {shift.get('account')}")
            
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 2: Check a single day
    print("\n\nTest 2: Checking single day (2025-06-01)...")
    try:
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2025-06-01|2025-06-01',
                'limit': 5
            }
        )
        response.raise_for_status()
        data = response.json()
        
        total_count = data.get('meta', {}).get('count', 0)
        print(f"  Total shifts on 2025-06-01: {total_count}")
        
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 3: Check recent 7 days
    print("\n\nTest 3: Checking last 7 days...")
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': f'{start_date}|{end_date}',
                'limit': 1
            }
        )
        response.raise_for_status()
        data = response.json()
        
        total_count = data.get('meta', {}).get('count', 0)
        print(f"  Total shifts from {start_date} to {end_date}: {total_count}")
        
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 4: Check with specific account filter
    print("\n\nTest 4: Testing account filter...")
    try:
        # Test with a known client ID
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2024-01-01|2025-12-31',
                'account': 314,  # One of your N California clients
                'limit': 1
            }
        )
        response.raise_for_status()
        data = response.json()
        
        total_count = data.get('meta', {}).get('count', 0)
        print(f"  Total shifts for account 314: {total_count}")
        
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 5: Find ANY shift in the system
    print("\n\nTest 5: Finding ANY shift in the system...")
    try:
        # Very wide date range, limit 1
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'limit': 1,
                'sort': '-id'  # Most recent by ID
            }
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('data'):
            shift = data['data'][0]
            print(f"  Found shift:")
            print(f"    ID: {shift.get('id')}")
            print(f"    Start: {shift.get('startDateTime')}")
            print(f"    End: {shift.get('endDateTime')}")
            print(f"    Status: {shift.get('status')}")
            
            # Print full structure
            print("\n  Full shift structure:")
            print(json.dumps(shift, indent=2, default=str))
        else:
            print("  No shifts found in system!")
            
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 6: Check available scopes
    print("\n\nTest 6: Testing with scopes...")
    scopes = ['IS_CURRENT', 'IN_THE_FUTURE', 'PUBLISHED']
    
    for scope in scopes:
        try:
            response = client.session.get(
                f"{client.base_url}/rest/v1/shifts",
                headers=client._get_headers(),
                params={
                    'scope': scope,
                    'limit': 1
                }
            )
            response.raise_for_status()
            data = response.json()
            
            total_count = data.get('meta', {}).get('count', 0)
            print(f"  Scope '{scope}': {total_count} shifts")
            
        except Exception as e:
            print(f"  Scope '{scope}': ERROR - {e}")

if __name__ == "__main__":
    test_shifts_api_limited()