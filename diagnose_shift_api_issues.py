#!/usr/bin/env python3
"""Diagnose shifts API issues"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracktik_etl.etl.tracktik_client import TrackTikClient
from datetime import datetime, timedelta
import json

def test_shifts_api():
    client = TrackTikClient()
    
    print("=== SHIFTS API DIAGNOSTIC ===\n")
    
    # Test 1: Check what date ranges have shifts
    print("Test 1: Checking different date ranges for shifts...")
    date_ranges = [
        ("2025-05-30", "2025-06-12"),  # Your billing period
        ("2025-06-01", "2025-06-07"),  # Just first week
        ("2025-01-01", "2025-01-31"),  # Earlier in year
        ("2024-11-01", "2024-11-30"),  # Last year
        ("2024-06-01", "2024-06-30"),  # Last year same month
    ]
    
    for start_date, end_date in date_ranges:
        try:
            # Just get first page to see if any exist
            params = {'limit': 10}
            shifts = client.get_shifts(start_date, end_date, **params)
            print(f"  {start_date} to {end_date}: {len(shifts)} shifts found")
            
            if shifts:
                print(f"    First shift date: {shifts[0].get('startDateTime')}")
                print(f"    Status: {shifts[0].get('status')}")
                
        except Exception as e:
            print(f"  {start_date} to {end_date}: ERROR - {e}")
    
    # Test 2: Get the most recent shifts
    print("\n\nTest 2: Finding most recent shifts...")
    # Try current date minus 30 days
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        params = {
            'limit': 5,
            'sort': '-startDateTime'  # Most recent first
        }
        shifts = client.get_shifts(start_date, end_date, **params)
        
        if shifts:
            print(f"Found {len(shifts)} recent shifts:")
            for i, shift in enumerate(shifts):
                print(f"\n  Shift {i+1}:")
                print(f"    ID: {shift.get('id')}")
                print(f"    Start: {shift.get('startDateTime')}")
                print(f"    Status: {shift.get('status')}")
                print(f"    Employee: {shift.get('employee')}")
                print(f"    Account: {shift.get('account')}")
                print(f"    Position: {shift.get('position')}")
        else:
            print("No recent shifts found")
            
    except Exception as e:
        print(f"Error getting recent shifts: {e}")
    
    # Test 3: Check specific client
    print("\n\nTest 3: Checking shifts for specific KAISER clients...")
    # Use some of the client IDs we know are in N California
    test_client_ids = [314, 315, 316, 317, 318]  # From your error messages
    
    for client_id in test_client_ids[:3]:  # Just test first 3
        try:
            params = {
                'account': client_id,
                'limit': 5
            }
            # Use a wider date range
            shifts = client.get_shifts('2024-01-01', '2025-12-31', **params)
            
            if shifts:
                print(f"\n  Client {client_id}: {len(shifts)} shifts found")
                print(f"    First shift: {shifts[0].get('startDateTime')}")
            else:
                print(f"\n  Client {client_id}: No shifts found")
                
        except Exception as e:
            print(f"\n  Client {client_id}: ERROR - {e}")
    
    # Test 4: Check all available statuses
    print("\n\nTest 4: Checking shifts by status...")
    statuses = ['CANCELLED', 'ACTIVE', 'SCHEDULED', 'COMPLETED', 'IN_PROGRESS', 'APPROVED']
    
    for status in statuses:
        try:
            params = {
                'status': status,
                'limit': 1
            }
            shifts = client.get_shifts('2024-01-01', '2025-12-31', **params)
            
            if shifts:
                print(f"  Status '{status}': Found shifts")
            else:
                print(f"  Status '{status}': No shifts")
                
        except Exception as e:
            print(f"  Status '{status}': ERROR - {e}")
    
    # Test 5: Raw API call to see response
    print("\n\nTest 5: Raw API response...")
    try:
        import requests
        
        # Get headers
        headers = client._get_headers()
        
        # Make a simple request
        url = f"{client.base_url}/rest/v1/shifts"
        params = {
            'startDateTime:between': '2024-01-01|2025-12-31',
            'limit': 1
        }
        
        response = requests.get(url, headers=headers, params=params)
        print(f"  Status Code: {response.status_code}")
        print(f"  Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  Total Count: {data.get('meta', {}).get('count', 'Unknown')}")
            print(f"  Data Length: {len(data.get('data', []))}")
            
            if data.get('data'):
                print("\n  First shift structure:")
                print(json.dumps(data['data'][0], indent=2))
                
    except Exception as e:
        print(f"  Raw API Error: {e}")

if __name__ == "__main__":
    test_shifts_api()