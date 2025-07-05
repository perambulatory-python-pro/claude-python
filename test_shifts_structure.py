#!/usr/bin/env python3
"""Test shift structure and filtering"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracktik_etl.etl.tracktik_client import TrackTikClient
import json

def test_shift_structure():
    client = TrackTikClient()
    
    print("=== SHIFT STRUCTURE ANALYSIS ===\n")
    
    # Test 1: Get a few shifts with full includes
    print("Test 1: Getting shifts with full includes...")
    try:
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2025-05-30|2025-06-12',
                'limit': 3,
                'include': 'employee,position,account'
            }
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('data'):
            for i, shift in enumerate(data['data']):
                print(f"\nShift {i+1}:")
                print(f"  ID: {shift.get('id')}")
                print(f"  Account: {shift.get('account')}")
                print(f"  Position: {shift.get('position')}")
                print(f"  Employee: {shift.get('employee')}")
                
                # Check if account is expanded
                if isinstance(shift.get('account'), dict):
                    print(f"  Account Name: {shift['account'].get('name')}")
                    print(f"  Account Company: {shift['account'].get('company')}")
                    print(f"  Account Region: {shift['account'].get('region')}")
                
                # Save one full shift for analysis
                if i == 0:
                    print("\nFull first shift structure:")
                    print(json.dumps(shift, indent=2, default=str))
                    
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 2: Try different filter approaches
    print("\n\nTest 2: Testing filter approaches...")
    
    # Try filtering by position.account
    filters_to_try = [
        {'position.account': 314},
        {'position.account.id': 314},
        {'account.id': 314},
        {'employee.region': 274},  # N California region ID
    ]
    
    for filter_params in filters_to_try:
        try:
            params = {
                'startDateTime:between': '2025-05-30|2025-06-12',
                'limit': 1,
                **filter_params
            }
            
            response = client.session.get(
                f"{client.base_url}/rest/v1/shifts",
                headers=client._get_headers(),
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                count = data.get('meta', {}).get('count', 0)
                print(f"  Filter {filter_params}: SUCCESS - {count} shifts")
            else:
                print(f"  Filter {filter_params}: {response.status_code} error")
                
        except Exception as e:
            print(f"  Filter {filter_params}: ERROR - {e}")
    
    # Test 3: Get shifts and check their account values
    print("\n\nTest 3: Analyzing account values in shifts...")
    try:
        response = client.session.get(
            f"{client.base_url}/rest/v1/shifts",
            headers=client._get_headers(),
            params={
                'startDateTime:between': '2025-05-30|2025-06-12',
                'limit': 100,
                'include': 'position,account'
            }
        )
        response.raise_for_status()
        data = response.json()
        
        account_counts = {}
        position_accounts = {}
        
        for shift in data.get('data', []):
            # Direct account
            account = shift.get('account')
            if account:
                account_counts[account] = account_counts.get(account, 0) + 1
            
            # Account through position
            if isinstance(shift.get('position'), dict):
                pos_account = shift['position'].get('account')
                if pos_account:
                    if isinstance(pos_account, dict):
                        pos_account_id = pos_account.get('id')
                    else:
                        pos_account_id = pos_account
                    position_accounts[pos_account_id] = position_accounts.get(pos_account_id, 0) + 1
        
        print(f"  Direct account values: {list(account_counts.keys())[:10]}")
        print(f"  Accounts via position: {list(position_accounts.keys())[:10]}")
        print(f"  Total unique direct accounts: {len(account_counts)}")
        print(f"  Total unique position accounts: {len(position_accounts)}")
        
    except Exception as e:
        print(f"  ERROR: {e}")

if __name__ == "__main__":
    test_shift_structure()