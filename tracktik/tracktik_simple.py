"""
tracktik_simple.py
A simplified version to understand how TrackTik API works
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SimpleTrackTikClient:
    def __init__(self):
        self.client_id = os.getenv('TRACKTIK_CLIENT_ID')
        self.client_secret = os.getenv('TRACKTIK_CLIENT_SECRET')
        self.username = os.getenv('TRACKTIK_USERNAME')
        self.password = os.getenv('TRACKTIK_PASSWORD')
        self.base_url = "https://blackstoneconsulting.staffr.us"
        self.access_token = None
        self.refresh_token = None
        
        print("TrackTik client initialized")
        print(f"Base URL: {self.base_url}")
    
    def authenticate(self):
        """Get an access token from TrackTik using Password Flow"""
        print("\nTrying to authenticate using OAuth 2 Password Flow...")
        
        # The exact URL they provided
        auth_url = "https://blackstoneconsulting.staffr.us/rest/oauth2/access_token"
        
        # Match their PHP example exactly
        auth_data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': self.password,
            'grant_type': 'password'
        }
        
        print(f"Auth URL: {auth_url}")
        print(f"Using username: {self.username}")
        
        # Make the request
        response = requests.post(auth_url, data=auth_data)
        
        # Check if it worked
        if response.status_code == 200:
            auth_response = response.json()
            self.access_token = auth_response.get('access_token')
            self.refresh_token = auth_response.get('refresh_token')
            
            print("✅ Authentication successful!")
            print(f"Access token received (first 20 chars): {self.access_token[:20]}...")
            if self.refresh_token:
                print(f"Refresh token received (first 20 chars): {self.refresh_token[:20]}...")
            print(f"\nFull response keys: {list(auth_response.keys())}")
            
            # Show token details if available
            if 'expires_in' in auth_response:
                print(f"Token expires in: {auth_response['expires_in']} seconds")
            if 'token_type' in auth_response:
                print(f"Token type: {auth_response['token_type']}")
                
        else:
            print(f"❌ Authentication failed!")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
    
    def get_employees(self):
        """Get a list of employees (testing API access)"""
        if not self.access_token:
            print("❌ Not authenticated! Please run authenticate() first.")
            return
        
        print("\n📋 Getting employees list...")
        
        # Use the correct API path from the documentation
        url = f"{self.base_url}/rest/v1/employees"
        
        # Add the access token to headers
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        # Add parameters to limit results
        params = {
            'limit': 5  # Just get 5 employees for testing
        }
        
        print(f"Making request to: {url}")
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Successfully got employees!")
            
            # The response should have 'data' and 'meta' keys
            if 'data' in data:
                employees = data['data']
                print(f"Found {len(employees)} employees")
                
                # Show first employee details if available
                if employees:
                    emp = employees[0]
                    print(f"\nFirst employee:")
                    print(f"  ID: {emp.get('id')}")
                    print(f"  Name: {emp.get('firstName')} {emp.get('lastName')}")
                    print(f"  Email: {emp.get('email')}")
            else:
                print("Response structure:", list(data.keys()))
                
        else:
            print(f"❌ Request failed!")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")

    def get_sites(self):
        """Get a list of sites/clients"""
        if not self.access_token:
            print("❌ Not authenticated! Please run authenticate() first.")
            return
        
        print("\n🏢 Getting sites list...")
        
        # Use the clients endpoint from the documentation
        url = f"{self.base_url}/rest/v1/clients"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        params = {
            'limit': 5  # Just get 5 sites for testing
        }
        
        print(f"Making request to: {url}")
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Successfully got sites!")
            
            if 'data' in data:
                sites = data['data']
                print(f"Found {len(sites)} sites")
                
                # Show first site details if available
                if sites:
                    site = sites[0]
                    print(f"\nFirst site:")
                    print(f"  ID: {site.get('id')}")
                    print(f"  Name: {site.get('name')}")
                    print(f"  Custom ID: {site.get('customId')}")
            else:
                print("Response structure:", list(data.keys()))
                
        else:
            print(f"❌ Request failed!")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")

    def get_shifts_for_week(self):
        """Get shifts for the current week"""
        if not self.access_token:
            print("❌ Not authenticated! Please run authenticate() first.")
            return
        
        print("\n📅 Getting shifts for this week...")
        
        # Calculate this week's dates
        from datetime import datetime, timedelta
        today = datetime.now()
        # Get Monday of this week
        monday = today - timedelta(days=today.weekday())
        # Get Sunday of this week
        sunday = monday + timedelta(days=6)
        
        url = f"{self.base_url}/rest/v1/shifts"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        # Filter shifts by date range
        params = {
            'limit': 10,  # Just get 10 shifts for testing
            'startsOn:gte': monday.strftime('%Y-%m-%d'),  # Greater than or equal to Monday
            'startsOn:lte': sunday.strftime('%Y-%m-%d')   # Less than or equal to Sunday
        }
        
        print(f"Date range: {monday.strftime('%Y-%m-%d')} to {sunday.strftime('%Y-%m-%d')}")
        print(f"Making request to: {url}")
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Successfully got shifts!")
            
            if 'data' in data:
                shifts = data['data']
                print(f"Found {len(shifts)} shifts")
                
                # Show first shift details if available
                if shifts:
                    shift = shifts[0]
                    print(f"\nFirst shift:")
                    print(f"  ID: {shift.get('id')}")
                    print(f"  Start: {shift.get('startsOn')}")
                    print(f"  End: {shift.get('endsOn')}")
                    print(f"  Hours: {shift.get('billableHours')}")
                    print(f"  Position ID: {shift.get('position', {}).get('id') if isinstance(shift.get('position'), dict) else shift.get('position')}")
            else:
                print("Response structure:", list(data.keys()))
                
        else:
            print(f"❌ Request failed!")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")

# Test it
if __name__ == "__main__":
    client = SimpleTrackTikClient()
    
    # First authenticate
    client.authenticate()
    
    # Then try to get employees
    client.get_employees()
    
    # Get sites
    client.get_sites()
    
    # Get shifts for this week
    client.get_shifts_for_week()