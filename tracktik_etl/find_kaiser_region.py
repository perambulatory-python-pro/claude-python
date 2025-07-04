# find_kaiser_region.py
import os
from dotenv import load_dotenv

load_dotenv()

from etl.tracktik_client import TrackTikClient

client = TrackTikClient()

print("Finding KAISER region information...")
print("=" * 50)

# Get all clients/sites to find KAISER
clients = client.get_clients()

# Search for KAISER
kaiser_clients = []
for client_data in clients:
    name = client_data.get('name', '').upper()
    custom_id = str(client_data.get('customId', '')).upper()
    
    if 'KAISER' in name or 'KAISER' in custom_id:
        kaiser_clients.append({
            'id': client_data.get('id'),
            'name': client_data.get('name'),
            'customId': client_data.get('customId'),
            'region': client_data.get('region')
        })

print(f"\nFound {len(kaiser_clients)} KAISER-related clients:")
for kc in kaiser_clients[:10]:  # Show first 10
    print(f"  ID: {kc['id']}, Name: {kc['name']}, CustomID: {kc['customId']}")

# Get regions to understand structure
print("\n\nChecking regions...")
# Note: You might need to check if there's a regions endpoint
# or if region info is embedded in other objects