"""
test_tracktik_simple.py
A very simple test to check our TrackTik connection
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get our credentials from the .env file
client_id = os.getenv('TRACKTIK_CLIENT_ID')
client_secret = os.getenv('TRACKTIK_CLIENT_SECRET')

# Let's just print them (partially hidden) to make sure they loaded
print("Testing if credentials loaded from .env file:")
print(f"Client ID: {client_id[:10]}..." if client_id else "Client ID not found!")
print(f"Client Secret: {client_secret[:10]}..." if client_secret else "Client Secret not found!")