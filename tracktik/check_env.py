"""
check_env.py
Let's check what environment variables are loaded
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("Checking .env file contents:")
print("=" * 50)

# Check each variable
env_vars = [
    'TRACKTIK_CLIENT_ID',
    'TRACKTIK_CLIENT_SECRET', 
    'TRACKTIK_USERNAME',
    'TRACKTIK_PASSWORD'
]

for var in env_vars:
    value = os.getenv(var)
    if value:
        # Show first 5 characters only for security
        print(f"{var}: {value[:5]}... (length: {len(value)})")
    else:
        print(f"{var}: NOT FOUND")

print("\n" + "=" * 50)
print("Make sure your .env file has these exact variable names!")