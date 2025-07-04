# check_credentials.py
"""
Check for common credential issues
"""
import os
from dotenv import load_dotenv

load_dotenv()

def check_credentials():
    """Check credentials for common issues"""
    
    # Get credentials
    client_id = os.getenv('TRACKTIK_CLIENT_ID')
    client_secret = os.getenv('TRACKTIK_CLIENT_SECRET')
    username = os.getenv('TRACKTIK_USERNAME')
    password = os.getenv('TRACKTIK_PASSWORD')
    
    print("=== Checking for Common Issues ===\n")
    
    # Check for trailing/leading spaces
    print("1. Checking for whitespace:")
    
    if client_id != client_id.strip():
        print(f"  ⚠️  CLIENT_ID has extra whitespace!")
        print(f"     Original length: {len(client_id)}")
        print(f"     Stripped length: {len(client_id.strip())}")
    else:
        print(f"  ✓ CLIENT_ID: No extra whitespace (length: {len(client_id)})")
    
    if client_secret != client_secret.strip():
        print(f"  ⚠️  CLIENT_SECRET has extra whitespace!")
        print(f"     Original length: {len(client_secret)}")
        print(f"     Stripped length: {len(client_secret.strip())}")
    else:
        print(f"  ✓ CLIENT_SECRET: No extra whitespace (length: {len(client_secret)})")
    
    # Show exact lengths
    print(f"\n2. Exact credential lengths:")
    print(f"  CLIENT_ID: {len(client_id)} characters")
    print(f"  CLIENT_SECRET: {len(client_secret)} characters")
    print(f"  USERNAME: {len(username)} characters")
    print(f"  PASSWORD: {len(password)} characters")
    
    # Check for quotes in values
    print(f"\n3. Checking for quotes in values:")
    if '"' in client_id or "'" in client_id:
        print(f"  ⚠️  CLIENT_ID contains quotes!")
    if '"' in client_secret or "'" in client_secret:
        print(f"  ⚠️  CLIENT_SECRET contains quotes!")
    
    # Show first/last few characters (safely)
    print(f"\n4. Credential preview:")
    print(f"  CLIENT_ID starts with: {client_id[:10]}...")
    print(f"  CLIENT_ID ends with: ...{client_id[-10:]}")
    print(f"  CLIENT_SECRET starts with: {client_secret[:5]}...")
    print(f"  CLIENT_SECRET ends with: ...{client_secret[-5:]}")

if __name__ == "__main__":
    check_credentials()