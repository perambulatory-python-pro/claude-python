import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

try:
    # Test with your actual email
    your_email = "brendon@blackstone-consulting.com" # or your KP email if different
    
    print(f"🔍 Testing email access for {your_email}...")
    
    url = f'https://graph.microsoft.com/v1.0/users/{your_email}/messages'
    params = {
        '$top': 1,
        '$select': 'id,subject,receivedDateTime,sender'
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        messages = response.json().get('value', [])
        print("✅ Graph API email access successful!")
        if messages:
            msg = messages[0]
            print(f"   Latest email: '{msg.get('subject', 'No subject')}'")
            print(f"   From: {msg.get('sender', {}).get('emailAddress', {}).get('address', 'Unknown')}")
        else:
            print("   No messages found (but access is working)")
    else:
        print(f"❌ Graph API email access failed: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ Graph API test failed: {e}")