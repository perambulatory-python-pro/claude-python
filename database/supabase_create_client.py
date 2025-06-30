# supabase_create_client.py
from supabase import create_client, Client

# Replace these with your actual values from Supabase dashboard
url = "https://rzgxlojsgvygpccwckm.supabase.co"  # Your actual project URL
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ6Z3hsb2pzZ3Z5Z3BjY3dja20iLCJyb2xlIjoiYW5vbiIsImlhdCI6MTczNDcxNzUzMCwiZXhwIjoyMDUwMjkzNTMwfQ.Gv0rGu8YZVRr88yL5rqjyOLJukqsn0N5_5UcL-PQhXs"  # Your actual anon key

# Create the client
supabase: Client = create_client(url, key)

# Test it with a simple query
try:
    # Since we haven't created tables yet, let's just test the connection
    # This will list all tables (should be empty or show system tables)
    response = supabase.table('invoice_master').select("*").limit(1).execute()
    print("✅ Connected successfully!")
    print(f"Response: {response}")
except Exception as e:
    print(f"❌ Error: {e}")