from supabase import create_client, Client

# Get these from your Supabase dashboard
url = "https://rzgxlojsgvygpccwckm.supabase.co"  # Your project URL
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ6Z3hsb2pzZ3Z5Z3BjY2N3Y2ttIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA2NDM4ODcsImV4cCI6MjA2NjIxOTg4N30.ARIsHaiv2tjdRQJgDJSY1AUYrq8_488oNtM6La-G9L0"  # Get this from Settings > API

# Create client
supabase: Client = create_client(url, key)

# Test query
response = supabase.table('invoice_master').select("*").limit(1).execute()
print(response)