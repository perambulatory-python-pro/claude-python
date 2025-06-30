# test_direct_connection.py
import psycopg2

# Direct connection (better for persistent connections like your app)
DATABASE_URL = "postgresql://postgres:8KrHkhZBfl4JjYMs@db.rzgxlojsgvygpccwckm.supabase.co:5432/postgres"

try:
    print("Testing direct connection...")
    connection = psycopg2.connect(DATABASE_URL)
    print("✅ Connection successful!")
    
    cursor = connection.cursor()
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print(f"Current database time: {result[0]}")
    
    cursor.close()
    connection.close()
    
except Exception as e:
    print(f"❌ Failed to connect: {type(e).__name__}: {e}")