import os
import psycopg2
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

print("Testing Neon connection with .env file...")
print("-" * 50)

# Method 1: Using DATABASE_URL (most common)
print("\nMethod 1: Using DATABASE_URL")
try:
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not found in .env file")
    else:
        print(f"✓ DATABASE_URL found (length: {len(database_url)})")
        
        conn = psycopg2.connect(database_url)
        print("✅ Connected successfully using DATABASE_URL!")
        
        # Quick test query
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user;")
        db, user = cur.fetchone()
        print(f"   Connected to: {db} as {user}")
        
        cur.close()
        conn.close()
        
except Exception as e:
    print(f"❌ Connection failed: {e}")

# Method 2: Using individual PG* variables
print("\n\nMethod 2: Using individual PG* variables")
try:
    conn = psycopg2.connect(
        host=os.getenv('PGHOST'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        port=int(os.getenv('PGPORT', 5432)),
        sslmode='require'
    )
    print("✅ Connected successfully using PG* variables!")
    conn.close()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")