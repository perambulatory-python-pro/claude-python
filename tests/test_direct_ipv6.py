# test_direct_ipv6.py
import psycopg2

# Direct connection parameters (supports IPv6)
connection_params = {
    "user": "postgres",
    "password": "8KrHkhZBfl4JjYMs",  # Your actual password
    "host": "db.rzgxlojsgvygpcccwckm.supabase.co",
    "port": "5432",
    "dbname": "postgres",
    "connect_timeout": 10
}

try:
    print("Testing direct connection (IPv6 compatible)...")
    connection = psycopg2.connect(**connection_params)
    print("✅ Connection successful!")
    
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    result = cursor.fetchone()
    print(f"PostgreSQL version: {result[0]}")
    
    cursor.close()
    connection.close()
    
except Exception as e:
    print(f"❌ Failed: {e}")