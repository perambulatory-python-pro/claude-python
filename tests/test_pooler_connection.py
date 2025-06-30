# test_pooler_connection.py
import psycopg2

# Session pooler connection
connection_params = {
    "user": "postgres.rzgxlojsgvygpccwckm",
    "password": "8KrHkhZBfl4JjYMs",  
    "host": "aws-0-us-east-1.pooler.supabase.com",
    "port": "5432",
    "dbname": "postgres"
}

try:
    print(f"Testing pooler connection to {connection_params['host']}...")
    connection = psycopg2.connect(**connection_params)
    print("✅ Connection successful!")
    
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    result = cursor.fetchone()
    print(f"PostgreSQL version: {result[0]}")
    
    cursor.close()
    connection.close()
    
except Exception as e:
    print(f"❌ Failed to connect: {e}")