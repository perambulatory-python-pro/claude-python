# create_schema_runner.py
"""
Run the schema creation SQL using Python
"""
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def run_schema_creation():
    """Execute the schema creation SQL file"""
    # Get connection parameters
    conn_params = {
        'host': os.getenv('PGHOST'),
        'port': os.getenv('PGPORT'),
        'database': os.getenv('PGDATABASE'),
        'user': os.getenv('PGUSER'),
        'password': os.getenv('PGPASSWORD')
    }
    
    print("Connecting to database...")
    print(f"Host: {conn_params['host']}")
    print(f"Database: {conn_params['database']}")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True  # Important for CREATE statements
        cursor = conn.cursor()
        
        # Read SQL file
        print("\nReading create_schema.sql...")
        with open('create_schema.sql', 'r') as f:
            sql_content = f.read()
        
        # Execute SQL
        print("Executing schema creation...")
        cursor.execute(sql_content)
        
        # Verify schema was created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'tracktik'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"\n✅ Schema created successfully!")
        print(f"Created {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    print("TrackTik Schema Creation")
    print("=" * 50)
    
    if run_schema_creation():
        print("\n✅ Schema creation complete!")
    else:
        print("\n❌ Schema creation failed!")