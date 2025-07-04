# test_connection.py
"""
Test database and API connections
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_env_vars():
    """Check if all required environment variables are set"""
    print("Checking environment variables...")
    
    required_vars = {
        'TrackTik': ['TRACKTIK_CLIENT_ID', 'TRACKTIK_CLIENT_SECRET', 
                     'TRACKTIK_USERNAME', 'TRACKTIK_PASSWORD', 'TRACKTIK_BASE_URL'],
        'PostgreSQL': ['PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD']
    }
    
    all_good = True
    for category, vars in required_vars.items():
        print(f"\n{category}:")
        for var in vars:
            value = os.getenv(var)
            if value:
                # Mask sensitive values
                if 'PASSWORD' in var or 'SECRET' in var:
                    print(f"  ✓ {var}: {'*' * 8}")
                else:
                    print(f"  ✓ {var}: {value[:20]}..." if len(value) > 20 else f"  ✓ {var}: {value}")
            else:
                print(f"  ✗ {var}: NOT SET")
                all_good = False
    
    return all_good

def test_database():
    """Test PostgreSQL connection"""
    print("\n\nTesting PostgreSQL connection...")
    try:
        from etl.database import db
        
        # Test basic connection
        result = db.execute_query("SELECT version()")
        print(f"✓ Connected to PostgreSQL")
        print(f"  Version: {result[0]['version'][:50]}...")
        
        # Check schema
        schema_check = db.execute_query("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'tracktik'
        """)
        
        if schema_check:
            print("✓ TrackTik schema exists")
            
            # Check tables
            tables = db.execute_query("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'tracktik'
                ORDER BY table_name
            """)
            
            print(f"  Found {len(tables)} tables:")
            for table in tables[:5]:  # Show first 5
                print(f"    - {table['table_name']}")
            if len(tables) > 5:
                print(f"    ... and {len(tables) - 5} more")
        else:
            print("✗ TrackTik schema not found - run create_schema.sql first")
            return False
            
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {str(e)}")
        return False

def test_api():
    """Test TrackTik API connection"""
    print("\n\nTesting TrackTik API connection...")
    try:
        from etl.tracktik_client import TrackTikClient
        from etl.config import config
        
        # Debug: Show what base URL we're using
        print(f"  Using base URL: {config.TRACKTIK_BASE_URL}")
        
        client = TrackTikClient()
        print("  Authenticating...")
        client.authenticate()
        print("✓ Authentication successful")
        print(f"  Token expires at: {client.token_expires_at}")
        
        # Try a simple API call
        print("  Testing API call...")
        employees = client.get_paginated_data('/rest/v1/employees', {'limit': 1})
        print(f"✓ API call successful")
        
        return True
        
    except Exception as e:
        print(f"✗ API connection failed: {str(e)}")
        return False


# Run all tests if executed as a script
if __name__ == "__main__":
    print("=== Environment Variable Check ===")
    env_ok = test_env_vars()
    print("\n=== Database Connection Test ===")
    db_ok = test_database()
    print("\n=== API Connection Test ===")
    api_ok = test_api()
    print("\nSummary:")
    print(f"  Env Vars: {'OK' if env_ok else 'FAIL'}")
    print(f"  Database: {'OK' if db_ok else 'FAIL'}")
    print(f"  API: {'OK' if api_ok else 'FAIL'}")