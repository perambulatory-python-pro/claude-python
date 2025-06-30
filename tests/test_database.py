"""
Test Database Connection Script
Run this to verify your database setup is working correctly

This script will:
1. Load environment variables
2. Test database connection
3. Show table statistics
4. Demonstrate basic operations

Run with: python test_database.py
"""

import os
from dotenv import load_dotenv
from database_manager import DatabaseManager
import logging

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_setup():
    """Test all aspects of our database setup"""
    
    print("🔍 TESTING DATABASE SETUP")
    print("=" * 50)
    
    # Step 1: Load environment variables
    print("\n1. Loading environment variables...")
    load_dotenv()
    
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        # Hide the password in the output for security
        safe_url = database_url.split('@')[1] if '@' in database_url else database_url
        print(f"   ✓ Database URL loaded: ...@{safe_url}")
    else:
        print("   ❌ DATABASE_URL not found in .env file")
        return False
    
    # Step 2: Initialize database manager
    print("\n2. Initializing database manager...")
    try:
        db = DatabaseManager()
        print("   ✓ Database manager initialized")
    except Exception as e:
        print(f"   ❌ Error initializing database: {e}")
        return False
    
    # Step 3: Test connection
    print("\n3. Testing database connection...")
    if db.test_connection():
        print("   ✓ Database connection successful")
    else:
        print("   ❌ Database connection failed")
        return False
    
    # Step 4: Check table existence and get stats
    print("\n4. Checking table statistics...")
    try:
        stats = db.get_table_stats()
        for table, count in stats.items():
            print(f"   📊 {table}: {count:,} records")
    except Exception as e:
        print(f"   ⚠️ Error getting stats (tables might not exist yet): {e}")
    
    # Step 5: Test a simple query
    print("\n5. Testing sample query...")
    try:
        # Test getting invoices (should work even if table is empty)
        invoices_df = db.get_invoices()
        print(f"   ✓ Retrieved {len(invoices_df)} invoices")
        
        if len(invoices_df) > 0:
            print("   📋 Sample invoice columns:")
            for col in invoices_df.columns[:5]:  # Show first 5 columns
                print(f"      - {col}")
        
    except Exception as e:
        print(f"   ⚠️ Error testing query: {e}")
    
    print("\n✅ Database setup test completed!")
    return True

def demonstrate_data_operations():
    """Demonstrate basic database operations"""
    
    print("\n🧪 DEMONSTRATING DATABASE OPERATIONS")
    print("=" * 50)
    
    try:
        db = DatabaseManager()
        
        # Example: Insert a test invoice
        print("\n1. Inserting test invoice...")
        test_invoice = {
            'invoice_no': 'TEST-001',
            'emid': 'TEST-EMID',
            'service_area': 'Test Area',
            'invoice_total': 1000.00
        }
        
        result = db.upsert_invoices([test_invoice])
        print(f"   ✓ Result: {result}")
        
        # Example: Search for the test invoice
        print("\n2. Searching for test invoice...")
        search_results = db.search_invoices('TEST-001')
        print(f"   ✓ Found {len(search_results)} matching invoices")
        
        # Example: Get filtered invoices
        print("\n3. Getting filtered invoices...")
        filtered_invoices = db.get_invoices(filters={'service_area': 'Test Area'})
        print(f"   ✓ Found {len(filtered_invoices)} invoices in Test Area")
        
        print("\n✅ Database operations demonstration completed!")
        
    except Exception as e:
        print(f"   ❌ Error during demonstration: {e}")

if __name__ == "__main__":
    # Run the tests
    success = test_database_setup()
    
    if success:
        demonstrate_data_operations()
    
    print("\n🎯 NEXT STEPS:")
    print("1. If tests passed: Your database connection is ready!")
    print("2. If tests failed: Check your .env file and database credentials")
    print("3. Ready to move to data mapping and Streamlit integration")
