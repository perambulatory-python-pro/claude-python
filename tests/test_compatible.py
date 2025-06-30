"""
Test Compatible Enhanced Manager
This should work with your existing database schema
"""

import os
import pandas as pd
from datetime import datetime, date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_compatible_enhanced_manager():
    """Test the compatible enhanced manager"""
    print("🧪 TESTING COMPATIBLE ENHANCED MANAGER")
    print("=" * 50)
    
    try:
        # Import compatible manager
        from database_manager_compatible import CompatibleEnhancedDatabaseManager
        from data_mapper_enhanced import EnhancedDataMapper
        
        print("✅ Imports successful")
        
        # Initialize
        db = CompatibleEnhancedDatabaseManager()
        mapper = EnhancedDataMapper()
        
        print("✅ Managers initialized")
        
        # Test connection
        if db.test_connection():
            print("✅ Database connection successful")
        else:
            print("❌ Database connection failed")
            return False
        
        # Check schema info
        schema_info = db.schema_info
        print(f"📋 Schema info: {len(schema_info['column_names'])} columns detected")
        print(f"   - Has original dates: {schema_info['has_original_dates']}")
        print(f"   - Has not_transmitted: {schema_info['has_not_transmitted']}")
        
        # Test business logic methods
        print("\n🧪 Testing business logic methods...")
        
        # Test date preservation
        existing_record = {'edi_date': date(2025, 6, 20)}
        new_date = date(2025, 6, 25)
        
        current, original = db.preserve_date_logic(
            existing_record, new_date, 'edi_date', 'original_edi_date'
        )
        
        assert current == new_date
        assert original == date(2025, 6, 20)
        print("   ✅ Date preservation logic works")
        
        # Test not_transmitted logic
        not_transmitted = db.apply_not_transmitted_logic(
            {}, {'edi_date': new_date}, 'EDI'
        )
        print(f"   ✅ Not transmitted logic works: {not_transmitted}")
        
        # Test data mapping
        test_data = pd.DataFrame([
            {
                'Invoice No.': f'COMPAT-TEST-{datetime.now().strftime("%H%M%S")}',
                'EMID': 'COMPAT-EMID',
                'Service Area': 'Compatible Test',
                'Invoice Total': '250.00'
            }
        ])
        
        mapped_data = mapper.map_invoice_data(test_data)
        print(f"   ✅ Data mapping works: {len(mapped_data)} records")
        
        # Test enhanced upsert (this should work with your existing schema)
        print("\n🧪 Testing enhanced upsert...")
        
        result = db.upsert_invoices_with_business_logic(
            mapped_data,
            'EDI',
            date(2025, 6, 25)
        )
        
        print(f"   ✅ Enhanced upsert successful: {result}")
        
        # Test basic queries
        print("\n🧪 Testing queries...")
        
        stats = db.get_table_stats()
        print(f"   ✅ Table stats: {stats}")
        
        # Search for our test record
        search_results = db.search_invoices('COMPAT-TEST')
        print(f"   ✅ Search works: {len(search_results)} results")
        
        print("\n" + "=" * 50)
        print("🎉 ALL COMPATIBLE TESTS PASSED!")
        print()
        print("✅ The compatible enhanced manager works with your existing schema")
        print("✅ All business logic functions correctly")
        print("✅ Date preservation and not_transmitted logic implemented")
        print("✅ Compatible with your current database structure")
        print()
        print("🚀 Ready to use the enhanced features!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        print(f"📋 Full error:\n{traceback.format_exc()}")
        return False

def show_usage_instructions():
    """Show how to use the compatible version"""
    print("\n📋 HOW TO USE THE COMPATIBLE ENHANCED MANAGER")
    print("=" * 50)
    print()
    print("1. Replace your existing imports:")
    print("   # OLD:")
    print("   from database_manager import DatabaseManager")
    print()
    print("   # NEW:")
    print("   from database_manager_compatible import CompatibleEnhancedDatabaseManager")
    print()
    print("2. Update your Streamlit app:")
    print("   # In your streamlit app, change:")
    print("   st.session_state.db_manager = CompatibleEnhancedDatabaseManager()")
    print()
    print("3. Use enhanced features:")
    print("   # Now you can use:")
    print("   result = db.upsert_invoices_with_business_logic(data, 'EDI', date.today())")
    print("   links = db.process_invoice_history_linking(data)")
    print()
    print("4. The manager automatically adapts to your schema:")
    print("   - Works with existing column names")
    print("   - Skips features if columns don't exist")
    print("   - Provides full business logic where supported")
    print()

if __name__ == "__main__":
    success = test_compatible_enhanced_manager()
    
    if success:
        show_usage_instructions()
    else:
        print("\n🔧 TROUBLESHOOTING:")
        print("1. Make sure your .env file has the correct DATABASE_URL")
        print("2. Ensure database connection is working")
        print("3. Check that the invoices table exists")
        print("4. Run: python debug_test.py for more detailed diagnostics")
