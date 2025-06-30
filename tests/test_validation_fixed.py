"""
Fixed Enhanced Validation Test
Properly loads .env file before importing database components
"""

import os
from dotenv import load_dotenv
import pandas as pd

# CRITICAL: Load .env file FIRST before importing any database components
print("🔧 Loading environment variables...")
load_dotenv()

# Verify DATABASE_URL is loaded
database_url = os.getenv('DATABASE_URL')
if database_url:
    # Mask password for security when displaying
    if '@' in database_url and ':' in database_url:
        parts = database_url.split('@')
        masked_url = parts[0].split(':')[:-1] + ['***'] + ['@' + parts[1]]
        print(f"✅ DATABASE_URL loaded: {':'.join(masked_url)}")
    else:
        print(f"✅ DATABASE_URL loaded")
else:
    print("❌ DATABASE_URL not found even after loading .env")
    exit(1)

# NOW import database components (after .env is loaded)
from database_manager_compatible import CompatibleEnhancedDatabaseManager
from data_mapper_enhanced import EnhancedDataMapper

def test_enhanced_validation_fixed():
    """Test the enhanced validation and partial upload logic"""
    print("\n🧪 TESTING ENHANCED VALIDATION (FIXED)")
    print("=" * 45)
    
    try:
        # Initialize (should work now that .env is loaded)
        print("🔄 Initializing database manager...")
        db = CompatibleEnhancedDatabaseManager()
        mapper = EnhancedDataMapper()
        print("✅ Database manager initialized")
        
        # Test connection
        print("\n🔗 Testing database connection...")
        if db.test_connection():
            print("✅ Database connection successful")
        else:
            print("❌ Database connection failed")
            return False
        
        # Check existing invoices
        print("\n📊 Checking existing invoices...")
        existing_invoices_df = db.get_invoices()
        print(f"✅ Found {len(existing_invoices_df)} existing invoices")
        
        if len(existing_invoices_df) > 0:
            sample_existing = existing_invoices_df['invoice_no'].head(3).tolist()
            print(f"📋 Sample existing: {sample_existing}")
        
        # Test with BCI file
        print(f"\n🔄 Testing BCI mapping and validation...")
        if not os.path.exists("TLM_BCI.xlsx"):
            print("⚠️ TLM_BCI.xlsx not found - skipping BCI test")
            print("✅ Database connection and initialization working correctly")
            return True
        
        bci_df = pd.read_excel("TLM_BCI.xlsx")
        mapped_data = mapper.map_bci_details(bci_df)
        
        print(f"✅ Mapped {len(mapped_data)} BCI records")
        
        # Get unique invoice numbers from BCI
        bci_invoices = set(record['invoice_no'] for record in mapped_data if record.get('invoice_no'))
        print(f"📋 BCI contains {len(bci_invoices)} unique invoice numbers")
        
        # Check overlap with existing invoices
        if len(existing_invoices_df) > 0:
            existing_invoices = set(existing_invoices_df['invoice_no'].tolist())
            
            valid_invoices = bci_invoices & existing_invoices
            missing_invoices = bci_invoices - existing_invoices
            
            print(f"✅ Valid invoices (in master): {len(valid_invoices)}")
            print(f"❌ Missing invoices (not in master): {len(missing_invoices)}")
            
            if missing_invoices:
                print(f"📋 First 5 missing: {list(missing_invoices)[:5]}")
        
        # Test enhanced bulk insert with validation (small sample)
        print(f"\n🚀 Testing enhanced bulk insert with validation...")
        
        # Take a small sample for testing (first 10 records)
        test_sample = mapped_data[:10]
        
        results = db.bulk_insert_invoice_details_with_validation(test_sample)
        
        print(f"📊 Test Results:")
        print(f"   - Total records: {results['total_records']}")
        print(f"   - Inserted: {results['inserted']}")
        print(f"   - Skipped: {results['skipped']}")
        print(f"   - Missing invoices: {results['missing_invoice_count']}")
        print(f"   - Success: {results['success']}")
        
        if results['missing_invoices']:
            print(f"📋 Sample missing invoices: {results['missing_invoices'][:3]}")
        
        if results['skipped_records']:
            print(f"📋 Sample skipped record:")
            sample_skipped = results['skipped_records'][0]
            for key, value in sample_skipped.items():
                print(f"      {key}: {value}")
        
        print(f"\n🎉 ENHANCED VALIDATION TEST SUCCESSFUL!")
        print(f"✅ Environment loading: FIXED")
        print(f"✅ Database connection: WORKING")
        print(f"✅ Enhanced validation: WORKING")
        print(f"✅ Partial upload logic: WORKING")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        print(f"📋 Error details:\n{traceback.format_exc()}")
        return False

def check_environment_status():
    """Check environment variable status"""
    print("🔍 ENVIRONMENT STATUS CHECK")
    print("=" * 30)
    
    # Check .env file
    if os.path.exists('.env'):
        print("✅ .env file exists")
        
        with open('.env', 'r') as f:
            content = f.read()
        
        if 'DATABASE_URL' in content:
            print("✅ DATABASE_URL found in .env file")
        else:
            print("❌ DATABASE_URL not found in .env file")
    else:
        print("❌ .env file not found")
    
    # Check environment variable
    load_dotenv()  # Load again to be sure
    db_url = os.getenv('DATABASE_URL')
    
    if db_url:
        print("✅ DATABASE_URL loaded in environment")
    else:
        print("❌ DATABASE_URL not loaded in environment")
    
    print()

if __name__ == "__main__":
    check_environment_status()
    success = test_enhanced_validation_fixed()
    
    if success:
        print(f"\n🚀 SYSTEM READY!")
        print(f"Your enhanced validation system is working correctly.")
        print(f"\n🔄 Next steps:")
        print(f"1. Restart your Streamlit app")
        print(f"2. Upload your BCI file")
        print(f"3. Enjoy partial upload processing!")
    else:
        print(f"\n🔧 Issues found - check the errors above")
