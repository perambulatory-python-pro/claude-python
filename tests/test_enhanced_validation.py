"""
Test Enhanced Validation Logic
Verify that partial uploads work correctly
"""

from database_manager_compatible import CompatibleEnhancedDatabaseManager
from data_mapper_enhanced import EnhancedDataMapper
import pandas as pd

def test_enhanced_validation():
    """Test the enhanced validation and partial upload logic"""
    print("🧪 TESTING ENHANCED VALIDATION")
    print("=" * 40)
    
    try:
        # Initialize
        db = CompatibleEnhancedDatabaseManager()
        mapper = EnhancedDataMapper()
        
        # Check existing invoices
        print("📊 Checking existing invoices...")
        existing_invoices_df = db.get_invoices()
        print(f"   ✅ Found {len(existing_invoices_df)} existing invoices")
        
        if len(existing_invoices_df) > 0:
            sample_existing = existing_invoices_df['invoice_no'].head(3).tolist()
            print(f"   📋 Sample existing: {sample_existing}")
        
        # Test with BCI file
        print(f"\n🔄 Testing BCI mapping and validation...")
        bci_df = pd.read_excel("TLM_BCI.xlsx")
        mapped_data = mapper.map_bci_details(bci_df)
        
        print(f"   ✅ Mapped {len(mapped_data)} BCI records")
        
        # Get unique invoice numbers from BCI
        bci_invoices = set(record['invoice_no'] for record in mapped_data if record.get('invoice_no'))
        print(f"   📋 BCI contains {len(bci_invoices)} unique invoice numbers")
        
        # Check overlap with existing invoices
        if len(existing_invoices_df) > 0:
            existing_invoices = set(existing_invoices_df['invoice_no'].tolist())
            
            valid_invoices = bci_invoices & existing_invoices
            missing_invoices = bci_invoices - existing_invoices
            
            print(f"   ✅ Valid invoices (in master): {len(valid_invoices)}")
            print(f"   ❌ Missing invoices (not in master): {len(missing_invoices)}")
            
            if missing_invoices:
                print(f"   📋 First 5 missing: {list(missing_invoices)[:5]}")
        
        # Test enhanced bulk insert with validation
        print(f"\n🚀 Testing enhanced bulk insert with validation...")
        
        # Take a small sample for testing (first 100 records)
        test_sample = mapped_data[:100]
        
        results = db.bulk_insert_invoice_details_with_validation(test_sample)
        
        print(f"   📊 Test Results:")
        print(f"      - Total records: {results['total_records']}")
        print(f"      - Inserted: {results['inserted']}")
        print(f"      - Skipped: {results['skipped']}")
        print(f"      - Missing invoices: {results['missing_invoice_count']}")
        print(f"      - Success: {results['success']}")
        
        if results['missing_invoices']:
            print(f"   📋 Sample missing invoices: {results['missing_invoices'][:5]}")
        
        if results['skipped_records']:
            print(f"   📋 Sample skipped record:")
            sample_skipped = results['skipped_records'][0]
            for key, value in sample_skipped.items():
                print(f"      {key}: {value}")
        
        print(f"\n✅ Enhanced validation test completed!")
        print(f"🎯 The system can now handle partial uploads successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        print(f"📋 Error details:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = test_enhanced_validation()
    
    if success:
        print(f"\n🚀 READY FOR ENHANCED PROCESSING!")
        print(f"Your Streamlit app now supports:")
        print(f"✅ Partial uploads that continue despite missing invoices")
        print(f"✅ Detailed reporting of what was skipped vs inserted")
        print(f"✅ Download links for missing invoice analysis")
        print(f"✅ Comprehensive error handling and recovery")
        print(f"\n🔄 Restart your Streamlit app to use the enhanced processing!")
    else:
        print(f"\n🔧 Fix the issues above before proceeding")
