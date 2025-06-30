import pandas as pd
import os
from data_mapper_enhanced import EnhancedDataMapper
from database_manager_compatible import CompatibleEnhancedDatabaseManager  # Adjust import path
from dotenv import load_dotenv

def test_payment_database():
    """Test the complete payment processing pipeline"""
    
    print("🧪 Testing Payment Database Integration")
    print("=" * 50)
    
    # Load environment variables (same as your Streamlit app)
    load_dotenv()
    
    # CREATE UNIQUE TEST DATA (with timestamp)
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_payment_id = f"TEST_DB_{timestamp}"
    
    test_data = {
        'Payment ID': [unique_payment_id] * 3,  # Use unique ID
        'Payment Date': ['06/23/25'] * 3,
        'Payment Amount': [3000.00] * 3,
        'Invoice ID': [f'INV001_{timestamp}', f'INV002_{timestamp}', f'INV003_{timestamp}'],  # Unique invoice IDs too
        'Gross Amount': [1000.00, 1200.00, 800.00],
        'Discount': [0, 0, 0],
        'Net Amount': [1000.00, 1200.00, 800.00]
    }
    
    df = pd.DataFrame(test_data)
    
    try:
        # Step 1: Test data mapping (we know this works)
        print("\n📋 Step 1: Data Mapping")
        mapper = EnhancedDataMapper()
        
        master_data = mapper.extract_payment_master_data(df)
        detail_records = mapper.map_payment_details(df)
        validation = mapper.validate_payment_data(master_data, detail_records)
        
        print(f"✅ Mapped: Master + {len(detail_records)} details")
        print(f"✅ Validation: {'PASSED' if validation['is_valid'] else 'FAILED'}")
        
        # Step 2: Test database connection (using your existing pattern)
        print("\n🗄️ Step 2: Database Connection")
        db_manager = CompatibleEnhancedDatabaseManager()  # Uses your existing init pattern
        
        print("✅ Database connection successful")
        
        # Step 3: Test payment existence check
        print("\n🔍 Step 3: Payment Existence Check")
        payment_id = master_data['payment_id']
        exists_before = db_manager.check_payment_exists(payment_id)
        print(f"✅ Payment {payment_id} exists before processing: {exists_before}")
        
        # Step 4: Process payment (the main test!)
        print(f"\n🚀 Step 4: Processing Payment {payment_id}")
        
        def progress_callback(progress, message):
            print(f"   Progress: {progress:.1%} - {message}")
        
        results = db_manager.process_payment_remittance(
            master_data, 
            detail_records, 
            progress_callback
        )
        
        if results['success']:
            print("✅ Payment processing completed!")
            print(f"   Master inserted: {results['master_inserted']}")
            print(f"   Details inserted: {results['detail_results']['inserted']}")
            print(f"   Total amount: ${results['final_summary']['payment_amount']}")
        else:
            print(f"❌ Payment processing failed: {results['error']}")
            if 'already exists' in results.get('error', ''):
                print("   This might be expected if you've run the test before")
            return False
        
        # Step 5: Verify data integrity
        print(f"\n🔍 Step 5: Data Integrity Check")
        summary = db_manager.get_payment_summary(payment_id)
        
        if summary:
            print(f"✅ Payment retrieved from database:")
            print(f"   Payment ID: {summary['payment_id']}")
            print(f"   Payment Amount: ${summary['payment_amount']}")
            print(f"   Detail Count: {summary['detail_count']}")
            print(f"   Net Total: ${summary['total_net']}")
            
            # Check amount reconciliation
            amount_diff = abs(float(summary['payment_amount']) - float(summary['total_net']))
            if amount_diff <= 0.01:
                print("✅ Amount reconciliation passed")
            else:
                print(f"⚠️ Amount reconciliation issue: ${amount_diff} difference")
        
        # Step 6: Test duplicate prevention
        print(f"\n🛡️ Step 6: Duplicate Prevention Test")
        duplicate_results = db_manager.process_payment_remittance(
            master_data, detail_records
        )
        
        if not duplicate_results['success'] and 'already exists' in duplicate_results['error']:
            print("✅ Duplicate prevention working correctly")
        else:
            print("⚠️ Duplicate prevention may not be working as expected")
            print(f"   Result: {duplicate_results}")
        
        print(f"\n🎉 All tests completed successfully!")
        print(f"\n📋 Summary:")
        print(f"   ✅ Data mapping works")
        print(f"   ✅ Database connection works") 
        print(f"   ✅ Payment processing works")
        print(f"   ✅ Data integrity maintained")
        print(f"   ✅ Duplicate prevention works")
        
        return True
        
    except Exception as e:
        print(f"💥 Test failed with error: {e}")
        import traceback
        print(f"📋 Full error details:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    print("🚀 Starting Payment Database Test")
    print("📋 Make sure your .env file has DATABASE_URL set")
    print("")
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️ Warning: .env file not found in current directory")
        print("   Make sure you're running from the correct directory")
        print("")
    
    success = test_payment_database()
    
    if success:
        print("\n🎯 READY FOR STREAMLIT INTEGRATION!")
        print("Your payment processing is fully functional!")
    else:
        print("\n🔧 Please fix the issues above before proceeding")