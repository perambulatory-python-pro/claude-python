"""
Test Schema-Compatible BCI Mapping
Ensures mapping only uses columns that exist in your database
"""

import pandas as pd
from data_mapper_enhanced import EnhancedDataMapper

def test_schema_compatible_mapping():
    """Test the schema-compatible BCI mapping"""
    print("🔧 TESTING SCHEMA-COMPATIBLE BCI MAPPING")
    print("=" * 45)
    
    try:
        # Read the BCI file
        print("📁 Reading TLM_BCI.xlsx...")
        df = pd.read_excel("TLM_BCI.xlsx")
        print(f"   ✅ File loaded: {len(df)} rows")
        
        # Test mapping
        print("\n🔄 Testing schema-compatible mapper...")
        mapper = EnhancedDataMapper()
        mapped_data = mapper.map_bci_details(df)
        
        print(f"   ✅ Mapping successful!")
        print(f"   📊 Records mapped: {len(mapped_data)}")
        
        if len(mapped_data) > 0:
            # Check that only valid schema fields are present
            sample_record = mapped_data[0]
            
            # Your actual database schema fields
            valid_schema_fields = {
                'id', 'hours_total', 'rate_regular', 'rate_overtime', 'rate_holiday',
                'amount_regular', 'amount_overtime', 'amount_holiday', 'amount_total',
                'shift_in', 'shift_out', 'pay_rate', 'in_time', 'out_time', 'lunch_hours',
                'created_at', 'updated_at', 'work_date', 'hours_regular', 'hours_overtime',
                'hours_holiday', 'invoice_no', 'source_system', 'bill_category',
                'employee_id', 'employee_name', 'location_code', 'location_name',
                'building_code', 'emid', 'position_code', 'position_description',
                'job_number', 'po', 'customer_number', 'customer_name', 'business_unit'
            }
            
            # Check for invalid fields
            print(f"\n🔍 Schema Compatibility Check:")
            invalid_fields = []
            valid_fields = []
            
            for field in sample_record.keys():
                if field in valid_schema_fields:
                    valid_fields.append(field)
                else:
                    invalid_fields.append(field)
            
            print(f"   ✅ Valid schema fields: {len(valid_fields)}")
            for field in sorted(valid_fields):
                print(f"      - {field}")
            
            if invalid_fields:
                print(f"   ❌ Invalid schema fields: {len(invalid_fields)}")
                for field in invalid_fields:
                    print(f"      - {field} (NOT IN SCHEMA)")
            else:
                print(f"   ✅ All fields are schema-compatible!")
            
            # Show sample data
            print(f"\n📋 Sample Mapped Record:")
            key_fields = [
                'invoice_no', 'employee_id', 'employee_name', 'work_date',
                'hours_regular', 'amount_total', 'source_system'
            ]
            
            for field in key_fields:
                value = sample_record.get(field)
                print(f"   {field}: {value}")
            
            # Data quality check
            print(f"\n📊 Data Quality:")
            has_invoice = sum(1 for r in mapped_data if r.get('invoice_no'))
            has_employee = sum(1 for r in mapped_data if r.get('employee_id'))
            has_name = sum(1 for r in mapped_data if r.get('employee_name'))
            
            print(f"   - Records with invoice_no: {has_invoice:,}")
            print(f"   - Records with employee_id: {has_employee:,}")
            print(f"   - Records with employee_name: {has_name:,}")
            
            if len(invalid_fields) == 0:
                print(f"\n🎉 SUCCESS!")
                print(f"✅ Schema-compatible mapping is working perfectly!")
                print(f"🚀 Ready for database insertion!")
                return True
            else:
                print(f"\n⚠️ Schema compatibility issues found")
                return False
        
    except Exception as e:
        print(f"❌ Error testing mapping: {e}")
        import traceback
        print(f"📋 Full error:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = test_schema_compatible_mapping()
    
    if success:
        print(f"\n🔄 NEXT STEPS:")
        print(f"1. Restart your Streamlit app")
        print(f"2. Upload TLM_BCI.xlsx again")
        print(f"3. It should now process without schema errors!")
    else:
        print(f"\n🔧 Issues found - check the errors above")
