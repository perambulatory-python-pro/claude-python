"""
Test the updated BCI mapping with your actual file structure
"""

import pandas as pd
from data_mapper_enhanced import EnhancedDataMapper

def test_updated_bci_mapping():
    """Test the updated BCI mapping with actual TLM_BCI.xlsx structure"""
    print("🧪 TESTING UPDATED BCI MAPPING")
    print("=" * 40)
    
    try:
        # Read the actual file
        print("📁 Reading TLM_BCI.xlsx...")
        df = pd.read_excel("TLM_BCI.xlsx")
        print(f"   ✅ File loaded: {len(df)} rows")
        
        # Test with updated mapper
        print("\n🔄 Testing updated BCI mapper...")
        mapper = EnhancedDataMapper()
        mapped_data = mapper.map_bci_details(df)
        
        print(f"   ✅ Mapping successful!")
        print(f"   📊 Records mapped: {len(mapped_data)}")
        
        if len(mapped_data) > 0:
            print(f"\n📋 Sample mapped record:")
            sample = mapped_data[0]
            
            # Show key fields
            key_fields = [
                'invoice_no', 'employee_id', 'employee_name', 'work_date',
                'hours_regular', 'hours_overtime', 'hours_holiday', 'hours_total',
                'rate_regular', 'amount_regular', 'amount_total',
                'location_code', 'location_name', 'customer_name'
            ]
            
            for field in key_fields:
                value = sample.get(field)
                print(f"   {field}: {value}")
            
            # Check data quality
            print(f"\n📊 Data Quality Check:")
            
            # Count records with key fields
            invoice_count = sum(1 for r in mapped_data if r.get('invoice_no'))
            employee_count = sum(1 for r in mapped_data if r.get('employee_id'))
            hours_count = sum(1 for r in mapped_data if r.get('hours_total') and r['hours_total'] > 0)
            amount_count = sum(1 for r in mapped_data if r.get('amount_total') and r['amount_total'] > 0)
            
            print(f"   - Records with Invoice No: {invoice_count:,}")
            print(f"   - Records with Employee ID: {employee_count:,}")
            print(f"   - Records with Hours: {hours_count:,}")
            print(f"   - Records with Amount: {amount_count:,}")
            
            # Show unique invoice numbers (sample)
            invoice_nos = list(set(r.get('invoice_no') for r in mapped_data[:100] if r.get('invoice_no')))[:5]
            print(f"   - Sample Invoice Numbers: {invoice_nos}")
            
            print(f"\n✅ BCI mapping is now working correctly!")
            print(f"🚀 Ready to process your BCI file through Streamlit!")
            
        else:
            print(f"\n❌ No records were mapped - check data quality")
            
        return len(mapped_data) > 0
        
    except Exception as e:
        print(f"❌ Error testing mapping: {e}")
        import traceback
        print(f"📋 Full error:\n{traceback.format_exc()}")
        return False

def compare_original_vs_mapped():
    """Compare a few original records with mapped versions"""
    print(f"\n🔍 COMPARISON: ORIGINAL vs MAPPED")
    print("=" * 40)
    
    try:
        df = pd.read_excel("TLM_BCI.xlsx")
        mapper = EnhancedDataMapper()
        mapped_data = mapper.map_bci_details(df)
        
        if len(mapped_data) > 0:
            # Show first original record
            print(f"📋 Original Record (Row 1):")
            original_row = df.iloc[0]
            for col in ['Invoice_No', 'Emp_No', 'Employee_First_Name', 'Employee_Last_Name', 
                       'Date', 'Billed_Regular_Hours', 'Billed_Total_Wages']:
                if col in original_row.index:
                    print(f"   {col}: {original_row[col]}")
            
            print(f"\n📋 Mapped Record:")
            mapped_row = mapped_data[0]
            for field in ['invoice_no', 'employee_id', 'employee_name', 'work_date', 
                         'hours_regular', 'amount_total']:
                print(f"   {field}: {mapped_row.get(field)}")
        
    except Exception as e:
        print(f"❌ Error in comparison: {e}")

if __name__ == "__main__":
    success = test_updated_bci_mapping()
    
    if success:
        compare_original_vs_mapped()
        
        print(f"\n🎉 SUCCESS!")
        print(f"Your BCI file is now ready to process!")
        print(f"\n🚀 Next steps:")
        print(f"1. Go back to your Streamlit app")
        print(f"2. Upload TLM_BCI.xlsx again")
        print(f"3. It should now process successfully!")
    else:
        print(f"\n❌ Issues remain - check the errors above")
