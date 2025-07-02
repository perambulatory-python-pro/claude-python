"""
Detailed BCI Data Analysis
Find out exactly what's happening with the invoice mapping and validation
"""

import pandas as pd
from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper
from database.database_manager_compatible import CompatibleEnhancedDatabaseManager

def analyze_bci_processing_issue():
    """
    Detailed analysis of why 26K records showed 0 inserted
    """
    print("🔍 DETAILED BCI PROCESSING ANALYSIS")
    print("=" * 60)
    
    try:
        # Step 1: Load and examine the raw BCI file
        print("\n1. Loading BCI file...")
        bci_df = pd.read_excel("TLM_BCI.xlsx")
        print(f"   📊 Raw file: {len(bci_df):,} rows")
        print(f"   📋 Columns: {list(bci_df.columns)}")
        
        # Check invoice number column specifically
        if 'Invoice_No' in bci_df.columns:
            unique_invoices_raw = bci_df['Invoice_No'].nunique()
            invoice_list_raw = bci_df['Invoice_No'].unique()
            print(f"   📋 Unique invoice numbers in raw data: {unique_invoices_raw}")
            print(f"   📋 Invoice numbers: {invoice_list_raw[:10]}...")  # Show first 10
        else:
            print("   ❌ Invoice_No column not found in raw data!")
            print(f"   📋 Available columns: {bci_df.columns.tolist()}")
            return False
        
        # Step 2: Test the data mapping
        print(f"\n2. Testing data mapping...")
        mapper = EnhancedDataMapper()
        mapped_data = mapper.map_bci_details(bci_df)
        print(f"   📊 Mapped records: {len(mapped_data):,}")
        
        if len(mapped_data) > 0:
            # Analyze invoice numbers in mapped data
            mapped_invoices = [record.get('invoice_no') for record in mapped_data if record.get('invoice_no')]
            unique_mapped_invoices = len(set(mapped_invoices))
            print(f"   📋 Records with invoice numbers: {len(mapped_invoices):,}")
            print(f"   📋 Unique invoice numbers after mapping: {unique_mapped_invoices}")
            
            # Show sample mapped record
            print(f"\n   🔍 Sample mapped record:")
            sample = mapped_data[0]
            for key, value in sample.items():
                print(f"      {key}: {value}")
            
            # Count records per invoice
            invoice_counts = {}
            for record in mapped_data:
                inv_no = record.get('invoice_no')
                if inv_no:
                    invoice_counts[inv_no] = invoice_counts.get(inv_no, 0) + 1
            
            print(f"\n   📊 Records per invoice number:")
            for inv_no, count in sorted(invoice_counts.items()):
                print(f"      {inv_no}: {count:,} records")
        else:
            print("   ❌ No records were mapped!")
            return False
        
        # Step 3: Check database for existing invoices
        print(f"\n3. Checking database for existing invoices...")
        db = CompatibleEnhancedDatabaseManager()
        existing_invoices_df = db.get_invoices()
        print(f"   📊 Invoices in database: {len(existing_invoices_df):,}")
        
        if len(existing_invoices_df) > 0:
            existing_invoices = set(existing_invoices_df['invoice_no'].tolist())
            print(f"   📋 Sample existing invoices: {list(existing_invoices)[:5]}")
            
            # Check which BCI invoices exist in database
            bci_invoices = set(invoice_counts.keys())
            valid_invoices = bci_invoices & existing_invoices
            missing_invoices = bci_invoices - existing_invoices
            
            print(f"\n   📊 Validation Results:")
            print(f"      BCI invoice numbers: {len(bci_invoices)}")
            print(f"      Valid (in database): {len(valid_invoices)}")
            print(f"      Missing (not in database): {len(missing_invoices)}")
            
            if valid_invoices:
                print(f"      ✅ Valid invoices: {valid_invoices}")
                # Count how many records SHOULD be inserted
                should_insert = sum(invoice_counts[inv] for inv in valid_invoices)
                print(f"      ✅ Records that SHOULD be inserted: {should_insert:,}")
            
            if missing_invoices:
                print(f"      ❌ Missing invoices: {missing_invoices}")
                should_skip = sum(invoice_counts[inv] for inv in missing_invoices)
                print(f"      ❌ Records that SHOULD be skipped: {should_skip:,}")
        else:
            print("   ❌ No invoices found in database!")
            print("   💡 This explains why ALL records were skipped")
        
        # Step 4: Manual validation test
        print(f"\n4. Manual validation test...")
        
        # Simulate the validation logic manually
        records_to_insert = []
        records_to_skip = []
        
        if len(existing_invoices_df) > 0:
            existing_invoices = set(existing_invoices_df['invoice_no'].tolist())
            
            for record in mapped_data:
                invoice_no = record.get('invoice_no')
                
                if not invoice_no:
                    records_to_skip.append(record)
                elif invoice_no not in existing_invoices:
                    records_to_skip.append(record)
                else:
                    records_to_insert.append(record)
        else:
            # No invoices in database, so all records would be skipped
            records_to_skip = mapped_data
        
        print(f"   📊 Manual validation results:")
        print(f"      Records to insert: {len(records_to_insert):,}")
        print(f"      Records to skip: {len(records_to_skip):,}")
        print(f"      Total: {len(records_to_insert) + len(records_to_skip):,}")
        
        # Step 5: Conclusion
        print(f"\n5. CONCLUSION:")
        if len(existing_invoices_df) == 0:
            print(f"   🎯 ROOT CAUSE: No master invoices in database")
            print(f"   💡 ALL {len(mapped_data):,} BCI records are being skipped")
            print(f"   💡 because NO master invoices exist to validate against")
        elif len(records_to_insert) == 0:
            print(f"   🎯 ROOT CAUSE: None of the BCI invoice numbers exist in master database")
            print(f"   💡 ALL {len(mapped_data):,} BCI records are being skipped")
            print(f"   💡 because NONE of the required master invoices exist")
        else:
            print(f"   🎯 EXPECTED BEHAVIOR:")
            print(f"   💡 {len(records_to_insert):,} records should be inserted")
            print(f"   💡 {len(records_to_skip):,} records should be skipped")
            print(f"   ⚠️ If actual results differ, there's a bug in the validation logic")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error in analysis: {e}")
        import traceback
        print(f"   📋 Error details: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = analyze_bci_processing_issue()
    
    if success:
        print(f"\n🚀 NEXT STEPS:")
        print(f"1. If no master invoices exist: Upload master invoice files first")
        print(f"2. If master invoices exist but don't match BCI: Check invoice number formats")
        print(f"3. If everything looks correct: There may be a bug in the validation logic")
        print(f"\n💡 The validation system is working as designed - it's protecting data integrity!")
    else:
        print(f"\n🔧 Fix the issues above and re-run the analysis")