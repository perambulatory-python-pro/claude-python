"""
Duplicate Records Analyzer
Investigate what constitutes "duplicates" in your BCI data
"""

import pandas as pd
from data_mapper_enhanced import EnhancedDataMapper
from database_manager_compatible import CompatibleEnhancedDatabaseManager

def analyze_duplicate_patterns():
    """
    Analyze patterns in the detected duplicate records
    """
    print("🔍 ANALYZING DUPLICATE RECORD PATTERNS")
    print("=" * 60)
    
    try:
        # Load and map BCI data
        print("\n1. Loading BCI data...")
        bci_df = pd.read_excel("TLM_BCI.xlsx")
        mapper = EnhancedDataMapper()
        mapped_data = mapper.map_bci_details(bci_df)
        
        print(f"   📊 Total mapped records: {len(mapped_data):,}")
        
        # Analyze duplicates by our current logic (invoice_no + employee_id + work_date)
        print(f"\n2. Analyzing current duplicate detection logic...")
        
        duplicate_keys = {}
        all_records_by_key = {}
        
        for i, record in enumerate(mapped_data):
            # Create the key we use for duplicate detection
            key = (
                record.get('invoice_no'),
                record.get('employee_id'), 
                record.get('work_date')
            )
            
            if key not in all_records_by_key:
                all_records_by_key[key] = []
            all_records_by_key[key].append((i, record))
            
            # Track keys that appear more than once
            if key in duplicate_keys:
                duplicate_keys[key] += 1
            else:
                duplicate_keys[key] = 1
        
        # Find actual duplicates (keys with count > 1)
        true_duplicates = {k: v for k, v in duplicate_keys.items() if v > 1}
        
        print(f"   📋 Unique employee/date combinations: {len(all_records_by_key):,}")
        print(f"   📋 Combinations with multiple records: {len(true_duplicates):,}")
        print(f"   📋 Total 'duplicate' records: {sum(true_duplicates.values()) - len(true_duplicates):,}")
        
        # Analyze the nature of these duplicates
        if true_duplicates:
            print(f"\n3. Analyzing duplicate record differences...")
            
            # Look at first few duplicate groups
            sample_duplicates = list(true_duplicates.items())[:5]
            
            for i, (key, count) in enumerate(sample_duplicates):
                invoice_no, employee_id, work_date = key
                records_for_key = all_records_by_key[key]
                
                print(f"\n   🔍 Duplicate Group {i+1}:")
                print(f"      Invoice: {invoice_no}, Employee: {employee_id}, Date: {work_date}")
                print(f"      Number of records: {count}")
                
                # Compare the records in this group
                print(f"      Record differences:")
                
                for j, (record_idx, record) in enumerate(records_for_key):
                    print(f"         Record {j+1} (row {record_idx}):")
                    
                    # Show key differentiating fields
                    fields_to_check = [
                        'hours_regular', 'hours_overtime', 'hours_total',
                        'rate_regular', 'amount_total', 'position_code',
                        'location_code', 'bill_category', 'shift_in', 'shift_out'
                    ]
                    
                    for field in fields_to_check:
                        value = record.get(field)
                        if value is not None:
                            print(f"            {field}: {value}")
                
                # Check if records are truly identical
                if len(records_for_key) == 2:
                    record1 = records_for_key[0][1]
                    record2 = records_for_key[1][1]
                    
                    differences = []
                    for field in record1.keys():
                        if record1.get(field) != record2.get(field):
                            differences.append(field)
                    
                    if differences:
                        print(f"      ❗ Records differ in: {', '.join(differences)}")
                    else:
                        print(f"      ✅ Records appear identical (true duplicate)")
        
        # Suggest alternative duplicate detection strategies
        print(f"\n4. Alternative duplicate detection strategies:")
        
        # Strategy 1: More fields in duplicate key
        print(f"\n   📌 Strategy 1: Extended Key Detection")
        print(f"      Current: invoice_no + employee_id + work_date")
        print(f"      Extended: + position_code + hours_total + amount_total")
        
        extended_duplicate_keys = {}
        for record in mapped_data:
            extended_key = (
                record.get('invoice_no'),
                record.get('employee_id'),
                record.get('work_date'),
                record.get('position_code'),
                record.get('hours_total'),
                record.get('amount_total')
            )
            extended_duplicate_keys[extended_key] = extended_duplicate_keys.get(extended_key, 0) + 1
        
        extended_duplicates = sum(1 for count in extended_duplicate_keys.values() if count > 1)
        print(f"      Result: {extended_duplicates} duplicate groups with extended key")
        
        # Strategy 2: Hash-based full record comparison
        print(f"\n   📌 Strategy 2: Full Record Hash")
        
        import hashlib
        import json
        
        record_hashes = {}
        for record in mapped_data:
            # Create hash of all significant fields
            significant_fields = {k: v for k, v in record.items() 
                                if k not in ['created_at', 'updated_at']}
            record_str = json.dumps(significant_fields, sort_keys=True, default=str)
            record_hash = hashlib.md5(record_str.encode()).hexdigest()
            
            record_hashes[record_hash] = record_hashes.get(record_hash, 0) + 1
        
        hash_duplicates = sum(count - 1 for count in record_hashes.values() if count > 1)
        print(f"      Result: {hash_duplicates} truly identical records")
        
        return true_duplicates
        
    except Exception as e:
        print(f"❌ Error analyzing duplicates: {e}")
        import traceback
        print(f"📋 Error details: {traceback.format_exc()}")
        return {}

def recommend_duplicate_strategy():
    """Recommend the best duplicate detection strategy"""
    
    print(f"\n💡 DUPLICATE DETECTION RECOMMENDATIONS")
    print("=" * 60)
    
    print(f"Based on your business context (duplicates extremely rare):")
    print(f"")
    print(f"🎯 **Option 1: Disable Duplicate Detection**")
    print(f"   - Process all records without duplicate checking")
    print(f"   - Fastest performance")
    print(f"   - Risk: True duplicates would be inserted")
    print(f"")
    print(f"🎯 **Option 2: Extended Key Detection**") 
    print(f"   - Add position_code + hours_total + amount_total to key")
    print(f"   - Allows same employee/date with different work details")
    print(f"   - Better business logic alignment")
    print(f"")
    print(f"🎯 **Option 3: Full Record Comparison**")
    print(f"   - Only skip truly identical records")
    print(f"   - Most conservative approach")
    print(f"   - Slightly slower but most accurate")
    print(f"")
    print(f"🎯 **Option 4: Database Constraint Handling**")
    print(f"   - Let database handle duplicates via unique constraints")
    print(f"   - Use INSERT ... ON CONFLICT DO NOTHING")
    print(f"   - Fastest and most reliable")

if __name__ == "__main__":
    duplicates = analyze_duplicate_patterns()
    recommend_duplicate_strategy()
    
    print(f"\n🔧 NEXT STEPS:")
    print(f"1. Review the duplicate analysis above")
    print(f"2. Decide which strategy fits your business needs")
    print(f"3. Let me know your preference and I'll update the code")
    print(f"4. The 937 'duplicates' may actually be legitimate different records")