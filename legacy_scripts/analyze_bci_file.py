"""
BCI File Analyzer
Diagnose issues with TLM_BCI.xlsx file processing
"""

import pandas as pd
import numpy as np
from data_mapper_enhanced import EnhancedDataMapper

def analyze_bci_file(filename: str = "TLM_BCI.xlsx"):
    """Analyze the BCI file to understand its structure"""
    print("🔍 BCI FILE ANALYSIS")
    print("=" * 50)
    
    try:
        # Read the file
        print(f"📁 Reading file: {filename}")
        df = pd.read_excel(filename)
        print(f"   ✅ File loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Show basic info
        print(f"\n📊 Basic Information:")
        print(f"   - Total rows: {len(df)}")
        print(f"   - Total columns: {len(df.columns)}")
        print(f"   - Memory usage: {df.memory_usage(deep=True).sum():,} bytes")
        
        # Show column names
        print(f"\n📋 Column Names ({len(df.columns)} total):")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2d}. '{col}'")
        
        # Check for expected BCI columns
        print(f"\n🔍 BCI Column Mapping Check:")
        
        expected_bci_columns = {
            'Invoice Number': 'invoice_no',
            'Employee Number': 'employee_id', 
            'First': 'first_name',
            'Last': 'last_name',
            'MI': 'middle_initial',
            'Work Date': 'work_date',
            'Hours': 'hours_regular',
            'OT Hours': 'hours_overtime',
            'Holiday Hours': 'hours_holiday',
            'Billing Rate': 'rate_regular',
            'OT Rate': 'rate_overtime',
            'Holiday Rate': 'rate_holiday',
            'Regular Amount': 'amount_regular',
            'OT Amount': 'amount_overtime',
            'Holiday Amount': 'amount_holiday',
            'Total Amount': 'amount_total'
        }
        
        found_columns = []
        missing_columns = []
        
        for excel_col, db_col in expected_bci_columns.items():
            if excel_col in df.columns:
                found_columns.append(excel_col)
                print(f"   ✅ '{excel_col}'")
            else:
                missing_columns.append(excel_col)
                print(f"   ❌ '{excel_col}' - MISSING")
        
        print(f"\n📈 Column Match Summary:")
        print(f"   - Found: {len(found_columns)}/{len(expected_bci_columns)} expected columns")
        print(f"   - Missing: {len(missing_columns)} expected columns")
        
        # Check for similar column names (case-insensitive, partial matches)
        if missing_columns:
            print(f"\n🔍 Possible Alternative Column Names:")
            actual_columns_lower = [col.lower() for col in df.columns]
            
            for missing_col in missing_columns:
                missing_lower = missing_col.lower()
                # Look for partial matches
                possible_matches = []
                for actual_col in df.columns:
                    actual_lower = actual_col.lower()
                    if (missing_lower in actual_lower or 
                        actual_lower in missing_lower or
                        any(word in actual_lower for word in missing_lower.split())):
                        possible_matches.append(actual_col)
                
                if possible_matches:
                    print(f"   '{missing_col}' might be: {possible_matches}")
        
        # Show sample data
        print(f"\n📋 Sample Data (first 3 rows):")
        if len(df) > 0:
            # Show only first few columns to avoid overwhelming output
            sample_cols = df.columns[:8] if len(df.columns) > 8 else df.columns
            sample_df = df[sample_cols].head(3)
            
            for i, (_, row) in enumerate(sample_df.iterrows()):
                print(f"   Row {i+1}:")
                for col in sample_cols:
                    value = row[col]
                    if pd.isna(value):
                        value_str = "NULL"
                    else:
                        value_str = str(value)[:30]  # Truncate long values
                    print(f"     {col}: {value_str}")
                print()
        
        # Check for empty rows
        print(f"🔍 Data Quality Check:")
        empty_rows = df.isnull().all(axis=1).sum()
        print(f"   - Completely empty rows: {empty_rows}")
        
        # Check key columns for data
        key_columns = ['Invoice Number', 'Employee Number']
        for col in key_columns:
            if col in df.columns:
                non_null_count = df[col].notna().sum()
                print(f"   - '{col}' has data: {non_null_count}/{len(df)} rows")
            else:
                print(f"   - '{col}' column not found")
        
        # Try mapping with current mapper
        print(f"\n🧪 Testing Current BCI Mapper:")
        try:
            mapper = EnhancedDataMapper()
            mapped_data = mapper.map_bci_details(df)
            print(f"   ✅ Mapper succeeded: {len(mapped_data)} records mapped")
            
            if len(mapped_data) > 0:
                print(f"   📋 Sample mapped record:")
                sample_record = mapped_data[0]
                for key, value in list(sample_record.items())[:10]:  # Show first 10 fields
                    print(f"     {key}: {value}")
            
        except Exception as e:
            print(f"   ❌ Mapper failed: {e}")
        
        # Suggestions
        print(f"\n💡 RECOMMENDATIONS:")
        if len(found_columns) < 3:
            print("   🔧 Column name mismatch - file structure may be different than expected")
            print("   📋 Compare your actual column names with expected BCI format")
        
        if empty_rows > len(df) * 0.1:  # More than 10% empty rows
            print("   🧹 Consider cleaning empty rows before processing")
        
        critical_missing = [col for col in ['Invoice Number', 'Employee Number'] if col not in df.columns]
        if critical_missing:
            print(f"   ⚠️ Critical columns missing: {critical_missing}")
            print(f"   🔍 These are required for BCI processing")
        
        return df
        
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        import traceback
        print(f"📋 Full error:\n{traceback.format_exc()}")
        return None

def suggest_column_mapping(df: pd.DataFrame):
    """Suggest custom column mapping based on actual file structure"""
    print(f"\n🎯 SUGGESTED CUSTOM MAPPING:")
    print("=" * 30)
    
    # Common variations of BCI column names
    mapping_suggestions = {}
    
    for col in df.columns:
        col_lower = col.lower().strip()
        
        # Invoice number variations
        if any(word in col_lower for word in ['invoice', 'inv']):
            if any(word in col_lower for word in ['number', 'num', 'no']):
                mapping_suggestions['Invoice Number'] = col
        
        # Employee number variations
        if any(word in col_lower for word in ['employee', 'emp']):
            if any(word in col_lower for word in ['number', 'num', 'id']):
                mapping_suggestions['Employee Number'] = col
        
        # Name field variations
        if col_lower in ['first', 'firstname', 'first_name', 'fname']:
            mapping_suggestions['First'] = col
        if col_lower in ['last', 'lastname', 'last_name', 'lname']:
            mapping_suggestions['Last'] = col
        if col_lower in ['mi', 'middle', 'middle_initial']:
            mapping_suggestions['MI'] = col
        
        # Date variations
        if any(word in col_lower for word in ['work', 'date', 'service']):
            if 'date' in col_lower:
                mapping_suggestions['Work Date'] = col
        
        # Hours variations
        if 'hour' in col_lower:
            if any(word in col_lower for word in ['regular', 'reg', 'standard']):
                mapping_suggestions['Hours'] = col
            elif any(word in col_lower for word in ['overtime', 'ot']):
                mapping_suggestions['OT Hours'] = col
            elif any(word in col_lower for word in ['holiday', 'hol']):
                mapping_suggestions['Holiday Hours'] = col
    
    if mapping_suggestions:
        print("Based on your column names, try this mapping:")
        for expected_col, actual_col in mapping_suggestions.items():
            print(f"   '{expected_col}' → '{actual_col}'")
    else:
        print("Could not auto-suggest mapping. Manual inspection needed.")

if __name__ == "__main__":
    # Analyze the BCI file
    df = analyze_bci_file("TLM_BCI.xlsx")
    
    if df is not None:
        suggest_column_mapping(df)
        
        print(f"\n🚀 NEXT STEPS:")
        print("1. Check if column names match expected BCI format")
        print("2. If different, we can create a custom mapping")
        print("3. Verify that Invoice Number and Employee Number columns have data")
        print("4. Consider cleaning the file if there are many empty rows")
