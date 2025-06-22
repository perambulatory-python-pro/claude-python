"""
Debug script to identify issues with reference data files
"""

import pandas as pd
import os


def check_emid_file(filepath='emid_job_bu_table.xlsx'):
    """Check for issues in EMID reference file"""
    print(f"\n{'='*60}")
    print(f"Checking EMID file: {filepath}")
    print('='*60)
    
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return
    
    try:
        # Check sheet names
        xl_file = pd.ExcelFile(filepath)
        print(f"\nSheets found: {xl_file.sheet_names}")
        
        # Check EMID sheet
        if 'emid_job_code' in xl_file.sheet_names:
            print("\n📋 Checking 'emid_job_code' sheet...")
            emid_df = pd.read_excel(filepath, sheet_name='emid_job_code')
            
            print(f"   Rows: {len(emid_df)}")
            print(f"   Columns: {list(emid_df.columns)}")
            
            # Check for required columns
            required_cols = ['emid', 'job_code']
            missing_cols = [col for col in required_cols if col not in emid_df.columns]
            if missing_cols:
                print(f"   ❌ Missing required columns: {missing_cols}")
            
            # Check for duplicates
            if 'job_code' in emid_df.columns:
                duplicates = emid_df[emid_df['job_code'].duplicated()]
                if len(duplicates) > 0:
                    print(f"\n   ⚠️  Found {len(duplicates)} duplicate job_codes:")
                    for _, row in duplicates.iterrows():
                        print(f"      - {row['job_code']} (EMID: {row.get('emid', 'N/A')})")
                else:
                    print("   ✅ No duplicate job_codes found")
            
            # Check for null values
            null_counts = emid_df.isnull().sum()
            if null_counts.any():
                print("\n   ⚠️  Null values found:")
                for col, count in null_counts[null_counts > 0].items():
                    print(f"      - {col}: {count} nulls")
        else:
            print("   ❌ Sheet 'emid_job_code' not found!")
        
        # Check buildings sheet
        if 'buildings' in xl_file.sheet_names:
            print("\n📋 Checking 'buildings' sheet...")
            buildings_df = pd.read_excel(filepath, sheet_name='buildings')
            
            print(f"   Rows: {len(buildings_df)}")
            print(f"   Columns: {list(buildings_df.columns)}")
            
            # Check for duplicates
            if 'building_code' in buildings_df.columns:
                dup_buildings = buildings_df[buildings_df['building_code'].duplicated()]
                if len(dup_buildings) > 0:
                    print(f"\n   ⚠️  Found {len(dup_buildings)} duplicate building_codes:")
                    for _, row in dup_buildings.head(5).iterrows():
                        print(f"      - {row['building_code']}")
                    if len(dup_buildings) > 5:
                        print(f"      ... and {len(dup_buildings) - 5} more")
                else:
                    print("   ✅ No duplicate building_codes found")
            
            if 'kp_loc_ref' in buildings_df.columns:
                # Check data type
                print(f"\n   kp_loc_ref data type: {buildings_df['kp_loc_ref'].dtype}")
                
                # Check for non-numeric values
                non_numeric = buildings_df[pd.to_numeric(buildings_df['kp_loc_ref'], errors='coerce').isna() & buildings_df['kp_loc_ref'].notna()]
                if len(non_numeric) > 0:
                    print(f"   ⚠️  Found {len(non_numeric)} non-numeric kp_loc_ref values")
                    for _, row in non_numeric.head(5).iterrows():
                        print(f"      - {row['kp_loc_ref']} (building: {row.get('building_code', 'N/A')})")
        else:
            print("   ❌ Sheet 'buildings' not found!")
            
    except Exception as e:
        print(f"\n❌ Error reading file: {str(e)}")
        import traceback
        traceback.print_exc()


def check_master_lookup(filepath='2025_Master Lookup_Validation Location with GL Reference_V3.xlsx'):
    """Check for issues in master lookup file"""
    print(f"\n{'='*60}")
    print(f"Checking Master Lookup: {filepath}")
    print('='*60)
    
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return
    
    try:
        # Read with header on row 2 (index 1)
        master_df = pd.read_excel(filepath, sheet_name='Master Lookup', header=1)
        
        print(f"\nRows: {len(master_df)}")
        print(f"Columns: {len(master_df.columns)}")
        
        # Check key columns
        key_columns = ['Location/Job No', 'Tina- Building Code']
        
        print("\n🔍 Checking for key columns...")
        for col in key_columns:
            found = False
            for actual_col in master_df.columns:
                if col in str(actual_col):
                    print(f"   ✅ Found '{col}' as '{actual_col}'")
                    found = True
                    break
            if not found:
                print(f"   ❌ Column '{col}' not found")
        
        # Find the actual column names
        job_col = None
        building_col = None
        
        for col in master_df.columns:
            if 'Location/Job No' in str(col) or col == 'Location/Job No':
                job_col = col
            if 'Tina- Building Code' in str(col) or 'Tina-' in str(col):
                building_col = col
        
        if job_col and building_col:
            print(f"\n📊 Analyzing mappings...")
            print(f"   Job column: '{job_col}'")
            print(f"   Building column: '{building_col}'")
            
            # Count valid mappings
            valid_mappings = master_df[master_df[job_col].notna() & master_df[building_col].notna()]
            print(f"   Valid mappings: {len(valid_mappings)}")
            
            # Check for duplicates
            if job_col in master_df.columns:
                dup_jobs = master_df[master_df[job_col].duplicated() & master_df[job_col].notna()]
                if len(dup_jobs) > 0:
                    print(f"\n   ⚠️  Found {len(dup_jobs)} duplicate job numbers")
                    for _, row in dup_jobs.head(5).iterrows():
                        print(f"      - {row[job_col]} → {row.get(building_col, 'N/A')}")
        
    except Exception as e:
        print(f"\n❌ Error reading file: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Run all diagnostics"""
    print("REFERENCE DATA DIAGNOSTICS")
    print("=" * 60)
    
    # Check both files
    check_emid_file()
    check_master_lookup()
    
    print("\n" + "="*60)
    print("Diagnostics complete!")
    print("\nIf you see duplicate warnings above, the enhanced lookup manager")
    print("will automatically handle them by keeping the first occurrence.")
    print("=" * 60)


if __name__ == "__main__":
    main()
