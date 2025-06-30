def test_master_lookup_loading():
    """Test if master lookup is loading correctly"""
    import pandas as pd
    
    print("Testing Master Lookup Loading")
    print("=" * 60)
    
    # Load the file - use the correct filename with spaces
    master_df = pd.read_excel(
        "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx",  # Added underscore after 2025
        sheet_name="Master Lookup",
        header=0
    )
    
    print(f"\n1. Loaded {len(master_df)} records")
    print(f"\n2. Columns: {list(master_df.columns)}")
    
    # Check for key columns
    print(f"\n3. Column Check:")
    print(f"   'Location/Job No' exists: {'Location/Job No' in master_df.columns}")
    print(f"   'building_code' exists: {'building_code' in master_df.columns}")
    print(f"   'EMID' exists: {'EMID' in master_df.columns}")
    
    # Sample data
    print(f"\n4. Sample Data:")
    if 'Location/Job No' in master_df.columns and 'building_code' in master_df.columns:
        sample = master_df[['Location/Job No', 'building_code']].dropna().head()
        print(sample)
    
    # Test a specific job lookup
    print(f"\n5. Test Job Lookup:")
    test_job = '207168'  # From your earlier test
    if 'Location/Job No' in master_df.columns:
        matches = master_df[master_df['Location/Job No'].astype(str).str.strip() == test_job]
        print(f"   Looking for job '{test_job}': Found {len(matches)} matches")
        if len(matches) > 0 and 'building_code' in master_df.columns:
            print(f"   Building code: {matches.iloc[0]['building_code']}")

if __name__ == "__main__":
    test_master_lookup_loading()