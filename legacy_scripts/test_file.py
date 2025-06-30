"""
Test the exact lookup logic used in dual_lookup_transformer
"""
import pandas as pd

def test_aus_lookup_logic():
    print("Testing AUS Lookup Logic")
    print("=" * 60)
    
    # Load files exactly as the transformer does
    master_lookup_df = pd.read_excel(
        "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx",
        sheet_name="Master Lookup",
        header=0
    )
    
    aus_df = pd.read_csv("invoice_details_aus.csv")
    
    print(f"\n1. Loaded Data:")
    print(f"   Master lookup: {len(master_lookup_df)} records")
    print(f"   AUS invoices: {len(aus_df)} records")
    
    # Test the exact lookup logic
    print(f"\n2. Testing Lookup Logic:")
    
    # Take first 5 AUS records
    for idx, row in aus_df.head(5).iterrows():
        invoice_no = str(row['Invoice Number'])
        job_number = str(row.get('Job Number', ''))
        
        print(f"\n   Invoice: {invoice_no}, Job: {job_number}")
        
        # Check if master_lookup_df exists and is not empty
        if master_lookup_df is not None and not master_lookup_df.empty:
            # Do the lookup
            matches = master_lookup_df[
                master_lookup_df['Location/Job No'].astype(str).str.strip() == job_number.strip()
            ]
            
            print(f"     Master lookup not empty: ✓")
            print(f"     Matches found: {len(matches)}")
            
            if len(matches) > 0:
                unique_buildings = matches['building_code'].dropna().unique()
                print(f"     Unique buildings: {list(unique_buildings)}")
            else:
                print(f"     No matches for job '{job_number}'")
        else:
            print(f"     Master lookup is None or empty!")
    
    # Check if the columns exist
    print(f"\n3. Column Verification:")
    print(f"   'Location/Job No' in columns: {'Location/Job No' in master_lookup_df.columns}")
    print(f"   'building_code' in columns: {'building_code' in master_lookup_df.columns}")
    
    # Count how many AUS jobs should match
    print(f"\n4. Match Statistics:")
    aus_jobs = set(aus_df['Job Number'].dropna().astype(str).str.strip())
    master_jobs = set(master_lookup_df['Location/Job No'].dropna().astype(str).str.strip())
    matching_jobs = aus_jobs.intersection(master_jobs)
    
    print(f"   Unique AUS jobs: {len(aus_jobs)}")
    print(f"   Unique master jobs: {len(master_jobs)}")
    print(f"   Matching jobs: {len(matching_jobs)}")
    print(f"   Match rate: {len(matching_jobs)/len(aus_jobs)*100:.1f}%")

if __name__ == "__main__":
    test_aus_lookup_logic()