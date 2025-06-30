"""
Diagnostic script to investigate AUS job number matching issues
"""

import pandas as pd
import numpy as np

def diagnose_aus_matching():
    """Diagnose why AUS job numbers aren't matching"""
    
    print("AUS JOB NUMBER MATCHING DIAGNOSTIC")
    print("=" * 60)
    
    # Load the data
    print("\n1. Loading data files...")
    
    # Load AUS invoice details
    aus_df = pd.read_csv("invoice_details_aus.csv")
    print(f"   ✓ Loaded {len(aus_df)} AUS records")
    
    # Load master lookup with proper header
    master_df = pd.read_excel(
        "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx",
        sheet_name="Master Lookup",
        header=0  # Use row 1 as header
    )
    print(f"   ✓ Loaded {len(master_df)} master lookup records")
    
    # 2. Examine AUS job numbers
    print("\n2. Examining AUS Job Numbers:")
    
    # Get unique job numbers from AUS
    aus_jobs = aus_df['Job Number'].dropna().unique()
    print(f"   Unique AUS job numbers: {len(aus_jobs)}")
    print(f"   Sample AUS job numbers: {list(aus_jobs[:10])}")
    print(f"   AUS job number data type: {type(aus_jobs[0])}")
    
    # Check for any patterns
    print("\n   AUS job number patterns:")
    # Check if they're numeric
    numeric_count = sum(1 for job in aus_jobs if str(job).isdigit())
    print(f"   - Purely numeric: {numeric_count}/{len(aus_jobs)}")
    
    # Check lengths
    lengths = pd.Series([len(str(job)) for job in aus_jobs]).value_counts()
    print(f"   - Length distribution: {lengths.head()}")
    
    # 3. Examine Master Lookup job numbers
    print("\n3. Examining Master Lookup Job Numbers:")
    
    # Find the correct column name
    job_col = None
    for col in master_df.columns:
        if 'Location/Job No' in str(col) or col == 'Location/Job No':
            job_col = col
            break
    
    if not job_col:
        print("   ❌ Could not find 'Location/Job No' column!")
        print(f"   Available columns: {list(master_df.columns[:10])}")
        return
    
    print(f"   Using column: '{job_col}'")
    
    # Get unique job numbers from master
    master_jobs = master_df[job_col].dropna().unique()
    print(f"   Unique master job numbers: {len(master_jobs)}")
    print(f"   Sample master job numbers: {list(master_jobs[:10])}")
    print(f"   Master job number data type: {type(master_jobs[0])}")
    
    # 4. Compare formats
    print("\n4. Format Comparison:")
    
    # Convert both to strings for comparison
    aus_jobs_str = set(str(job).strip() for job in aus_jobs)
    master_jobs_str = set(str(job).strip() for job in master_jobs)
    
    # Find exact matches
    exact_matches = aus_jobs_str.intersection(master_jobs_str)
    print(f"   Exact string matches: {len(exact_matches)}")
    
    if len(exact_matches) > 0:
        print(f"   Sample matches: {list(exact_matches)[:5]}")
    
    # Check for case sensitivity
    aus_jobs_upper = set(str(job).strip().upper() for job in aus_jobs)
    master_jobs_upper = set(str(job).strip().upper() for job in master_jobs)
    case_insensitive_matches = aus_jobs_upper.intersection(master_jobs_upper)
    print(f"   Case-insensitive matches: {len(case_insensitive_matches)}")
    
    # 5. Check for common transformation issues
    print("\n5. Common Issues Check:")
    
    # Check if AUS has leading zeros that master doesn't
    sample_aus = list(aus_jobs[:20])
    sample_master = list(master_jobs[:20])
    
    print("\n   Sample comparisons:")
    print("   AUS Job Number -> Master Lookup Format")
    for aus_job in sample_aus[:5]:
        # Try to find similar in master
        aus_str = str(aus_job).strip()
        found = False
        
        # Direct match
        if aus_str in master_jobs_str:
            print(f"   '{aus_job}' -> FOUND (exact match)")
            found = True
        
        # Try without leading zeros
        if not found and aus_str.isdigit():
            no_zeros = aus_str.lstrip('0')
            if no_zeros in master_jobs_str:
                print(f"   '{aus_job}' -> '{no_zeros}' (removed leading zeros)")
                found = True
        
        # Try with leading zeros
        if not found and aus_str.isdigit():
            with_zeros = aus_str.zfill(6)  # Try 6 digits
            if with_zeros in master_jobs_str:
                print(f"   '{aus_job}' -> '{with_zeros}' (added leading zeros)")
                found = True
        
        if not found:
            print(f"   '{aus_job}' -> NOT FOUND")
    
    # 6. Check specific test cases
    print("\n6. Testing Specific Job Numbers:")
    test_jobs = ['207168', '281084T', '207169']  # From your original test
    
    for test_job in test_jobs:
        in_aus = test_job in aus_jobs_str
        in_master = test_job in master_jobs_str
        print(f"   '{test_job}': In AUS? {in_aus}, In Master? {in_master}")
    
    # 7. Export unmatched for review
    print("\n7. Exporting Analysis...")
    
    # Create comparison DataFrame
    comparison_data = []
    
    # Add all AUS jobs with their match status
    for job in list(aus_jobs[:100]):  # First 100 for review
        job_str = str(job).strip()
        comparison_data.append({
            'AUS_Job_Number': job,
            'AUS_Job_String': job_str,
            'In_Master_Exact': job_str in master_jobs_str,
            'In_Master_Upper': job_str.upper() in master_jobs_upper,
            'Length': len(job_str),
            'Is_Numeric': job_str.isdigit()
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    comparison_df.to_csv('aus_job_diagnostic.csv', index=False)
    print("   ✓ Exported diagnostic data to: aus_job_diagnostic.csv")
    
    # Also export sample of master job numbers
    master_sample_df = pd.DataFrame({
        'Master_Job_Number': list(master_jobs[:100]),
        'Master_Job_String': [str(job).strip() for job in master_jobs[:100]],
        'Length': [len(str(job).strip()) for job in master_jobs[:100]]
    })
    master_sample_df.to_csv('master_job_sample.csv', index=False)
    print("   ✓ Exported master sample to: master_job_sample.csv")

if __name__ == "__main__":
    diagnose_aus_matching()