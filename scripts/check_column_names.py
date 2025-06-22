"""
Quick diagnostic to check actual column names in master lookup
"""

import pandas as pd

# Read the master lookup file
master_file = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx"

# Try different header rows
print("Checking Master Lookup column names...\n")

for header_row in [0, 1, 2]:
    try:
        df = pd.read_excel(master_file, sheet_name="Master Lookup", header=header_row)
        print(f"Header row {header_row}:")
        print(f"  Columns: {list(df.columns)[:10]}...")  # First 10 columns
        
        # Look for columns containing "Tina" or "Building"
        tina_cols = [col for col in df.columns if 'tina' in str(col).lower() or 'building' in str(col).lower()]
        if tina_cols:
            print(f"  Found relevant columns: {tina_cols}")
        
        # Check specific columns we need
        if 'Location/Job No' in df.columns:
            print("  ✓ Found 'Location/Job No'")
        else:
            # Look for similar
            job_cols = [col for col in df.columns if 'job' in str(col).lower() or 'location' in str(col).lower()]
            print(f"  Job/Location columns: {job_cols[:5]}")
        
        print()
    except Exception as e:
        print(f"  Error with header row {header_row}: {e}\n")

# Also check the exact column at position X (column 24)
print("\nChecking column X (position 23/24):")
df = pd.read_excel(master_file, sheet_name="Master Lookup", header=1)
if len(df.columns) > 23:
    print(f"  Column X (index 23): '{df.columns[23]}'")
    print(f"  Column Y (index 24): '{df.columns[24]}'" if len(df.columns) > 24 else "  No column Y")
