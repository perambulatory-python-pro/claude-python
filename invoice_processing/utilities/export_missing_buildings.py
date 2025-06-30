"""
Export Missing Buildings Analysis to CSV
Standalone script to create CSV files of missing buildings
"""

import pandas as pd
import numpy as np


def export_missing_buildings():
    """Export detailed analysis of missing buildings to CSV files"""
    
    print("EXPORTING MISSING BUILDINGS ANALYSIS")
    print("=" * 60)
    
    # Load data sources
    print("\n1. Loading data sources...")
    
    # Load EDI data
    edi_df = pd.read_excel("all_edi_2025.xlsx", sheet_name="All 2025_EDI")
    print(f"   ✓ Loaded {len(edi_df)} EDI records")
    
    # Load master lookup
    master_df = pd.read_excel("2025_Master Lookup_Validation Location with GL Reference_V3.xlsx", 
                             sheet_name="Master Lookup", header=1)
    print(f"   ✓ Loaded {len(master_df)} master lookup records")
    
    # Get unique buildings from EDI
    edi_buildings = set(edi_df['KP bldg'].dropna().astype(str).str.strip())
    print(f"   ✓ Found {len(edi_buildings)} unique buildings in EDI")
    
    # Find the Tina column
    tina_col = None
    for col in master_df.columns:
        if 'Tina' in str(col) or 'Building Code' in str(col):
            tina_col = col
            break
    
    if not tina_col:
        print("   ❌ Could not find Tina column!")
        return
    
    print(f"   ✓ Using column: '{tina_col}'")
    
    # Get AUS buildings
    job_col = 'Location/Job No'
    aus_data = master_df[[job_col, tina_col]].dropna()
    aus_buildings = set(aus_data[tina_col].astype(str).str.strip())
    
    print(f"   ✓ Found {len(aus_buildings)} unique buildings in AUS")
    
    # Find matches and mismatches
    print("\n2. Analyzing building matches...")
    
    exact_matches = edi_buildings.intersection(aus_buildings)
    aus_only = aus_buildings - edi_buildings
    edi_only = edi_buildings - aus_buildings
    
    print(f"   Exact matches: {len(exact_matches)}")
    print(f"   In AUS but not EDI: {len(aus_only)}")
    print(f"   In EDI but not AUS: {len(edi_only)}")
    
    # Create detailed exports
    print("\n3. Creating CSV exports...")
    
    # 1. Missing buildings with job details
    missing_details = []
    for _, row in aus_data.iterrows():
        building = str(row[tina_col]).strip()
        if building in aus_only:
            missing_details.append({
                'Job_Number': row[job_col],
                'Building_Code': building,
                'Status': 'Not in 2025 EDI'
            })
    
    missing_df = pd.DataFrame(missing_details)
    missing_df.to_csv('missing_buildings_detailed.csv', index=False)
    print(f"   ✓ Exported {len(missing_df)} missing building records to: missing_buildings_detailed.csv")
    
    # 2. Summary by building
    if len(missing_df) > 0:
        summary_df = missing_df.groupby('Building_Code').agg({
            'Job_Number': ['count', lambda x: ', '.join(x.astype(str).head(5))]
        }).reset_index()
        summary_df.columns = ['Building_Code', 'Job_Count', 'Sample_Jobs']
        summary_df = summary_df.sort_values('Job_Count', ascending=False)
        summary_df.to_csv('missing_buildings_summary.csv', index=False)
        print(f"   ✓ Exported summary to: missing_buildings_summary.csv")
    
    # 3. Complete building reference
    all_buildings = []
    
    # Add EDI buildings
    for building in edi_buildings:
        all_buildings.append({
            'Building_Code': building,
            'In_EDI': 'Yes',
            'In_AUS': 'Yes' if building in aus_buildings else 'No'
        })
    
    # Add AUS-only buildings
    for building in aus_only:
        all_buildings.append({
            'Building_Code': building,
            'In_EDI': 'No',
            'In_AUS': 'Yes'
        })
    
    reference_df = pd.DataFrame(all_buildings)
    reference_df = reference_df.drop_duplicates().sort_values('Building_Code')
    reference_df.to_csv('building_reference_complete.csv', index=False)
    print(f"   ✓ Exported complete reference to: building_reference_complete.csv")
    
    # 4. Statistical summary
    stats = {
        'Category': [
            'Total Unique Buildings (Combined)',
            'Buildings in EDI',
            'Buildings in AUS Master Lookup',
            'Buildings in Both (Matched)',
            'Buildings Only in AUS (Missing from EDI)',
            'Buildings Only in EDI',
            'Match Rate'
        ],
        'Count': [
            len(reference_df),
            len(edi_buildings),
            len(aus_buildings),
            len(exact_matches),
            len(aus_only),
            len(edi_only),
            f"{len(exact_matches)/len(aus_buildings)*100:.1f}%"
        ]
    }
    
    stats_df = pd.DataFrame(stats)
    stats_df.to_csv('building_analysis_statistics.csv', index=False)
    print(f"   ✓ Exported statistics to: building_analysis_statistics.csv")
    
    # 5. List of missing buildings only
    if aus_only:
        missing_only_df = pd.DataFrame({
            'Building_Code': sorted(list(aus_only))
        })
        missing_only_df.to_csv('missing_buildings_list.csv', index=False)
        print(f"   ✓ Exported missing buildings list to: missing_buildings_list.csv")
    
    print("\n" + "="*60)
    print("✅ EXPORT COMPLETE!")
    print("="*60)
    print("\nFiles created:")
    print("  1. missing_buildings_detailed.csv - All AUS jobs with missing buildings")
    print("  2. missing_buildings_summary.csv - Summary by building code")
    print("  3. building_reference_complete.csv - Complete reference of all buildings")
    print("  4. building_analysis_statistics.csv - Statistical summary")
    print("  5. missing_buildings_list.csv - Simple list of missing building codes")
    
    return reference_df


if __name__ == "__main__":
    export_missing_buildings()
