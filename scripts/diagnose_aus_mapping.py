"""
Diagnose AUS Job Mapping Issues
Figure out why AUS buildings aren't matching
"""

import pandas as pd
import numpy as np


def diagnose_aus_mappings():
    """Diagnose why AUS job mappings are failing"""
    
    print("AUS JOB MAPPING DIAGNOSTIC")
    print("=" * 60)
    
    # Load the data sources
    print("\n1. Loading data sources...")
    
    # Load EDI data to get actual building codes
    edi_df = pd.read_excel("all_edi_2025.xlsx", sheet_name="All 2025_EDI")
    print(f"   ✓ Loaded {len(edi_df)} EDI records")
    
    # Load master lookup
    master_df = pd.read_excel("2025_Master Lookup_Validation Location with GL Reference_V3.xlsx", 
                             sheet_name="Master Lookup", header=1)
    print(f"   ✓ Loaded {len(master_df)} master lookup records")
    
    # Load clean dimensions to see what we created
    building_dim = pd.read_excel("clean_dimensions.xlsx", sheet_name="Building_Dim")
    print(f"   ✓ Loaded {len(building_dim)} buildings from dimensions")
    
    # 2. Analyze EDI building codes
    print("\n2. EDI Building Codes Analysis:")
    edi_buildings = edi_df['KP bldg'].dropna().unique()
    print(f"   Total unique buildings in EDI: {len(edi_buildings)}")
    print(f"   Sample EDI buildings: {sorted(edi_buildings)[:10]}")
    
    # 3. Find the Tina column
    print("\n3. Master Lookup Column Analysis:")
    tina_cols = [col for col in master_df.columns if 'Tina' in str(col) or 'Building' in str(col)]
    print(f"   Found columns: {tina_cols}")
    
    if tina_cols:
        tina_col = tina_cols[0]
        print(f"   Using column: '{tina_col}'")
        
        # Get AUS building codes
        aus_buildings = master_df[tina_col].dropna().unique()
        print(f"   Total unique buildings in master lookup: {len(aus_buildings)}")
        print(f"   Sample AUS buildings: {sorted(aus_buildings)[:10]}")
        
        # 4. Compare formats
        print("\n4. Format Comparison:")
        
        # Check for format differences
        print("\n   EDI building format examples:")
        for i, building in enumerate(list(edi_buildings)[:5]):
            print(f"     '{building}' (type: {type(building).__name__}, length: {len(str(building))})")
        
        print("\n   AUS building format examples:")
        for i, building in enumerate(list(aus_buildings)[:5]):
            print(f"     '{building}' (type: {type(building).__name__}, length: {len(str(building))})")
        
        # 5. Find exact matches
        print("\n5. Exact Match Analysis:")
        edi_set = set(str(b).strip() for b in edi_buildings)
        aus_set = set(str(b).strip() for b in aus_buildings)
        
        exact_matches = edi_set.intersection(aus_set)
        print(f"   Exact matches found: {len(exact_matches)}")
        if exact_matches:
            print(f"   Examples: {list(exact_matches)[:5]}")
        
        # 6. Find near matches
        print("\n6. Potential Issues:")
        
        # Check for case differences
        edi_lower = set(str(b).strip().lower() for b in edi_buildings)
        aus_lower = set(str(b).strip().lower() for b in aus_buildings)
        case_matches = edi_lower.intersection(aus_lower)
        if len(case_matches) > len(exact_matches):
            print(f"   ⚠️ Found {len(case_matches) - len(exact_matches)} additional matches with case-insensitive comparison")
        
        # Check for whitespace issues
        edi_no_space = set(str(b).replace(' ', '').replace('-', '') for b in edi_buildings)
        aus_no_space = set(str(b).replace(' ', '').replace('-', '') for b in aus_buildings)
        space_matches = edi_no_space.intersection(aus_no_space)
        if len(space_matches) > len(exact_matches):
            print(f"   ⚠️ Found {len(space_matches) - len(exact_matches)} additional matches after removing spaces/hyphens")
        
        # Check what's in AUS but not in EDI
        aus_only = aus_set - edi_set
        print(f"\n   Buildings in AUS but not in EDI: {len(aus_only)}")
        if aus_only:
            print(f"   Examples: {list(aus_only)[:10]}")
        
        # Check what's in EDI but not in AUS
        edi_only = edi_set - aus_set
        print(f"\n   Buildings in EDI but not in AUS: {len(edi_only)}")
        if edi_only:
            print(f"   Examples: {list(edi_only)[:10]}")
        
        # 7. Check specific AUS job examples
        print("\n7. Specific AUS Job Analysis:")
        
        # Get some AUS jobs with their buildings
        job_col = None
        for col in master_df.columns:
            if 'Location/Job No' in str(col) or col == 'Location/Job No':
                job_col = col
                break
        
        if job_col:
            sample_jobs = master_df[[job_col, tina_col]].dropna().head(10)
            print("\n   Sample AUS jobs and their buildings:")
            for _, row in sample_jobs.iterrows():
                job = row[job_col]
                building = row[tina_col]
                in_edi = 'YES' if str(building).strip() in edi_set else 'NO'
                print(f"     Job {job} → Building '{building}' → In EDI: {in_edi}")
        
        # 8. Data type analysis
        print("\n8. Data Type Issues:")
        
        # Check for numeric vs string issues
        edi_types = {}
        for b in edi_buildings[:20]:
            t = type(b).__name__
            edi_types[t] = edi_types.get(t, 0) + 1
        print(f"   EDI building types: {edi_types}")
        
        aus_types = {}
        for b in aus_buildings[:20]:
            t = type(b).__name__
            aus_types[t] = aus_types.get(t, 0) + 1
        print(f"   AUS building types: {aus_types}")
        
        # 9. Export missing buildings to CSV
        print("\n9. Exporting Missing Buildings to CSV...")
        
        # Create detailed comparison
        all_comparisons = []
        
        # Add all AUS buildings with their status
        for building in aus_buildings:
            # Count jobs for this building
            job_count = len(aus_mappings[aus_mappings[tina_col] == building])
            
            all_comparisons.append({
                'Building_Code': building,
                'Source': 'AUS',
                'In_EDI': 'Yes' if str(building).strip() in edi_set else 'No',
                'Job_Count': job_count
            })
        
        # Add EDI-only buildings
        for building in edi_only:
            all_comparisons.append({
                'Building_Code': building,
                'Source': 'EDI_Only',
                'In_EDI': 'Yes',
                'Job_Count': 0
            })
        
        comparison_df = pd.DataFrame(all_comparisons)
        
        # Export different views
        # 1. Buildings missing from EDI
        missing_from_edi = comparison_df[comparison_df['In_EDI'] == 'No']
        missing_from_edi.to_csv('buildings_missing_from_edi.csv', index=False)
        print(f"   ✓ Missing from EDI: buildings_missing_from_edi.csv ({len(missing_from_edi)} buildings)")
        
        # 2. Complete comparison
        comparison_df.to_csv('building_comparison_complete.csv', index=False)
        print(f"   ✓ Complete comparison: building_comparison_complete.csv ({len(comparison_df)} buildings)")
        
        # 3. Summary statistics
        summary_stats = {
            'Metric': [
                'Total AUS Buildings',
                'Buildings in EDI',
                'Buildings Missing from EDI',
                'EDI-Only Buildings',
                'Match Rate'
            ],
            'Value': [
                len(aus_buildings),
                len(exact_matches),
                len(aus_only),
                len(edi_only),
                f"{len(exact_matches)/len(aus_buildings)*100:.1f}%"
            ]
        }
        summary_df = pd.DataFrame(summary_stats)
        summary_df.to_csv('building_mapping_summary.csv', index=False)
        print(f"   ✓ Summary statistics: building_mapping_summary.csv")
        
    else:
        print("   ❌ Could not find Tina/Building column!")
    
    print("\n" + "="*60)
    print("DIAGNOSTIC COMPLETE")
    print("="*60)


if __name__ == "__main__":
    diagnose_aus_mappings()
