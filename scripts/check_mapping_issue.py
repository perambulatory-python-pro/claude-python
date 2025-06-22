"""
Check why business_unit and job_code are blank in unified data
"""

import pandas as pd


def check_mapping_issue():
    """Diagnose why business_unit and job_code are missing"""
    
    print("CHECKING BUSINESS_UNIT AND JOB_CODE MAPPING")
    print("=" * 60)
    
    # 1. Check what's in the dimension tables
    print("\n1. Checking dimension tables...")
    
    # Load clean dimensions
    building_dim = pd.read_excel("clean_dimensions.xlsx", sheet_name="Building_Dim")
    service_area_dim = pd.read_excel("clean_dimensions.xlsx", sheet_name="Service_Area_Dim")
    
    print(f"\n   Building Dimension columns: {list(building_dim.columns)}")
    print(f"   Sample building data:")
    print(building_dim[['BUILDING_CODE', 'EMID', 'BUSINESS_UNIT']].head())
    
    # Check how many have business_unit
    bu_filled = building_dim['BUSINESS_UNIT'].notna().sum()
    print(f"\n   Buildings with BUSINESS_UNIT: {bu_filled}/{len(building_dim)} ({bu_filled/len(building_dim)*100:.1f}%)")
    
    print(f"\n   Service Area Dimension columns: {list(service_area_dim.columns)}")
    print(f"   Sample service area data:")
    print(service_area_dim.head())
    
    # 2. Check original reference data
    print("\n2. Checking original reference data...")
    
    # Load EMID reference
    emid_ref = pd.read_excel("emid_job_bu_table.xlsx", sheet_name="emid_job_code")
    building_ref = pd.read_excel("emid_job_bu_table.xlsx", sheet_name="buildings")
    
    print(f"\n   EMID reference columns: {list(emid_ref.columns)}")
    print(f"   Building reference columns: {list(building_ref.columns)}")
    
    # Check business_unit coverage in building reference
    bu_in_ref = building_ref['business_unit'].notna().sum()
    print(f"\n   Buildings with business_unit in reference: {bu_in_ref}/{len(building_ref)} ({bu_in_ref/len(building_ref)*100:.1f}%)")
    
    # 3. Check a specific example
    print("\n3. Tracing a specific example...")
    
    # Take a building from EDI
    edi_df = pd.read_excel("all_edi_2025.xlsx", sheet_name="All 2025_EDI")
    sample_building = edi_df[edi_df['KP bldg'].notna()]['KP bldg'].iloc[0]
    sample_emid = edi_df[edi_df['KP bldg'] == sample_building]['EMID'].iloc[0]
    
    print(f"\n   Sample building from EDI: {sample_building}")
    print(f"   Its EMID: {sample_emid}")
    
    # Check if this building is in our references
    in_building_dim = building_dim[building_dim['BUILDING_CODE'] == sample_building]
    print(f"\n   In building dimension? {len(in_building_dim) > 0}")
    if len(in_building_dim) > 0:
        print(f"   Business Unit: {in_building_dim.iloc[0]['BUSINESS_UNIT']}")
    
    in_building_ref = building_ref[building_ref['building_code'] == sample_building]
    print(f"\n   In building reference? {len(in_building_ref) > 0}")
    if len(in_building_ref) > 0:
        print(f"   Business Unit: {in_building_ref.iloc[0]['business_unit']}")
    
    # Check job_code for this EMID
    emid_info = emid_ref[emid_ref['emid'] == sample_emid]
    print(f"\n   EMID in reference? {len(emid_info) > 0}")
    if len(emid_info) > 0:
        print(f"   Job Code: {emid_info.iloc[0]['job_code']}")
    
    # 4. Check the transformer lookup
    print("\n4. Checking transformer lookups...")
    
    # Load the unified data to see what we got
    import glob
    unified_files = glob.glob("unified_invoice_data_edi_based_*.xlsx")
    if unified_files:
        latest_file = sorted(unified_files)[-1]
        print(f"\n   Checking latest unified file: {latest_file}")
        
        unified_df = pd.read_excel(latest_file, sheet_name="Unified_Invoice_Details")
        print(f"   Total records: {len(unified_df)}")
        
        # Check field population
        for field in ['business_unit', 'job_code', 'emid', 'building_code']:
            if field in unified_df.columns:
                filled = unified_df[field].notna().sum()
                print(f"   {field}: {filled}/{len(unified_df)} filled ({filled/len(unified_df)*100:.1f}%)")
    
    print("\n" + "="*60)
    print("DIAGNOSIS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    check_mapping_issue()
