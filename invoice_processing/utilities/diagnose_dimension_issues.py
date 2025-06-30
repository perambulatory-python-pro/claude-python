"""
Deep diagnostic to trace the exact lookup failure
"""

import pandas as pd
from fixed_edi_transformer import FixedEDIBasedTransformer


def trace_single_invoice_lookup():
    """Trace a single invoice through the entire lookup process"""
    
    print("TRACING SINGLE INVOICE LOOKUP")
    print("=" * 60)
    
    # Initialize transformer
    transformer = FixedEDIBasedTransformer("clean_dimensions.xlsx")
    
    # Get a test invoice
    bci_df = pd.read_csv("invoice_details_bci.csv", nrows=1)
    test_invoice = str(bci_df['Invoice_No'].iloc[0])
    print(f"\nTest invoice: {test_invoice}")
    
    # Step 1: Check if invoice is in lookup
    print("\n1. Checking invoice lookup...")
    if test_invoice in transformer.invoice_lookup:
        print("   ✓ Invoice found in lookup")
        invoice_data = transformer.invoice_lookup[test_invoice]
        print(f"   Keys available: {list(invoice_data.keys())}")
        print(f"   BUILDING_CODE: {invoice_data.get('BUILDING_CODE')}")
        print(f"   EMID: {invoice_data.get('EMID')}")
    else:
        print("   ❌ Invoice NOT in lookup")
        print(f"   Sample keys in lookup: {list(transformer.invoice_lookup.keys())[:5]}")
    
    # Step 2: Test get_invoice_dimensions
    print("\n2. Testing get_invoice_dimensions method...")
    dims = transformer.get_invoice_dimensions(test_invoice)
    if dims:
        print("   ✓ Got dimensions")
        print(f"   BUILDING_CODE: {dims.get('BUILDING_CODE')}")
        print(f"   EMID: {dims.get('EMID')}")
    else:
        print("   ❌ No dimensions returned")
    
    # Step 3: Check building lookup
    if dims and dims.get('BUILDING_CODE'):
        building_code = dims['BUILDING_CODE']
        print(f"\n3. Checking building lookup for: {building_code}")
        
        if building_code in transformer.building_lookup:
            print("   ✓ Building found in lookup")
            building_info = transformer.building_lookup[building_code]
            print(f"   Keys: {list(building_info.keys())}")
            print(f"   BLDG_BUSINESS_UNIT: {building_info.get('BLDG_BUSINESS_UNIT')}")
        else:
            print("   ❌ Building NOT in lookup")
            print(f"   Sample building codes: {list(transformer.building_lookup.keys())[:5]}")
    
    # Step 4: Test full transform
    print("\n4. Testing full transform_bci_row...")
    test_row = bci_df.iloc[0]
    result = transformer.transform_bci_row(test_row, 0, "test.csv")
    
    if result:
        print("   ✓ Transform successful")
        result_dict = result.to_dict()
        print(f"   building_code: {result_dict.get('building_code')}")
        print(f"   business_unit: {result_dict.get('business_unit')}")
        print(f"   emid: {result_dict.get('emid')}")
        print(f"   job_code: {result_dict.get('job_code')}")
    else:
        print("   ❌ Transform failed")
    
    # Step 5: Check raw dimension data
    print("\n5. Double-checking raw dimension data...")
    invoice_dim = pd.read_excel("clean_dimensions.xlsx", sheet_name="Invoice_Dim")
    invoice_match = invoice_dim[invoice_dim['INVOICE_NUMBER'].astype(str) == test_invoice]
    
    if len(invoice_match) > 0:
        print("   ✓ Found in raw invoice dimension")
        row = invoice_match.iloc[0]
        print(f"   Raw BUILDING_CODE: {row.get('BUILDING_CODE')}")
        print(f"   Raw EMID: {row.get('EMID')}")
        
        # Check if this building exists in building dim
        if pd.notna(row.get('BUILDING_CODE')):
            building_dim = pd.read_excel("clean_dimensions.xlsx", sheet_name="Building_Dim")
            building_match = building_dim[building_dim['BUILDING_CODE'] == row['BUILDING_CODE']]
            if len(building_match) > 0:
                print(f"   ✓ Building {row['BUILDING_CODE']} found in Building_Dim")
                print(f"   Raw BUSINESS_UNIT: {building_match.iloc[0].get('BUSINESS_UNIT')}")
            else:
                print(f"   ❌ Building {row['BUILDING_CODE']} NOT in Building_Dim")
    else:
        print("   ❌ Not found in raw dimension")
    
    print("\n" + "="*60)


def check_invoice_dim_structure():
    """Check the exact structure of invoice dimension"""
    
    print("\nCHECKING INVOICE DIMENSION STRUCTURE")
    print("=" * 60)
    
    invoice_dim = pd.read_excel("clean_dimensions.xlsx", sheet_name="Invoice_Dim")
    
    # Show exact column names
    print("\nExact column names in Invoice_Dim:")
    for i, col in enumerate(invoice_dim.columns):
        print(f"  {i}: '{col}'")
    
    # Check data types
    print("\nData types:")
    print(invoice_dim.dtypes)
    
    # Sample data
    print("\nFirst row as dict:")
    if len(invoice_dim) > 0:
        first_row = invoice_dim.iloc[0].to_dict()
        for key, value in first_row.items():
            print(f"  '{key}': {value} (type: {type(value).__name__})")


if __name__ == "__main__":
    trace_single_invoice_lookup()
    check_invoice_dim_structure()