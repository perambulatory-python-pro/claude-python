"""
Quick test to validate the content detection fixes
"""

import pandas as pd
from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper

def test_specific_failures():
    """Test the specific cases that failed"""
    print("🔧 TESTING CONTENT DETECTION FIXES")
    print("=" * 40)
    
    mapper = EnhancedDataMapper()
    
    # Test case 1: AUS data (should detect as AUS Details, not BCI Details)
    print("\n1. Testing AUS data structure:")
    aus_data = pd.DataFrame([
        {
            'Invoice Number': '12345',
            'Employee Number': '67890',
            'Employee Name': 'John A Doe',  # Combined name - key difference from BCI
            'Hours': 8.0,
            'Rate': 25.00
            # NO 'First', 'Last', 'MI' columns
        }
    ])
    
    aus_result = mapper.detect_file_type_from_content(aus_data)
    print(f"   AUS structure → {aus_result}")
    print(f"   Expected: AUS Details")
    print(f"   Status: {'✅ PASS' if aus_result == 'AUS Details' else '❌ FAIL'}")
    
    # Test case 2: Master invoice data (should detect as EDI, not Kaiser)
    print("\n2. Testing Master invoice structure:")
    master_data = pd.DataFrame([
        {
            'Invoice No.': '12345',  # Key: 'Invoice No.' not 'Invoice Number'
            'EMID': 'ABC123',
            'Service Area': 'Test Area',
            'Invoice Total': 1000.00
            # NO employee columns, NO building/GL LOC columns
        }
    ])
    
    master_result = mapper.detect_file_type_from_content(master_data)
    print(f"   Master structure → {master_result}")
    print(f"   Expected: EDI")
    print(f"   Status: {'✅ PASS' if master_result == 'EDI' else '❌ FAIL'}")
    
    # Test case 3: BCI data (should still work correctly)
    print("\n3. Testing BCI data structure:")
    bci_data = pd.DataFrame([
        {
            'Invoice Number': '12345',
            'Employee Number': '67890',
            'First': 'John',      # Separate name fields - key difference from AUS
            'Last': 'Doe',
            'MI': 'A',
            'Hours': 8.0
        }
    ])
    
    bci_result = mapper.detect_file_type_from_content(bci_data)
    print(f"   BCI structure → {bci_result}")
    print(f"   Expected: BCI Details")
    print(f"   Status: {'✅ PASS' if bci_result == 'BCI Details' else '❌ FAIL'}")
    
    # Test case 4: Kaiser SCR (should still work correctly)
    print("\n4. Testing Kaiser SCR structure:")
    kaiser_data = pd.DataFrame([
        {
            'Building Code': 'CO203-1',
            'GL LOC': '16203',     # Key Kaiser identifiers
            'Service Area': 'Northern Colorado',
            'Building Name': 'Test Building'
        }
    ])
    
    kaiser_result = mapper.detect_file_type_from_content(kaiser_data)
    print(f"   Kaiser structure → {kaiser_result}")
    print(f"   Expected: Kaiser SCR Building Data")
    print(f"   Status: {'✅ PASS' if kaiser_result == 'Kaiser SCR Building Data' else '❌ FAIL'}")
    
    # Summary
    results = [
        aus_result == 'AUS Details',
        master_result == 'EDI',
        bci_result == 'BCI Details',
        kaiser_result == 'Kaiser SCR Building Data'
    ]
    
    print("\n" + "=" * 40)
    if all(results):
        print("🎉 ALL FIXES SUCCESSFUL!")
        print("✅ Content detection now works correctly")
        print("✅ Ready to run full test suite")
    else:
        print("⚠️ Some issues remain:")
        if not results[0]:
            print("   - AUS detection still failing")
        if not results[1]:
            print("   - Master invoice detection still failing")
        if not results[2]:
            print("   - BCI detection broken")
        if not results[3]:
            print("   - Kaiser detection broken")
    
    return all(results)

if __name__ == "__main__":
    test_specific_failures()
