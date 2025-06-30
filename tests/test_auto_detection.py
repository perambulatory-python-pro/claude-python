"""
Test Auto-Detection for Specific File Names
Validates that AUS_Invoice.xlsx and TLM_BCI.xlsx are detected correctly
"""

import pandas as pd
from data_mapper_enhanced import EnhancedDataMapper

def test_filename_detection():
    """Test that specific filenames are detected correctly"""
    print("🧪 TESTING AUTO-DETECTION FOR YOUR FILE NAMES")
    print("=" * 50)
    
    mapper = EnhancedDataMapper()
    
    # Test your specific file names
    test_cases = [
        ("TLM_BCI.xlsx", "BCI Details"),
        ("AUS_Invoice.xlsx", "AUS Details"),
        ("weekly_release.xlsx", "Release"),
        ("weekly_edi.xlsx", "EDI"),
        ("weekly_addons.xlsx", "Add-On"),
        ("kaiser_scr.xlsx", "Kaiser SCR Building Data"),
        # Test variations
        ("bci_data.xlsx", "BCI Details"),
        ("aus_data.xlsx", "AUS Details"),
        ("random_file.xlsx", "Unknown")
    ]
    
    print("📋 Testing filename detection:")
    all_passed = True
    
    for filename, expected_type in test_cases:
        detected_type = mapper.detect_file_type_from_filename(filename)
        
        if detected_type == expected_type:
            print(f"   ✅ {filename:<20} → {detected_type}")
        else:
            print(f"   ❌ {filename:<20} → {detected_type} (expected {expected_type})")
            all_passed = False
    
    return all_passed

def test_content_detection():
    """Test content-based detection as fallback"""
    print("\n🧪 Testing content-based detection...")
    
    mapper = EnhancedDataMapper()
    
    # Create test DataFrames that simulate your file structures more accurately
    
    # BCI-style data (has separate First, Last, MI columns)
    bci_data = pd.DataFrame([
        {
            'Invoice Number': '12345',
            'Employee Number': '67890',
            'First': 'John',
            'Last': 'Doe',
            'MI': 'A',
            'Hours': 8.0,
            'Billing Rate': 25.00,
            'Work Date': '2025-06-25'
        }
    ])
    
    # AUS-style data (has combined Employee Name, no First/Last/MI)
    aus_data = pd.DataFrame([
        {
            'Invoice Number': '12345',
            'Employee Number': '67890',
            'Employee Name': 'John A Doe',  # Combined name
            'Hours': 8.0,
            'Rate': 25.00,
            'Work Date': '2025-06-25'
            # Notice: NO 'First', 'Last', 'MI' columns
        }
    ])
    
    # Master invoice data (has Invoice No., not Invoice Number)
    master_data = pd.DataFrame([
        {
            'Invoice No.': '12345',  # Note: 'Invoice No.' not 'Invoice Number'
            'EMID': 'ABC123',
            'Service Area': 'Test Area',
            'Invoice Total': 1000.00,
            'Release Date': '2025-06-25'
            # Notice: NO employee-related columns
        }
    ])
    
    # Kaiser SCR data (has building/location info)
    kaiser_data = pd.DataFrame([
        {
            'Building Code': 'CO203-1',
            'GL LOC': '16203',
            'Service Area': 'Northern Colorado',
            'Building Name': 'Test Building'
        }
    ])
    
    test_cases = [
        (bci_data, "BCI Details", "BCI data structure"),
        (aus_data, "AUS Details", "AUS data structure"),
        (master_data, "Release", "Master invoice structure"),  # Should detect Release due to Release Date column
        (kaiser_data, "Kaiser SCR Building Data", "Kaiser SCR structure")
    ]
    
    all_passed = True
    
    for df, expected_type, description in test_cases:
        detected_type = mapper.detect_file_type_from_content(df)
        
        if detected_type == expected_type:
            print(f"   ✅ {description:<25} → {detected_type}")
        else:
            print(f"   ❌ {description:<25} → {detected_type} (expected {expected_type})")
            all_passed = False
    
    return all_passed

def test_comprehensive_detection():
    """Test the comprehensive auto-detection method"""
    print("\n🧪 Testing comprehensive auto-detection...")
    
    mapper = EnhancedDataMapper()
    
    # Create sample data for testing
    sample_data = pd.DataFrame([
        {
            'Invoice Number': '12345',
            'Employee Number': '67890',
            'First': 'John',
            'Last': 'Doe'
        }
    ])
    
    # Test with your specific filenames
    test_cases = [
        ("TLM_BCI.xlsx", sample_data, "BCI Details"),
        ("AUS_Invoice.xlsx", sample_data, "AUS Details"),  # Will override content detection
        ("unknown_bci_file.xlsx", sample_data, "BCI Details")  # Should fall back to content
    ]
    
    all_passed = True
    
    for filename, df, expected_type in test_cases:
        detected_type = mapper.auto_detect_file_type(filename, df)
        
        if detected_type == expected_type:
            print(f"   ✅ {filename:<25} → {detected_type}")
        else:
            print(f"   ❌ {filename:<25} → {detected_type} (expected {expected_type})")
            all_passed = False
    
    return all_passed

def main():
    """Run all auto-detection tests"""
    print("🤖 AUTO-DETECTION TEST SUITE")
    print("Testing recognition of your specific file names")
    print()
    
    try:
        # Test filename detection
        filename_test = test_filename_detection()
        
        # Test content detection
        content_test = test_content_detection()
        
        # Test comprehensive detection
        comprehensive_test = test_comprehensive_detection()
        
        print("\n" + "=" * 50)
        
        if filename_test and content_test and comprehensive_test:
            print("🎉 ALL AUTO-DETECTION TESTS PASSED!")
            print()
            print("✅ Your specific file names are recognized:")
            print("   - TLM_BCI.xlsx → BCI Details")
            print("   - AUS_Invoice.xlsx → AUS Details") 
            print()
            print("✅ Content-based detection works as fallback")
            print("✅ Comprehensive detection prioritizes filename over content")
            print()
            print("🚀 Ready to use auto-detection!")
            print("   Run: streamlit run invoice_app_auto_detect.py")
            
        else:
            print("❌ SOME TESTS FAILED")
            print("Please check the failed tests above")
        
    except Exception as e:
        print(f"❌ TEST ERROR: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
