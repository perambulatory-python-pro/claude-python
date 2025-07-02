# Test script - save as test_payment_mapper.py
import pandas as pd
from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper  

# Create test data
test_data = {
    'Payment ID': ['TEST123'] * 3,
    'Payment Date': ['06/23/25'] * 3,
    'Payment Amount': [3000.00] * 3,
    'Invoice ID': ['INV001', 'INV002', 'INV003'],
    'Gross Amount': [1000.00, 1200.00, 800.00],
    'Discount': [0, 0, 0],
    'Net Amount': [1000.00, 1200.00, 800.00]
}

df = pd.DataFrame(test_data)

# Test your mapper
mapper = EnhancedDataMapper()

# Test auto-detection
file_type = mapper.auto_detect_file_type("Payment_test.xlsx", df)
print(f"Detected file type: {file_type}")

if file_type == "KP_Payment_Excel":
    # Test extraction
    master_data = mapper.extract_payment_master_data(df)
    print(f"Master data: {master_data}")
    
    # Test mapping
    detail_records = mapper.map_payment_details(df)
    print(f"Mapped {len(detail_records)} detail records")
    
    # Test validation
    validation = mapper.validate_payment_data(master_data, detail_records)
    print(f"Validation passed: {validation['is_valid']}")