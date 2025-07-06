# Save this as trace_date_issue.py and run it

import sys
import os
import pandas as pd
from datetime import date, datetime

# Add parent directories to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper

# Create test data similar to your payment file
test_data = {
    'Payment ID': ['100097420'],
    'Payment Date': ['07/04/2025'],  # This might be the issue
    'Payment Amount': [1000.00],
    'Invoice ID': ['INV001'],
    'Gross Amount': [1000.00],
    'Discount': [0],
    'Net Amount': [1000.00]
}

df = pd.DataFrame(test_data)
mapper = EnhancedDataMapper()

print("=== TESTING PAYMENT PROCESSING FLOW ===")

# Test 1: Check what convert_excel_date returns
print("\n1. Testing convert_excel_date:")
test_date = df.iloc[0]['Payment Date']
print(f"   Input: {test_date} (type: {type(test_date)})")
result = mapper.convert_excel_date(test_date)
print(f"   Output: {result} (type: {type(result)})")

# Test 2: Test extract_payment_master_data
print("\n2. Testing extract_payment_master_data:")
try:
    master_data = mapper.extract_payment_master_data(df)
    print(f"   payment_date: {master_data.get('payment_date')} (type: {type(master_data.get('payment_date'))})")
    print(f"   Full master_data: {master_data}")
except Exception as e:
    print(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test map_kp_payment_excel
print("\n3. Testing map_kp_payment_excel:")
try:
    mapped_records = mapper.map_kp_payment_excel(df)
    if mapped_records:
        first_record = mapped_records[0]
        print(f"   payment_date in first record: {first_record.get('payment_date')} (type: {type(first_record.get('payment_date'))})")
        print(f"   First record keys: {list(first_record.keys())}")
except Exception as e:
    print(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Check if the date converter was patched
print("\n4. Checking if date converter was patched:")
print(f"   convert_excel_date method: {mapper.convert_excel_date}")
print(f"   Is it the original method? {mapper.convert_excel_date.__name__}")

# Test 5: Direct test of problem date
print("\n5. Testing the specific date from your error:")
problem_date = date(2025, 7, 4)
print(f"   Input: {problem_date} (type: {type(problem_date)})")
try:
    converted = mapper.convert_excel_date(problem_date)
    print(f"   Converted: {converted} (type: {type(converted)})")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 6: Check standardize_payment_data
print("\n6. Testing standardize_payment_data:")
if hasattr(mapper, 'standardize_payment_data'):
    test_data_with_date = {
        'payment_id': '100097420',
        'payment_date': date(2025, 7, 4),
        'payment_amount': 1000.00
    }
    standardized = mapper.standardize_payment_data(test_data_with_date)
    print(f"   payment_date after standardization: {standardized.get('payment_date')} (type: {type(standardized.get('payment_date'))})")
else:
    print("   standardize_payment_data method NOT FOUND!")