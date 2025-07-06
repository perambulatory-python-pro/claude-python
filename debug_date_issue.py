# Save this as debug_date_issue.py and run it

import sys
import os

# Add parent directories to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper
from datetime import date

# Test if methods exist
mapper = EnhancedDataMapper()

print("=== CHECKING METHODS ===")
print(f"Has clean_amount_value: {hasattr(mapper, 'clean_amount_value')}")
print(f"Has standardize_payment_data: {hasattr(mapper, 'standardize_payment_data')}")
print(f"Has parse_date: {hasattr(mapper, 'parse_date')}")

# Test the standardize_payment_data method
if hasattr(mapper, 'standardize_payment_data'):
    test_data = {
        'payment_id': '1234567890',
        'payment_date': date(2025, 7, 4),  # This is a datetime.date object
        'payment_amount': 1000.00
    }
    
    print("\n=== TESTING STANDARDIZATION ===")
    print(f"Before: payment_date type = {type(test_data['payment_date'])}")
    print(f"Before: payment_date value = {test_data['payment_date']}")
    
    standardized = mapper.standardize_payment_data(test_data)
    
    print(f"After: payment_date type = {type(standardized['payment_date'])}")
    print(f"After: payment_date value = {standardized['payment_date']}")
else:
    print("\n❌ standardize_payment_data method NOT FOUND!")

# Show where the error might be coming from
print("\n=== CHECKING FILE LOCATIONS ===")
import invoice_processing.core.data_mapper_enhanced as dme
print(f"data_mapper_enhanced location: {dme.__file__}")

# Check if there are multiple Python installations
print("\n=== PYTHON INFO ===")
print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")