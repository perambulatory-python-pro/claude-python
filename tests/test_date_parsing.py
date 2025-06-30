# test_date_parsing.py
from data_mapper_enhanced import EnhancedDataMapper

mapper = EnhancedDataMapper()

# Test the problematic dates from your log
test_dates = [
    "12/10/2024 12:00:00 AM",
    "12/11/2024 12:00:00 AM", 
    "12/12/2024 12:00:00 AM"
]

for date_str in test_dates:
    result = mapper.parse_kaiser_date(date_str)
    print(f"'{date_str}' -> '{result}'")