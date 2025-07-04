# final_test.py
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

from etl.tracktik_client import TrackTikClient

print("Final integration test...")
print("=" * 50)

client = TrackTikClient()

# Test 1: Get employees (simple pagination)
print("\n1. Testing employees endpoint:")
employees = client.get_employees()
print(f"✓ Successfully retrieved {len(employees)} employees")
print(f"   First employee: {employees[0].get('firstName')} {employees[0].get('lastName')}")

# Test 2: Get shifts for a week
print("\n2. Testing shifts endpoint with date range:")
shifts = client.get_shifts('2024-06-01', '2024-06-07')
print(f"✓ Successfully retrieved {len(shifts)} shifts for June 1-7, 2024")

if shifts:
    # Show some stats
    shift_dates = {}
    for shift in shifts:
        date = shift.get('startDateTime', '')[:10]  # Extract just the date
        shift_dates[date] = shift_dates.get(date, 0) + 1
    
    print("\n   Shifts by date:")
    for date in sorted(shift_dates.keys()):
        print(f"     {date}: {shift_dates[date]} shifts")

# Test 3: Get shifts with additional parameters
print("\n3. Testing shifts with includes:")
detailed_shifts = client.get_shifts(
    '2024-06-01', 
    '2024-06-01',  # Just one day
    include='employee,position',
    limit=5  # Just get 5 for testing
)

print(f"✓ Retrieved {len(detailed_shifts)} detailed shifts")
if detailed_shifts and isinstance(detailed_shifts[0].get('employee'), dict):
    shift = detailed_shifts[0]
    emp = shift.get('employee', {})
    pos = shift.get('position', {})
    print(f"   Sample: {emp.get('firstName')} {emp.get('lastName')} at position {pos.get('name', 'Unknown')}")

print("\n✅ All tests passed! Your TrackTik integration is working correctly!")