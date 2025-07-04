# test_data_extraction.py
from etl.tracktik_client import TrackTikClient
from datetime import datetime

client = TrackTikClient()

# Get employees
print("Fetching employees...")
employees = client.get_employees()
print(f"Found {len(employees)} employees")

# Show first few employees
for emp in employees[:3]:
    print(f"  - {emp.get('firstName')} {emp.get('lastName')} (ID: {emp.get('id')})")

# Get recent shifts - let's try a more recent date range
print("\nFetching June 2024 shifts...")
shifts = client.get_shifts('2024-06-01', '2024-06-30')
print(f"Found {len(shifts)} shifts")

# Show some shift details
if shifts:
    print("\nFirst few shifts:")
    for shift in shifts[:3]:
        print(f"  - Shift ID: {shift.get('id')}")
        print(f"    Start: {shift.get('startDateTime')}")
        print(f"    End: {shift.get('endDateTime')}")
        if 'employee' in shift and shift['employee']:
            emp = shift['employee']
            print(f"    Employee: {emp.get('firstName')} {emp.get('lastName')}")
        if 'account' in shift and shift['account']:
            print(f"    Client: {shift['account'].get('name')}")
        print()