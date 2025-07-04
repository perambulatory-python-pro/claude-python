from etl.tracktik_client import TrackTikClient


client = TrackTikClient()
# Get employees
employees = client.get_employees()
print(f"Found {len(employees)} employees")

# Get recent shifts
shifts = client.get_shifts('2024-01-01', '2024-01-31')
print(f"Found {len(shifts)} shifts in January")