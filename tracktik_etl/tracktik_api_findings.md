# Date filtering that works:
params = {
    'startDateTime:between': '2024-06-01|2024-06-07',  # Date range
    'include': 'employee,position',  # Include related data
    'status': 'APPROVED'  # Optional status filter
}

# Pagination uses:
- meta.count: Total records matching query
- offset/limit: For pagination
- Stop when: no records, all retrieved, or less than limit

# Key endpoints:
- /rest/v1/employees
- /rest/v1/shifts
- /rest/v1/clients
- /rest/v1/positions