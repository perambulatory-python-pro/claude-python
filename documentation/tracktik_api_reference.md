markdown# TrackTik API Reference for Future Development

## Overview
This document contains all necessary information for building TrackTik API integrations. Use this as a reference when working with Claude or other developers on API-related tasks.

## API Connection Details

### Base Configuration
- **Base URL**: `https://blackstoneconsulting.staffr.us`
- **API Version**: `/rest/v1`
- **Authentication Type**: OAuth2 Password Flow
- **Token Endpoint**: `https://blackstoneconsulting.staffr.us/rest/oauth2/access_token`
- **Token Expiry**: 3600 seconds (1 hour)

### Environment Variables Required
```env
TRACKTIK_CLIENT_ID=your_client_id
TRACKTIK_CLIENT_SECRET=your_client_secret
TRACKTIK_USERNAME=your_username
TRACKTIK_PASSWORD=your_password
Working Authentication Code
pythonimport os
import requests
from dotenv import load_dotenv

load_dotenv()

# OAuth2 Password Flow
auth_data = {
    'client_id': os.getenv('TRACKTIK_CLIENT_ID'),
    'client_secret': os.getenv('TRACKTIK_CLIENT_SECRET'),
    'username': os.getenv('TRACKTIK_USERNAME'),
    'password': os.getenv('TRACKTIK_PASSWORD'),
    'grant_type': 'password'
}

response = requests.post(
    'https://blackstoneconsulting.staffr.us/rest/oauth2/access_token',
    data=auth_data
)

tokens = response.json()
access_token = tokens['access_token']
refresh_token = tokens['refresh_token']
API Request Format
Headers Required for All API Calls
pythonheaders = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json'
}
Response Structure
All API responses follow this format:
json{
    "data": [...],  // Array of objects
    "meta": {       // Pagination info
        "pagination": {
            "total": 100,
            "count": 20,
            "per_page": 20,
            "current_page": 1,
            "total_pages": 5
        }
    }
}
Tested and Working Endpoints
1. Employees

Endpoint: /rest/v1/employees
Method: GET
Purpose: Retrieve employee/guard information
Key Fields: id, firstName, lastName, email, customId
Useful Filters:

status: Filter by ACTIVE/INACTIVE
region.id: Filter by region



2. Sites/Clients

Endpoint: /rest/v1/clients
Method: GET
Purpose: Retrieve client/site information
Key Fields: id, name, customId, region
Note: Sites may use customId as primary identifier (e.g., "CA AMS")

3. Shifts

Endpoint: /rest/v1/shifts
Method: GET
Purpose: Retrieve scheduled shifts for billing
Key Fields: id, startedOn, endedOn, hoursCount, position, employee
Critical Filters:
pythonparams = {
    'startedOn:gte': '2024-01-01',  # Start date
    'startedOn:lte': '2024-01-31',  # End date
    'client.id': 123,               # Filter by client
    'status': 'APPROVED'            # Only approved shifts
}


Common Query Patterns
Date Filtering
python# Date range queries
'startedOn:gte': '2024-01-01'  # Greater than or equal
'startedOn:lte': '2024-01-31'  # Less than or equal
'startedOn:between': '2024-01-01|2024-01-31'  # Between dates

# Relative dates
'startedOn:gte': 'yesterday'
'startedOn:lte': 'today'
Pagination
pythonparams = {
    'limit': 100,    # Max 100 per page
    'offset': 0,     # Start from record 0
    'page': 1        # Or use page number
}
Including Related Data
pythonparams = {
    'include': 'employee,position,client'  # Get full objects instead of just IDs
}
Sorting
pythonparams = {
    'sort': '-startedOn'  # Descending by start date
}
Other Available Endpoints (Per Documentation)
Scheduling & Time

/rest/v1/work-sessions - Actual worked hours
/rest/v1/schedule-off-periods - Time off
/rest/v1/positions - Security posts/positions
/rest/v1/break-sessions - Break tracking

Billing Related

/rest/v1/bill-items - Billable items
/rest/v1/invoices - Invoice data
/rest/v1/contracts - Client contracts
/rest/v1/pay-codes - Pay code definitions

Payroll Related

/rest/v1/payroll-codes - Payroll codes
/rest/v1/overtime-rules - OT calculations
/rest/v1/holiday-groups - Holiday definitions

Business Context
Use Cases

Weekly Billing: Pull approved shifts for date range, group by client
Payroll Export: Get work sessions with employee details
Client Validation: Match shifts to SCR (Security Coverage Requests)
Invoice Reconciliation: Compare TrackTik hours to invoice details

Important Business Rules

Shifts must be APPROVED status for billing
Use customId field to match with internal systems
Building codes should match SCR format: building_code + postPositionCode
Pay types: Regular, Overtime, Holiday, Sick, Vacation

Error Handling
Common Status Codes

200: Success
401: Authentication failed/token expired
403: Permission denied
404: Resource not found
429: Rate limited
500: Server error

Rate Limits

Check headers: X-RateLimit-Remaining, X-RateLimit-Reset
If 429 error, wait for Retry-After header value

Next Development Steps

Implement Pagination - Handle results > 100 records
Add Work Sessions - Get actual vs scheduled hours
Create Billing Reports - Aggregate by client/week
Handle Token Refresh - Auto-refresh before expiry
Add Error Retry Logic - Handle network/timeout issues
Build SCR Matching - Compare TrackTik to SCR data

Sample Advanced Query
python# Get all approved shifts for a client in a date range
def get_billable_hours(client_id, start_date, end_date):
    url = f"{base_url}/rest/v1/shifts"
    
    all_shifts = []
    page = 1
    
    while True:
        params = {
            'client.id': client_id,
            'startedOn:gte': start_date,
            'startedOn:lte': end_date,
            'status': 'APPROVED',
            'include': 'employee,position',
            'limit': 100,
            'page': page
        }
        
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        
        all_shifts.extend(data['data'])
        
        if page >= data['meta']['pagination']['total_pages']:
            break
            
        page += 1
    
    return all_shifts
Notes for Future Development

The API uses REST standards - GET for reading, POST for creating, PUT for updating
All dates should be in YYYY-MM-DD format
The include parameter is powerful - use it to reduce API calls
Consider caching frequently accessed data (employees, positions)
Always handle token expiration gracefully