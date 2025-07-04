# TrackTik API Integration Troubleshooting Guide

## Overview
This document chronicles the complete troubleshooting process for fixing the TrackTik API integration, including all issues encountered, debugging approaches, and final solutions.

## Initial Problem
The TrackTik API integration was failing with a `400 Bad Request` error when attempting to retrieve employee data:
```
✗ API connection failed: 400 Client Error: Bad Request for url: https://blackstoneconsulting.staffr.us/rest/v1/employees?limit=100&page=1
```

## Root Causes Discovered

### 1. Pagination Parameter Issue
**Problem**: The API was using `page` parameter for pagination
```python
params = {'limit': 100, 'page': 1}  # ❌ Incorrect
```

**Error Message**: `"page is not a field in employees"`

**Solution**: TrackTik uses offset-based pagination
```python
params = {'limit': 100, 'offset': 0}  # ✅ Correct
```

### 2. Date Filtering Issues
**Problem**: Date filters using `:gte` and `:lte` operators were being ignored
```python
params = {
    'startDateTime:gte': '2024-06-01',  # ❌ Doesn't filter
    'startDateTime:lte': '2024-06-30'   # ❌ Doesn't filter
}
```

**Solution**: Use `:between` operator with pipe delimiter
```python
params = {
    'startDateTime:between': '2024-06-01|2024-06-30'  # ✅ Works correctly
}
```

### 3. Pagination Loop Issues
**Problem**: The pagination logic was looking for `meta.pagination` which doesn't exist in TrackTik's response

**TrackTik's Actual Response Structure**:
```json
{
    "meta": {
        "count": 907949,      // Total matching records
        "itemCount": 100,     // Records in this response
        "limit": 100,
        "offset": 0
    },
    "data": [...]
}
```

**Solution**: Use `meta.count` for total records and proper stop conditions

## Debugging Process

### Step 1: Environment Verification
```python
# test_connection.py confirmed:
✓ Environment variables loaded correctly
✓ Database connection working
✓ Authentication successful
✗ API calls failing
```

### Step 2: Isolate the Issue
Created minimal test to identify exact parameter causing failure:
```python
# Test different parameter combinations
test_cases = [
    {'limit': 1},                    # ✅ Works
    {'limit': 100},                  # ✅ Works
    {'limit': 100, 'page': 1},       # ❌ Fails - "page is not a field"
    {'limit': 100, 'offset': 0},     # ✅ Works
]
```

### Step 3: Date Filter Investigation
Discovered date filters weren't working by examining returned data:
```python
# Requested: 2024-06-01
# Returned: 2023-06-19T08:00:00-04:00  # Wrong year!
```

Tested multiple date formats:
```python
# All these formats were ignored:
'2024-06-01'                    # Simple date
'2024-06-01T00:00:00'          # With time
'2024-06-01T00:00:00Z'         # With UTC
'2024-06-01T00:00:00+00:00'   # With timezone

# This format works:
'startDateTime:between': '2024-06-01|2024-06-07'  # ✅
```

### Step 4: API Requirements Discovery
Found that TrackTik API requires at least one filter:
```
filters
required
object non-empty
At least one of the filters (on properties or scope) is required.
```

## Final Working Implementation

### Updated Pagination Method
```python
def get_paginated_data(self, endpoint: str, params: Dict[str, Any] = None) -> List[Dict]:
    """Get all pages of data from an endpoint"""
    if params is None:
        params = {}
        
    all_records = []
    offset = 0
    total_count = None
    
    while True:
        current_params = params.copy()
        current_params.update({
            'limit': config.API_PAGE_SIZE,
            'offset': offset  # Use offset, not page
        })
        
        response = self.session.get(
            f"{self.base_url}{endpoint}",
            headers=self._get_headers(),
            params=current_params
        )
        response.raise_for_status()
        
        data = response.json()
        records = data.get('data', [])
        all_records.extend(records)
        
        # Get total from meta.count
        meta = data.get('meta', {})
        if total_count is None and 'count' in meta:
            total_count = meta['count']
            logger.info(f"Fetching {endpoint}: {total_count} total records")
        
        # Stop conditions
        if not records:
            break
        if total_count and len(all_records) >= total_count:
            break
        if len(records) < config.API_PAGE_SIZE:
            break
            
        offset += config.API_PAGE_SIZE
        
    return all_records
```

### Updated Shifts Method
```python
def get_shifts(self, start_date: str, end_date: str, **kwargs) -> List[Dict]:
    """Get shifts for a date range"""
    # Validate date range (max 31 days)
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if (end - start).days > 31:
        raise ValueError("Date range cannot exceed 31 days")
    
    # Use :between filter
    params = {
        'startDateTime:between': f'{start_date}|{end_date}',
        **kwargs
    }
    
    return self.get_paginated_data('/rest/v1/shifts', params)
```

## Test Results

### Final Integration Test Output
```
1. Testing employees endpoint:
✓ Successfully retrieved 5100 employees
   First employee: TrackTik Support

2. Testing shifts endpoint with date range:
✓ Successfully retrieved 8628 shifts for June 1-7, 2024
   Shifts by date:
     2024-06-01: 902 shifts
     2024-06-02: 885 shifts
     2024-06-03: 1340 shifts
     2024-06-04: 1345 shifts
     2024-06-05: 1401 shifts
     2024-06-06: 1324 shifts
     2024-06-07: 1431 shifts

3. Testing shifts with includes:
✓ Retrieved 902 detailed shifts
   Sample: EMMANUEL ASIGBETSE at position ED SPECIALIST
```

## Key Learnings

### API Integration Lessons
1. **Always test API responses**: Documentation may be outdated or incomplete
2. **Start with minimal parameters**: Build up complexity gradually
3. **Examine actual response structure**: Don't assume standard formats
4. **Check if filters are working**: Verify returned data matches your criteria

### Python Debugging Techniques
1. **Isolate variables**: Test one parameter at a time
2. **Create minimal reproducible examples**: Simplify to find exact issue
3. **Add logging/debug output**: Understand what's happening at each step
4. **Handle edge cases**: Empty results, rate limits, large datasets

### TrackTik API Specifics
- Uses offset/limit pagination (not page/limit)
- Requires at least one filter parameter
- Date ranges limited to 31 days
- `:between` operator works, `:gte`/`:lte` don't for dates
- `meta.count` contains total matching records
- Use pipe `|` delimiter for between ranges

## Quick Reference

### Working Examples
```python
# Get employees
employees = client.get_employees()

# Get shifts for a date range
shifts = client.get_shifts('2024-06-01', '2024-06-07')

# Get shifts with related data
detailed_shifts = client.get_shifts(
    '2024-06-01',
    '2024-06-07',
    include='employee,position',
    status='APPROVED'
)

# Using scopes
current_shifts = client.get_paginated_data(
    '/rest/v1/shifts',
    {'scope': 'IS_CURRENT'}
)
```

### Common Parameters
- `limit`: Max records per request (default 100)
- `offset`: Starting position for pagination
- `include`: Comma-separated related objects to include
- `scope`: Predefined filters like IS_CURRENT, APPROVED
- Date filters: Use `:between` with pipe delimiter

## Troubleshooting Checklist

If you encounter issues:

1. ✓ Check environment variables are set
2. ✓ Verify authentication is working
3. ✓ Test with minimal parameters first
4. ✓ Check actual response structure
5. ✓ Verify filters are being applied
6. ✓ Look for rate limit headers
7. ✓ Check date formats and ranges
8. ✓ Ensure required filters are present

## Files Created During Debugging

- `debug_api.py` - Initial connection testing
- `debug_shifts.py` - Shift endpoint testing
- `debug_pagination.py` - Pagination structure analysis
- `debug_date_filters.py` - Date format testing
- `test_between_filter.py` - Between operator validation
- `final_test.py` - Complete integration test

These debug scripts can be kept for future troubleshooting or removed after verification.

## Conclusion

Through systematic debugging and testing, we successfully:
- Fixed pagination to use offset-based approach
- Corrected date filtering using `:between` operator
- Implemented proper pagination stop conditions
- Created a robust, working TrackTik API integration

The integration is now production-ready and can reliably extract employee and shift data for ETL processes.