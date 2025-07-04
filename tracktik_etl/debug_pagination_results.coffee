
# debug_pagination Output:
==================================================

Testing with params: {'startDateTime:gte': '2024-06-01', 'startDateTime:lte': '2024-06-02', 'limit': 10, 'offset': 0}
Status: 200

Response structure:
  Keys: ['meta', 'data']
  Number of records: 10
  Meta keys: ['count', 'request', 'itemCount', 'security', 'debug', 'resource', 'limit', 'offset']


Testing second page (offset=10):
  Number of records: 10

# test_with_small_range Output:
==================================================
Testing with debug client...

DEBUG: Starting pagination for /rest/v1/shifts
DEBUG: Initial params: {'startDateTime:gte': '2024-06-01', 'startDateTime:lte': '2024-06-01'}

DEBUG: Page 1, offset=0
DEBUG: Retrieved 10 records
DEBUG: No pagination meta, assuming done

Success! Found 10 shifts total

# test_updated_pagination Output:
Testing updated pagination logic...
==================================================

1. Testing one day of shifts:
Fetching offset 0...
  Got 100 records (total so far: 100)
Fetching offset 100...
  Got 100 records (total so far: 200)
Fetching offset 200...
  Got 100 records (total so far: 300)
Fetching offset 300...
  Got 100 records (total so far: 400)
Fetching offset 400...
  Got 100 records (total so far: 500)
Fetching offset 500...
  Got 100 records (total so far: 600)
Fetching offset 600...
  Got 100 records (total so far: 700)
Fetching offset 700...
  Got 100 records (total so far: 800)
Fetching offset 800...
  Got 100 records (total so far: 900)
Fetching offset 900...
  Got 100 records (total so far: 1000)
Fetching offset 1000...
  Got 100 records (total so far: 1100)
Fetching offset 1100...
  Got 100 records (total so far: 1200)
  Safety limit reached - stopping
Success! Found 1200 shifts
First shift: 2023-06-19T08:00:00-04:00 to 2023-06-19T18:00:00-04:00

2. Testing employees endpoint:
Fetching offset 0...
  Got 100 records (total so far: 100)
Fetching offset 100...
  Got 100 records (total so far: 200)
Fetching offset 200...
  Got 100 records (total so far: 300)
Fetching offset 300...
  Got 100 records (total so far: 400)
Fetching offset 400...
  Got 100 records (total so far: 500)
Fetching offset 500...
  Got 100 records (total so far: 600)
Fetching offset 600...
  Got 100 records (total so far: 700)
Fetching offset 700...
  Got 100 records (total so far: 800)
Fetching offset 800...
  Got 100 records (total so far: 900)
Fetching offset 900...
  Got 100 records (total so far: 1000)
Fetching offset 1000...
  Got 100 records (total so far: 1100)
Fetching offset 1100...
  Got 100 records (total so far: 1200)
  Safety limit reached - stopping
Success! Found 1200 employees