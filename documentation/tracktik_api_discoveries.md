# TrackTik API Key Discoveries and Best Practices

## Overview
This document summarizes key discoveries made during the implementation of TrackTik ETL pipelines for KAISER billing period processing, focusing on API behavior, data patterns, and architectural decisions.

## Critical API Behavior Discoveries

### 1. API Parameter Limitations
**Discovery**: Several documented API parameters cause 400 errors when used with the TrackTik API.

**Failed Parameters**:
- `include` parameter causes API failures when requesting shifts
- `account.id:in` filter format is not supported for shifts endpoint
- Sort parameters are often rejected

**Working Approach**:
```python
# Instead of using include parameter:
params = {
    'employee.region': region_id  # Filter by employee region
}
# Avoid: include='employee,position,account'
```

**Impact**: API calls must be simplified to avoid documented but non-functional parameters.

### 2. Data Structure Inconsistencies
**Discovery**: API responses contain different data formats than expected from documentation.

**Key Findings**:
- `region` field returns integer ID, not object with details
- `account` field in shifts is always `None` (not useful for client identification)
- Shifts contain only ID references, not full nested objects
- Employee data in shifts is minimal (ID only)

**Example**:
```json
{
  "shift": {
    "id": 12345,
    "employee": 4567,  // Just ID, not object
    "position": 8901,  // Just ID, not object
    "account": null    // Always null in our testing
  }
}
```

### 3. Regional Hierarchy Structure
**Discovery**: KAISER operates as a parent region with 8 sub-regions where actual operations occur.

**Regional Structure**:
```
KAISER (Parent Region)
├── N California (customId: Kaiser-NCA)
├── S California (customId: Kaiser-SCA)  
├── Hawaii (customId: Kaiser-HI)
├── Washington (customId: Kaiser-WA)
├── Colorado (customId: Kaiser-CO)
├── Georgia (customId: Kaiser-GA)
├── MidAtlantic (customId: Kaiser-MidAtlantic)
└── Northwest (customId: 1029)
```

**Filtering Strategy**: Use `employee.region` filter with sub-region IDs, not parent region.

## Data Architecture Insights

### 4. Employee Data Management Strategy
**Discovery**: Timing conflicts occur when both billing period processing and employee sync attempt to manage employee dimensions.

**Problem**: 
- Billing periods encounter new employees not yet in database
- Simple UPDATE approach conflicts with SCD Type 2 temporal tracking
- Employee sync timing doesn't align with shift processing needs

**Solution Implemented**:
```python
# Billing Period Processing:
# 1. Create minimal employee records for new employees
minimal_employee = {
    'employee_id': emp_id,
    'region_id': region_id,
    'status': 'ACTIVE',
    # All other fields: None
}

# 2. Employee Sync Script (separate):
# - Fetches complete employee data from API
# - Updates minimal records with full information
# - Uses SCD Type 2 for change tracking
```

### 5. Client-Position Relationship Mapping
**Discovery**: Shifts don't contain direct client information, requiring lookup through positions.

**Data Flow**:
```
Shift → Position ID → Position Table → Client ID
```

**Implementation**:
```python
# Query positions to get client mappings
position_client_map = {}
query = """
    SELECT position_id, client_id 
    FROM dim_positions 
    WHERE position_id = ANY(%(position_ids)s) 
    AND is_current = TRUE
"""
```

**Gotcha**: Some positions (rare cases) may not have client mappings - filter these out.

## Database Constraints and SCD Type 2

### 6. Temporal Exclusion Constraints
**Discovery**: PostgreSQL exclusion constraints prevent overlapping time ranges for the same employee.

**Constraint Pattern**:
```sql
CONSTRAINT no_overlap_employee 
EXCLUDE USING gist (employee_id WITH =, tstzrange(valid_from, valid_to) WITH &&)
```

**Conflict Scenario**:
- Employee 4203 has record: `valid_from: 03:38:48, valid_to: 14:28:31`
- New record attempts: `valid_from: 10:28:31, valid_to: NULL`
- Result: Overlapping time ranges = constraint violation

**Solution**: Separate employee management from shift processing entirely.

### 7. Billing Period Partitioning
**Discovery**: fact_shifts table is partitioned by billing_period_id.

**Benefits**:
- Improved query performance
- Easier data management
- Partition pruning for date range queries

**Requirement**: All fact table operations must include `billing_period_id` in the WHERE clause.

## ETL Pipeline Architecture Decisions

### 8. Separation of Concerns Strategy
**Final Architecture**:

```
┌─────────────────────────────────────────────────────────┐
│                 Billing Period Processing               │
│  - Processes shifts for specific date ranges            │
│  - Creates minimal employee records for new employees   │
│  - References existing clients and positions            │
│  - Fast, focused on transactional data                  │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                   Employee Sync Script                  │
│  - Fetches complete employee data from API              │
│  - Updates minimal records with full information        │
│  - Runs weekly/monthly (separate schedule)              │
│  - Handles SCD Type 2 properly                          │
└─────────────────────────────────────────────────────────┘
```

### 9. Error Handling and Data Quality
**Discoveries**:
- API can return employees not in company database
- Some shifts may reference non-existent positions
- Client mappings may be incomplete

**Quality Controls Implemented**:
```python
# Filter shifts with missing references
valid_shifts = []
for shift in transformed_shifts:
    employee_id = shift.get('employee_id')
    client_id = shift.get('client_id')
    
    if employee_id in existing_emp_ids and client_id is not None:
        valid_shifts.append(shift)
    else:
        # Log and track data quality issues
        stats['data_quality_issues'] += 1
```

## Performance Optimizations

### 10. Batch Processing Strategies
**Region-by-Region Processing**:
- Process each KAISER sub-region separately
- Provides checkpointing and progress tracking
- Allows for granular error handling
- Enables parallel processing in future

**API Pagination**:
- Use `limit=1000` for employee endpoints
- Implement proper pagination for large datasets
- Track API call counts for rate limiting

### 11. Database Bulk Operations
**Partition-Aware Inserts**:
```python
# Use partition-aware bulk inserts
db.insert_fact_batch_partitioned(
    'fact_shifts', 
    transformed_shifts, 
    'billing_period_id'
)
```

**Dimension Lookups**:
- Batch dimension lookups to reduce database round trips
- Cache region mappings during processing
- Use EXISTS queries for employee validation

## API Best Practices Summary

### DO:
- ✅ Filter by `employee.region` for regional data
- ✅ Use simple parameter sets to avoid API errors  
- ✅ Implement defensive programming for data format variations
- ✅ Separate dimension management from fact processing
- ✅ Use batch operations for database inserts
- ✅ Implement comprehensive logging and error tracking

### DON'T:
- ❌ Use `include` parameter with shifts endpoint
- ❌ Rely on `account` field in shift responses
- ❌ Mix employee updates with shift processing
- ❌ Assume API documentation matches actual behavior
- ❌ Process all regions in single transaction
- ❌ Ignore data quality issues silently

## Future Considerations

### Potential Enhancements:
1. **Parallel Processing**: Process multiple regions concurrently with proper rate limiting
2. **Incremental Updates**: Only process new/changed shifts since last run
3. **Real-time Monitoring**: Implement data quality dashboards
4. **API Rate Limiting**: Add intelligent throttling for large data volumes
5. **Error Recovery**: Implement retry logic with exponential backoff

### Monitoring Metrics:
- Shifts processed per region per billing period
- Employee records created vs updated
- Data quality issue rates
- API response times and error rates
- Database constraint violations

---

*This document should be updated as new discoveries are made during TrackTik API integration work.*