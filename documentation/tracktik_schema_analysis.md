# TrackTik ETL Schema Analysis and Alignment Guide

## Executive Summary
This document analyzes the discrepancies between the current Python ETL code and the actual PostgreSQL schema for the TrackTik data warehouse. Significant differences were found in table structures, naming conventions, and data processing patterns that need to be addressed.

## Schema Comparison Analysis

### 1. Table Naming Discrepancies

| Current Code | Actual Schema | Notes |
|--------------|---------------|-------|
| `region_mapping` | `dim_regions` | Follows dimensional modeling convention |
| `etl_run_log` | `etl_sync_status` + `etl_batches` | Split into two specialized tables |
| Direct table references | Partitioned `fact_shifts` | Must use partition-aware inserts |

### 2. Dimension Table Structure Differences

#### Current Code Structure
```python
# Simple structure with basic fields
{
    'tracktik_id': INTEGER,
    'name': VARCHAR,
    'active': BOOLEAN,
    'valid_from': TIMESTAMP
}
```

#### Actual Schema Structure
```sql
-- Full SCD Type 2 implementation
{
    surrogate_key: BIGSERIAL PRIMARY KEY,
    employee_id: INTEGER NOT NULL,  -- Note: not tracktik_id
    -- dimension attributes --
    valid_from: TIMESTAMPTZ NOT NULL,
    valid_to: TIMESTAMPTZ,
    is_current: BOOLEAN NOT NULL,
    etl_batch_id: UUID
}
```

**Key Differences:**
- Uses `surrogate_key` as primary key
- ID fields named differently (e.g., `employee_id` not `tracktik_id`)
- Full temporal tracking with `valid_from`/`valid_to`
- Includes `etl_batch_id` for lineage tracking
- Uses `TIMESTAMPTZ` for timezone awareness

### 3. Fact Table Structure

#### Major Differences:
1. **Partitioning**: Fact table is partitioned by `billing_period_id`
2. **Additional Columns**: 
   - `raw_data JSONB` - stores complete API response
   - Separate hours columns: `scheduled_hours`, `clocked_hours`, `approved_hours`, `billable_hours`, `payable_hours`
   - Billing calculations: `bill_overtime_hours`, `bill_overtime_impact`, `bill_total`
3. **Primary Key**: Composite key `(billing_period_id, shift_id)`

### 4. ETL Support Tables

#### etl_batches (Not Currently Used)
```sql
CREATE TABLE etl_batches (
    batch_id UUID PRIMARY KEY,
    batch_type VARCHAR(50),
    status VARCHAR(20),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    records_processed INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    metadata JSONB
);
```

#### etl_sync_status (Different Structure)
Current code expects: `sync_type`, `last_sync_time`, `status`, `records_processed`  
Actual schema has: `table_name`, `last_sync_timestamp`, `last_successful_batch_id`, `sync_metadata`

## Required Code Changes

### 1. Update models.py

**SCD Type 2 Implementation**
```python
def upsert_dimension(self, table: str, records: List[Dict], 
                    natural_key: str, tracking_fields: List[str],
                    batch_id: UUID) -> Dict[str, int]:
    """
    Proper SCD Type 2 upsert:
    1. Check if record exists by natural key
    2. If exists and changed:
       - Set valid_to = NOW() and is_current = FALSE on old record
       - Insert new record with is_current = TRUE
    3. Add etl_batch_id to all records
    """
```

### 2. Update kaiser_billing_processor.py

**Required Changes:**
1. Create ETL batch at start of processing
2. Calculate `billing_period_id` using database function
3. Store raw API responses in JSONB
4. Map fields correctly to schema
5. Handle partitioned inserts

**Example Flow:**
```python
# 1. Start ETL batch
batch_id = create_etl_batch('kaiser_billing_period', metadata)

# 2. For each shift, calculate billing period
billing_period_id = db.execute_query(
    "SELECT get_billing_period(%(shift_date)s)",
    {'shift_date': shift_date}
)

# 3. Store with raw data
shift_record = {
    'shift_id': shift['id'],
    'billing_period_id': billing_period_id,
    'raw_data': json.dumps(shift),  # Complete API response
    'etl_batch_id': batch_id,
    # ... other fields
}
```

### 3. Fix Region Sync

**Update to use dim_regions:**
```sql
INSERT INTO tracktik.dim_regions (
    region_id,  -- Note: not surrogate_key
    custom_id,
    name,
    parent_region_id,
    parent_region_name,
    valid_from,
    is_current,
    etl_batch_id
) VALUES (...)
ON CONFLICT (region_id) WHERE is_current = TRUE
DO UPDATE SET ...
```

### 4. Implement Missing Business Logic

**Hours Calculations:**
- `billable_hours`: Hours that can be billed to client
- `payable_hours`: Hours to pay employee (may include non-billable time)
- `bill_overtime_hours`: OT hours for billing
- `bill_overtime_impact`: Additional billing amount due to OT rates

**Bill Rate Logic:**
- `bill_rate_regular`: Standard billing rate
- `bill_rate_effective`: Actual rate used (may include multipliers)

## Migration Strategy

### Phase 1: Schema Alignment (Immediate)
1. Update all model classes to match actual schema
2. Fix column name mappings
3. Implement ETL batch tracking
4. Add raw data storage

### Phase 2: SCD Type 2 Implementation
1. Implement proper temporal tracking
2. Add surrogate key handling
3. Update dimension lookups to use current records only

### Phase 3: Business Logic
1. Add hours calculations
2. Implement billing rate logic
3. Add data quality checks

### Phase 4: Optimization
1. Use partition-aware bulk inserts
2. Implement incremental loading using `etl_sync_status`
3. Add data quality monitoring

## Code Structure Recommendations

### 1. Create Base Classes
```python
class SCDType2Dimension:
    """Base class for all dimension operations"""
    
class PartitionedFact:
    """Base class for partitioned fact table operations"""
    
class ETLBatch:
    """Manages ETL batch lifecycle"""
```

### 2. Separate Concerns
- `transformers/`: Data transformation logic
- `loaders/`: Database loading logic
- `validators/`: Data quality checks
- `calculators/`: Business logic calculations

### 3. Configuration-Driven Mappings
Create mapping configurations for:
- API field → Database column
- Business rule definitions
- Validation rules

## Next Implementation Steps

1. **Immediate Priority:**
   - Fix the models.py to use correct table/column names
   - Update ETL sync status structure
   - Add batch tracking

2. **Short Term:**
   - Implement SCD Type 2 logic
   - Add billing period calculation
   - Store raw API data

3. **Medium Term:**
   - Add hours calculations
   - Implement data quality checks
   - Create monitoring views

## Notes for Future Development

1. **Partitioning Strategy**: The fact table uses LIST partitioning by billing period. Ensure all queries include `billing_period_id` for partition pruning.

2. **Timezone Handling**: All timestamps use `TIMESTAMPTZ`. Ensure proper timezone handling in Python code.

3. **JSONB Usage**: The schema leverages PostgreSQL's JSONB for flexible storage. Use this for evolving API responses.

4. **Performance Considerations**: 
   - Use bulk operations with the partition structure
   - Leverage the provided indexes
   - Consider materialized views for complex aggregations

5. **Data Quality**: The `data_quality_issues` table should be used to track any anomalies or validation failures during ETL.

## Conclusion

The actual schema is more sophisticated than the current code assumes, with proper dimensional modeling, temporal tracking, and partitioning. Aligning the code with this schema will require significant updates but will result in a more robust and maintainable ETL pipeline.