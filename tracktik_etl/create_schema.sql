-- =====================================================
-- TrackTik Data Warehouse Schema for PostgreSQL
-- Bi-weekly Billing Period Partitioned
-- PostgreSQL-specific optimizations included
-- =====================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS tracktik;
SET search_path TO tracktik;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gist";  -- For exclusion constraints

-- =====================================================
-- Reference Tables
-- =====================================================

-- Billing periods reference table
CREATE TABLE billing_periods (
    period_id VARCHAR(10) PRIMARY KEY, -- Format: YYYY_PP (e.g., '2025_01')
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    fiscal_year INTEGER NOT NULL,
    period_number INTEGER NOT NULL CHECK (period_number BETWEEN 1 AND 26),
    quarter INTEGER GENERATED ALWAYS AS (CEIL(period_number::numeric / 6.5)) STORED,
    days_in_period INTEGER GENERATED ALWAYS AS (end_date - start_date + 1) STORED,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- PostgreSQL-specific constraints
    CONSTRAINT uk_fiscal_year_period UNIQUE(fiscal_year, period_number),
    CONSTRAINT chk_date_order CHECK (end_date > start_date),
    
    -- Exclusion constraint to prevent overlapping periods
    CONSTRAINT no_overlap EXCLUDE USING gist (
        daterange(start_date, end_date, '[]') WITH &&
    )
);

-- Create indexes for common queries
CREATE INDEX idx_billing_periods_dates ON billing_periods(start_date, end_date);
CREATE INDEX idx_billing_periods_year ON billing_periods(fiscal_year);

-- =====================================================
-- Helper Functions (PostgreSQL-specific)
-- =====================================================

-- Function to get billing period for a given date
CREATE OR REPLACE FUNCTION get_billing_period(check_date DATE)
RETURNS VARCHAR(10) AS $$
BEGIN
    RETURN (
        SELECT period_id 
        FROM billing_periods 
        WHERE check_date BETWEEN start_date AND end_date
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to generate billing periods for a year
CREATE OR REPLACE FUNCTION generate_billing_periods(year_start INTEGER)
RETURNS VOID AS $$
DECLARE
    period_start DATE;
    period_end DATE;
    period_num INTEGER;
    period_id_text VARCHAR(10);
BEGIN
    -- Start with Dec 27 of previous year
    period_start := make_date(year_start - 1, 12, 27);
    
    FOR period_num IN 1..26 LOOP
        period_end := period_start + INTERVAL '13 days';
        period_id_text := year_start || '_' || LPAD(period_num::text, 2, '0');
        
        INSERT INTO billing_periods (period_id, start_date, end_date, fiscal_year, period_number)
        VALUES (period_id_text, period_start, period_end, year_start, period_num)
        ON CONFLICT (period_id) DO NOTHING;
        
        period_start := period_end + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Generate 2025 billing periods
SELECT generate_billing_periods(2025);

-- =====================================================
-- Dimension Tables with SCD Type 2
-- =====================================================

-- Employee dimension with temporal tracking
CREATE TABLE dim_employees (
    surrogate_key BIGSERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    custom_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    status VARCHAR(20),
    region_id INTEGER,
    region_name VARCHAR(100),
    
    -- SCD Type 2 columns
    valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit columns
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id UUID,
    
    -- PostgreSQL-specific: Exclude overlapping validity periods for same employee
    CONSTRAINT no_overlap_employee EXCLUDE USING gist (
        employee_id WITH =,
        tstzrange(valid_from, valid_to) WITH &&
    )
);

-- Indexes for performance
CREATE INDEX idx_dim_employees_current ON dim_employees(employee_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_employees_temporal ON dim_employees USING gist(employee_id, tstzrange(valid_from, valid_to));

-- Client/Site dimension
CREATE TABLE dim_clients (
    surrogate_key BIGSERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    custom_id VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    region_id INTEGER,
    region_name VARCHAR(100),
    parent_region_name VARCHAR(100),
    time_zone VARCHAR(50),
    address JSONB,  -- PostgreSQL JSONB for flexible address storage
    
    -- SCD Type 2 columns
    valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit columns
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id UUID
);

CREATE INDEX idx_dim_clients_current ON dim_clients(client_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_clients_custom_id ON dim_clients(custom_id) WHERE is_current = TRUE;

-- Position dimension
CREATE TABLE dim_positions (
    surrogate_key BIGSERIAL PRIMARY KEY,
    position_id INTEGER NOT NULL,
    custom_id VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    client_id INTEGER NOT NULL,
    status VARCHAR(20),
    position_type VARCHAR(50),
    
    -- Denormalized for performance
    client_name VARCHAR(255),
    client_custom_id VARCHAR(50),
    
    -- SCD Type 2 columns
    valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit columns
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id UUID
);

CREATE INDEX idx_dim_positions_current ON dim_positions(position_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_positions_client ON dim_positions(client_id) WHERE is_current = TRUE;

-- =====================================================
-- Fact Table (Partitioned by Billing Period)
-- =====================================================

-- Main shifts fact table (partitioned)
CREATE TABLE fact_shifts (
    shift_id BIGINT NOT NULL,
    billing_period_id VARCHAR(10) NOT NULL,
    
    -- Dimensions
    employee_id INTEGER NOT NULL,
    position_id INTEGER NOT NULL,
    client_id INTEGER NOT NULL,
    
    -- Dates and times (stored in account timezone)
    shift_date DATE NOT NULL,
    start_datetime TIMESTAMPTZ NOT NULL,
    end_datetime TIMESTAMPTZ NOT NULL,
    
    -- Hours metrics
    scheduled_hours NUMERIC(10,2),
    clocked_hours NUMERIC(10,2),
    approved_hours NUMERIC(10,2),
    billable_hours NUMERIC(10,2),
    payable_hours NUMERIC(10,2),
    
    -- Billing metrics
    bill_rate_regular NUMERIC(10,2),
    bill_rate_effective NUMERIC(10,2),
    bill_overtime_hours NUMERIC(10,2),
    bill_overtime_impact NUMERIC(10,2),
    bill_total NUMERIC(10,2),
    
    -- Status and metadata
    status VARCHAR(20),
    approved_by INTEGER,
    approved_at TIMESTAMPTZ,
    
    -- Store raw API response for validation
    raw_data JSONB,
    
    -- Audit columns
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id UUID,
    
    -- Primary key includes billing period for partition pruning
    PRIMARY KEY (billing_period_id, shift_id)
) PARTITION BY LIST (billing_period_id);

-- Create partitions for each billing period
CREATE TABLE fact_shifts_2025_01 PARTITION OF fact_shifts FOR VALUES IN ('2025_01');
CREATE TABLE fact_shifts_2025_02 PARTITION OF fact_shifts FOR VALUES IN ('2025_02');
CREATE TABLE fact_shifts_2025_03 PARTITION OF fact_shifts FOR VALUES IN ('2025_03');
CREATE TABLE fact_shifts_2025_04 PARTITION OF fact_shifts FOR VALUES IN ('2025_04');
CREATE TABLE fact_shifts_2025_05 PARTITION OF fact_shifts FOR VALUES IN ('2025_05');
CREATE TABLE fact_shifts_2025_06 PARTITION OF fact_shifts FOR VALUES IN ('2025_06');
CREATE TABLE fact_shifts_2025_07 PARTITION OF fact_shifts FOR VALUES IN ('2025_07');
CREATE TABLE fact_shifts_2025_08 PARTITION OF fact_shifts FOR VALUES IN ('2025_08');
CREATE TABLE fact_shifts_2025_09 PARTITION OF fact_shifts FOR VALUES IN ('2025_09');
CREATE TABLE fact_shifts_2025_10 PARTITION OF fact_shifts FOR VALUES IN ('2025_10');
CREATE TABLE fact_shifts_2025_11 PARTITION OF fact_shifts FOR VALUES IN ('2025_11');
CREATE TABLE fact_shifts_2025_12 PARTITION OF fact_shifts FOR VALUES IN ('2025_12');
CREATE TABLE fact_shifts_2025_13 PARTITION OF fact_shifts FOR VALUES IN ('2025_13');

-- Function to automatically create partitions
CREATE OR REPLACE FUNCTION create_shift_partition(period_id VARCHAR(10))
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
BEGIN
    partition_name := 'fact_shifts_' || period_id;
    
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I PARTITION OF fact_shifts
        FOR VALUES IN (%L)',
        partition_name, period_id
    );
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- ETL Support Tables
-- =====================================================

-- ETL batch tracking
CREATE TABLE etl_batches (
    batch_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);

-- ETL sync tracking for incremental loads
CREATE TABLE etl_sync_status (
    table_name VARCHAR(100) PRIMARY KEY,
    last_sync_timestamp TIMESTAMPTZ,
    last_successful_batch_id UUID,
    sync_metadata JSONB
);

-- Data quality tracking
CREATE TABLE data_quality_issues (
    issue_id BIGSERIAL PRIMARY KEY,
    batch_id UUID,
    table_name VARCHAR(100),
    record_id VARCHAR(100),
    issue_type VARCHAR(50),
    issue_description TEXT,
    issue_data JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- Views for Reporting
-- =====================================================

-- Current employee view
CREATE VIEW v_current_employees AS
SELECT 
    employee_id,
    custom_id,
    first_name,
    last_name,
    email,
    status,
    region_name
FROM dim_employees
WHERE is_current = TRUE;

-- Standby shifts view (matching your TQL query)
CREATE VIEW v_standby_shifts AS
SELECT 
    s.shift_id,
    bp.period_id as billing_period,
    c.parent_region_name as contract,
    c.region_name as market,
    c.name as location,
    c.custom_id as contract_uid,
    p.custom_id as position_uid,
    p.name as position_name,
    e.first_name,
    e.last_name,
    s.shift_date,
    s.start_datetime::time as time_clock_in,
    s.end_datetime::time as time_clock_out,
    s.clocked_hours as hours_clocked,
    s.approved_hours as hours_approved,
    s.payable_hours as hours_paid,
    s.billable_hours as hours_billed,
    s.bill_rate_regular as bill_rate_reg,
    s.bill_rate_effective as bill_rate_eff,
    s.bill_overtime_hours as bill_ot_hours,
    s.bill_overtime_impact as bill_ot_impact,
    s.bill_total
FROM fact_shifts s
JOIN billing_periods bp ON s.billing_period_id = bp.period_id
JOIN dim_employees e ON s.employee_id = e.employee_id AND e.is_current = TRUE
JOIN dim_positions p ON s.position_id = p.position_id AND p.is_current = TRUE
JOIN dim_clients c ON s.client_id = c.client_id AND c.is_current = TRUE
WHERE p.name ILIKE '%STAND%' OR p.name ILIKE '%WATCH%';

-- =====================================================
-- Maintenance Functions
-- =====================================================

-- Function to refresh materialized data
CREATE OR REPLACE FUNCTION refresh_period_aggregates(period_id VARCHAR(10))
RETURNS VOID AS $$
BEGIN
    -- Add any period-end calculations here
    RAISE NOTICE 'Refreshing aggregates for period %', period_id;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update modified timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables with updated_at column
CREATE TRIGGER update_dim_employees_modtime BEFORE UPDATE ON dim_employees
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_dim_clients_modtime BEFORE UPDATE ON dim_clients
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_dim_positions_modtime BEFORE UPDATE ON dim_positions
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- =====================================================
-- Permissions (adjust as needed)
-- =====================================================

-- Create read-only role for DOMO
CREATE ROLE domo_reader;
GRANT USAGE ON SCHEMA tracktik TO domo_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA tracktik TO domo_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA tracktik GRANT SELECT ON TABLES TO domo_reader;

-- Create ETL role
CREATE ROLE etl_writer;
GRANT USAGE ON SCHEMA tracktik TO etl_writer;
GRANT ALL ON ALL TABLES IN SCHEMA tracktik TO etl_writer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA tracktik TO etl_writer;