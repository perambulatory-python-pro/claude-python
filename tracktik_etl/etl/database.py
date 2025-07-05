# etl/database.py
"""
Database connection and utilities
UPDATED FOR PROPER SCD TYPE 2 AND SCHEMA ALIGNMENT
"""
import logging
from contextlib import contextmanager
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime

from .config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage PostgreSQL connections and operations"""
    
    def __init__(self):
        self.pool = SimpleConnectionPool(
            1, 20,  # min and max connections
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD
        )
        
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
            
    @contextmanager
    def get_cursor(self, cursor_factory=RealDictCursor):
        """Get a cursor with automatic commit/rollback"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
                
    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute a SELECT query"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
            
    def execute_batch_insert(self, query: str, data: List[Dict], page_size: int = 100):
        """Execute batch insert with psycopg2 execute_batch"""
        with self.get_cursor() as cursor:
            execute_batch(cursor, query, data, page_size=page_size)
            return cursor.rowcount
            
    def upsert_dimension(self, table: str, records: List[Dict], 
                        id_field: str, scd_fields: List[str]) -> Dict[str, int]:
        """
        Proper SCD Type 2 upsert for dimension tables
        
        Args:
            table: Target table name
            records: List of records to upsert
            id_field: Natural key field name (e.g., 'employee_id')
            scd_fields: Fields to track for changes
            
        Returns:
            Dict with counts of inserted, updated, and unchanged records
        """
        if not records:
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}
            
        inserted = 0
        updated = 0
        unchanged = 0
        
        with self.get_cursor() as cursor:
            for record in records:
                natural_key_value = record[id_field]
                
                # 1. Check if current record exists
                check_query = f"""
                    SELECT surrogate_key, {', '.join(scd_fields)}
                    FROM {config.POSTGRES_SCHEMA}.{table}
                    WHERE {id_field} = %s AND is_current = TRUE
                """
                cursor.execute(check_query, (natural_key_value,))
                existing = cursor.fetchone()
                
                if not existing:
                    # 2a. No current record exists - insert new
                    self._insert_new_dimension_record(cursor, table, record)
                    inserted += 1
                    
                else:
                    # 2b. Current record exists - check for changes
                    has_changed = self._has_scd_fields_changed(existing, record, scd_fields)
                    
                    if has_changed:
                        # 3a. Fields changed - close current and insert new
                        self._close_current_record(cursor, table, id_field, natural_key_value)
                        self._insert_new_dimension_record(cursor, table, record)
                        updated += 1
                    else:
                        # 3b. No changes - optionally update etl_batch_id
                        if 'etl_batch_id' in record:
                            update_query = f"""
                                UPDATE {config.POSTGRES_SCHEMA}.{table}
                                SET etl_batch_id = %s, updated_at = CURRENT_TIMESTAMP
                                WHERE {id_field} = %s AND is_current = TRUE
                            """
                            cursor.execute(update_query, (record['etl_batch_id'], natural_key_value))
                        unchanged += 1
                        
        result = {'inserted': inserted, 'updated': updated, 'unchanged': unchanged}
        logger.info(f"Dimension {table}: {result}")
        return result
    
    def _has_scd_fields_changed(self, existing: Dict, new_record: Dict, scd_fields: List[str]) -> bool:
        """Check if any SCD fields have changed"""
        for field in scd_fields:
            existing_val = existing.get(field)
            new_val = new_record.get(field)
            
            # Handle None values and type conversions
            if existing_val is None and new_val is None:
                continue
            elif existing_val is None or new_val is None:
                return True
            elif str(existing_val).strip() != str(new_val).strip():
                return True
                
        return False
    
    def _close_current_record(self, cursor, table: str, id_field: str, natural_key_value: Any):
        """Close the current record by setting valid_to and is_current = FALSE"""
        close_query = f"""
            UPDATE {config.POSTGRES_SCHEMA}.{table}
            SET valid_to = CURRENT_TIMESTAMP, 
                is_current = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE {id_field} = %s AND is_current = TRUE
        """
        cursor.execute(close_query, (natural_key_value,))
    
    def _insert_new_dimension_record(self, cursor, table: str, record: Dict):
        """Insert a new dimension record"""
        # Ensure temporal fields are set
        if 'valid_from' not in record:
            record['valid_from'] = datetime.now()
        if 'is_current' not in record:
            record['is_current'] = True
            
        # Build INSERT query
        columns = list(record.keys())
        placeholders = [f'%({col})s' for col in columns]
        
        insert_query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.{table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        cursor.execute(insert_query, record)
    
    def insert_fact_batch_partitioned(self, table: str, records: List[Dict], 
                                    partition_key: str) -> int:
        """
        Insert fact records into partitioned table with conflict handling
        
        Args:
            table: Target fact table name
            records: List of fact records
            partition_key: Partition key field name (e.g., 'billing_period_id')
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
            
        # Group records by partition for better performance
        partitions = {}
        for record in records:
            partition_value = record[partition_key]
            if partition_value not in partitions:
                partitions[partition_value] = []
            partitions[partition_value].append(record)
        
        total_inserted = 0
        
        with self.get_cursor() as cursor:
            for partition_value, partition_records in partitions.items():
                logger.info(f"Inserting {len(partition_records)} records into {table} partition {partition_value}")
                
                # Build bulk insert query
                if partition_records:
                    columns = list(partition_records[0].keys())
                    placeholders = [f'%({col})s' for col in columns]
                    
                    # For fact_shifts, use composite primary key conflict resolution
                    if table == 'fact_shifts':
                        insert_query = f"""
                            INSERT INTO {config.POSTGRES_SCHEMA}.{table} ({', '.join(columns)})
                            VALUES ({', '.join(placeholders)})
                            ON CONFLICT (billing_period_id, shift_id) 
                            DO UPDATE SET
                                status = EXCLUDED.status,
                                approved_by = EXCLUDED.approved_by,
                                approved_at = EXCLUDED.approved_at,
                                raw_data = EXCLUDED.raw_data,
                                etl_batch_id = EXCLUDED.etl_batch_id,
                                updated_at = CURRENT_TIMESTAMP
                        """
                    else:
                        # Generic fact table insert
                        insert_query = f"""
                            INSERT INTO {config.POSTGRES_SCHEMA}.{table} ({', '.join(columns)})
                            VALUES ({', '.join(placeholders)})
                            ON CONFLICT DO NOTHING
                        """
                    
                    # Use execute_batch for performance
                    execute_batch(cursor, insert_query, partition_records, page_size=config.BATCH_SIZE)
                    total_inserted += len(partition_records)
        
        logger.info(f"Inserted {total_inserted} total records into {table}")
        return total_inserted
    
    def get_dimension_lookup(self, table: str, natural_key: str, 
                           lookup_fields: List[str] = None) -> Dict[Any, Dict]:
        """
        Get current dimension records for lookup purposes
        
        Args:
            table: Dimension table name
            natural_key: Natural key field name
            lookup_fields: Fields to include in lookup (default: all)
            
        Returns:
            Dict mapping natural_key values to record data
        """
        fields = lookup_fields or ['*']
        field_list = ', '.join(fields)
        
        query = f"""
            SELECT {natural_key}, {field_list}
            FROM {config.POSTGRES_SCHEMA}.{table}
            WHERE is_current = TRUE
        """
        
        results = self.execute_query(query)
        return {row[natural_key]: dict(row) for row in results}
    
    def validate_data_quality(self, table: str, records: List[Dict]) -> List[Dict]:
        """
        Basic data quality validation
        
        Returns:
            List of data quality issues found
        """
        issues = []
        
        for i, record in enumerate(records):
            # Check for required fields based on table
            if table == 'fact_shifts':
                required_fields = ['shift_id', 'billing_period_id', 'employee_id', 'position_id', 'client_id']
            elif table.startswith('dim_'):
                # For dimensions, check that natural key exists
                if table == 'dim_employees':
                    required_fields = ['employee_id', 'first_name', 'last_name']
                elif table == 'dim_clients':
                    required_fields = ['client_id', 'name']
                elif table == 'dim_positions':
                    required_fields = ['position_id', 'name', 'client_id']
                else:
                    required_fields = []
            else:
                required_fields = []
            
            # Check for missing required fields
            for field in required_fields:
                if not record.get(field):
                    issues.append({
                        'record_index': i,
                        'issue_type': 'missing_required_field',
                        'field': field,
                        'record_id': record.get('shift_id') or record.get('employee_id') or record.get('client_id'),
                        'details': f"Required field '{field}' is missing or null"
                    })
        
        if issues:
            logger.warning(f"Found {len(issues)} data quality issues in {table}")
            # Optionally log to data_quality_issues table
            self._log_data_quality_issues(table, issues)
        
        return issues
    
    def _log_data_quality_issues(self, table: str, issues: List[Dict]):
        """Log data quality issues to the database"""
        try:
            issue_records = []
            for issue in issues:
                issue_records.append({
                    'table_name': table,
                    'issue_type': issue['issue_type'],
                    'field_name': issue.get('field'),
                    'record_identifier': str(issue.get('record_id', '')),
                    'issue_description': issue['details'],
                    'detected_at': datetime.now()
                })
            
            if issue_records:
                columns = list(issue_records[0].keys())
                placeholders = [f'%({col})s' for col in columns]
                
                insert_query = f"""
                    INSERT INTO {config.POSTGRES_SCHEMA}.data_quality_issues ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                
                self.execute_batch_insert(insert_query, issue_records)
                
        except Exception as e:
            logger.error(f"Failed to log data quality issues: {e}")
    
    def get_billing_period_id(self, date_str: str) -> str:
        """Get billing period ID for a given date using database function"""
        query = f"""
            SELECT period_id 
            FROM {config.POSTGRES_SCHEMA}.billing_periods
            WHERE %(date)s BETWEEN start_date AND end_date
        """
        
        result = self.execute_query(query, {'date': date_str})
        if result:
            return result[0]['period_id']
        else:
            raise ValueError(f"No billing period found for date: {date_str}")
    
    def cleanup_old_batches(self, days_old: int = 30):
        """Clean up old ETL batch records"""
        query = f"""
            DELETE FROM {config.POSTGRES_SCHEMA}.etl_batches
            WHERE started_at < CURRENT_TIMESTAMP - INTERVAL '{days_old} days'
            AND status IN ('COMPLETED', 'FAILED')
        """
        
        with self.get_cursor() as cursor:
            cursor.execute(query)
            deleted_count = cursor.rowcount
            
        logger.info(f"Cleaned up {deleted_count} old ETL batch records")
        return deleted_count


# Create singleton instance
db = DatabaseManager()
