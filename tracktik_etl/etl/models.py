# etl/models.py
"""
Data models and table operations for TrackTik ETL
UPDATED TO MATCH ACTUAL SCHEMA
"""
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import uuid
import json

from tracktik_etl.etl.database import db
from tracktik_etl.etl.config import config

logger = logging.getLogger(__name__)

class DimEmployee:
    """Employee dimension operations - SCD Type 2
    
    NOTE: Complete employee records should be managed through the dedicated 
    employee sync script (sync_kaiser_employees.py).
    Billing period processing may create minimal records for new employees.
    """
    
    @staticmethod
    def upsert(employees: List[Dict], batch_id: str) -> Dict[str, int]:
        """Upsert employee records with proper SCD Type 2
        
        This method is used by the employee sync script for complete employee data.
        """
        
        if not employees:
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}
        
        logger.info(f"Processing {len(employees)} employees with SCD Type 2")
        
        try:
            # Transform TrackTik data to our schema
            records = []
            for emp in employees:
                # Handle both API format (from employee sync) and minimal format (from billing)
                if 'id' in emp:
                    # API format from employee sync
                    employee_id = emp['id']
                    custom_id = emp.get('customId')
                    first_name = emp.get('firstName')
                    last_name = emp.get('lastName')
                    email = emp.get('email')
                    phone = emp.get('primaryPhone') or emp.get('phone')
                    status = emp.get('status')
                    
                    # Extract region info
                    region_info = emp.get('region')
                    if isinstance(region_info, dict):
                        region_id = region_info.get('id')
                    elif isinstance(region_info, (int, str)):
                        region_id = int(region_info) if region_info else None
                    else:
                        region_id = None
                else:
                    # Already transformed format
                    employee_id = emp['employee_id']
                    custom_id = emp.get('custom_id')
                    first_name = emp.get('first_name')
                    last_name = emp.get('last_name')
                    email = emp.get('email')
                    phone = emp.get('phone')
                    status = emp.get('status')
                    region_id = emp.get('region_id')
                
                records.append({
                    'employee_id': employee_id,
                    'custom_id': custom_id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'email': email,
                    'phone': phone,
                    'status': status,
                    'region_id': region_id,
                    'valid_from': datetime.now(),
                    'is_current': True,
                    'etl_batch_id': batch_id
                })
            
            result = db.upsert_dimension(
                table='dim_employees',
                records=records,
                id_field='employee_id',
                scd_fields=['custom_id', 'first_name', 'last_name', 'email', 'phone', 'status', 'region_id']
            )
            
            return result
                
        except Exception as e:
            logger.error(f"Error in DimEmployee.upsert: {str(e)}")
            return {'inserted': 0, 'updated': 0, 'unchanged': 0, 'error': str(e)}
    
    @staticmethod
    def get_existing_employees(employee_ids: List[int]) -> Dict[int, Dict]:
        """Get existing employee records for reference (no updates)"""
        if not employee_ids:
            return {}
            
        query = f"""
            SELECT employee_id, custom_id, first_name, last_name, region_id
            FROM {config.POSTGRES_SCHEMA}.dim_employees
            WHERE employee_id = ANY(%(employee_ids)s) AND is_current = TRUE
        """
        
        results = db.execute_query(query, {'employee_ids': employee_ids})
        return {row['employee_id']: dict(row) for row in results}
    
    @staticmethod
    def get_existing_employees(employee_ids: List[int]) -> Dict[int, Dict]:
        """Get existing employee records for reference (no updates)"""
        if not employee_ids:
            return {}
            
        query = f"""
            SELECT employee_id, custom_id, first_name, last_name, region_id
            FROM {config.POSTGRES_SCHEMA}.dim_employees
            WHERE employee_id = ANY(%(employee_ids)s) AND is_current = TRUE
        """
        
        results = db.execute_query(query, {'employee_ids': employee_ids})
        return {row['employee_id']: dict(row) for row in results}


class DimClient:
    """Client/Site dimension operations - SCD Type 2"""
    
    @staticmethod
    def upsert(clients: List[Dict], batch_id: str) -> Dict[str, int]:
        """Upsert client records with SCD Type 2"""
        
        records = []
        for client in clients:
            # Extract region info - handle both int and dict formats
            region_info = client.get('region')
            region_id = None
            region_name = None
            parent_region_name = None
            
            if isinstance(region_info, dict):
                # If it's a dict (shouldn't happen based on API, but defensive coding)
                region_id = region_info.get('id')
                region_name = region_info.get('name')
                parent_region = region_info.get('parentRegion', {}) or {}
                if isinstance(parent_region, dict):
                    parent_region_name = parent_region.get('name')
            elif isinstance(region_info, (int, str)):
                # If it's just an ID (this is what the API returns)
                region_id = int(region_info) if region_info else None
                # We'll need to look up the name separately
                region_name = None
                parent_region_name = None
            
            # Build address JSONB
            address_data = None
            if client.get('address'):
                address_data = {
                    'street': client.get('address'),
                    'city': client.get('city'),
                    'state': client.get('state'),
                    'zip': client.get('zip')
                }
            
            records.append({
                'client_id': client['id'],
                'custom_id': client.get('customId'),
                'name': client.get('company') or client.get('name', ''),  # Use company field
                'region_id': region_id,
                'region_name': region_name,
                'parent_region_name': parent_region_name,
                'time_zone': client.get('timeZone'),
                'address': json.dumps(address_data) if address_data else None,
                'valid_from': datetime.now(),
                'is_current': True,
                'etl_batch_id': batch_id
            })
        
        return db.upsert_dimension(
            table='dim_clients',
            records=records,
            id_field='client_id',
            scd_fields=['name', 'region_id', 'region_name', 'parent_region_name', 'time_zone', 'address']
        )

class DimPosition:
    """Position/Post dimension operations - SCD Type 2"""
    
    @staticmethod
    def upsert(positions: List[Dict], batch_id: str) -> Dict[str, int]:
        """Upsert position records with SCD Type 2"""
        
        records = []
        for pos in positions:
            # Extract account/client info - handle both int and dict formats
            account_info = pos.get('account')
            client_id = None
            client_name = None
            client_custom_id = None
            
            if isinstance(account_info, dict):
                client_id = account_info.get('id')
                client_name = account_info.get('name')
                client_custom_id = account_info.get('customId')
            elif isinstance(account_info, (int, str)):
                client_id = int(account_info) if account_info else None
                # Will need to look up name separately
                client_name = None
                client_custom_id = None
            
            records.append({
                'position_id': pos['id'],
                'custom_id': pos.get('customId'),
                'name': pos['name'],
                'client_id': client_id,
                'status': pos.get('status'),
                'position_type': pos.get('type'),
                'client_name': client_name,
                'client_custom_id': client_custom_id,
                'valid_from': datetime.now(),
                'is_current': True,
                'etl_batch_id': batch_id
            })
        
        return db.upsert_dimension(
            table='dim_positions',
            records=records,
            id_field='position_id',
            scd_fields=['name', 'client_id', 'status', 'position_type', 'client_name', 'client_custom_id']
        )


class DimRegion:
    """Region hierarchy operations - SCD Type 2"""
    
    @staticmethod
    def upsert(regions: List[Dict], batch_id: str) -> Dict[str, int]:
        """Upsert region records"""
        
        records = []
        for region in regions:
            # Extract parent region info
            parent_info = region.get('parentRegion', {}) or {}
            
            records.append({
                'region_id': region['id'],  # Using actual TrackTik region ID
                'custom_id': region.get('customId'),
                'name': region['name'],
                'parent_region_id': parent_info.get('id'),
                'parent_region_name': parent_info.get('name'),
                'company': region.get('company'),
                'status': region.get('status'),
                'valid_from': datetime.now(),
                'is_current': True,
                'etl_batch_id': batch_id
            })
        
        return db.upsert_dimension(
            table='dim_regions',
            records=records,
            id_field='region_id',
            scd_fields=['name', 'parent_region_id', 'parent_region_name', 'company', 'status']
        )


class FactShift:
    """Shift fact table operations"""
    
    @staticmethod
    def insert_batch(shifts: List[Dict], billing_period_id: str, batch_id: str) -> int:
        """Insert shift records with proper fact table structure"""
        
        records = []
        for shift in shifts:
            # Parse shift data
            start_dt = None
            end_dt = None
            
            if shift.get('start_datetime'):
                # It's already transformed
                start_dt = shift['start_datetime']
            elif shift.get('startDateTime'):
                # It's raw from API
                try:
                    start_dt = datetime.fromisoformat(shift['startDateTime'].replace('Z', '+00:00'))
                except:
                    start_dt = datetime.fromisoformat(shift['startDateTime'])
            
            if shift.get('end_datetime'):
                # It's already transformed
                end_dt = shift['end_datetime']
            elif shift.get('endDateTime'):
                # It's raw from API
                try:
                    end_dt = datetime.fromisoformat(shift['endDateTime'].replace('Z', '+00:00'))
                except:
                    end_dt = datetime.fromisoformat(shift['endDateTime'])
            
            # Get the shift date
            shift_date = shift.get('shift_date')
            if not shift_date and start_dt:
                shift_date = start_dt.date() if hasattr(start_dt, 'date') else start_dt
            
            # Build record matching exact table structure
            record = {
                'shift_id': shift.get('shift_id') or shift.get('id'),
                'billing_period_id': billing_period_id,
                'employee_id': shift.get('employee_id') or shift.get('employee'),
                'position_id': shift.get('position_id') or shift.get('position'),
                'client_id': shift.get('client_id'),  # This should be set by the lookup
                'shift_date': shift_date,
                'start_datetime': start_dt,
                'end_datetime': end_dt,
                
                # Hours columns
                'scheduled_hours': shift.get('scheduled_hours') or shift.get('plannedDurationHours'),
                'clocked_hours': shift.get('clocked_hours') or shift.get('clockedHours'),
                'approved_hours': shift.get('approved_hours') or shift.get('approvedHours'),
                'billable_hours': shift.get('billable_hours') or shift.get('billableHours'),
                'payable_hours': shift.get('payable_hours') or shift.get('payableHours') or shift.get('plannedPayableHours'),
                
                # Billing rate information - set defaults for now
                'bill_rate_regular': shift.get('bill_rate_regular'),
                'bill_rate_effective': shift.get('bill_rate_effective'),
                'bill_overtime_hours': shift.get('bill_overtime_hours'),
                'bill_overtime_impact': shift.get('bill_overtime_impact'),
                'bill_total': shift.get('bill_total'),
                
                # Status and approval
                'status': shift.get('status'),
                'approved_by': shift.get('approved_by'),
                'approved_at': shift.get('approved_at'),
                
                # Store complete API response
                'raw_data': shift.get('raw_data') or json.dumps(shift),
                'etl_batch_id': batch_id
            }
            
            # Only add records with required fields
            if all([record['shift_id'], record['employee_id'], record['position_id'], 
                    record['client_id'], record['shift_date'], record['start_datetime'], 
                    record['end_datetime']]):
                records.append(record)
            else:
                logger.warning(f"Skipping shift {record['shift_id']} due to missing required fields")
        
        if not records:
            return 0
        
        # Use partition-aware insert
        columns = list(records[0].keys())
        placeholders = [f"%({col})s" for col in columns]
        
        # Insert into partitioned table
        query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.fact_shifts ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (billing_period_id, shift_id) DO UPDATE SET
                status = EXCLUDED.status,
                raw_data = EXCLUDED.raw_data,
                etl_batch_id = EXCLUDED.etl_batch_id,
                updated_at = CURRENT_TIMESTAMP
        """
        
        return db.execute_batch_insert(query, records)


class BillingPeriod:
    """Billing period operations"""
    
    @staticmethod
    def get_period_id(date_str: str) -> str:
        """Get billing period ID for a given date"""
        query = f"""
            SELECT period_id 
            FROM {config.POSTGRES_SCHEMA}.billing_periods
            WHERE %(date)s BETWEEN start_date AND end_date
        """
        result = db.execute_query(query, {'date': date_str})
        
        if result:
            return result[0]['period_id']
        else:
            raise ValueError(f"No billing period found for date: {date_str}")
    
    @staticmethod
    def get_period_dates(period_id: str) -> Dict[str, str]:
        """Get start and end dates for a billing period"""
        query = f"""
            SELECT start_date, end_date
            FROM {config.POSTGRES_SCHEMA}.billing_periods
            WHERE period_id = %(period_id)s
        """
        result = db.execute_query(query, {'period_id': period_id})
        
        if result:
            return {
                'start_date': result[0]['start_date'].strftime('%Y-%m-%d'),
                'end_date': result[0]['end_date'].strftime('%Y-%m-%d')
            }
        else:
            raise ValueError(f"Billing period not found: {period_id}")


class ETLBatch:
    """ETL batch tracking operations"""
    
    @staticmethod
    def create_batch(batch_type: str, metadata: Dict = None) -> str:
        """Create new ETL batch and return batch_id"""
        batch_id = str(uuid.uuid4())
        
        query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.etl_batches 
            (batch_id, batch_type, status, started_at, metadata)
            VALUES (%(batch_id)s, %(batch_type)s, 'RUNNING', CURRENT_TIMESTAMP, %(metadata)s)
        """
        
        with db.get_cursor() as cursor:
            cursor.execute(query, {
                'batch_id': batch_id,
                'batch_type': batch_type,
                'metadata': json.dumps(metadata) if metadata else None
            })
        
        logger.info(f"Created ETL batch {batch_id} for {batch_type}")
        return batch_id
    
    @staticmethod
    def complete_batch(batch_id: str, records_processed: int, records_failed: int = 0, error_message: str = None):
        """Complete ETL batch with results"""
        status = 'FAILED' if error_message else 'COMPLETED'
        
        query = f"""
            UPDATE {config.POSTGRES_SCHEMA}.etl_batches
            SET status = %(status)s, 
                completed_at = CURRENT_TIMESTAMP,
                records_processed = %(records_processed)s,
                records_failed = %(records_failed)s,
                error_message = %(error_message)s
            WHERE batch_id = %(batch_id)s
        """
        
        with db.get_cursor() as cursor:
            cursor.execute(query, {
                'batch_id': batch_id,
                'status': status,
                'records_processed': records_processed,
                'records_failed': records_failed,
                'error_message': error_message
            })
        
        logger.info(f"Completed batch {batch_id}: {status}, {records_processed} records")


class ETLSyncStatus:
    """ETL sync status tracking"""
    
    @staticmethod
    def update_sync_status(table_name: str, last_sync_timestamp: datetime, 
                          last_successful_batch_id: str, sync_metadata: Dict = None):
        """Update sync status for a table"""
        query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.etl_sync_status 
            (table_name, last_sync_timestamp, last_successful_batch_id, sync_metadata)
            VALUES (%(table_name)s, %(last_sync_timestamp)s, %(last_successful_batch_id)s, %(sync_metadata)s)
            ON CONFLICT (table_name) 
            DO UPDATE SET
                last_sync_timestamp = EXCLUDED.last_sync_timestamp,
                last_successful_batch_id = EXCLUDED.last_successful_batch_id,
                sync_metadata = EXCLUDED.sync_metadata
        """
        
        with db.get_cursor() as cursor:
            cursor.execute(query, {
                'table_name': table_name,
                'last_sync_timestamp': last_sync_timestamp,
                'last_successful_batch_id': last_successful_batch_id,
                'sync_metadata': json.dumps(sync_metadata) if sync_metadata else None
            })
