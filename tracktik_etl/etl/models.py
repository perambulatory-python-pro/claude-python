# etl/models.py
"""
Data models and table operations for TrackTik ETL
"""
from datetime import datetime
from typing import Dict, List, Optional
import logging

from .database import db
from .config import config

logger = logging.getLogger(__name__)


class DimEmployee:
    """Employee dimension operations"""
    
    @staticmethod
    def upsert(employees: List[Dict]) -> Dict[str, int]:
        """Upsert employee records with SCD Type 2"""
        
        # Transform TracktTik data to our schema
        records = []
        for emp in employees:
            records.append({
                'tracktik_id': emp['id'],
                'employee_code': emp.get('customId'),
                'first_name': emp.get('firstName'),
                'last_name': emp.get('lastName'),
                'email': emp.get('email'),
                'phone': emp.get('primaryPhone'),
                'job_title': emp.get('jobTitle'),
                'active': emp.get('status') == 'ACTIVE',
                'valid_from': datetime.now()
            })
        
        return db.upsert_dimension(
            table='dim_employees',
            records=records,
            id_field='tracktik_id',
            scd_fields=['first_name', 'last_name', 'email', 'job_title', 'active']
        )


class DimClient:
    """Client/Site dimension operations"""
    
    @staticmethod
    def upsert(clients: List[Dict]) -> Dict[str, int]:
        """Upsert client records"""
        
        records = []
        for client in clients:
            records.append({
                'tracktik_id': client['id'],
                'client_code': client.get('customId'),
                'name': client['name'],
                'region': client.get('region', {}).get('name') if isinstance(client.get('region'), dict) else None,
                'address': client.get('address', {}).get('street1') if isinstance(client.get('address'), dict) else None,
                'city': client.get('address', {}).get('city') if isinstance(client.get('address'), dict) else None,
                'state': client.get('address', {}).get('state') if isinstance(client.get('address'), dict) else None,
                'active': client.get('active', True),
                'valid_from': datetime.now()
            })
        
        return db.upsert_dimension(
            table='dim_clients',
            records=records,
            id_field='tracktik_id',
            scd_fields=['name', 'region', 'address', 'active']
        )


class DimPosition:
    """Position/Post dimension operations"""
    
    @staticmethod
    def upsert(positions: List[Dict]) -> Dict[str, int]:
        """Upsert position records"""
        
        records = []
        for pos in positions:
            records.append({
                'tracktik_id': pos['id'],
                'position_code': pos.get('customId'),
                'name': pos['name'],
                'client_tracktik_id': pos.get('account', {}).get('id') if isinstance(pos.get('account'), dict) else None,
                'bill_rate': float(pos.get('billRate', 0)),
                'pay_rate': float(pos.get('payRate', 0)),
                'active': pos.get('status') == 'ACTIVE',
                'valid_from': datetime.now()
            })
        
        return db.upsert_dimension(
            table='dim_positions',
            records=records,
            id_field='tracktik_id',
            scd_fields=['name', 'bill_rate', 'pay_rate', 'active']
        )


class DimRegion:
    """Region hierarchy operations"""
    
    @staticmethod
    def upsert(regions: List[Dict]) -> Dict[str, int]:
        """Upsert region records"""
        
        records = []
        for region in regions:
            records.append({
                'name': region['name'],
                'parent_region': region.get('parent_region'),
                'level': region.get('level', 1),
                'active': region.get('active', True),
                'valid_from': datetime.now()
            })
        
        # Regions use name as ID since they don't have TracktTik IDs
        return db.upsert_dimension(
            table='dim_regions',
            records=records,
            id_field='name',
            scd_fields=['parent_region', 'level', 'active']
        )


class FactShift:
    """Shift fact table operations"""
    
    @staticmethod
    def insert_batch(shifts: List[Dict]) -> int:
        """Insert shift records in batch"""
        
        records = []
        for shift in shifts:
            # Parse the shift data
            start_time = datetime.fromisoformat(shift['startedOn'].replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(shift['endedOn'].replace('Z', '+00:00')) if shift.get('endedOn') else None
            
            records.append({
                'tracktik_id': shift['id'],
                'employee_tracktik_id': shift.get('employee', {}).get('id') if isinstance(shift.get('employee'), dict) else None,
                'position_tracktik_id': shift.get('position', {}).get('id') if isinstance(shift.get('position'), dict) else None,
                'client_tracktik_id': shift.get('account', {}).get('id') if isinstance(shift.get('account'), dict) else None,
                'shift_date': start_time.date(),
                'start_time': start_time,
                'end_time': end_time,
                'scheduled_hours': float(shift.get('scheduledHours', 0)),
                'actual_hours': float(shift.get('actualHours', 0)),
                'bill_rate': float(shift.get('billRate', 0)),
                'pay_rate': float(shift.get('payRate', 0)),
                'status': shift.get('status'),
                'created_at': datetime.now()
            })
        
        # Use the query format your database manager expects
        columns = records[0].keys() if records else []
        placeholders = [f"%({col})s" for col in columns]
        
        query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.fact_shifts ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (tracktik_id) DO NOTHING
        """
        
        return db.execute_batch_insert(query, records)


class BillingPeriod:
    """Billing period operations"""
    
    @staticmethod
    def create_or_get(start_date: str, end_date: str) -> int:
        """Create or get billing period ID"""
        
        # Check if exists
        query = f"""
            SELECT id FROM {config.POSTGRES_SCHEMA}.billing_periods
            WHERE start_date = %(start_date)s AND end_date = %(end_date)s
        """
        result = db.execute_query(query, {'start_date': start_date, 'end_date': end_date})
        
        if result:
            return result[0]['id']
        
        # Create new
        insert_query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.billing_periods (start_date, end_date, created_at)
            VALUES (%(start_date)s, %(end_date)s, %(created_at)s)
            RETURNING id
        """
        result = db.execute_query(
            insert_query,
            {'start_date': start_date, 'end_date': end_date, 'created_at': datetime.now()}
        )
        
        return result[0]['id']


class ETLRunLog:
    """ETL run logging operations"""
    
    @staticmethod
    def log_run(stats: Dict):
        """Log ETL run statistics"""
        
        query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.etl_run_log 
            (run_date, pipeline_name, parameters, rows_processed, status, error_message, created_at)
            VALUES (%(run_date)s, %(pipeline_name)s, %(parameters)s::jsonb, 
                    %(rows_processed)s, %(status)s, %(error_message)s, %(created_at)s)
        """
        
        log_data = {
            'run_date': datetime.now(),
            'pipeline_name': 'kaiser_billing_period',
            'parameters': str(stats),  # Convert to JSON string
            'rows_processed': stats.get('total_shifts', 0),
            'status': 'ERROR' if stats.get('errors') else 'SUCCESS',
            'error_message': '\n'.join(stats.get('errors', [])) if stats.get('errors') else None,
            'created_at': datetime.now()
        }
        
        db.execute_query(query, log_data)