# etl/etl_pipeline.py
"""
Main ETL Pipeline
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional
import uuid

from .config import config
from .tracktik_client import TrackTikClient
from .database import db
from .transformers import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self):
        self.client = TrackTikClient()
        self.transformer = DataTransformer()
        self.batch_id = None
        
    def start_batch(self, batch_type: str) -> uuid.UUID:
        """Start a new ETL batch"""
        self.batch_id = uuid.uuid4()
        
        query = f"""
            INSERT INTO {config.POSTGRES_SCHEMA}.etl_batches 
            (batch_id, batch_type, status)
            VALUES (%s, %s, %s)
        """
        
        with db.get_cursor() as cursor:
            cursor.execute(query, (str(self.batch_id), batch_type, 'RUNNING'))
            
        logger.info(f"Started batch {self.batch_id} for {batch_type}")
        return self.batch_id
        
    def complete_batch(self, records_processed: int, records_failed: int = 0, error: str = None):
        """Complete the current batch"""
        status = 'FAILED' if error else 'COMPLETED'
        
        query = f"""
            UPDATE {config.POSTGRES_SCHEMA}.etl_batches
            SET status = %s, completed_at = CURRENT_TIMESTAMP,
                records_processed = %s, records_failed = %s, error_message = %s
            WHERE batch_id = %s
        """
        
        with db.get_cursor() as cursor:
            cursor.execute(query, (status, records_processed, records_failed, error, str(self.batch_id)))
            
    def load_dimensions(self):
        """Load/refresh all dimension tables"""
        logger.info("Loading dimension tables...")
        
        try:
            # Load employees
            logger.info("Loading employees...")
            employees = self.client.get_employees()
            transformed_employees = [self.transformer.transform_employee(e) for e in employees]
            
            results = db.upsert_dimension(
                'dim_employees',
                transformed_employees,
                'employee_id',
                ['first_name', 'last_name', 'email', 'status', 'region_name']
            )
            logger.info(f"Employees: {results['inserted']} inserted, {results['updated']} updated")
            
            # Load clients
            logger.info("Loading clients...")
            clients = self.client.get_clients()
            transformed_clients = [self.transformer.transform_client(c) for c in clients]
            
            results = db.upsert_dimension(
                'dim_clients',
                transformed_clients,
                'client_id',
                ['name', 'region_name', 'time_zone', 'address']
            )
            logger.info(f"Clients: {results['inserted']} inserted, {results['updated']} updated")
            
            # Load positions
            logger.info("Loading positions...")
            positions = self.client.get_positions()
            transformed_positions = [self.transformer.transform_position(p) for p in positions]
            
            results = db.upsert_dimension(
                'dim_positions',
                transformed_positions,
                'position_id',
                ['name', 'client_id', 'status', 'position_type']
            )
            logger.info(f"Positions: {results['inserted']} inserted, {results['updated']} updated")
            
        except Exception as e:
            logger.error(f"Error loading dimensions: {str(e)}")
            raise
            
    def load_shifts_for_period(self, period_id: str, start_date: str, end_date: str):
        """Load shifts for a specific billing period"""
        logger.info(f"Loading shifts for period {period_id} ({start_date} to {end_date})")
        
        try:
            # Get shifts from API
            shifts = self.client.get_shifts(start_date, end_date)
            logger.info(f"Retrieved {len(shifts)} shifts from API")
            
            # Transform shifts
            transformed_shifts = []
            for shift in shifts:
                try:
                    transformed = self.transformer.transform_shift(shift, period_id)
                    transformed_shifts.append(transformed)
                except Exception as e:
                    logger.error(f"Error transforming shift {shift.get('id')}: {str(e)}")
                    
            # Delete existing shifts for this period (for reprocessing)
            delete_query = f"""
                DELETE FROM {config.POSTGRES_SCHEMA}.fact_shifts
                WHERE billing_period_id = %s
            """
            with db.get_cursor() as cursor:
                cursor.execute(delete_query, (period_id,))
                deleted_count = cursor.rowcount
                
            if deleted_count > 0:
                logger.info(f"Deleted {deleted_count} existing shifts for period {period_id}")
                
            # Insert transformed shifts
            if transformed_shifts:
                insert_query = f"""
                    INSERT INTO {config.POSTGRES_SCHEMA}.fact_shifts (
                        shift_id, billing_period_id, employee_id, position_id, client_id,
                        shift_date, start_datetime, end_datetime,
                        scheduled_hours, clocked_hours, approved_hours, billable_hours, payable_hours,
                        bill_rate_regular, bill_rate_effective, bill_overtime_hours, 
                        bill_overtime_impact, bill_total,
                        status, approved_by, approved_at, raw_data, etl_batch_id
                    ) VALUES (
                        %(shift_id)s, %(billing_period_id)s, %(employee_id)s, %(position_id)s, %(client_id)s,
                        %(shift_date)s, %(start_datetime)s, %(end_datetime)s,
                        %(scheduled_hours)s, %(clocked_hours)s, %(approved_hours)s, %(billable_hours)s, %(payable_hours)s,
                        %(bill_rate_regular)s, %(bill_rate_effective)s, %(bill_overtime_hours)s,
                        %(bill_overtime_impact)s, %(bill_total)s,
                        %(status)s, %(approved_by)s, %(approved_at)s, %(raw_data)s::jsonb, %s
                    )
                """
                
                # Add batch_id to each record
                for record in transformed_shifts:
                    record['etl_batch_id'] = str(self.batch_id)
                    
                # Use execute_batch for better performance
                with db.get_cursor() as cursor:
                    from psycopg2.extras import execute_batch
                    execute_batch(
                        cursor,
                        insert_query.replace('%s', '%(etl_batch_id)s'),
                        transformed_shifts,
                        page_size=config.BATCH_SIZE
                    )
                    inserted_count = cursor.rowcount
                    
                logger.info(f"Inserted {inserted_count} shifts for period {period_id}")
                
            return len(transformed_shifts)
            
        except Exception as e:
            logger.error(f"Error loading shifts for period {period_id}: {str(e)}")
            raise
            
    def run_initial_load(self, period_id: str):
        """Run initial load for a single billing period"""
        logger.info(f"Running initial load for period {period_id}")
        
        # Start batch
        self.start_batch(f'INITIAL_LOAD_{period_id}')
        
        try:
            # Get period dates
            query = f"""
                SELECT start_date, end_date 
                FROM {config.POSTGRES_SCHEMA}.billing_periods
                WHERE period_id = %s
            """
            period_info = db.execute_query(query, {'period_id': period_id})[0]
            
            # Load dimensions first
            self.load_dimensions()
            
            # Load shifts for the period
            records_processed = self.load_shifts_for_period(
                period_id,
                period_info['start_date'].strftime('%Y-%m-%d'),
                period_info['end_date'].strftime('%Y-%m-%d')
            )
            
            # Complete batch
            self.complete_batch(records_processed)
            
            logger.info(f"Initial load complete for period {period_id}")
            
        except Exception as e:
            logger.error(f"Initial load failed: {str(e)}")
            self.complete_batch(0, 0, str(e))
            raise


def main():
    """Main entry point"""
    pipeline = ETLPipeline()
    
    # Run initial load for period 2025_12 (May 30 - June 12, 2025)
    pipeline.run_initial_load('2025_12')
    

if __name__ == "__main__":
    main()