"""
Corrected Database Manager - Fixes Both Issues
1. Uses correct column name: position_description (not post_description)
2. Removes overly aggressive duplicate constraints
3. Simplified, efficient insertion

Python Learning: Sometimes a quick, targeted fix is better than over-engineering
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class CorrectedDatabaseManager:
    """
    Corrected database manager that fixes the column name and constraint issues
    """
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in .env file")
        
        logger.info("Corrected Database Manager initialized")
        
        if not self.test_connection():
            raise ConnectionError("Failed to connect to database")
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            conn = psycopg2.connect(self.database_url)
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def insert_invoice_details_corrected(self, invoice_details: List[Dict]) -> Dict[str, Any]:
        """
        Insert invoice details with CORRECTED column names and NO duplicate constraints
        
        Key fixes:
        1. Uses position_description (not post_description)
        2. No ON CONFLICT clause - just insert everything
        3. Batch processing for efficiency
        """
        result = {
            'success': False,
            'total_records': len(invoice_details),
            'inserted': 0,
            'errors': [],
            'processing_time': 0
        }
        
        if not invoice_details:
            result['success'] = True
            return result
        
        start_time = datetime.now()
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            logger.info(f"Processing {len(invoice_details)} invoice detail records...")
            
            # Process in batches for better performance
            batch_size = 500
            total_inserted = 0
            
            for i in range(0, len(invoice_details), batch_size):
                batch = invoice_details[i:i + batch_size]
                batch_inserted = 0
                
                for record in batch:
                    try:
                        # Clean the record
                        clean_record = self.clean_record_data(record)
                        
                        # Skip records without required fields
                        if not clean_record.get('invoice_no') or not clean_record.get('employee_id'):
                            continue
                        
                        # Build employee name if needed
                        employee_name = clean_record.get('employee_name')
                        if not employee_name:
                            first = clean_record.get('first_name', '') or ''
                            last = clean_record.get('last_name', '') or ''
                            middle = clean_record.get('middle_initial', '') or ''
                            
                            if first or last:
                                name_parts = [part for part in [first, middle, last] if part]
                                employee_name = ' '.join(name_parts)
                        
                        # CORRECTED INSERT QUERY - Uses position_description, no constraints
                        cursor.execute("""
                            INSERT INTO invoice_details (
                                invoice_no, source_system, work_date, employee_id, employee_name,
                                location_code, location_name, building_code, emid, position_code,
                                position_description, job_number, hours_regular, hours_overtime,
                                hours_holiday, hours_total, rate_regular, rate_overtime,
                                rate_holiday, amount_regular, amount_overtime, amount_holiday,
                                amount_total, customer_number, customer_name, business_unit,
                                in_time, out_time, bill_category, pay_rate, lunch_hours,
                                po, created_at, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s
                            )
                        """, (
                            clean_record.get('invoice_no'),
                            clean_record.get('source_system', 'UNKNOWN'),
                            clean_record.get('work_date'),
                            clean_record.get('employee_id'),
                            employee_name,
                            clean_record.get('location_code'),
                            clean_record.get('location_name'),
                            clean_record.get('building_code'),
                            clean_record.get('emid'),
                            clean_record.get('position_code'),
                            clean_record.get('position_description'),  # ✅ CORRECT COLUMN NAME
                            clean_record.get('job_number'),
                            clean_record.get('hours_regular', 0),
                            clean_record.get('hours_overtime', 0),
                            clean_record.get('hours_holiday', 0),
                            clean_record.get('hours_total', 0),
                            clean_record.get('rate_regular', 0),
                            clean_record.get('rate_overtime', 0),
                            clean_record.get('rate_holiday', 0),
                            clean_record.get('amount_regular', 0),
                            clean_record.get('amount_overtime', 0),
                            clean_record.get('amount_holiday', 0),
                            clean_record.get('amount_total', 0),
                            clean_record.get('customer_number'),
                            clean_record.get('customer_name'),
                            clean_record.get('business_unit'),
                            clean_record.get('in_time'),
                            clean_record.get('out_time'),
                            clean_record.get('bill_category'),
                            clean_record.get('pay_rate', 0),
                            clean_record.get('lunch_hours', 0),
                            clean_record.get('po'),
                            datetime.now(),
                            datetime.now()
                        ))
                        
                        batch_inserted += 1
                        
                    except Exception as e:
                        error_msg = f"Error inserting record {clean_record.get('invoice_no', 'unknown')}: {e}"
                        result['errors'].append(error_msg)
                        logger.error(error_msg)
                        # Continue processing other records
                
                # Commit the batch
                conn.commit()
                total_inserted += batch_inserted
                
                logger.info(f"Batch {i//batch_size + 1}: {batch_inserted} records inserted")
            
            conn.close()
            
            result['inserted'] = total_inserted
            result['success'] = True
            result['processing_time'] = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Successfully inserted {total_inserted} invoice detail records in {result['processing_time']:.2f} seconds")
            
        except Exception as e:
            error_msg = f"Database error in invoice details insertion: {e}"
            result['errors'].append(error_msg)
            logger.error(error_msg)
        
        return result
    
    def clean_record_data(self, record: Dict) -> Dict:
        """Clean and validate record data before insertion"""
        cleaned = {}
        
        for key, value in record.items():
            if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                cleaned[key] = None
            elif key == 'work_date':
                # Handle date fields
                if isinstance(value, str) and ' 00:00:00' in value:
                    cleaned[key] = value.split(' ')[0]
                else:
                    cleaned[key] = value
            elif key in ['hours_regular', 'hours_overtime', 'hours_holiday', 'hours_total',
                        'rate_regular', 'rate_overtime', 'rate_holiday', 'pay_rate', 'lunch_hours',
                        'amount_regular', 'amount_overtime', 'amount_holiday', 'amount_total']:
                # Handle numeric fields
                try:
                    cleaned[key] = float(value) if value is not None else 0.0
                except (ValueError, TypeError):
                    cleaned[key] = 0.0
            else:
                # String fields
                cleaned[key] = str(value).strip() if value is not None else None
        
        return cleaned
    
    def process_aus_file_corrected(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Process AUS file with corrected approach"""
        from data_mapper_enhanced import EnhancedDataMapper
        
        mapper = EnhancedDataMapper()
        mapped_records = mapper.map_aus_details(df)
        
        logger.info(f"Processing AUS file with {len(mapped_records)} records using corrected manager")
        
        return self.insert_invoice_details_corrected(mapped_records)
    
    def process_bci_file_corrected(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Process BCI file with corrected approach"""
        from data_mapper_enhanced import EnhancedDataMapper
        
        mapper = EnhancedDataMapper()
        mapped_records = mapper.map_bci_details(df)
        
        logger.info(f"Processing BCI file with {len(mapped_records)} records using corrected manager")
        
        return self.insert_invoice_details_corrected(mapped_records)
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get record counts for all major tables"""
        stats = {}
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            tables = ['invoices', 'invoice_details', 'building_dimension', 'emid_reference']
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    stats[table] = count
                except psycopg2.Error:
                    stats[table] = 0
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
        
        return stats
    
    def get_recent_insertion_stats(self) -> Dict[str, Any]:
        """Get statistics about recent insertions"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Records inserted today
            cursor.execute("""
                SELECT COUNT(*) 
                FROM invoice_details 
                WHERE created_at >= CURRENT_DATE
            """)
            today_count = cursor.fetchone()[0]
            
            # Records inserted in last hour
            cursor.execute("""
                SELECT COUNT(*) 
                FROM invoice_details 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
            """)
            last_hour_count = cursor.fetchone()[0]
            
            # Most recent record
            cursor.execute("""
                SELECT invoice_no, employee_id, created_at
                FROM invoice_details 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            recent_record = cursor.fetchone()
            
            conn.close()
            
            return {
                'records_today': today_count,
                'records_last_hour': last_hour_count,
                'most_recent_record': {
                    'invoice_no': recent_record[0] if recent_record else None,
                    'employee_id': recent_record[1] if recent_record else None,
                    'created_at': recent_record[2] if recent_record else None
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting insertion stats: {e}")
            return {}

# Quick test function
def test_corrected_manager():
    """Test the corrected database manager"""
    manager = CorrectedDatabaseManager()
    
    print("Testing corrected database manager...")
    print(f"Connection test: {'✅ PASSED' if manager.test_connection() else '❌ FAILED'}")
    
    # Get current stats
    stats = manager.get_table_stats()
    print(f"Current invoice_details count: {stats.get('invoice_details', 0):,}")
    
    # Get recent insertion stats
    recent_stats = manager.get_recent_insertion_stats()
    print(f"Records inserted today: {recent_stats.get('records_today', 0):,}")
    print(f"Records inserted in last hour: {recent_stats.get('records_last_hour', 0):,}")
    
    return manager

if __name__ == "__main__":
    # Test the corrected manager
    test_corrected_manager()
