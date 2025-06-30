"""
Enhanced Compatible Database Manager - FINAL SCHEMA-COMPATIBLE VERSION
Complete database manager with fixed validation functionality that matches your actual database schema

Key Features:
- Schema-compatible INSERT statements
- Enhanced validation with detailed reporting
- Comprehensive error handling
- All CRUD operations for invoices and invoice details
- Transaction management and rollback protection
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class CompatibleEnhancedDatabaseManager:
    """
    Enhanced database manager with validation and schema compatibility
    
    Python Concepts:
    - Class initialization (__init__): Sets up the database connection
    - Instance variables (self.*): Store configuration and state
    - Method organization: Related operations grouped together
    - Error handling: Graceful handling of database issues
    - Schema compatibility: INSERT statements match actual database structure
    """
    
    def __init__(self):
        """Initialize the database manager with connection details"""
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in .env file")
        
        logger.info("Enhanced Database Manager initialized")
        
        # Test connection on initialization
        if not self.test_connection():
            raise ConnectionError("Failed to connect to database")
    
    def test_connection(self) -> bool:
        """
        Test database connectivity
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            conn = psycopg2.connect(self.database_url)
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_table_stats(self) -> Dict[str, int]:
        """
        Get record counts for all major tables
        
        Returns:
            Dictionary with table names and record counts
        """
        stats = {}
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # List of tables to check
            tables = ['invoices', 'invoice_details', 'building_dimension', 'emid_reference']
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    stats[table] = count
                except psycopg2.Error:
                    # Table might not exist
                    stats[table] = 0
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
        
        return stats
    
    def get_invoices(self, filters: Optional[Dict] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Retrieve invoices from the database
        
        Args:
            filters: Optional dictionary of column filters
            limit: Optional limit on number of records
            
        Returns:
            DataFrame containing invoice records
        """
        try:
            conn = psycopg2.connect(self.database_url)
            
            # Build query
            query = "SELECT * FROM invoices"
            params = []
            
            if filters:
                where_clauses = []
                for column, value in filters.items():
                    where_clauses.append(f"{column} = %s")
                    params.append(value)
                query += " WHERE " + " AND ".join(where_clauses)
            
            query += " ORDER BY created_at DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving invoices: {e}")
            return pd.DataFrame()
    
    def get_invoice_details(self, invoice_no: Optional[str] = None, limit: Optional[int] = 1000) -> pd.DataFrame:
        """
        Retrieve invoice details from the database
        
        Args:
            invoice_no: Optional specific invoice number to filter by
            limit: Maximum number of records to return
            
        Returns:
            DataFrame containing invoice detail records
        """
        try:
            conn = psycopg2.connect(self.database_url)
            
            if invoice_no:
                query = "SELECT * FROM invoice_details WHERE invoice_no = %s ORDER BY work_date DESC"
                params = [invoice_no]
            else:
                query = "SELECT * FROM invoice_details ORDER BY created_at DESC LIMIT %s"
                params = [limit]
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving invoice_details: {e}")
            return pd.DataFrame()
    
    def search_invoices(self, search_term: str) -> pd.DataFrame:
        """
        Search for invoices using a search term
        
        Args:
            search_term: Term to search for in invoice fields
            
        Returns:
            DataFrame containing matching invoices
        """
        try:
            conn = psycopg2.connect(self.database_url)
            
            query = """
                SELECT * FROM invoices 
                WHERE invoice_no ILIKE %s 
                   OR emid ILIKE %s 
                   OR service_area ILIKE %s
                   OR post_name ILIKE %s
                ORDER BY created_at DESC
                LIMIT 100
            """
            
            search_pattern = f"%{search_term}%"
            params = [search_pattern, search_pattern, search_pattern, search_pattern]
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error searching invoices: {e}")
            return pd.DataFrame()
    
    def upsert_invoices(self, invoice_records: List[Dict]) -> int:
        """
        Insert or update invoice master records
        
        Args:
            invoice_records: List of dictionaries containing invoice data
            
        Returns:
            Number of records processed
        """
        if not invoice_records:
            return 0
        
        processed_count = 0
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for record in invoice_records:
                try:
                    # Use PostgreSQL's ON CONFLICT for upsert functionality
                    cursor.execute("""
                        INSERT INTO invoices (
                            invoice_no, emid, nuid, service_reqd_by, service_area, post_name,
                            chartfield, invoice_from, invoice_to, invoice_date, edi_date,
                            release_date, add_on_date, original_edi_date, original_add_on_date,
                            original_release_date, invoice_total, not_transmitted,
                            invoice_no_history, notes, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (invoice_no) DO UPDATE SET
                            emid = EXCLUDED.emid,
                            nuid = EXCLUDED.nuid,
                            service_reqd_by = EXCLUDED.service_reqd_by,
                            service_area = EXCLUDED.service_area,
                            post_name = EXCLUDED.post_name,
                            chartfield = EXCLUDED.chartfield,
                            invoice_from = EXCLUDED.invoice_from,
                            invoice_to = EXCLUDED.invoice_to,
                            invoice_date = EXCLUDED.invoice_date,
                            edi_date = EXCLUDED.edi_date,
                            release_date = EXCLUDED.release_date,
                            add_on_date = EXCLUDED.add_on_date,
                            invoice_total = EXCLUDED.invoice_total,
                            not_transmitted = EXCLUDED.not_transmitted,
                            notes = EXCLUDED.notes,
                            updated_at = %s
                    """, (
                        record.get('invoice_no'),
                        record.get('emid'),
                        record.get('nuid'),
                        record.get('service_reqd_by'),
                        record.get('service_area'),
                        record.get('post_name'),
                        record.get('chartfield'),
                        record.get('invoice_from'),
                        record.get('invoice_to'),
                        record.get('invoice_date'),
                        record.get('edi_date'),
                        record.get('release_date'),
                        record.get('add_on_date'),
                        record.get('original_edi_date'),
                        record.get('original_add_on_date'),
                        record.get('original_release_date'),
                        record.get('invoice_total', 0),
                        record.get('not_transmitted', False),
                        record.get('invoice_no_history'),
                        record.get('notes'),
                        datetime.now(),
                        datetime.now(),
                        datetime.now()  # For the UPDATE SET updated_at
                    ))
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing invoice record {record.get('invoice_no')}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully processed {processed_count} invoice records")
            return processed_count
            
        except Exception as e:
            logger.error(f"Error in upsert_invoices: {e}")
            return 0
    
    def bulk_insert_invoice_details(self, invoice_details: List[Dict]) -> int:
        """
        Basic bulk insert for invoice details (schema-compatible version)
        
        Args:
            invoice_details: List of dictionaries containing invoice detail records
            
        Returns:
            Number of records inserted
        """
        if not invoice_details:
            return 0
        
        inserted_count = 0
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for record in invoice_details:
                try:
                    # Check if record already exists
                    cursor.execute("""
                        SELECT COUNT(*) FROM invoice_details 
                        WHERE invoice_no = %s AND employee_id = %s AND work_date = %s
                    """, (
                        record.get('invoice_no'),
                        record.get('employee_id'),
                        record.get('work_date')
                    ))
                    
                    if cursor.fetchone()[0] > 0:
                        continue  # Skip duplicate
                    
                    # Build employee name from parts if needed
                    employee_name = record.get('employee_name')
                    if not employee_name:
                        # Combine name parts for BCI records
                        first = record.get('first_name', '')
                        last = record.get('last_name', '')
                        middle = record.get('middle_initial', '')
                        
                        if first or last:
                            name_parts = [first, middle, last]
                            employee_name = ' '.join(part for part in name_parts if part and str(part).strip())
                    
                    # Schema-compatible INSERT statement
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
                        # Core fields
                        record.get('invoice_no'),
                        record.get('source_system', 'BCI'),
                        record.get('work_date'),
                        record.get('employee_id'),
                        employee_name,
                        
                        # Location and building info
                        record.get('location_code'),
                        record.get('location_name'),
                        record.get('building_code'),
                        record.get('emid'),
                        record.get('position_code'),
                        
                        # Position and job info
                        record.get('position_description'),
                        record.get('job_number'),
                        
                        # Hours
                        record.get('hours_regular', 0),
                        record.get('hours_overtime', 0),
                        record.get('hours_holiday', 0),
                        record.get('hours_total', 0),
                        
                        # Rates
                        record.get('rate_regular', 0),
                        record.get('rate_overtime', 0),
                        record.get('rate_holiday', 0),
                        
                        # Amounts
                        record.get('amount_regular', 0),
                        record.get('amount_overtime', 0),
                        record.get('amount_holiday', 0),
                        record.get('amount_total', 0),
                        
                        # Customer info
                        record.get('customer_number'),
                        record.get('customer_name'),
                        record.get('business_unit'),
                        
                        # Time tracking
                        record.get('shift_in') or record.get('in_time'),
                        record.get('shift_out') or record.get('out_time'),
                        record.get('bill_category'),
                        record.get('pay_rate', 0),
                        record.get('lunch_hours', 0),
                        
                        # PO and timestamps
                        record.get('po_number') or record.get('po'),
                        datetime.now(),
                        datetime.now()
                    ))
                    
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting detail record: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error in bulk_insert_invoice_details: {e}")
            return 0
    
    def bulk_insert_invoice_details_with_validation(self, invoice_details: List[Dict]) -> Dict:
        """
        Enhanced bulk insert with validation - SCHEMA COMPATIBLE VERSION
        
        This version matches your actual database schema exactly and includes
        comprehensive validation and error reporting.
        """
        
        # Initialize results tracking
        results = {
            'total_records': len(invoice_details),
            'inserted': 0,
            'skipped': 0, 
            'missing_invoice_count': 0,
            'success': False,
            'missing_invoices': [],
            'skipped_records': []
        }
        
        if not invoice_details:
            results['success'] = True
            return results
        
        try:
            # Use psycopg2 connection directly
            conn = psycopg2.connect(self.database_url)
            with conn:
                with conn.cursor() as cursor:
                    
                    # Step 1: Get all existing invoice numbers for validation
                    print("   📊 Loading existing invoices for validation...")
                    cursor.execute("SELECT DISTINCT invoice_no FROM invoices WHERE invoice_no IS NOT NULL")
                    existing_invoices = {row[0] for row in cursor.fetchall()}
                    print(f"   ✅ Found {len(existing_invoices)} existing invoices in master")
                    
                    # Step 2: Validate each record before processing
                    valid_records = []
                    
                    for record in invoice_details:
                        invoice_no = record.get('invoice_no')
                        
                        if not invoice_no:
                            # Skip records with no invoice number
                            results['skipped'] += 1
                            results['skipped_records'].append({
                                'record': record,
                                'reason': 'Missing invoice number'
                            })
                            continue
                        
                        if invoice_no not in existing_invoices:
                            # Track missing invoice numbers
                            if invoice_no not in results['missing_invoices']:
                                results['missing_invoices'].append(invoice_no)
                                results['missing_invoice_count'] += 1
                            
                            results['skipped'] += 1
                            results['skipped_records'].append({
                                'record': record,
                                'reason': f'Invoice {invoice_no} not found in master'
                            })
                            continue
                        
                        # Record is valid - add to processing list
                        valid_records.append(record)
                    
                    print(f"   📋 Validation complete:")
                    print(f"      - Valid records: {len(valid_records)}")
                    print(f"      - Skipped records: {results['skipped']}")
                    print(f"      - Missing invoices: {results['missing_invoice_count']}")
                    
                    # Step 3: Process valid records with SCHEMA-COMPATIBLE insert
                    if valid_records:
                        print(f"   🚀 Processing {len(valid_records)} valid records...")
                        
                        inserted_count = 0
                        failed_count = 0
                        
                        # Process records in smaller batches for better error handling
                        batch_size = 100
                        for i in range(0, len(valid_records), batch_size):
                            batch = valid_records[i:i + batch_size]
                            batch_inserted = 0
                            
                            for record in batch:
                                try:
                                    # Check if this specific record already exists (duplicate prevention)
                                    cursor.execute("""
                                        SELECT COUNT(*) FROM invoice_details 
                                        WHERE invoice_no = %s AND employee_id = %s AND work_date = %s
                                    """, (
                                        record.get('invoice_no'),
                                        record.get('employee_id'),
                                        record.get('work_date')
                                    ))
                                    
                                    if cursor.fetchone()[0] > 0:
                                        # Record already exists, skip it
                                        continue
                                    
                                    # Build employee name from parts if needed
                                    employee_name = record.get('employee_name')
                                    if not employee_name:
                                        # Combine name parts for BCI records
                                        first = record.get('first_name', '')
                                        last = record.get('last_name', '')
                                        middle = record.get('middle_initial', '')
                                        
                                        if first or last:
                                            name_parts = [first, middle, last]
                                            employee_name = ' '.join(part for part in name_parts if part and str(part).strip())
                                    
                                    # SCHEMA-COMPATIBLE INSERT - matches your actual database columns exactly
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
                                        # Core fields
                                        record.get('invoice_no'),
                                        record.get('source_system', 'BCI'),
                                        record.get('work_date'),
                                        record.get('employee_id'),
                                        employee_name,
                                        
                                        # Location and building info (from BCI mapping)
                                        record.get('location_code'),        # Location_Number → location_code
                                        record.get('location_name'),        # Location → location_name  
                                        record.get('building_code'),        # May need mapping
                                        record.get('emid'),                 # May need lookup
                                        record.get('position_code'),        # Position_Number → position_code
                                        
                                        # Position and job info
                                        record.get('position_description'), # Position → position_description
                                        record.get('job_number'),
                                        
                                        # Hours
                                        record.get('hours_regular', 0),
                                        record.get('hours_overtime', 0),
                                        record.get('hours_holiday', 0),
                                        record.get('hours_total', 0),
                                        
                                        # Rates  
                                        record.get('rate_regular', 0),
                                        record.get('rate_overtime', 0),
                                        record.get('rate_holiday', 0),
                                        
                                        # Amounts
                                        record.get('amount_regular', 0),
                                        record.get('amount_overtime', 0),
                                        record.get('amount_holiday', 0),
                                        record.get('amount_total', 0),
                                        
                                        # Customer info
                                        record.get('customer_number'),      # Customer_Number → customer_number
                                        record.get('customer_name'),        # Customer → customer_name
                                        record.get('business_unit'),        # Business_Unit → business_unit
                                        
                                        # Time tracking (shift_in/out → in_time/out_time)
                                        record.get('shift_in') or record.get('in_time'),    # Shift_In → in_time
                                        record.get('shift_out') or record.get('out_time'),  # Shift_Out → out_time
                                        record.get('bill_category'),        # Billing_Code → bill_category
                                        record.get('pay_rate', 0),          # May need calculation
                                        record.get('lunch_hours', 0),
                                        
                                        # PO and timestamps
                                        record.get('po_number') or record.get('po'),  # po_number → po
                                        datetime.now(),
                                        datetime.now()
                                    ))
                                    
                                    batch_inserted += 1
                                    inserted_count += 1
                                    
                                except Exception as e:
                                    failed_count += 1
                                    print(f"      ❌ Failed to insert record for invoice {record.get('invoice_no')}, employee {record.get('employee_id')}: {e}")
                                    # Log specific error details for debugging
                                    import traceback
                                    print(f"      📋 Error details: {traceback.format_exc()}")
                                    continue
                            
                            # Commit each batch
                            conn.commit()
                            print(f"      ✅ Batch {i//batch_size + 1}: {batch_inserted} inserted")
                        
                        results['inserted'] = inserted_count
                        
                        print(f"   ✅ Final results: {inserted_count} inserted, {failed_count} failed")
                    
                    # Final commit
                    conn.commit()
                    results['success'] = True
            
            # Close connection
            conn.close()
            return results
                    
        except Exception as e:
            print(f"   ❌ Error in bulk insert with validation: {e}")
            import traceback
            print(f"   📋 Error details: {traceback.format_exc()}")
            results['success'] = False
            return results
    
    def insert_building_data(self, building_records: List[Dict]) -> int:
        """
        Insert building/reference data
        
        Args:
            building_records: List of dictionaries containing building data
            
        Returns:
            Number of records inserted
        """
        if not building_records:
            return 0
        
        inserted_count = 0
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for record in building_records:
                try:
                    cursor.execute("""
                        INSERT INTO building_dimension (
                            building_code, building_name, emid, mc_service_area,
                            region, address, business_unit, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (building_code) DO UPDATE SET
                            building_name = EXCLUDED.building_name,
                            emid = EXCLUDED.emid,
                            mc_service_area = EXCLUDED.mc_service_area,
                            region = EXCLUDED.region,
                            address = EXCLUDED.address,
                            business_unit = EXCLUDED.business_unit
                    """, (
                        record.get('building_code'),
                        record.get('building_name'),
                        record.get('emid'),
                        record.get('mc_service_area'),
                        record.get('region'),
                        record.get('address'),
                        record.get('business_unit'),
                        datetime.now()
                    ))
                    
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting building record: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error in insert_building_data: {e}")
            return 0
    
    def get_building_data(self, building_code: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve building/reference data
        
        Args:
            building_code: Optional specific building code to filter by
            
        Returns:
            DataFrame containing building records
        """
        try:
            conn = psycopg2.connect(self.database_url)
            
            if building_code:
                query = "SELECT * FROM building_dimension WHERE building_code = %s"
                params = [building_code]
            else:
                query = "SELECT * FROM building_dimension ORDER BY building_code"
                params = []
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving building data: {e}")
            return pd.DataFrame()
    
    def execute_custom_query(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """
        Execute a custom SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
            params: Optional list of parameters for the query
            
        Returns:
            DataFrame containing query results
        """
        try:
            conn = psycopg2.connect(self.database_url)
            df = pd.read_sql_query(query, conn, params=params or [])
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            return pd.DataFrame()
    
    def delete_invoice_details(self, invoice_no: str) -> int:
        """
        Delete all detail records for a specific invoice
        
        Args:
            invoice_no: Invoice number to delete details for
            
        Returns:
            Number of records deleted
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM invoice_details WHERE invoice_no = %s", (invoice_no,))
            deleted_count = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Deleted {deleted_count} detail records for invoice {invoice_no}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting invoice_details: {e}")
            return 0
    
    def get_processing_summary(self, days: int = 7) -> Dict:
        """
        Get a summary of recent processing activity
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get recent invoice counts
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_invoices,
                    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '%s days' THEN 1 END) as recent_invoices
                FROM invoices
            """, (days,))
            
            invoice_stats = cursor.fetchone()
            
            # Get recent detail counts
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_details,
                    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '%s days' THEN 1 END) as recent_details
                FROM invoice_details
            """, (days,))
            
            detail_stats = cursor.fetchone()
            
            conn.close()
            
            return {
                'total_invoices': invoice_stats[0],
                'recent_invoices': invoice_stats[1],
                'total_details': detail_stats[0],
                'recent_details': detail_stats[1],
                'days_analyzed': days
            }
            
        except Exception as e:
            logger.error(f"Error getting processing summary: {e}")
            return {}

# For backward compatibility, create an alias
EnhancedDatabaseManager = CompatibleEnhancedDatabaseManager