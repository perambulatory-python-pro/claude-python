"""
Complete Fixed Database Manager - Transaction Safe Version
Updated for new invoice_details column structure with separate name fields
Handles transaction aborts, constraint violations, and data conversion issues
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
import os
import streamlit as st
from datetime import datetime, date, timedelta 
from typing import Dict, List, Optional, Union, Any
import logging
from dotenv import load_dotenv
from sqlalchemy import text 

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class CompatibleEnhancedDatabaseManager:
    """
    Transaction-safe database manager that handles constraint violations gracefully
    Updated for new column structure with separate employee name fields
    """
    
    def __init__(self):
        """Initialize the database manager with connection details"""
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in .env file")
        
        logger.info("Transaction-Safe Database Manager initialized")
        
        # Test connection on initialization
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
    
    def get_invoices(self, filters: Optional[Dict] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Retrieve invoices from the database"""
        try:
            conn = psycopg2.connect(self.database_url)
            
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
        """Retrieve invoice details from the database"""
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
        """Search for invoices using a search term"""
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
        """Insert or update invoice master records"""
        if not invoice_records:
            return 0
        
        processed_count = 0
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for record in invoice_records:
                try:
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
                        datetime.now()
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
    
    def clean_record_data(self, record: Dict) -> Dict:
        """Clean and validate record data before insertion"""
        cleaned = {}
        
        for key, value in record.items():
            if value is None or value == '' or str(value).strip() == '':
                cleaned[key] = None
            elif key in ['work_date', 'week_ending']:
                # Handle date fields specially
                if isinstance(value, str) and ' 00:00:00' in value:
                    cleaned[key] = value.split(' ')[0]
                else:
                    cleaned[key] = value
            elif key in ['hours_regular', 'hours_overtime', 'hours_holiday', 'hours_total',
                        'rate_regular', 'rate_overtime', 'rate_holiday',
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
    
    def bulk_insert_invoice_details(self, invoice_details: List[Dict]) -> int:
        """Basic bulk insert with transaction safety - updated for new column structure"""
        if not invoice_details:
            return 0
        
        inserted_count = 0
        
        try:
            conn = psycopg2.connect(self.database_url)
            
            print(f"   🚀 Processing {len(invoice_details):,} records with transaction safety...")
            
            # Process each record in its own transaction to prevent cascading failures
            for i, record in enumerate(invoice_details):
                try:
                    with conn:
                        with conn.cursor() as cursor:
                            # Clean the record data
                            clean_record = self.clean_record_data(record)
                            
                            # Insert with constraint handling - updated column list
                            cursor.execute("""
                                INSERT INTO invoice_details (
                                    invoice_no, source_system, work_date, employee_id,
                                    employee_name_last, employee_name_first, employee_middle_initial,
                                    location_code, location_name, position_code,
                                    position_description, hours_regular, hours_overtime,
                                    hours_holiday, hours_total, rate_regular, rate_overtime,
                                    rate_holiday, amount_regular, amount_overtime, amount_holiday,
                                    amount_total, customer_number, customer_name, customer_number_ext,
                                    customer_ext_description, business_unit, shift_in, shift_out,
                                    bill_category, week_ending, created_at, updated_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s
                                )
                                ON CONFLICT (invoice_no, work_date, employee_id, position_code, hours_total) 
                                DO NOTHING
                            """, (
                                clean_record.get('invoice_no'),
                                clean_record.get('source_system', 'Unknown'),
                                clean_record.get('work_date'),
                                clean_record.get('employee_id'),
                                clean_record.get('employee_name_last'),
                                clean_record.get('employee_name_first'),
                                clean_record.get('employee_middle_initial'),
                                clean_record.get('location_code'),
                                clean_record.get('location_name'),
                                clean_record.get('position_code'),
                                clean_record.get('position_description'),
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
                                clean_record.get('customer_number_ext'),
                                clean_record.get('customer_ext_description'),
                                clean_record.get('business_unit'),
                                clean_record.get('shift_in'),
                                clean_record.get('shift_out'),
                                clean_record.get('bill_category'),
                                clean_record.get('week_ending'),
                                datetime.now(),
                                datetime.now()
                            ))
                            
                            if cursor.rowcount > 0:
                                inserted_count += 1
                
                except psycopg2.Error as e:
                    # Log the error but continue processing
                    print(f"      ⚠️  Skipped record {i+1}: {clean_record.get('invoice_no')}/{clean_record.get('employee_id')} - {str(e)[:100]}")
                    continue
                except Exception as e:
                    print(f"      ❌ Error processing record {i+1}: {e}")
                    continue
                
                # Progress indicator every 1000 records
                if (i + 1) % 1000 == 0:
                    print(f"      ✅ Processed {i+1:,} records, {inserted_count:,} inserted so far")
            
            conn.close()
            print(f"   🎯 Completed: {inserted_count:,} new records inserted")
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error in bulk_insert_invoice_details: {e}")
            return 0
    
    def bulk_insert_invoice_details_with_validation(self, invoice_details: List[Dict]) -> Dict:
        """Enhanced bulk insert with validation and transaction safety - updated for new columns"""
        
        results = {
            'total_records': len(invoice_details),
            'inserted': 0,
            'skipped': 0, 
            'duplicates_handled': 0,
            'failed': 0,
            'missing_invoice_count': 0,
            'success': False,
            'missing_invoices': [],
            'skipped_records': [],
            'failed_records': []
        }
        
        if not invoice_details:
            results['success'] = True
            return results
        
        try:
            conn = psycopg2.connect(self.database_url)
            
            # Step 1: Get existing invoice numbers for validation
            print("   📊 Loading existing invoices for validation...")
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT invoice_no FROM invoices WHERE invoice_no IS NOT NULL")
                existing_invoices = {row[0] for row in cursor.fetchall()}
                print(f"   ✅ Found {len(existing_invoices)} existing invoices in master")
            
            # Step 2: Validate records
            valid_records = []
            
            for record in invoice_details:
                invoice_no = record.get('invoice_no')
                
                if not invoice_no:
                    results['skipped'] += 1
                    results['skipped_records'].append({
                        'record': record,
                        'reason': 'Missing invoice number'
                    })
                    continue
                
                if invoice_no not in existing_invoices:
                    if invoice_no not in results['missing_invoices']:
                        results['missing_invoices'].append(invoice_no)
                        results['missing_invoice_count'] += 1
                    
                    results['skipped'] += 1
                    results['skipped_records'].append({
                        'record': record,
                        'reason': f'Invoice {invoice_no} not found in master'
                    })
                    continue
                
                valid_records.append(record)
            
            print(f"   📋 Validation complete:")
            print(f"      - Valid records: {len(valid_records)}")
            print(f"      - Skipped records: {results['skipped']}")
            print(f"      - Missing invoices: {results['missing_invoice_count']}")
            
            # Step 3: Process valid records with individual transaction safety
            if valid_records:
                print(f"   🚀 Processing {len(valid_records)} valid records with transaction safety...")
                
                inserted_count = 0
                failed_count = 0
                duplicate_count = 0
                
                for i, record in enumerate(valid_records):
                    try:
                        with conn:
                            with conn.cursor() as cursor:
                                # Clean the record data
                                clean_record = self.clean_record_data(record)
                                
                                # Insert with constraint handling - updated columns
                                cursor.execute("""
                                    INSERT INTO invoice_details (
                                        invoice_no, source_system, work_date, employee_id,
                                        employee_name_last, employee_name_first, employee_middle_initial,
                                        location_code, location_name, position_code,
                                        position_description, hours_regular, hours_overtime,
                                        hours_holiday, hours_total, rate_regular, rate_overtime,
                                        rate_holiday, amount_regular, amount_overtime, amount_holiday,
                                        amount_total, customer_number, customer_name, customer_number_ext,
                                        customer_ext_description, business_unit, shift_in, shift_out,
                                        bill_category, week_ending, created_at, updated_at
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s
                                    )
                                    ON CONFLICT (invoice_no, work_date, employee_id, position_code, hours_total) 
                                    DO NOTHING
                                """, (
                                    clean_record.get('invoice_no'),
                                    clean_record.get('source_system', 'Unknown'),
                                    clean_record.get('work_date'),
                                    clean_record.get('employee_id'),
                                    clean_record.get('employee_name_last'),
                                    clean_record.get('employee_name_first'),
                                    clean_record.get('employee_middle_initial'),
                                    clean_record.get('location_code'),
                                    clean_record.get('location_name'),
                                    clean_record.get('position_code'),
                                    clean_record.get('position_description'),
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
                                    clean_record.get('customer_number_ext'),
                                    clean_record.get('customer_ext_description'),
                                    clean_record.get('business_unit'),
                                    clean_record.get('shift_in'),
                                    clean_record.get('shift_out'),
                                    clean_record.get('bill_category'),
                                    clean_record.get('week_ending'),
                                    datetime.now(),
                                    datetime.now()
                                ))
                                
                                if cursor.rowcount > 0:
                                    inserted_count += 1
                                else:
                                    duplicate_count += 1
                    
                    except psycopg2.Error as e:
                        failed_count += 1
                        error_msg = str(e).strip()
                        
                        results['failed_records'].append({
                            'invoice_no': clean_record.get('invoice_no'),
                            'employee_id': clean_record.get('employee_id'),
                            'error': error_msg
                        })
                        
                        continue
                        
                    except Exception as e:
                        failed_count += 1
                        continue
                    
                    # Progress indicator
                    if (i + 1) % 1000 == 0:
                        print(f"      ✅ Processed {i+1:,} records, {inserted_count:,} inserted so far")
                
                results['inserted'] = inserted_count
                results['duplicates_handled'] = duplicate_count
                results['failed'] = failed_count
                
                print(f"   ✅ Final results: {inserted_count} inserted, {duplicate_count} duplicates, {failed_count} failed")
            
            # Generate report
            print(f"\n📊 FINAL RECONCILIATION REPORT (Transaction Safe)")
            print("=" * 60)
            
            print(f"📋 Source File Analysis:")
            print(f"   Total records in source file: {results['total_records']:,}")
            print(f"   Records passed validation: {len(valid_records):,}")
            print(f"   Records failed validation: {results['skipped']:,}")
            
            print(f"\n📋 Processing Results:")
            print(f"   ✅ New records inserted: {results['inserted']:,}")
            print(f"   🔄 Duplicates handled: {results['duplicates_handled']:,}")
            print(f"   ❌ Processing failures: {results['failed']:,}")
            print(f"   ⚠️ Records skipped (validation): {results['skipped']:,}")
            
            if results['failed'] > 0:
                print(f"\n📋 Failure Summary:")
                print(f"   Transaction-safe processing prevented cascading failures")
                print(f"   Each failed record was isolated and processing continued")
            
            total_processed = results['inserted'] + results['duplicates_handled']
            processing_rate = (results['inserted'] / len(valid_records) * 100) if len(valid_records) > 0 else 0
            overall_rate = (total_processed / results['total_records'] * 100) if results['total_records'] > 0 else 0
            
            print(f"\n📋 Success Metrics:")
            print(f"   New record insertion rate: {processing_rate:.1f}% (of validated records)")
            print(f"   Overall processing rate: {overall_rate:.1f}% (of total source records)")
            
            print("=" * 60)
            
            conn.close()
            results['success'] = True
            return results
                    
        except Exception as e:
            print(f"   ❌ Critical error in bulk insert: {e}")
            import traceback
            print(f"   📋 Error details: {traceback.format_exc()}")
            results['success'] = False
            return results
    
    def execute_custom_query(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute a custom SQL query and return results as DataFrame"""
        try:
            conn = psycopg2.connect(self.database_url)
            df = pd.read_sql_query(query, conn, params=params or [])
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            return pd.DataFrame()
    
    def delete_invoice_details(self, invoice_no: str) -> int:
        """Delete all detail records for a specific invoice"""
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
        """Get a summary of recent processing activity"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_invoices,
                    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '%s days' THEN 1 END) as recent_invoices
                FROM invoices
            """, (days,))
            
            invoice_stats = cursor.fetchone()
            
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
        
    def bulk_insert_invoice_details_no_validation(self, invoice_details: List[Dict]) -> Dict:
        """
        Fast bulk insert without duplicate checking - updated for new columns
        Returns detailed results including any errors
        """
        results = {
            'total_records': len(invoice_details),
            'inserted': 0,
            'failed': 0,
            'errors': [],
            'success': False
        }
        
        if not invoice_details:
            results['success'] = True
            return results
        
        try:
            conn = psycopg2.connect(self.database_url)
            inserted_count = 0
            failed_count = 0
            
            # Process records in batches for better performance
            batch_size = 1000
            total_batches = (len(invoice_details) + batch_size - 1) // batch_size
            
            print(f"   🚀 Processing {len(invoice_details)} records in {total_batches} batches...")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(invoice_details))
                batch = invoice_details[start_idx:end_idx]
                
                print(f"   📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch)} records)...")
                
                with conn:
                    with conn.cursor() as cursor:
                        for record in batch:
                            try:
                                # Clean the record data
                                clean_record = self.clean_record_data(record)
                                
                                # Simple insert without duplicate checking - updated columns
                                cursor.execute("""
                                    INSERT INTO invoice_details (
                                        invoice_no, source_system, work_date, employee_id,
                                        employee_name_last, employee_name_first, employee_middle_initial,
                                        location_code, location_name, position_code,
                                        position_description, hours_regular, hours_overtime,
                                        hours_holiday, hours_total, rate_regular, rate_overtime,
                                        rate_holiday, amount_regular, amount_overtime, amount_holiday,
                                        amount_total, customer_number, customer_name, customer_number_ext,
                                        customer_ext_description, business_unit, shift_in, shift_out,
                                        bill_category, week_ending, created_at, updated_at
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s
                                    )
                                """, (
                                    clean_record.get('invoice_no'),
                                    clean_record.get('source_system', 'Unknown'),
                                    clean_record.get('work_date'),
                                    clean_record.get('employee_id'),
                                    clean_record.get('employee_name_last'),
                                    clean_record.get('employee_name_first'),
                                    clean_record.get('employee_middle_initial'),
                                    clean_record.get('location_code'),
                                    clean_record.get('location_name'),
                                    clean_record.get('position_code'),
                                    clean_record.get('position_description'),
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
                                    clean_record.get('customer_number_ext'),
                                    clean_record.get('customer_ext_description'),
                                    clean_record.get('business_unit'),
                                    clean_record.get('shift_in'),
                                    clean_record.get('shift_out'),
                                    clean_record.get('bill_category'),
                                    clean_record.get('week_ending'),
                                    datetime.now(),
                                    datetime.now()
                                ))
                                
                                inserted_count += 1
                                
                            except psycopg2.Error as e:
                                failed_count += 1
                                error_msg = str(e)[:200]  # Truncate long error messages
                                
                                # Only log unique constraint violations at debug level
                                if "duplicate key value violates unique constraint" in error_msg:
                                    logger.debug(f"Duplicate record skipped: {clean_record.get('invoice_no')}")
                                else:
                                    logger.warning(f"Insert error: {error_msg}")
                                    results['errors'].append({
                                        'invoice_no': clean_record.get('invoice_no'),
                                        'employee_id': clean_record.get('employee_id'),
                                        'error': error_msg
                                    })
                                continue
                
                print(f"      ✅ Batch {batch_num + 1} complete: {inserted_count} inserted so far")
            
            conn.close()
            
            results['inserted'] = inserted_count
            results['failed'] = failed_count
            results['success'] = True
            
            print(f"   🎯 Upload complete: {inserted_count:,} records inserted, {failed_count:,} failed")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in bulk insert: {e}")
            results['success'] = False
            results['errors'].append({'general_error': str(e)})
            return results
    
    # Commenting out this method for now - we may be able to remove it altogether later
    #def validate_invoice_totals(self) -> Dict:
        """
        Post-upload validation: Compare invoice_details totals against invoices table
        Only checks invoices that have details, not the other way around
        Allows for small rounding differences - up to 0.5% variance
        """
        validation_results = {
            'total_invoices_checked': 0,
            'matching_invoices': 0,
            'mismatched_invoices': 0,
            'discrepancies': [],
            'validation_timestamp': datetime.now()
        }
        
        try:
            conn = psycopg2.connect(self.database_url)
            
            with conn.cursor() as cursor:
                # Only check invoices that have details - correct direction!
                # Adjusted tolerance from $1.00 to 0.5%
                cursor.execute("""
                    WITH invoice_detail_totals AS (
                        SELECT 
                            invoice_no,
                            SUM(amount_total) as detail_total,
                            COUNT(*) as detail_count,
                            MIN(source_system) as source_system,
                            -- Check if any details have VOID in key fields
                            BOOL_OR(
                                UPPER(COALESCE(location_name, '')) LIKE '%VOID%' OR
                                UPPER(COALESCE(customer_ext_description, '')) LIKE '%VOID%' OR
                                UPPER(COALESCE(position_description, '')) LIKE '%VOID%'
                            ) as has_void_records
                        FROM invoice_details
                        WHERE amount_total IS NOT NULL
                        GROUP BY invoice_no
                    )
                    SELECT 
                        idt.invoice_no,
                        i.invoice_total,
                        idt.detail_total,
                        idt.detail_count,
                        idt.source_system,
                        idt.has_void_records,
                        CASE 
                            -- If detail_total is 0 AND has VOID records, it's a MATCH (expected)
                            WHEN idt.detail_total = 0 AND idt.has_void_records THEN 'MATCH'
                            -- Otherwise, apply the 0.5% tolerance check
                            WHEN ABS(COALESCE(i.invoice_total, 0) - idt.detail_total) <= (COALESCE(i.invoice_total, 0) * 0.005) THEN 'MATCH'
                            ELSE 'MISMATCH'
                        END as status
                    FROM invoice_detail_totals idt
                    LEFT JOIN invoices i ON idt.invoice_no = i.invoice_no
                    WHERE i.invoice_no IS NOT NULL
                    ORDER BY 
                        ABS(COALESCE(i.invoice_total, 0) - idt.detail_total) DESC
                """)
                
                results = cursor.fetchall()
                
                for row in results:
                    invoice_no, invoice_total, detail_total, detail_count, source_system, has_void_records, status = row
                    validation_results['total_invoices_checked'] += 1
                    
                    if status == 'MATCH':
                        validation_results['matching_invoices'] += 1
                    else:  # MISMATCH
                        validation_results['mismatched_invoices'] += 1
                        inv_total = float(invoice_total) if invoice_total is not None else 0
                        det_total = float(detail_total) if detail_total is not None else 0
                        discrepancy = inv_total - det_total
                        
                        validation_results['discrepancies'].append({
                            'invoice_no': invoice_no,
                            'invoice_total': inv_total,
                            'detail_total': det_total,
                            'discrepancy': discrepancy,
                            'discrepancy_pct': (discrepancy / inv_total * 100) if inv_total != 0 else 0,
                            'detail_count': detail_count,
                            'source': source_system,
                            'has_void_records': has_void_records
                        })
            
            conn.close()
            
            # Sort discrepancies by absolute amount
            validation_results['discrepancies'].sort(key=lambda x: abs(x['discrepancy']), reverse=True)
            
            # Add summary statistics
            if validation_results['discrepancies']:
                total_discrepancy = sum(d['discrepancy'] for d in validation_results['discrepancies'])
                validation_results['total_discrepancy_amount'] = total_discrepancy
                validation_results['largest_discrepancy'] = validation_results['discrepancies'][0]
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error in invoice validation: {e}")
            validation_results['error'] = str(e)
            return validation_results
    
    def validate_specific_invoice_totals(self, invoice_numbers: List[str]) -> Dict:
        """
        Validate only specific invoice numbers that were just uploaded
        """
        validation_results = {
            'total_invoices_checked': 0,
            'matching_invoices': 0,
            'mismatched_invoices': 0,
            'discrepancies': [],
            'validation_timestamp': datetime.now()
        }
        
        if not invoice_numbers:
            return validation_results
        
        try:
            conn = psycopg2.connect(self.database_url)
            
            with conn.cursor() as cursor:
                # Create a temporary table with the invoice numbers
                cursor.execute("CREATE TEMP TABLE temp_invoice_numbers (invoice_no TEXT)")
                
                # Use execute_values for efficient insertion
                from psycopg2.extras import execute_values
                execute_values(
                    cursor,
                    "INSERT INTO temp_invoice_numbers (invoice_no) VALUES %s",
                    [(inv,) for inv in invoice_numbers]
                )
                
                # Now run the validation query using the temp table
                query = """
                    WITH invoice_detail_totals AS (
                        SELECT 
                            d.invoice_no,
                            SUM(d.amount_total) as detail_total,
                            COUNT(*) as detail_count,
                            MIN(d.source_system) as source_system,
                            BOOL_OR(
                                UPPER(COALESCE(d.location_name, '')) LIKE '%VOID%' OR
                                UPPER(COALESCE(d.customer_ext_description, '')) LIKE '%VOID%' OR
                                UPPER(COALESCE(d.position_description, '')) LIKE '%VOID%'
                            ) as has_void_records
                        FROM invoice_details d
                        INNER JOIN temp_invoice_numbers t ON d.invoice_no = t.invoice_no
                        WHERE d.amount_total IS NOT NULL
                        GROUP BY d.invoice_no
                    )
                    SELECT 
                        idt.invoice_no,
                        i.invoice_total,
                        idt.detail_total,
                        idt.detail_count,
                        idt.source_system,
                        idt.has_void_records,
                        CASE 
                            WHEN idt.detail_total = 0 AND idt.has_void_records THEN 'MATCH'
                            WHEN ABS(COALESCE(i.invoice_total, 0) - idt.detail_total) <= (ABS(COALESCE(i.invoice_total, 0)) * 0.005) THEN 'MATCH'
                            ELSE 'MISMATCH'
                        END as status
                    FROM invoice_detail_totals idt
                    LEFT JOIN invoices i ON idt.invoice_no = i.invoice_no
                    WHERE i.invoice_no IS NOT NULL
                    ORDER BY ABS(COALESCE(i.invoice_total, 0) - idt.detail_total) DESC
                """
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                # Clean up temp table
                cursor.execute("DROP TABLE temp_invoice_numbers")
                
                #print(f"DEBUG: Query returned {len(results)} rows")
                
                for row in results:
                    if len(row) != 7:
                        print(f"ERROR: Expected 7 columns, got {len(row)}")
                        continue
                    
                    invoice_no, invoice_total, detail_total, detail_count, source_system, has_void_records, status = row
                    validation_results['total_invoices_checked'] += 1
                    
                    if status == 'MATCH':
                        validation_results['matching_invoices'] += 1
                    else:  # MISMATCH
                        validation_results['mismatched_invoices'] += 1
                        inv_total = float(invoice_total) if invoice_total is not None else 0
                        det_total = float(detail_total) if detail_total is not None else 0
                        discrepancy = inv_total - det_total
                        
                        validation_results['discrepancies'].append({
                            'invoice_no': invoice_no,
                            'invoice_total': inv_total,
                            'detail_total': det_total,
                            'discrepancy': discrepancy,
                            'discrepancy_pct': (discrepancy / inv_total * 100) if inv_total != 0 else 0,
                            'detail_count': detail_count,
                            'source': source_system,
                            'has_void_records': has_void_records
                        })
                
            conn.close()
            
            # Sort discrepancies by absolute amount
            validation_results['discrepancies'].sort(key=lambda x: abs(x['discrepancy']), reverse=True)
            
            # Add summary statistics
            if validation_results['discrepancies']:
                total_discrepancy = sum(d['discrepancy'] for d in validation_results['discrepancies'])
                validation_results['total_discrepancy_amount'] = total_discrepancy
                validation_results['largest_discrepancy'] = validation_results['discrepancies'][0]
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error in specific invoice validation: {e}")
            import traceback
            traceback.print_exc()
            validation_results['error'] = str(e)
            return validation_results

    def get_validation_summary_report(self, validation_results: Dict) -> str:
        """
        Generate a formatted summary report of validation results
        """
        report = []
        report.append("\n" + "="*60)
        report.append("INVOICE VALIDATION REPORT")
        report.append(f"Generated: {validation_results['validation_timestamp']}")
        report.append("="*60)
        
        report.append(f"\n📊 SUMMARY:")
        report.append(f"   Total Invoices with Details: {validation_results['total_invoices_checked']:,}")
        report.append(f"   ✅ Matching Totals: {validation_results['matching_invoices']:,}")
        report.append(f"   ❌ Mismatched Totals: {validation_results['mismatched_invoices']:,}")
        
        if validation_results.get('total_discrepancy_amount'):
            report.append(f"\n💰 FINANCIAL IMPACT:")
            report.append(f"   Total Discrepancy: ${validation_results['total_discrepancy_amount']:,.2f}")
            
            largest = validation_results['largest_discrepancy']
            report.append(f"   Largest Discrepancy: Invoice {largest['invoice_no']} - ${largest['discrepancy']:,.2f}")
        
        if validation_results['mismatched_invoices'] > 0:
            report.append(f"\n📋 TOP 10 DISCREPANCIES:")
            report.append(f"{'Invoice No':<15} {'Master Total':>12} {'Detail Total':>12} {'Difference':>12} {'%':>6}")
            report.append("-" * 60)
            
            for disc in validation_results['discrepancies'][:10]:
                report.append(
                    f"{disc['invoice_no']:<15} "
                    f"${disc['invoice_total']:>11,.2f} "
                    f"${disc['detail_total']:>11,.2f} "
                    f"${disc['discrepancy']:>11,.2f} "
                    f"{disc['discrepancy_pct']:>5.1f}%"
                )
        
        report.append("\n" + "="*60)
        
        return '\n'.join(report)
    
    def export_validation_results(self, validation_results: Dict) -> str:
        """Export validation results to Excel with CSV fallback"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Try Excel export first
            filename = f"validation_results_{timestamp}.xlsx"
            
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                # Summary sheet
                summary_data = {
                    'Metric': ['Total Invoices Checked', 'Matching Invoices', 'Mismatched Invoices'],
                    'Value': [
                        validation_results.get('total_invoices_checked', 0),
                        validation_results.get('matching_invoices', 0),
                        validation_results.get('mismatched_invoices', 0)
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Discrepancies sheet
                if validation_results.get('discrepancies'):
                    disc_df = pd.DataFrame(validation_results['discrepancies'])
                    disc_df.to_excel(writer, sheet_name='Discrepancies', index=False)
            
            logger.info(f"Successfully exported validation results to Excel: {filename}")
            return filename
            
        except Exception as excel_error:
            # Excel export failed, fall back to CSV
            logger.warning(f"Excel export failed: {excel_error}. Falling back to CSV.")
            
            try:
                # CSV fallback
                csv_filename = f"validation_results_{timestamp}.csv"
                
                # Combine summary and discrepancies into one CSV
                summary_data = {
                    'Section': ['SUMMARY', 'SUMMARY', 'SUMMARY'],
                    'Metric': ['Total Invoices Checked', 'Matching Invoices', 'Mismatched Invoices'],
                    'Value': [
                        validation_results.get('total_invoices_checked', 0),
                        validation_results.get('matching_invoices', 0),
                        validation_results.get('mismatched_invoices', 0)
                    ]
                }
                
                # Create combined dataframe
                combined_data = []
                
                # Add summary section
                for i in range(len(summary_data['Section'])):
                    combined_data.append({
                        'Section': summary_data['Section'][i],
                        'Metric': summary_data['Metric'][i],
                        'Value': summary_data['Value'][i]
                    })
                
                # Add blank row
                combined_data.append({})
                
                # Add discrepancies section
                if validation_results.get('discrepancies'):
                    combined_data.append({
                        'Section': 'DISCREPANCIES',
                        'Metric': 'Invoice Details Below',
                        'Value': ''
                    })
                    
                    for disc in validation_results['discrepancies']:
                        combined_data.append({
                            'Invoice_No': disc.get('invoice_no'),
                            'Invoice_Total': disc.get('invoice_total'),
                            'Detail_Total': disc.get('detail_total'),
                            'Discrepancy': disc.get('discrepancy'),
                            'Discrepancy_Pct': disc.get('discrepancy_pct'),
                            'Source': disc.get('source'),
                            'Has_Void_Records': disc.get('has_void_records')
                        })
                
                # Write to CSV
                df = pd.DataFrame(combined_data)
                df.to_csv(csv_filename, index=False)
                
                logger.info(f"Successfully exported validation results to CSV: {csv_filename}")
                return csv_filename
                
            except Exception as csv_error:
                logger.error(f"Both Excel and CSV exports failed: {csv_error}")
                return None
           
        
    def bulk_insert_invoice_details_fast_validated(self, invoice_details: List[Dict], progress_callback=None) -> Dict:
        """
        Optimized bulk insert using execute_values for much faster performance
        Validates invoice numbers exist in master table - updated for new columns
        """
        results = {
            'total_records': len(invoice_details),
            'inserted': 0,
            'failed': 0,
            'missing_invoice_count': 0,
            'missing_invoices': [],
            'missing_invoice_records': [],
            'error_records': [],
            'inserted_invoice_numbers': [],
            'success': False
        }
        
        if not invoice_details:
            results['success'] = True
            return results
        
        try:
            conn = psycopg2.connect(self.database_url)
            
            # Step 1: Get all existing invoice numbers (one query for speed)
            print("   📊 Loading existing invoice numbers for validation...")
            if progress_callback:
                progress_callback(0.05, "Loading invoice numbers...")
                
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT invoice_no FROM invoices WHERE invoice_no IS NOT NULL")
                existing_invoices = {row[0] for row in cursor.fetchall()}
            print(f"   ✅ Found {len(existing_invoices)} existing invoices in master")
            
            # Step 2: Separate records into valid and invalid
            if progress_callback:
                progress_callback(0.1, "Validating records...")
                
            valid_records = []
            seen_missing_invoices = set()
            
            for record in invoice_details:
                invoice_no = record.get('invoice_no')
                
                if not invoice_no:
                    results['error_records'].append({
                        'record': record,
                        'error': 'Missing invoice number'
                    })
                    results['failed'] += 1
                    continue
                
                if invoice_no not in existing_invoices:
                    # Track this missing invoice
                    if invoice_no not in seen_missing_invoices:
                        seen_missing_invoices.add(invoice_no)
                        results['missing_invoices'].append(invoice_no)
                    
                    # Store the full record for download - updated to handle new name fields
                    employee_name = (
                        f"{record.get('employee_name_first', '')} {record.get('employee_name_last', '')}".strip() 
                        or record.get('employee_name', '')
                    )
                    
                    results['missing_invoice_records'].append({
                        'invoice_no': invoice_no,
                        'employee_id': record.get('employee_id'),
                        'employee_name': employee_name,
                        'work_date': record.get('work_date'),
                        'amount_total': record.get('amount_total', 0),
                        'reason': 'Invoice not found in master table'
                    })
                    results['missing_invoice_count'] += 1
                    results['failed'] += 1
                else:
                    valid_records.append(record)
            
            print(f"   📋 Validation complete:")
            print(f"      - Valid records: {len(valid_records)}")
            print(f"      - Missing invoices: {len(results['missing_invoices'])}")
            print(f"      - Other errors: {len(results['error_records'])}")
            
            # Step 3: Bulk insert valid records using execute_values
            if valid_records:
                inserted_count = 0
                batch_size = 5000  # Increased from 1000
                total_batches = (len(valid_records) + batch_size - 1) // batch_size
                
                print(f"   🚀 Processing {len(valid_records)} valid records in {total_batches} batches...")
                
                # Prepare the column list - updated for new structure
                columns = [
                    'invoice_no', 'source_system', 'work_date', 'employee_id',
                    'employee_name_last', 'employee_name_first', 'employee_middle_initial',
                    'location_code', 'location_name', 'position_code',
                    'position_description', 'hours_regular', 'hours_overtime',
                    'hours_holiday', 'hours_total', 'rate_regular', 'rate_overtime',
                    'rate_holiday', 'amount_regular', 'amount_overtime', 'amount_holiday',
                    'amount_total', 'customer_number', 'customer_name', 'customer_number_ext',
                    'customer_ext_description', 'business_unit', 'shift_in', 'shift_out',
                    'bill_category', 'week_ending', 'created_at', 'updated_at'
                ]
                
                insert_query = f"""
                    INSERT INTO invoice_details ({', '.join(columns)})
                    VALUES %s
                """
                
                for batch_num in range(total_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, len(valid_records))
                    batch = valid_records[start_idx:end_idx]
                    
                    # Update progress
                    progress = 0.1 + (0.85 * (batch_num + 1) / total_batches)
                    if progress_callback:
                        progress_callback(progress, f"Processing batch {batch_num + 1}/{total_batches} ({len(batch)} records)...")
                    
                    # Prepare batch data
                    batch_data = []
                    if batch_num == 0:
                        #print(f"\n      🔍 DEBUG - Column Order:")
                        for i, col in enumerate(columns[:10]):  # First 10 columns
                            print(f"         Position {i}: {col}")
                        print(f"         ... and {len(columns) - 10} more columns")
                    
                    for record in batch:
                        # Clean the record data
                        clean_record = self.clean_record_data(record)
                        
                        # Create tuple of values in same order as columns
                        values = (
                            clean_record.get('invoice_no'),
                            clean_record.get('source_system', 'Unknown'),
                            clean_record.get('work_date'),
                            clean_record.get('employee_id'),
                            clean_record.get('employee_name_last'),
                            clean_record.get('employee_name_first'),
                            clean_record.get('employee_middle_initial'),
                            clean_record.get('location_code'),
                            clean_record.get('location_name'),
                            clean_record.get('position_code'),
                            clean_record.get('position_description'),
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
                            clean_record.get('customer_number_ext'),
                            clean_record.get('customer_ext_description'),
                            clean_record.get('business_unit'),
                            clean_record.get('shift_in'),
                            clean_record.get('shift_out'),
                            clean_record.get('bill_category'),
                            clean_record.get('week_ending'),
                            datetime.now(),
                            datetime.now()
                        )
                        batch_data.append(values)

                        # Debug: Check first record of first batch
                        if batch_num == 0 and len(batch_data) == 1:
                            #print(f"\n      🔍 DEBUG - First Record Values:")
                            #print(f"         invoice_no (pos 0): {values[0]}")
                            #print(f"         source_system (pos 1): {values[1]}")
                            #print(f"         employee_name_last (pos 4): {values[4]}")
                            #print(f"         employee_name_first (pos 5): {values[5]}")
                            #print(f"         Total values in tuple: {len(values)}")
                            pass
                    # Execute batch insert
                    try:
                        with conn:
                            with conn.cursor() as cursor:
                                result = execute_values(
                                    cursor,
                                    insert_query,
                                    batch_data,
                                    page_size=1000  # How many records to send at once
                                )
                                # Count successful inserts
                                inserted_in_batch = len(batch_data)
                                inserted_count += inserted_in_batch

                                # Track which invoice numbers were inserted in this batch
                                invoices_before = len(results['inserted_invoice_numbers'])
                                
                                for i, values in enumerate(batch_data):
                                    invoice_no = values[0]  # First value in tuple is invoice_no
                                    
                                    # Debug first few invoices
                                    if batch_num == 0 and i < 3:
                                        print(f"         Tracking invoice {i+1}: {invoice_no} (type: {type(invoice_no)})")
                                    
                                    if invoice_no and invoice_no not in results['inserted_invoice_numbers']:
                                        results['inserted_invoice_numbers'].append(invoice_no)
                                
                                invoices_after = len(results['inserted_invoice_numbers'])
                                if batch_num == 0:
                                    print(f"      📋 Invoices tracked in batch 1: {invoices_after - invoices_before}")
                                    print(f"      📋 Total unique invoices so far: {invoices_after}")
                                
                        print(f"      ✅ Batch {batch_num + 1} complete: {inserted_count} inserted so far")
                        
                    except psycopg2.IntegrityError as e:
                        # This batch had some duplicates, we need to handle it differently
                        logger.warning(f"Batch {batch_num + 1} had duplicates, inserting one by one")
                        
                        # Fall back to individual inserts for this batch
                        for values in batch_data:
                            try:
                                with conn:
                                    with conn.cursor() as cursor:
                                        cursor.execute(
                                            f"INSERT INTO invoice_details ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                                            values
                                        )
                                        inserted_count += 1
                                        # Track this invoice number
                                        invoice_no = values[0]  # First value is invoice_no
                                        if invoice_no and invoice_no not in results['inserted_invoice_numbers']:
                                            results['inserted_invoice_numbers'].append(invoice_no)

                            except psycopg2.IntegrityError:
                                # This specific record is a duplicate, skip it
                                continue
                            except Exception as e:
                                logger.error(f"Error inserting individual record: {e}")
                                continue
                        
                        print(f"      ✅ Batch {batch_num + 1} complete: {inserted_count} inserted so far")
                        
                    except Exception as e:
                        logger.error(f"Error in batch {batch_num + 1}: {e}")
                        # Continue with next batch
                        continue
                
                results['inserted'] = inserted_count
            
            conn.close()
            results['success'] = True

            # Get unique invoice numbers that were inserted
            results['inserted_invoice_numbers'] = list(set(results['inserted_invoice_numbers']))
            
            if progress_callback:
                progress_callback(1.0, "Upload complete!")
            
            # Final summary
            print(f"\n   🎯 Upload Summary:")
            print(f"      Total records: {results['total_records']:,}")
            print(f"      Inserted: {results['inserted']:,}")
            print(f"      Unique invoices tracked: {len(results['inserted_invoice_numbers']):,}")
            if results['inserted_invoice_numbers'][:3]:
                print(f"      First 3 invoices: {results['inserted_invoice_numbers'][:3]}")
            print(f"      Missing invoices: {results['missing_invoice_count']:,}")
            print(f"      Other errors: {len(results['error_records']):,}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in fast validated bulk insert: {e}")
            results['success'] = False
            results['error_records'].append({'general_error': str(e)})
            return results
    
    # =============================================================================
    # EMAIL HTML PROCESSING IMPLEMENTATION
    # Add these methods to your EnhancedDataMapper class
    # =============================================================================

    def detect_payment_email_html(self, content: str) -> bool:
        """
        Enhanced detection for Kaiser Permanente payment emails
        Based on the debug output showing 6/7 indicators consistently found
        """
        try:
            if isinstance(content, bytes):
                # Handle bytes content by decoding
                for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                    try:
                        content = content.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    content = content.decode('utf-8', errors='replace')
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for Kaiser Permanente specific indicators (based on debug output)
            content_text = content.lower()
            
            # Check for KP-specific patterns
            kp_indicators = [
                'payment id', 'vendor id', 'blackstone consulting', 
                'invoice id', 'gross amount', 'net amount', 'payment date'
            ]
            
            found_kp_indicators = sum(1 for indicator in kp_indicators if indicator in content_text)
            
            # Also check for table structure (should have 4 tables based on debug)
            tables = soup.find_all('table')
            
            logger.info(f"KP payment detection: {found_kp_indicators}/{len(kp_indicators)} indicators, {len(tables)} tables")
            
            # Need at least 5 KP indicators and 3+ tables for Kaiser emails
            return found_kp_indicators >= 5 and len(tables) >= 3
            
        except Exception as e:
            logger.warning(f"Error detecting payment email HTML: {e}")
            return False

    def extract_kaiser_payment_metadata(self, html_content: str) -> dict:
        """
        Extract payment header info from Kaiser email
        Based on debug output showing Table 3 contains payment metadata
        """
        import re
        from datetime import datetime
        
        # Handle bytes content
        if isinstance(html_content, bytes):
            for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                try:
                    html_content = html_content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                html_content = html_content.decode('utf-8', errors='replace')
        
        payment_id = None
        payment_date = None
        payment_amount = None
        vendor_name = 'BLACKSTONE CONSULTING INC'
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            
            # Based on debug output: Table 3 (index 2) contains payment metadata
            if len(tables) >= 3:
                payment_table = tables[2]  # Table 3 from debug output
                
                # Extract payment metadata from table 3
                rows = payment_table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        
                        if 'payment id' in label:
                            payment_id = value.zfill(10)  # Ensure 10 digits
                            logger.info(f"Found Payment ID: {payment_id}")
                        elif 'payment date' in label:
                            try:
                                payment_date_obj = self.convert_excel_date(value)
                                if payment_date_obj:
                                    payment_date = payment_date_obj.strftime('%Y-%m-%d')
                                    logger.info(f"Found Payment Date: {payment_date}")
                            except:
                                pass
                        elif 'payment amount' in label or 'amount' in label:
                            try:
                                # Clean up amount string
                                amount_str = re.sub(r'[^\d\.]', '', value)
                                if amount_str:
                                    payment_amount = float(amount_str)
                                    logger.info(f"Found Payment Amount: ${payment_amount:,.2f}")
                            except:
                                pass
            
            # Fallback: regex search in full content
            if not payment_id:
                payment_id_match = re.search(r'payment\s*id[:\s]*([0-9]{10})', html_content, re.IGNORECASE)
                if payment_id_match:
                    payment_id = payment_id_match.group(1)
                    logger.info(f"Found Payment ID via regex: {payment_id}")
            
            if not payment_amount:
                # Look for amount in various formats
                amount_patterns = [
                    r'payment\s*amount[:\s]*\$?([0-9,\.]+)',
                    r'amount[:\s]*\$?([0-9,\.]+)',
                    r'\$([0-9,\.]+)'
                ]
                
                for pattern in amount_patterns:
                    amount_match = re.search(pattern, html_content, re.IGNORECASE)
                    if amount_match:
                        try:
                            amount_str = amount_match.group(1).replace(',', '')
                            payment_amount = float(amount_str)
                            logger.info(f"Found Payment Amount via regex: ${payment_amount:,.2f}")
                            break
                        except:
                            continue
            
            # Calculate amount from invoice details if not found in header
            if not payment_amount and len(tables) >= 4:
                try:
                    details_table = tables[3]  # Table 4 from debug output
                    total_amount = 0
                    
                    rows = details_table.find_all('tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:  # Expecting Date, Invoice ID, Amount columns
                            # Get the last cell which should be the amount
                            amount_cell = cells[-1].get_text(strip=True)
                            amount_match = re.search(r'([0-9,\.]+)', amount_cell)
                            if amount_match:
                                try:
                                    amount = float(amount_match.group(1).replace(',', ''))
                                    total_amount += amount
                                except:
                                    continue
                    
                    if total_amount > 0:
                        payment_amount = total_amount
                        logger.info(f"Calculated Payment Amount from details: ${payment_amount:,.2f}")
                except Exception as e:
                    logger.debug(f"Error calculating amount from details: {e}")
            
            # Set defaults if not found
            if not payment_id:
                import hashlib
                content_hash = hashlib.md5(html_content.encode()).hexdigest()[:8]
                payment_id = f'EMAIL_{content_hash}'
            
            if not payment_date:
                payment_date = datetime.now().strftime('%Y-%m-%d')
            
            if not payment_amount:
                payment_amount = 0.0
            
            logger.info(f"Final extracted payment metadata: ID={payment_id}, Date={payment_date}, Amount=${payment_amount:,.2f}, Vendor={vendor_name}")
            
            return {
                'payment_id': payment_id,
                'payment_date': payment_date,
                'payment_amount': payment_amount,
                'vendor_name': vendor_name
            }
            
        except Exception as e:
            logger.error(f"Error extracting payment metadata: {e}")
            import hashlib
            content_hash = hashlib.md5(str(html_content).encode()).hexdigest()[:8]
            return {
                'payment_id': f'EMAIL_{content_hash}',
                'payment_date': datetime.now().strftime('%Y-%m-%d'),
                'payment_amount': 0.0,
                'vendor_name': 'BLACKSTONE CONSULTING INC'
            }

    def parse_payment_email_html(self, html_content: str) -> pd.DataFrame:
        """
        Parse payment data from Kaiser email HTML tables
        Based on debug showing Table 4 contains invoice details
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} total tables in email HTML")
            
            # Based on debug output: Table 4 (index 3) contains invoice details
            if len(tables) < 4:
                raise ValueError(f"Expected at least 4 tables, found {len(tables)}")
            
            payment_table = tables[3]  # Table 4 from debug output
            
            # Extract headers and data
            rows = payment_table.find_all('tr')
            logger.info(f"Found {len(rows)} rows in payment details table")
            
            if not rows:
                raise ValueError("No rows found in payment table")
            
            # Get headers from first row
            header_row = rows[0]
            headers = []
            for cell in header_row.find_all(['td', 'th']):
                header_text = cell.get_text(strip=True)
                headers.append(header_text)
            
            logger.info(f"Headers found: {headers}")
            
            # Extract data from remaining rows
            data_rows = []
            for row_idx, row in enumerate(rows[1:], 1):
                cells = row.find_all(['td', 'th'])
                
                if len(cells) == 0:
                    logger.debug(f"Skipping empty row {row_idx}")
                    continue
                
                # Extract cell values
                cell_values = []
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    cell_values.append(cell_text)
                
                # Only add rows with data
                if any(cell_values):
                    # Pad with empty strings if row has fewer cells than headers
                    while len(cell_values) < len(headers):
                        cell_values.append('')
                    
                    data_rows.append(cell_values[:len(headers)])  # Trim if too many cells
                    logger.debug(f"Row {row_idx}: {cell_values}")
            
            if not data_rows:
                raise ValueError("No data rows found in payment table")
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers[:len(data_rows[0])])
            logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            logger.error(f"Error parsing payment email HTML: {e}")
            raise

    def standardize_payment_html_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names from email HTML to match database schema
        Based on Kaiser email format from debug output
        """
        try:
            logger.info(f"Standardizing HTML columns: {list(df.columns)}")
            
            # Column mapping for Kaiser email format
            column_mapping = {
                'Date': 'payment_date',
                'Invoice ID': 'invoice_id',
                'Gross Amount': 'gross_amount',
                'Net Amount': 'net_amount',
                'Discount Amount': 'discount_amount',
                'Payment Date': 'payment_date',
                'Invoice Number': 'invoice_id',
                'Amount': 'net_amount',
                'Total': 'net_amount'
            }
            
            # Apply column mapping
            standardized_df = df.copy()
            for old_name, new_name in column_mapping.items():
                if old_name in standardized_df.columns:
                    standardized_df = standardized_df.rename(columns={old_name: new_name})
                    logger.info(f"Mapped column: '{old_name}' → '{new_name}'")
            
            # Clean up amount columns
            amount_columns = ['gross_amount', 'net_amount', 'discount_amount']
            for col in amount_columns:
                if col in standardized_df.columns:
                    standardized_df[col] = standardized_df[col].apply(self.clean_amount_value)
            
            # Clean up date columns
            if 'payment_date' in standardized_df.columns:
                standardized_df['payment_date'] = standardized_df['payment_date'].apply(
                    lambda x: self.convert_excel_date(x).strftime('%Y-%m-%d') if self.convert_excel_date(x) else None
                )
            
            logger.info(f"Standardized columns: {list(standardized_df.columns)}")
            return standardized_df
            
        except Exception as e:
            logger.error(f"Error standardizing HTML payment columns: {e}")
            raise

    def process_payment_email_html(self, html_content: str) -> tuple:
        """
        Process complete Kaiser payment email HTML
        Returns master data and detail records
        """
        try:
            logger.info("Processing Kaiser payment email HTML")
            
            # Extract payment metadata from header tables
            master_data = self.extract_kaiser_payment_metadata(html_content)
            
            # Parse invoice details from data table
            details_df = self.parse_payment_email_html(html_content)
            
            # Standardize column names
            standardized_df = self.standardize_payment_html_columns(details_df)
            
            # Convert to records format for database insertion
            detail_records = []
            for _, row in standardized_df.iterrows():
                record = {
                    'payment_id': master_data['payment_id'],
                    'payment_date': master_data['payment_date'],
                    'vendor_name': master_data['vendor_name'],
                    'invoice_id': row.get('invoice_id', ''),
                    'gross_amount': float(row.get('gross_amount', 0)),
                    'net_amount': float(row.get('net_amount', 0)),
                    'discount_amount': float(row.get('discount_amount', 0))
                }
                detail_records.append(record)
            
            logger.info(f"Processed email HTML: {len(detail_records)} detail records")
            return master_data, detail_records
            
        except Exception as e:
            logger.error(f"Error processing payment email HTML: {e}")
            raise

    # =============================================================================
    # STREAMLIT EMAIL PROCESSING FUNCTION
    # Add this function to your invoice_app_auto_detect.py file
    # =============================================================================

    def process_kp_payment_html(html_content: str, filename: str = "email_content") -> bool:
        """
        Process Kaiser Permanente payment HTML email content
        """
        st.subheader("📧 Processing Kaiser Permanente Payment Email")
        
        try:
            # Step 1: Process HTML content
            with st.spinner("Parsing HTML email content..."):
                mapper = st.session_state.data_mapper
                master_data, detail_records = mapper.process_payment_email_html(html_content)
            
            st.success(f"✅ Parsed email content: {len(detail_records)} invoice records found")
            
            # Step 2: Display parsed data preview
            with st.expander("📋 Parsed Email Data Preview"):
                if detail_records:
                    preview_df = pd.DataFrame(detail_records[:5])  # Show first 5
                    display_df = safe_dataframe_display(preview_df, 5)
                    st.dataframe(display_df)
                    
                    if len(detail_records) > 5:
                        st.info(f"Showing first 5 of {len(detail_records)} records")
            
            # Step 3: Display payment summary
            st.markdown("### 📋 Payment Summary")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Payment ID", master_data['payment_id'])
            with col2:
                st.metric("Payment Date", master_data['payment_date'])
            with col3:
                st.metric("Payment Amount", f"${master_data['payment_amount']:,.2f}")
            
            # Step 4: Check for existing payment
            db = st.session_state.enhanced_db_manager
            payment_exists = db.check_payment_exists(master_data['payment_id'])
            
            if payment_exists:
                st.error("⚠️ **Payment Already Processed**")
                existing_summary = db.get_payment_summary(master_data['payment_id'])
                st.json(existing_summary)
                return False
            
            # Step 5: Process to database
            if st.button("💾 Save Payment to Database", type="primary"):
                with st.spinner("Saving to database..."):
                    
                    # Insert master record
                    success = db.insert_payment_master(
                        payment_id=master_data['payment_id'],
                        payment_date=master_data['payment_date'],
                        payment_amount=master_data['payment_amount'],
                        vendor_name=master_data['vendor_name'],
                        source_file=filename
                    )
                    
                    if success:
                        # Insert detail records
                        details_success = db.insert_payment_details_batch(detail_records)
                        
                        if details_success:
                            st.success(f"✅ **Successfully processed payment {master_data['payment_id']}!**")
                            st.success(f"💾 Saved {len(detail_records)} invoice detail records")
                            
                            # Add to processing log
                            add_log(f"Processed Kaiser email payment: {master_data['payment_id']} with {len(detail_records)} details")
                            
                            return True
                        else:
                            st.error("❌ Failed to save payment details")
                            return False
                    else:
                        st.error("❌ Failed to save payment master record")
                        return False
            
            return True
            
        except Exception as e:
            st.error(f"❌ Error processing email HTML: {e}")
            add_log(f"Email HTML processing error: {e}")
            return False

    # =============================================================================
    # MSG FILE PROCESSING ENHANCEMENT
    # Update your process_msg_file function in invoice_app_auto_detect.py
    # =============================================================================

    def process_msg_file(uploaded_file) -> bool:
        """
        Process .msg Outlook email files containing payment remittance data
        Enhanced with proper HTML extraction and decoding
        """
        try:
            import tempfile
            import os
            
            st.info("📧 Processing Outlook .msg file...")
            
            # Save uploaded file to temporary location (required for extract-msg)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.msg') as tmp_file:
                tmp_file.write(uploaded_file.read())
                tmp_file_path = tmp_file.name
            
            try:
                # Import here to provide better error message if not installed
                try:
                    import extract_msg
                except ImportError:
                    st.error("❌ **Missing Dependency**")
                    st.error("To process Outlook .msg files, please install: `pip install extract-msg`")
                    st.info("💡 **Alternative:** Save the email as .eml format instead")
                    return False
                
                # Extract the message
                msg = extract_msg.Message(tmp_file_path)
                
                # Show email metadata
                with st.expander("📧 Email Information"):
                    st.write(f"**From:** {msg.sender or 'Unknown'}")
                    st.write(f"**Subject:** {msg.subject or 'No Subject'}")
                    st.write(f"**Date:** {msg.date or 'Unknown'}")
                    if hasattr(msg, 'attachments') and msg.attachments:
                        st.write(f"**Attachments:** {len(msg.attachments)} files")
                
                # Get HTML body and handle encoding properly
                html_content = msg.htmlBody
                
                if html_content:
                    # Handle bytes encoding
                    if isinstance(html_content, bytes):
                        try:
                            # Try different encodings
                            for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                                try:
                                    html_content = html_content.decode(encoding)
                                    st.success(f"✅ Successfully decoded HTML content using {encoding}")
                                    break
                                except UnicodeDecodeError:
                                    continue
                            else:
                                # If all encodings fail, use utf-8 with error handling
                                html_content = html_content.decode('utf-8', errors='replace')
                                st.warning("⚠️ Used UTF-8 with error replacement for HTML content")
                        except Exception as e:
                            st.error(f"Failed to decode HTML content: {e}")
                            return False
                    
                    st.success("✅ Found HTML content in Outlook message")
                    
                    # Check if it contains payment data
                    mapper = st.session_state.data_mapper
                    if mapper.detect_payment_email_html(html_content):
                        st.success("💰 Payment data detected in email HTML")
                        return process_kp_payment_html(html_content, uploaded_file.name)
                    else:
                        st.warning("⚠️ No payment data detected in HTML content")
                        
                        # Show preview of HTML content for debugging
                        with st.expander("🔍 HTML Content Preview"):
                            preview = html_content[:500]
                            st.text(preview)
                            if len(html_content) > 500:
                                st.write("... (truncated)")
                        
                        return False
                
                else:
                    # Try text content as fallback
                    text_content = msg.body
                    if text_content:
                        st.warning("📄 Only plain text content found - payment processing requires HTML tables")
                        
                        with st.expander("📄 Text Content Preview"):
                            preview = text_content[:500]
                            st.text(preview)
                            if len(text_content) > 500:
                                st.write("... (truncated)")
                        
                        st.info("💡 **Tip:** Ensure the original email was sent in HTML format with tables.")
                    else:
                        st.error("❌ No content found in .msg file")
                    
                    return False
                    
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)
            
        except Exception as e:
            st.error(f"Error processing .msg file: {e}")
            add_log(f"MSG processing error: {e}")
            return False

    def check_payment_exists(self, payment_id: str) -> bool:
        """
        Check if payment already exists in database
        """
        try:
            conn = psycopg2.connect(self.database_url)  # Use your existing pattern
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT 1 FROM kp_payment_master WHERE payment_id = %s",
                (payment_id,)
            )
            
            result = cursor.fetchone() is not None
            cursor.close()
            conn.close()
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking payment existence: {e}")
            raise

    def get_payment_summary(self, payment_id: str) -> Dict[str, Any]:
        """
        Get summary of existing payment
        """
        try:
            conn = psycopg2.connect(self.database_url)  # Use your existing pattern
            cursor = conn.cursor()
            
            # Get master payment info
            cursor.execute("""
                SELECT payment_id, payment_date, payment_amount, created_at
                FROM kp_payment_master 
                WHERE payment_id = %s
            """, (payment_id,))
            
            master_row = cursor.fetchone()
            if not master_row:
                cursor.close()
                conn.close()
                return None
            
            # Get detail count and totals
            cursor.execute("""
                SELECT 
                    COUNT(*) as detail_count,
                    COALESCE(SUM(gross_amount), 0) as total_gross,
                    COALESCE(SUM(discount), 0) as total_discount,
                    COALESCE(SUM(net_amount), 0) as total_net
                FROM kp_payment_details 
                WHERE payment_id = %s
            """, (payment_id,))
            
            detail_row = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return {
                'payment_id': master_row[0],
                'payment_date': master_row[1],
                'payment_amount': master_row[2],
                'created_at': master_row[3],
                'detail_count': detail_row[0] if detail_row else 0,
                'total_gross': detail_row[1] if detail_row else 0,
                'total_discount': detail_row[2] if detail_row else 0,
                'total_net': detail_row[3] if detail_row else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting payment summary: {e}")
            raise
    
    def insert_payment_master(self, master_data: Dict[str, Any]) -> bool:
        """
        Insert payment master record
        """
        try:
            conn = psycopg2.connect(self.database_url)  # Use your existing pattern
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO kp_payment_master (payment_id, payment_date, payment_amount)
                VALUES (%s, %s, %s)
                ON CONFLICT (payment_id) DO NOTHING
            """, (
                master_data['payment_id'],
                master_data['payment_date'],
                master_data['payment_amount']
            ))
            
            rows_affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"Inserted payment master: {master_data['payment_id']}")
                return True
            else:
                logger.warning(f"Payment master already exists: {master_data['payment_id']}")
                return False
                
        except Exception as e:
            logger.error(f"Error inserting payment master: {e}")
            raise

    def bulk_insert_payment_details(self, detail_records: List[Dict[str, Any]], 
                               progress_callback=None) -> Dict[str, Any]:
        """
        Bulk insert payment detail records with progress tracking
        """
        try:
            total_records = len(detail_records)
            batch_size = 1000
            inserted_count = 0
            error_count = 0
            error_records = []
            
            logger.info(f"Starting bulk insert of {total_records} payment detail records")
            
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for i in range(0, total_records, batch_size):
                batch = detail_records[i:i + batch_size]
                batch_values = []
                
                for record in batch:
                    try:
                        batch_values.append((
                            record['payment_id'],
                            record['invoice_no'],
                            record['payment_date'],
                            record.get('gross_amount', 0),
                            record.get('discount', 0),
                            record.get('net_amount', 0),
                            record.get('payment_message')
                        ))
                    except KeyError as e:
                        logger.error(f"Missing required field in record: {e}")
                        error_records.append({'record': record, 'error': f"Missing field: {e}"})
                        error_count += 1
                        continue
                
                if batch_values:
                    try:
                        # FIXED: Remove the local import
                        psycopg2.extras.execute_values(
                            cursor,
                            """
                            INSERT INTO kp_payment_details 
                            (payment_id, invoice_no, payment_date, gross_amount, discount, net_amount, payment_message)
                            VALUES %s
                            ON CONFLICT (payment_id, invoice_no) DO UPDATE SET
                                payment_date = EXCLUDED.payment_date,
                                gross_amount = EXCLUDED.gross_amount,
                                discount = EXCLUDED.discount,
                                net_amount = EXCLUDED.net_amount,
                                payment_message = EXCLUDED.payment_message
                            """,
                            batch_values,
                            template=None,
                            page_size=batch_size
                        )
                        
                        batch_inserted = cursor.rowcount
                        inserted_count += batch_inserted
                        
                        if progress_callback:
                            progress = min((i + len(batch)) / total_records, 1.0)
                            progress_callback(progress, f"Processed {i + len(batch):,} / {total_records:,} records")
                        
                    except Exception as e:
                        logger.error(f"Database error in batch {i//batch_size + 1}: {e}")
                        error_count += len(batch)
                        for record in batch:
                            error_records.append({'record': record, 'error': str(e)})
            
            conn.commit()
            cursor.close()
            conn.close()
            
            results = {
                'total_records': total_records,
                'inserted': inserted_count,
                'errors': error_count,
                'error_records': error_records,
                'success': error_count == 0
            }
            
            logger.info(f"Bulk insert completed: {inserted_count} inserted, {error_count} errors")
            return results
            
        except Exception as e:
            logger.error(f"Error in bulk insert payment details: {e}")
            raise    

    def process_payment_remittance(self, master_data: Dict[str, Any], 
                             detail_records: List[Dict[str, Any]],
                             progress_callback=None) -> Dict[str, Any]:
        """
        Process complete payment remittance (master + details) in transaction
        """
        payment_id = master_data['payment_id']
        
        try:
            # Check if payment already exists
            if self.check_payment_exists(payment_id):
                existing_summary = self.get_payment_summary(payment_id)
                return {
                    'success': False,
                    'error': 'Payment already exists',
                    'payment_id': payment_id,
                    'existing_payment': existing_summary
                }
            
            # FIXED: Use the insert_payment_master method instead of manual transaction
            master_inserted = self.insert_payment_master(master_data)
            
            if not master_inserted:
                return {
                    'success': False,
                    'error': 'Failed to insert payment master (may already exist)',
                    'payment_id': payment_id
                }
            
            # Insert detail records using bulk method
            detail_results = self.bulk_insert_payment_details(
                detail_records, 
                progress_callback
            )
            
            # Get final summary
            final_summary = self.get_payment_summary(payment_id)
            
            return {
                'success': True,
                'payment_id': payment_id,
                'master_inserted': True,
                'detail_results': detail_results,
                'final_summary': final_summary
            }
                
        except Exception as e:
            logger.error(f"Error processing payment remittance: {e}")
            return {
                'success': False,
                'error': str(e),
                'payment_id': payment_id
            }
      
# For backward compatibility
EnhancedDatabaseManager = CompatibleEnhancedDatabaseManager