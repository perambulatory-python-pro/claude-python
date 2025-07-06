"""
Enhanced Compatible Database Manager - FIXED VERSION WITHOUT CIRCULAR IMPORTS
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
import psycopg2.extras
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
    
    REMOVED the problematic imports from invoice_app_auto_detect to fix circular dependency
    """
    
    def __init__(self):
        """Initialize database connection from environment variables"""
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        # Initialize schema info
        self.schema_info = self._get_schema_info()
        
        logger.info("CompatibleEnhancedDatabaseManager initialized")
    
    def _get_schema_info(self) -> Dict:
        """Get information about database schema"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get column names from invoices table
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'invoices'
            """)
            columns = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return {
                'column_names': columns,
                'has_original_dates': any('original_' in col for col in columns),
                'has_not_transmitted': 'not_transmitted' in columns
            }
        except Exception as e:
            logger.error(f"Error getting schema info: {e}")
            return {
                'column_names': [],
                'has_original_dates': False,
                'has_not_transmitted': False
            }
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = psycopg2.connect(self.database_url)
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def preserve_date_logic(self, existing_record: Dict, new_date: Any, 
                           date_field: str, original_field: str) -> tuple:
        """
        Preserve original date logic
        """
        existing_date = existing_record.get(date_field)
        existing_original = existing_record.get(original_field)
        
        if existing_date and new_date != existing_date:
            # Date is changing, preserve the original
            if not existing_original:
                # No original stored yet, store the existing date
                return new_date, existing_date
            else:
                # Already have an original, keep it
                return new_date, existing_original
        else:
            # Date not changing, keep everything as is
            return existing_date, existing_original
    
    def apply_not_transmitted_logic(self, existing_record: Dict, 
                                  updates: Dict, processing_type: str) -> Optional[date]:
        """
        Apply not_transmitted logic based on processing type
        """
        if not self.schema_info['has_not_transmitted']:
            return None
            
        if processing_type == 'EDI' and updates.get('edi_date'):
            return None  # Clear not_transmitted
        elif processing_type == 'Release' and updates.get('release_date'):
            return None  # Clear not_transmitted
        elif processing_type == 'Add-On':
            # Keep existing not_transmitted for Add-On
            return existing_record.get('not_transmitted')
        
        return existing_record.get('not_transmitted')
    
    def upsert_invoices_with_business_logic(self, invoices: List[Dict], 
                                          processing_type: str,
                                          processing_date: date) -> Dict:
        """
        Upsert invoices with full business logic
        """
        results = {
            'inserted': 0,
            'updated': 0,
            'failed': 0,
            'errors': []
        }
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for invoice in invoices:
                try:
                    # Check if invoice exists
                    cursor.execute(
                        "SELECT * FROM invoices WHERE invoice_no = %s",
                        (invoice['invoice_no'],)
                    )
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Build column mapping
                        cursor.execute("""
                            SELECT column_name, ordinal_position 
                            FROM information_schema.columns 
                            WHERE table_name = 'invoices'
                            ORDER BY ordinal_position
                        """)
                        columns = [(row[0], row[1] - 1) for row in cursor.fetchall()]
                        existing_dict = {col[0]: existing[col[1]] for col in columns}
                        
                        # Apply business logic
                        updates = self._apply_update_logic(
                            existing_dict, invoice, processing_type, processing_date
                        )
                        
                        if updates:
                            # Perform update
                            set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                            values = list(updates.values()) + [invoice['invoice_no']]
                            
                            cursor.execute(
                                f"UPDATE invoices SET {set_clause} WHERE invoice_no = %s",
                                values
                            )
                            results['updated'] += 1
                    else:
                        # Insert new invoice
                        invoice['processing_date'] = processing_date
                        invoice['processing_type'] = processing_type
                        
                        columns = list(invoice.keys())
                        values = list(invoice.values())
                        placeholders = ", ".join(["%s"] * len(values))
                        
                        cursor.execute(
                            f"INSERT INTO invoices ({', '.join(columns)}) VALUES ({placeholders})",
                            values
                        )
                        results['inserted'] += 1
                        
                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append({
                        'invoice_no': invoice.get('invoice_no'),
                        'error': str(e)
                    })
                    logger.error(f"Error processing invoice {invoice.get('invoice_no')}: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error in upsert_invoices: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
        
        return results
    
    def _apply_update_logic(self, existing: Dict, new_data: Dict, 
                           processing_type: str, processing_date: date) -> Dict:
        """
        Apply update logic based on processing type
        """
        updates = {}
        
        # Always update processing info
        updates['processing_date'] = processing_date
        updates['processing_type'] = processing_type
        
        # Handle date fields with preservation logic
        if processing_type == 'EDI' and 'edi_date' in new_data:
            current, original = self.preserve_date_logic(
                existing, new_data['edi_date'], 'edi_date', 'original_edi_date'
            )
            updates['edi_date'] = current
            if self.schema_info['has_original_dates'] and original:
                updates['original_edi_date'] = original
                
        elif processing_type == 'Release' and 'release_date' in new_data:
            current, original = self.preserve_date_logic(
                existing, new_data['release_date'], 'release_date', 'original_release_date'
            )
            updates['release_date'] = current
            if self.schema_info['has_original_dates'] and original:
                updates['original_release_date'] = original
        
        # Handle not_transmitted
        if self.schema_info['has_not_transmitted']:
            not_transmitted = self.apply_not_transmitted_logic(
                existing, updates, processing_type
            )
            if not_transmitted is None:
                updates['not_transmitted'] = None
        
        # Update other fields as needed
        for field in ['amount', 'description', 'customer_code']:
            if field in new_data and new_data[field] != existing.get(field):
                updates[field] = new_data[field]
        
        return updates
    
    def get_table_stats(self) -> Dict:
        """Get statistics for all tables"""
        stats = {}
        tables = ['invoices', 'invoice_details', 'kp_payment_master', 'kp_payment_details']
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[table] = count
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
        
        return stats
    
    def bulk_insert_invoice_details_fast_validated(self, invoice_details: List[Dict], 
                                                  progress_callback=None) -> Dict:
        """
        Optimized bulk insert using execute_values for much faster performance
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
            
            # Step 1: Get all existing invoice numbers
            if progress_callback:
                progress_callback(0.05, "Loading invoice numbers...")
                
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT invoice_no FROM invoices WHERE invoice_no IS NOT NULL")
                existing_invoices = {row[0] for row in cursor.fetchall()}
            
            # Step 2: Validate and separate records
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
                    if invoice_no not in seen_missing_invoices:
                        seen_missing_invoices.add(invoice_no)
                        results['missing_invoices'].append(invoice_no)
                        results['missing_invoice_count'] += 1
                    
                    results['missing_invoice_records'].append(record)
                    results['failed'] += 1
                    continue
                
                valid_records.append(record)
            
            # Step 3: Bulk insert valid records
            if valid_records:
                if progress_callback:
                    progress_callback(0.2, f"Inserting {len(valid_records)} valid records...")
                
                # Prepare the data for execute_values
                columns = [
                    'invoice_no', 'job_number', 'last_name', 'first_name',
                    'middle_name', 'suffix', 'pay_hours', 'bill_hours', 
                    'amount', 'job_customer_code', 'job_description',
                    'employee_number', 'processed_date'
                ]
                
                values = []
                for record in valid_records:
                    values.append((
                        record.get('invoice_no'),
                        record.get('job_number'),
                        record.get('last_name'),
                        record.get('first_name'),
                        record.get('middle_name'),
                        record.get('suffix'),
                        record.get('pay_hours', 0),
                        record.get('bill_hours', 0),
                        record.get('amount', 0),
                        record.get('job_customer_code'),
                        record.get('job_description'),
                        record.get('employee_number'),
                        datetime.now().date()
                    ))
                
                # Execute bulk insert
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_values(
                        cursor,
                        f"""
                        INSERT INTO invoice_details ({', '.join(columns)})
                        VALUES %s
                        ON CONFLICT (invoice_no, job_number, employee_number) 
                        DO UPDATE SET
                            pay_hours = EXCLUDED.pay_hours,
                            bill_hours = EXCLUDED.bill_hours,
                            amount = EXCLUDED.amount,
                            processed_date = EXCLUDED.processed_date
                        """,
                        values,
                        template=None,
                        page_size=1000
                    )
                    
                    results['inserted'] = cursor.rowcount
                    
                    # Track inserted invoice numbers
                    results['inserted_invoice_numbers'] = list(set(
                        record.get('invoice_no') for record in valid_records
                    ))
                
                conn.commit()
                results['success'] = True
                
                if progress_callback:
                    progress_callback(1.0, "Complete!")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error in bulk_insert_invoice_details_fast_validated: {e}")
            results['error'] = str(e)
            if 'conn' in locals():
                conn.rollback()
                conn.close()
        
        return results
    
    def process_invoice_history_linking(self, invoices: List[Dict]) -> int:
        """
        Process invoice history linking for Add-On invoices
        """
        linked_count = 0
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            for invoice in invoices:
                if 'original_invoice_no' in invoice and invoice['original_invoice_no']:
                    try:
                        cursor.execute("""
                            INSERT INTO invoice_history (original_invoice_no, addon_invoice_no)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (invoice['original_invoice_no'], invoice['invoice_no']))
                        
                        if cursor.rowcount > 0:
                            linked_count += 1
                            
                    except Exception as e:
                        logger.error(f"Error linking invoice history: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error in process_invoice_history_linking: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
        
        return linked_count
    
    def export_validation_results(self, validation_results: Dict) -> Optional[str]:
        """
        Export validation results to Excel/CSV file
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create exports directory if it doesn't exist
            export_dir = "exports"
            if not os.path.exists(export_dir):
                os.makedirs(export_dir)
            
            # Prepare data for export
            all_records = []
            
            # Add missing invoice records
            for record in validation_results.get('missing_invoice_records', []):
                record_copy = record.copy()
                record_copy['validation_status'] = 'Missing Invoice'
                record_copy['validation_error'] = f"Invoice {record.get('invoice_no')} not found in master"
                all_records.append(record_copy)
            
            # Add error records
            for error_item in validation_results.get('error_records', []):
                record_copy = error_item['record'].copy()
                record_copy['validation_status'] = 'Error'
                record_copy['validation_error'] = error_item['error']
                all_records.append(record_copy)
            
            # Create DataFrame and export
            if all_records:
                df = pd.DataFrame(all_records)
                
                # Try Excel first
                try:
                    excel_filename = os.path.join(export_dir, f"validation_errors_{timestamp}.xlsx")
                    df.to_excel(excel_filename, index=False, engine='openpyxl')
                    logger.info(f"Exported validation results to {excel_filename}")
                    return excel_filename
                except Exception as excel_error:
                    # Fallback to CSV
                    logger.warning(f"Excel export failed: {excel_error}, trying CSV")
                    csv_filename = os.path.join(export_dir, f"validation_errors_{timestamp}.csv")
                    df.to_csv(csv_filename, index=False)
                    logger.info(f"Exported validation results to {csv_filename}")
                    return csv_filename
            else:
                logger.info("No validation errors to export")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting validation results: {e}")
            return None
    
    # Add more methods as needed...
    
    def check_payment_exists(self, payment_id: str) -> bool:
        """Check if a payment already exists"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM kp_payment_master WHERE payment_id = %s)",
                (payment_id,)
            )
            exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return exists
        except Exception as e:
            logger.error(f"Error checking payment existence: {e}")
            return False
    
    def get_payment_summary(self, payment_id: str) -> Dict:
        """Get summary of a payment"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get master info
            cursor.execute(
                "SELECT payment_date, payment_amount FROM kp_payment_master WHERE payment_id = %s",
                (payment_id,)
            )
            master = cursor.fetchone()
            
            # Get detail count
            cursor.execute(
                "SELECT COUNT(*), SUM(net_amount) FROM kp_payment_details WHERE payment_id = %s",
                (payment_id,)
            )
            details = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if master:
                return {
                    'payment_id': payment_id,
                    'payment_date': master[0],
                    'payment_amount': master[1],
                    'detail_count': details[0] if details else 0,
                    'detail_total': details[1] if details and details[1] else 0
                }
            return {}
            
        except Exception as e:
            logger.error(f"Error getting payment summary: {e}")
            return {}
    

    def get_payment_details_for_export(self, payment_id: str) -> List[Dict]:
        """
        Get payment details for export to CSV/Excel
        
        Args:
            payment_id: The payment ID to export details for
            
        Returns:
            List of dictionaries containing payment detail records
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Query to get all payment details with proper column names
            query = """
                SELECT 
                    pd.payment_id,
                    pd.payment_date,
                    pd.invoice_no,
                    pd.invoice_date,
                    pd.gross_amount,
                    pd.discount,
                    pd.net_amount,
                    pd.payment_message,
                    pd.created_at,
                    pd.updated_at
                FROM kp_payment_details pd
                WHERE pd.payment_id = %s
                ORDER BY pd.invoice_no
            """
            
            cursor.execute(query, (payment_id,))
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows and convert to list of dictionaries
            rows = cursor.fetchall()
            details = []
            
            for row in rows:
                detail = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    
                    # Convert date/datetime objects to strings for export
                    if hasattr(value, 'strftime'):
                        if hasattr(value, 'date'):  # datetime
                            detail[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:  # date
                            detail[col] = value.strftime('%Y-%m-%d')
                    # Convert Decimal to float for better Excel compatibility
                    elif hasattr(value, 'quantize'):  # Decimal
                        detail[col] = float(value)
                    else:
                        detail[col] = value
                
                details.append(detail)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Retrieved {len(details)} payment details for export (payment_id: {payment_id})")
            return details
            
        except Exception as e:
            logger.error(f"Error getting payment details for export: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def process_payment_remittance(self, master_data: Dict, detail_records: List[Dict], 
                                 progress_callback=None) -> Dict:
        """
        Process payment remittance data
        """
        payment_id = master_data['payment_id']
        logger.info(f"Processing payment remittance for Payment ID: {payment_id}")
        
        try:
            # Check if payment already exists
            if self.check_payment_exists(payment_id):
                existing_summary = self.get_payment_summary(payment_id)
                logger.warning(f"Payment already exists: {payment_id}")
                return {
                    'success': False,
                    'error': 'Payment already exists',
                    'payment_id': payment_id,
                    'existing_payment': existing_summary
                }
            
            # Use a single connection/transaction
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            try:
                # Insert payment master
                cursor.execute("""
                    INSERT INTO kp_payment_master (payment_id, payment_date, payment_amount)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (payment_id) DO NOTHING
                """, (
                    master_data['payment_id'],
                    master_data['payment_date'],
                    master_data['payment_amount']
                ))
                
                if cursor.rowcount == 0:
                    conn.rollback()
                    return {
                        'success': False,
                        'error': 'Failed to insert payment master',
                        'payment_id': payment_id
                    }
                
                # Insert detail records
                if detail_records:
                    columns = [
                        'payment_id', 'payment_date', 'invoice_no', 
                        'gross_amount', 'discount', 'net_amount', 'payment_message'
                    ]
                    
                    values = []
                    for rec in detail_records:
                        values.append((
                            payment_id,
                            rec.get('payment_date'),
                            rec.get('invoice_no'),
                            rec.get('gross_amount', 0),
                            rec.get('discount', 0),
                            rec.get('net_amount', 0),
                            rec.get('payment_message')
                        ))
                    
                    psycopg2.extras.execute_values(
                        cursor,
                        f"""
                        INSERT INTO kp_payment_details ({', '.join(columns)})
                        VALUES %s
                        ON CONFLICT DO NOTHING
                        """,
                        values
                    )
                
                conn.commit()
                cursor.close()
                conn.close()
                
                # Get final summary
                final_summary = self.get_payment_summary(payment_id)
                
                return {
                    'success': True,
                    'payment_id': payment_id,
                    'master_inserted': True,
                    'detail_results': {'inserted': len(detail_records), 'success': True},
                    'final_summary': final_summary
                }
                
            except Exception as e:
                conn.rollback()
                cursor.close()
                conn.close()
                logger.error(f"Error during transaction: {e}")
                return {
                    'success': False,
                    'error': str(e),
                    'payment_id': payment_id
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