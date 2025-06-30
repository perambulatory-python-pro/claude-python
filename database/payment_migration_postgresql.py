"""
Enhanced Payment Data Migration Script
Fixes existing payment data issues using the new email HTML processing methods:
1. Payment IDs without leading zeros
2. EMAIL_ prefixed payment IDs that should be actual IDs
3. Incorrect payment dates
4. Missing or incorrect payment amounts
5. Missing or incorrect invoice details
"""

import pandas as pd
from datetime import datetime
import logging
import re
from pathlib import Path
import os
import extract_msg
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress extract_msg stream warnings
extract_msg_logger = logging.getLogger('extract_msg')
extract_msg_logger.setLevel(logging.ERROR)

# Also suppress the specific stream warnings
import warnings
warnings.filterwarnings("ignore", message="Stream.*was requested but could not be found")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'payment_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedPaymentDataMigration:
    """
    Enhanced migration that uses the new email HTML processing methods
    """
    
    def __init__(self, data_mapper, source_folder):
        self.database_url = os.getenv('DATABASE_URL')
        self.mapper = data_mapper
        self.source_folder = source_folder
        self.migration_summary = {
            'payment_ids_fixed': 0,
            'email_ids_matched': 0,
            'dates_corrected': 0,
            'amounts_updated': 0,
            'details_updated': 0,
            'emails_processed': 0,
            'errors': []
        }
    
    def get_connection(self):
        """Get a database connection"""
        return psycopg2.connect(self.database_url)
    
    def run_full_migration(self):
        """
        Run all migration tasks
        """
        logger.info("Starting enhanced payment data migration...")
        
        # Step 1: Fix payment IDs without leading zeros
        self.fix_payment_id_leading_zeros()
        
        # Step 2: Enhanced EMAIL_ payment ID matching with complete email processing
        self.match_email_payment_ids_enhanced()
        
        # Step 3: Correct payment dates from source files
        self.correct_payment_dates()
        
        # Step 4: Verify and update amounts
        self.verify_payment_amounts()
        
        # Step 5: Fix invoice details from emails
        self.fix_invoice_details_from_emails()
        
        # Generate report
        self.generate_migration_report()
        
        logger.info("Enhanced migration completed!")
    
    def fix_payment_id_leading_zeros(self):
        """
        Fix payment IDs that are missing leading zeros
        """
        logger.info("=== Fixing Payment IDs with Missing Leading Zeros ===")
        
        conn = None
        try:
            query = """
                SELECT DISTINCT payment_id 
                FROM kp_payment_master 
                WHERE payment_id NOT LIKE 'EMAIL_%'
                AND LENGTH(payment_id) < 10
                AND payment_id ~ '^[0-9]+$'
            """
            
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                payment_ids = [row['payment_id'] for row in cursor.fetchall()]
            
            logger.info(f"Found {len(payment_ids)} payment IDs that may need leading zeros")
            
            for old_id in payment_ids:
                new_id = old_id.zfill(10)
                
                if old_id != new_id:
                    logger.info(f"Updating payment ID: {old_id} -> {new_id}")
                    
                    with conn.cursor() as cursor:
                        try:
                            # Update kp_payment_master
                            cursor.execute(
                                "UPDATE kp_payment_master SET payment_id = %s WHERE payment_id = %s",
                                (new_id, old_id)
                            )
                            
                            # Update kp_payment_details
                            cursor.execute(
                                "UPDATE kp_payment_details SET payment_id = %s WHERE payment_id = %s",
                                (new_id, old_id)
                            )
                            
                            conn.commit()
                            self.migration_summary['payment_ids_fixed'] += 1
                        except Exception as e:
                            conn.rollback()
                            logger.error(f"Error updating payment ID {old_id}: {e}")
            
        except Exception as e:
            logger.error(f"Error fixing payment ID leading zeros: {e}")
            self.migration_summary['errors'].append(f"Leading zeros fix: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def match_email_payment_ids_enhanced(self):
        """
        Enhanced EMAIL_ payment ID matching using complete email processing
        WITH IMPROVED ERROR HANDLING AND LOGGING
        """
        logger.info("=== Enhanced EMAIL_ Payment ID Matching ===")
        
        conn = None
        try:
            # Get all EMAIL_ payment records
            query = """
                SELECT payment_id, payment_date, payment_amount, created_at
                FROM kp_payment_master
                WHERE payment_id LIKE 'EMAIL_%'
                ORDER BY created_at
            """
            
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                email_payments = list(cursor.fetchall())
            
            logger.info(f"Found {len(email_payments)} EMAIL_ payment records to match")
            
            if not email_payments:
                logger.info("No EMAIL_ payment records found - this is expected if previous migrations worked")
                return
            
            # Process all email files
            msg_files = list(Path(self.source_folder).glob('*.msg'))
            logger.info(f"Processing {len(msg_files)} .msg files")
            
            # Temporarily suppress extract_msg logging
            extract_msg_logger = logging.getLogger('extract_msg')
            original_level = extract_msg_logger.level
            extract_msg_logger.setLevel(logging.ERROR)
            
            try:
                for msg_file in msg_files:
                    try:
                        logger.info(f"Processing: {msg_file.name}")
                        
                        # Extract HTML content properly
                        msg = extract_msg.Message(str(msg_file))
                        html_content = msg.htmlBody or msg.body
                        
                        if isinstance(html_content, bytes):
                            html_content = html_content.decode('utf-8', errors='ignore')
                        
                        if not html_content:
                            logger.debug(f"No HTML content found in {msg_file.name}")
                            continue
                        
                        # Check if this is a Kaiser payment email
                        if not self.mapper.detect_payment_email_html(html_content):
                            logger.debug(f"Not a Kaiser payment email: {msg_file.name}")
                            continue
                        
                        # Extract complete payment data using our enhanced methods
                        try:
                            master_data, detail_records = self.mapper.process_payment_email_html(html_content)
                            self.migration_summary['emails_processed'] += 1
                            
                            logger.info(f"✅ Extracted payment: {master_data['payment_id']} with {len(detail_records)} details")
                            
                            # Find matching EMAIL_ record by amount
                            for email_payment in email_payments[:]:  # Use slice to allow removal
                                amount_match = abs(float(email_payment['payment_amount']) - master_data['payment_amount']) < 0.01
                                
                                if amount_match:
                                    old_id = email_payment['payment_id']
                                    new_id = master_data['payment_id']
                                    
                                    logger.info(f"🔄 Matched {old_id} -> {new_id} (Amount: ${master_data['payment_amount']:,.2f})")
                                    
                                    # Update the payment in database
                                    success = self.update_email_payment_record(
                                        conn, old_id, new_id, master_data, detail_records
                                    )
                                    
                                    if success:
                                        self.migration_summary['email_ids_matched'] += 1
                                        email_payments.remove(email_payment)
                                        logger.info(f"✅ Successfully updated {old_id} -> {new_id}")
                                        break
                        
                        except Exception as e:
                            logger.error(f"❌ Error processing email content from {msg_file.name}: {e}")
                            continue
                            
                    except Exception as e:
                        logger.error(f"❌ Error processing {msg_file.name}: {e}")
                        continue
                        
            finally:
                # Restore original logging level
                extract_msg_logger.setLevel(original_level)
            
        except Exception as e:
            logger.error(f"Error in enhanced EMAIL_ matching: {e}")
            self.migration_summary['errors'].append(f"Enhanced EMAIL matching: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def update_email_payment_record(self, conn, old_id, new_id, master_data, detail_records):
        """
        Update EMAIL_ payment record with correct data
        """
        try:
            with conn.cursor() as cursor:
                # Check if new ID already exists
                cursor.execute("SELECT COUNT(*) FROM kp_payment_master WHERE payment_id = %s", (new_id,))
                exists = cursor.fetchone()[0]
                
                if exists:
                    logger.warning(f"Payment ID {new_id} already exists, skipping update for {old_id}")
                    return False
                
                # Update kp_payment_master with correct data
                cursor.execute("""
                    UPDATE kp_payment_master 
                    SET payment_id = %s, 
                        payment_date = %s, 
                        payment_amount = %s,
                        vendor_name = %s
                    WHERE payment_id = %s
                """, (
                    new_id, 
                    master_data['payment_date'], 
                    master_data['payment_amount'],
                    master_data['vendor_name'],
                    old_id
                ))
                
                # Delete old payment details (they're probably wrong anyway)
                cursor.execute("DELETE FROM kp_payment_details WHERE payment_id = %s", (old_id,))
                
                # Insert correct payment details
                for detail in detail_records:
                    cursor.execute("""
                        INSERT INTO kp_payment_details 
                        (payment_id, payment_date, vendor_name, invoice_no, gross_amount, net_amount, discount_amount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        new_id,  # Use new payment ID
                        detail.get('payment_date', master_data['payment_date']),
                        detail.get('vendor_name', master_data['vendor_name']),
                        detail.get('invoice_no', ''),
                        detail.get('gross_amount', 0),
                        detail.get('net_amount', 0),
                        detail.get('discount_amount', 0)
                    ))
                
                conn.commit()
                logger.info(f"Successfully updated {old_id} -> {new_id} with {len(detail_records)} details")
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating payment record {old_id}: {e}")
            return False
    
    # Fix the fix_invoice_details_from_emails method in your migration script

    def fix_invoice_details_from_emails(self):
        """
        Fix invoice details for payments that have correct payment IDs but wrong/missing details
        FIXED: Use correct column names from database
        """
        logger.info("=== Fixing Invoice Details from Emails ===")
        
        conn = None
        try:
            # Find payments with missing or suspicious details
            # FIXED: Use invoice_no instead of invoice_id
            query = """
                SELECT pm.payment_id, pm.payment_amount, COUNT(pd.invoice_no) as detail_count,
                    COALESCE(SUM(pd.net_amount), 0) as detail_total
                FROM kp_payment_master pm
                LEFT JOIN kp_payment_details pd ON pm.payment_id = pd.payment_id
                WHERE pm.payment_id NOT LIKE 'EMAIL_%'
                AND LENGTH(pm.payment_id) = 10
                GROUP BY pm.payment_id, pm.payment_amount
                HAVING COUNT(pd.invoice_no) = 0 
                OR ABS(pm.payment_amount - COALESCE(SUM(pd.net_amount), 0)) > 0.01
            """
            
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                payments_to_fix = list(cursor.fetchall())
            
            logger.info(f"Found {len(payments_to_fix)} payments that need detail fixes")
            
            # Process email files to find matching payment details
            msg_files = list(Path(self.source_folder).glob('*.msg'))
            
            for payment in payments_to_fix:
                payment_id = payment['payment_id']
                logger.info(f"Looking for details for payment {payment_id}")
                
                # Search through emails for this payment ID
                for msg_file in msg_files:
                    try:
                        msg = extract_msg.Message(str(msg_file))
                        html_content = msg.htmlBody or msg.body
                        
                        if isinstance(html_content, bytes):
                            html_content = html_content.decode('utf-8', errors='ignore')
                        
                        if not html_content:
                            continue
                        
                        # Check if this email contains our payment ID
                        if payment_id in html_content:
                            logger.info(f"Found payment {payment_id} in {msg_file.name}")
                            
                            try:
                                # Extract complete payment data
                                master_data, detail_records = self.mapper.process_payment_email_html(html_content)
                                
                                if master_data['payment_id'] == payment_id:
                                    # This is the right email - update the details
                                    self.update_payment_details(conn, payment_id, detail_records)
                                    self.migration_summary['details_updated'] += 1
                                    break
                            
                            except Exception as e:
                                logger.error(f"Error processing email for payment {payment_id}: {e}")
                                continue
                    
                    except Exception as e:
                        logger.error(f"Error reading {msg_file.name} for payment {payment_id}: {e}")
                        continue
            
        except Exception as e:
            logger.error(f"Error fixing invoice details: {e}")
            self.migration_summary['errors'].append(f"Invoice details fix: {str(e)}")
        finally:
            if conn:
                conn.close()

    def update_payment_details(self, conn, payment_id, detail_records):
        """
        Update payment details for a specific payment ID
        FIXED: Use correct column names
        """
        try:
            with conn.cursor() as cursor:
                # Delete existing details
                cursor.execute("DELETE FROM kp_payment_details WHERE payment_id = %s", (payment_id,))
                
                # Insert new details with correct column names
                for detail in detail_records:
                    cursor.execute("""
                        INSERT INTO kp_payment_details 
                        (payment_id, payment_date, vendor_name, invoice_no, gross_amount, net_amount, discount_amount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        payment_id,
                        detail.get('payment_date'),
                        detail.get('vendor_name'),
                        detail.get('invoice_id', ''),  # invoice_id from email maps to invoice_no in DB
                        detail.get('gross_amount', 0),
                        detail.get('net_amount', 0),
                        detail.get('discount_amount', 0)
                    ))
                
                conn.commit()
                logger.info(f"Updated details for payment {payment_id}: {len(detail_records)} records")
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating details for payment {payment_id}: {e}")
            raise

    # Also fix the update_email_payment_record method:

    def update_email_payment_record(self, conn, old_id, new_id, master_data, detail_records):
        """
        Update EMAIL_ payment record with correct data
        FIXED: Use correct column names
        """
        try:
            with conn.cursor() as cursor:
                # Check if new ID already exists
                cursor.execute("SELECT COUNT(*) FROM kp_payment_master WHERE payment_id = %s", (new_id,))
                exists = cursor.fetchone()[0]
                
                if exists:
                    logger.warning(f"Payment ID {new_id} already exists, skipping update for {old_id}")
                    return False
                
                # Update kp_payment_master with correct data
                cursor.execute("""
                    UPDATE kp_payment_master 
                    SET payment_id = %s, 
                        payment_date = %s, 
                        payment_amount = %s,
                        vendor_name = %s
                    WHERE payment_id = %s
                """, (
                    new_id, 
                    master_data['payment_date'], 
                    master_data['payment_amount'],
                    master_data['vendor_name'],
                    old_id
                ))
                
                # Delete old payment details (they're probably wrong anyway)
                cursor.execute("DELETE FROM kp_payment_details WHERE payment_id = %s", (old_id,))
                
                # Insert correct payment details with correct column names
                for detail in detail_records:
                    cursor.execute("""
                        INSERT INTO kp_payment_details 
                        (payment_id, payment_date, vendor_name, invoice_no, gross_amount, net_amount, discount_amount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        new_id,  # Use new payment ID
                        detail.get('payment_date', master_data['payment_date']),
                        detail.get('vendor_name', master_data['vendor_name']),
                        detail.get('invoice_id', ''),  # invoice_id from email maps to invoice_no in DB
                        detail.get('gross_amount', 0),
                        detail.get('net_amount', 0),
                        detail.get('discount_amount', 0)
                    ))
                
                conn.commit()
                logger.info(f"Successfully updated {old_id} -> {new_id} with {len(detail_records)} details")
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating payment record {old_id}: {e}")
            return False
    
    def correct_payment_dates(self):
        """
        Correct payment dates from source files (Excel and emails)
        """
        logger.info("=== Correcting Payment Dates ===")
        
        conn = None
        try:
            # Get payments where payment_date might be wrong
            query = """
                SELECT payment_id, payment_date, created_at
                FROM kp_payment_master
                WHERE DATE(payment_date) = DATE(created_at)
                OR payment_date >= '2025-06-01'
            """
            
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                suspect_payments = list(cursor.fetchall())
            
            logger.info(f"Found {len(suspect_payments)} payments with suspect dates")
            
            for payment in suspect_payments:
                correct_date = self.find_correct_payment_date(payment['payment_id'])
                
                if correct_date and correct_date != payment['payment_date'].strftime('%Y-%m-%d'):
                    logger.info(f"Correcting date for payment {payment['payment_id']}: {payment['payment_date']} -> {correct_date}")
                    
                    with conn.cursor() as cursor:
                        try:
                            # Update both master and details
                            cursor.execute(
                                "UPDATE kp_payment_master SET payment_date = %s WHERE payment_id = %s",
                                (correct_date, payment['payment_id'])
                            )
                            cursor.execute(
                                "UPDATE kp_payment_details SET payment_date = %s WHERE payment_id = %s",
                                (correct_date, payment['payment_id'])
                            )
                            
                            conn.commit()
                            self.migration_summary['dates_corrected'] += 1
                        except Exception as e:
                            conn.rollback()
                            logger.error(f"Error updating date for payment {payment['payment_id']}: {e}")
            
        except Exception as e:
            logger.error(f"Error correcting payment dates: {e}")
            self.migration_summary['errors'].append(f"Date correction: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def find_correct_payment_date(self, payment_id):
        """
        Find the correct payment date from Excel files and emails
        """
        # First check Excel files
        excel_files = list(Path(self.source_folder).glob('*.xlsx')) + list(Path(self.source_folder).glob('*.xls'))
        
        for file in excel_files:
            try:
                df = pd.read_excel(file)
                if 'Payment ID' in df.columns:
                    df['Payment ID'] = df['Payment ID'].astype(str).str.zfill(10)
                    
                    if payment_id in df['Payment ID'].values:
                        payment_row = df[df['Payment ID'] == payment_id].iloc[0]
                        if 'Payment Date' in payment_row:
                            date_obj = self.mapper.convert_excel_date(payment_row['Payment Date'])
                            if date_obj:
                                return date_obj.strftime('%Y-%m-%d')
            except Exception as e:
                logger.debug(f"Error checking file {file}: {e}")
                continue
        
        # Then check email files
        msg_files = list(Path(self.source_folder).glob('*.msg'))
        for msg_file in msg_files:
            try:
                msg = extract_msg.Message(str(msg_file))
                html_content = msg.htmlBody or msg.body
                
                if isinstance(html_content, bytes):
                    html_content = html_content.decode('utf-8', errors='ignore')
                
                if html_content and payment_id in html_content:
                    metadata = self.mapper.extract_kaiser_payment_metadata(html_content)
                    if metadata['payment_id'] == payment_id:
                        return metadata['payment_date']
            except Exception as e:
                logger.debug(f"Error checking email {msg_file.name}: {e}")
                continue
        
        return None
    
    def verify_payment_amounts(self):
        """
        Verify payment amounts match the sum of details
        """
        logger.info("=== Verifying Payment Amounts ===")
        
        conn = None
        try:
            query = """
                SELECT 
                    pm.payment_id,
                    pm.payment_amount as master_amount,
                    COALESCE(SUM(pd.net_amount), 0) as detail_total
                FROM kp_payment_master pm
                LEFT JOIN kp_payment_details pd ON pm.payment_id = pd.payment_id
                GROUP BY pm.payment_id, pm.payment_amount
                HAVING ABS(pm.payment_amount - COALESCE(SUM(pd.net_amount), 0)) > 0.01
            """
            
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                mismatched = list(cursor.fetchall())
            
            logger.info(f"Found {len(mismatched)} payments with amount mismatches")
            
            for payment in mismatched:
                logger.warning(
                    f"Payment {payment['payment_id']}: "
                    f"Master=${float(payment['master_amount']):,.2f}, "
                    f"Details=${float(payment['detail_total']):,.2f}, "
                    f"Diff=${abs(float(payment['master_amount']) - float(payment['detail_total'])):,.2f}"
                )
            
        except Exception as e:
            logger.error(f"Error verifying payment amounts: {e}")
            self.migration_summary['errors'].append(f"Amount verification: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def generate_migration_report(self):
        """
        Generate a detailed migration report
        """
        report = f"""

Enhanced Payment Data Migration Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

SUMMARY
-------
• Payment IDs Fixed (leading zeros): {self.migration_summary['payment_ids_fixed']}
• EMAIL IDs Matched: {self.migration_summary['email_ids_matched']}
• Dates Corrected: {self.migration_summary['dates_corrected']}
• Amounts Updated: {self.migration_summary['amounts_updated']}
• Invoice Details Updated: {self.migration_summary['details_updated']}
• Email Files Processed: {self.migration_summary['emails_processed']}
• Errors: {len(self.migration_summary['errors'])}

{'='*60}
"""
        
        if self.migration_summary['errors']:
            report += "\nERRORS:\n"
            for error in self.migration_summary['errors']:
                report += f"• {error}\n"
        
        # Save report
        report_file = f"enhanced_payment_migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        logger.info(f"Migration report saved to: {report_file}")
        print(report)


def run_enhanced_migration():
    """
    Main function to run the enhanced migration
    """
    from data_mapper_enhanced import EnhancedDataMapper
    
    # Configuration
    SOURCE_FOLDER = r"C:\Users\Brendon Jewell\Finance Ops Dropbox\Finance Ops Team Folder\DOMO Source Files\[DOMO] Accounts Receivable (AR)\Payment Notifications"
    
    print("Enhanced Payment Data Migration Tool")
    print("="*60)
    print("This enhanced version uses the new email HTML processing methods")
    print("to correctly extract and update payment data and invoice details.")
    
    # Initialize components
    data_mapper = EnhancedDataMapper()
    
    # Create migration instance
    migration = EnhancedPaymentDataMigration(data_mapper, SOURCE_FOLDER)
    
    # Confirm before running
    print("\nThis will update existing payment records to fix:")
    print("1. Payment IDs without leading zeros")
    print("2. EMAIL_ prefixed payment IDs (with complete email processing)")
    print("3. Incorrect payment dates (from emails and Excel files)")
    print("4. Missing or incorrect invoice details")
    print("5. Verify payment amounts")
    print(f"\nSource folder: {SOURCE_FOLDER}")
    
    confirm = input("\nProceed with enhanced migration? (yes/no): ").strip().lower()
    
    if confirm == 'yes':
        migration.run_full_migration()
    else:
        print("Migration cancelled.")


if __name__ == "__main__":
    run_enhanced_migration()
