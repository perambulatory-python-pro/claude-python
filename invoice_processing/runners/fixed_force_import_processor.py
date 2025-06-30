"""
Final Force Import Processor - Database Schema Matched
This version matches the exact database schema from your invoice_details table
"""

import pandas as pd
import psycopg2
from datetime import datetime
import os
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import logging
import numpy as np

# Load environment variables
load_dotenv()

class ForceImportProcessor:
    def __init__(self, log_file: str = 'force_import_processing.log'):
        """Initialize the force import processor"""
        
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in .env file")
        
        # Initialize processing stats
        self.processing_stats = {
            'duplicates_processed': 0,
            'missing_processed': 0,
            'duplicates_skipped': 0,
            'missing_skipped': 0,
            'errors': [],
            'successful_imports': [],
            'validation_failures': []
        }
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        print("🔄 FORCE IMPORT PROCESSOR - FINAL VERSION")
        print("=" * 50)
    
    def check_existing_record(self, cursor, record: pd.Series) -> bool:
        """Check if a record already exists in the database"""
        
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM invoice_details
                WHERE source_system = 'AUS'
                AND invoice_no = %s
                AND employee_id = %s
                AND work_date = %s
                AND hours_regular = %s
            """, (
                str(record.get('Invoice Number', '')),
                str(record.get('Employee Number', '')),
                record.get('Work Date'),
                float(record.get('Hours', 0))
            ))
            
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking existing record: {e}")
            raise
    
    def insert_invoice_record(self, cursor, record: pd.Series, record_type: str) -> bool:
        """Insert a single invoice record matching exact database schema"""
        
        try:
            # Check if record already exists
            if self.check_existing_record(cursor, record):
                self.processing_stats['validation_failures'].append({
                    'record': f"Invoice {record.get('Invoice Number')}, Employee {record.get('Employee Number')}",
                    'reason': 'Record already exists in database',
                    'type': record_type
                })
                return False
            
            # Validate required fields
            if pd.isna(record.get('Work Date')):
                self.processing_stats['validation_failures'].append({
                    'record': f"Invoice {record.get('Invoice Number')}, Employee {record.get('Employee Number')}",
                    'reason': 'Missing work date',
                    'type': record_type
                })
                return False
            
            # Prepare values matching database schema exactly
            invoice_no = str(record.get('Invoice Number', ''))
            employee_id = str(record.get('Employee Number', '')) if pd.notna(record.get('Employee Number')) else ''
            
            # Map CSV columns to database columns
            cursor.execute("""
                INSERT INTO invoice_details (
                    invoice_no, 
                    source_system, 
                    work_date, 
                    employee_id, 
                    employee_name,
                    location_code,
                    location_name,
                    building_code,
                    emid,
                    position_code,
                    position_description,
                    job_number, 
                    hours_regular,
                    hours_overtime,
                    hours_holiday,
                    hours_total,
                    rate_regular,
                    rate_overtime,
                    rate_holiday,
                    amount_regular,
                    amount_overtime,
                    amount_holiday,
                    amount_total,
                    customer_number,
                    customer_name,
                    business_unit,
                    shift_in,
                    shift_out,
                    bill_category,
                    pay_rate,
                    in_time,
                    out_time,
                    lunch_hours,
                    created_at,
                    po
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """, (
                # Basic fields
                invoice_no,                                                          # invoice_no
                'AUS',                                                              # source_system
                record.get('Work Date'),                                            # work_date
                employee_id,                                                        # employee_id
                str(record.get('Employee Name', '')) if pd.notna(record.get('Employee Name')) else '',  # employee_name
                
                # Location fields (not in AUS data, will be NULL)
                None,                                                               # location_code
                None,                                                               # location_name
                None,                                                               # building_code
                None,                                                               # emid
                
                # Position fields
                None,                                                               # position_code
                str(record.get('Post Description', '')) if pd.notna(record.get('Post Description')) else '',  # position_description
                
                # Job info
                str(record.get('Job Number', '')) if pd.notna(record.get('Job Number')) else '',  # job_number
                
                # Hours fields (AUS only has regular hours in this format)
                float(record.get('Hours', 0)) if pd.notna(record.get('Hours')) else 0,  # hours_regular
                0,                                                                  # hours_overtime
                0,                                                                  # hours_holiday
                float(record.get('Hours', 0)) if pd.notna(record.get('Hours')) else 0,  # hours_total
                
                # Rate fields
                float(record.get('Bill Rate', 0)) if pd.notna(record.get('Bill Rate')) else 0,  # rate_regular
                0,                                                                  # rate_overtime
                0,                                                                  # rate_holiday
                
                # Amount fields
                float(record.get('Bill Amount', 0)) if pd.notna(record.get('Bill Amount')) else 0,  # amount_regular
                0,                                                                  # amount_overtime
                0,                                                                  # amount_holiday
                float(record.get('Bill Amount', 0)) if pd.notna(record.get('Bill Amount')) else 0,  # amount_total
                
                # Customer fields
                str(record.get('Customer Number', '')) if pd.notna(record.get('Customer Number')) else '',  # customer_number
                str(record.get('Customer Name', '')) if pd.notna(record.get('Customer Name')) else '',  # customer_name
                None,                                                               # business_unit
                
                # Shift times
                str(record.get('In Time', '')) if pd.notna(record.get('In Time')) else None,  # shift_in
                str(record.get('Out Time', '')) if pd.notna(record.get('Out Time')) else None,  # shift_out
                
                # Other fields
                int(record.get('Bill Cat Number', 0)) if pd.notna(record.get('Bill Cat Number')) else None,  # bill_category
                float(record.get('Pay Rate', 0)) if pd.notna(record.get('Pay Rate')) else None,  # pay_rate
                str(record.get('In Time', '')) if pd.notna(record.get('In Time')) else None,  # in_time
                str(record.get('Out Time', '')) if pd.notna(record.get('Out Time')) else None,  # out_time
                float(record.get('Lunch', 0)) if pd.notna(record.get('Lunch')) else None,  # lunch_hours
                datetime.now(),                                                     # created_at
                str(record.get('PO', '')) if pd.notna(record.get('PO')) else ''   # po
            ))
            
            # Log successful import
            self.processing_stats['successful_imports'].append({
                'invoice_no': invoice_no,
                'employee': employee_id,
                'work_date': record.get('Work Date'),
                'hours': record.get('Hours'),
                'amount': record.get('Bill Amount'),
                'type': record_type
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Error inserting record: Invoice {record.get('Invoice Number')}, Employee {record.get('Employee Number')}: {e}"
            self.logger.error(error_msg)
            self.processing_stats['errors'].append(error_msg)
            raise
    
    def detect_file_type(self, df: pd.DataFrame, file_path: str) -> str:
        """Detect whether file is duplicate analysis or missing records"""
        
        columns = list(df.columns)
        
        # Duplicate analysis files have these specific columns
        duplicate_indicators = ['Group_ID', 'Pattern_Type', 'Is_Reversal_Pair', 'Group_Size']
        
        # Check for duplicate analysis columns
        if any(col in columns for col in duplicate_indicators):
            return 'duplicate'
        else:
            return 'missing'
    
    def validate_and_prepare_file(self, file_path: str) -> Tuple[bool, pd.DataFrame, str]:
        """Validate file structure and prepare for processing"""
        
        try:
            # Read file
            df = pd.read_csv(file_path, encoding='utf-8')
            self.logger.info(f"Loaded {len(df)} records from {file_path}")
            
            # Detect file type
            file_type = self.detect_file_type(df, file_path)
            self.logger.info(f"Detected file type: {file_type}")
            
            # Check for Force_Import column
            if 'Force_Import' not in df.columns:
                self.logger.error("No Force_Import column found!")
                self.logger.info(f"Available columns: {', '.join(df.columns)}")
                return False, pd.DataFrame(), file_type
            
            # Normalize Force_Import values
            df['Force_Import'] = df['Force_Import'].fillna('').astype(str).str.strip().str.upper()
            
            # Filter to only records marked for import
            import_df = df[df['Force_Import'] == 'TRUE'].copy()
            skip_df = df[df['Force_Import'] == 'FALSE'].copy()
            review_df = df[~df['Force_Import'].isin(['TRUE', 'FALSE'])].copy()
            
            self.logger.info(f"Records marked for import: {len(import_df)}")
            self.logger.info(f"Records marked to skip: {len(skip_df)}")
            self.logger.info(f"Records pending review: {len(review_df)}")
            
            if len(import_df) == 0:
                self.logger.warning("No records marked for import (Force_Import = TRUE)")
                return True, pd.DataFrame(), file_type
            
            return True, import_df, file_type
            
        except Exception as e:
            self.logger.error(f"Error validating file {file_path}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False, pd.DataFrame(), 'unknown'
    
    def process_batch_with_transaction_handling(self, batch: pd.DataFrame, conn, file_type: str) -> Tuple[int, int]:
        """Process a batch with proper transaction handling"""
        
        batch_success = 0
        batch_errors = 0
        
        for idx, record in batch.iterrows():
            try:
                with conn.cursor() as cursor:
                    success = self.insert_invoice_record(cursor, record, file_type)
                    if success:
                        conn.commit()  # Commit each successful record
                        batch_success += 1
                        if file_type == 'duplicate':
                            self.processing_stats['duplicates_processed'] += 1
                        else:
                            self.processing_stats['missing_processed'] += 1
                    else:
                        batch_errors += 1
                        if file_type == 'duplicate':
                            self.processing_stats['duplicates_skipped'] += 1
                        else:
                            self.processing_stats['missing_skipped'] += 1
                            
            except Exception as e:
                conn.rollback()  # Rollback on error
                batch_errors += 1
                if file_type == 'duplicate':
                    self.processing_stats['duplicates_skipped'] += 1
                else:
                    self.processing_stats['missing_skipped'] += 1
                self.logger.error(f"Transaction failed for record {idx}: {e}")
                
        return batch_success, batch_errors
    
    def process_imports(self, file_path: str, file_type: str) -> bool:
        """Process records marked for import"""
        
        self.logger.info(f"Processing {file_type} imports from: {file_path}")
        
        # Validate file
        is_valid, df_import, detected_type = self.validate_and_prepare_file(file_path)
        if not is_valid:
            return False
        
        if len(df_import) == 0:
            self.logger.info(f"No {file_type} records marked for import")
            return True
        
        # Process records
        try:
            with psycopg2.connect(self.database_url) as conn:
                batch_size = 100
                
                for start_idx in range(0, len(df_import), batch_size):
                    end_idx = min(start_idx + batch_size, len(df_import))
                    batch = df_import.iloc[start_idx:end_idx]
                    
                    batch_success, batch_errors = self.process_batch_with_transaction_handling(
                        batch, conn, file_type
                    )
                    
                    self.logger.info(
                        f"Batch {start_idx//batch_size + 1}: "
                        f"{batch_success} imported, {batch_errors} skipped"
                    )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error during import: {e}")
            return False
    
    def generate_import_report(self) -> str:
        """Generate a summary report of the import process"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f'force_import_report_{timestamp}.txt'
        
        total_processed = (
            self.processing_stats['duplicates_processed'] + 
            self.processing_stats['missing_processed']
        )
        total_skipped = (
            self.processing_stats['duplicates_skipped'] + 
            self.processing_stats['missing_skipped']
        )
        
        report_content = f"""
FORCE IMPORT PROCESSING REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

PROCESSING SUMMARY:
{'-'*30}
Total records imported: {total_processed:,}
Total records skipped: {total_skipped:,}
Total errors: {len(self.processing_stats['errors']):,}

BREAKDOWN BY TYPE:
{'-'*30}
Duplicate records imported: {self.processing_stats['duplicates_processed']:,}
Duplicate records skipped: {self.processing_stats['duplicates_skipped']:,}
Missing records imported: {self.processing_stats['missing_processed']:,}
Missing records skipped: {self.processing_stats['missing_skipped']:,}

"""
        
        # Add validation failures if any
        if self.processing_stats['validation_failures']:
            report_content += f"\nVALIDATION FAILURES:\n{'-'*30}\n"
            for failure in self.processing_stats['validation_failures'][:10]:
                report_content += f"- {failure['record']}: {failure['reason']}\n"
            
            if len(self.processing_stats['validation_failures']) > 10:
                report_content += f"... and {len(self.processing_stats['validation_failures']) - 10} more\n"
        
        # Add errors if any
        if self.processing_stats['errors']:
            report_content += f"\nERRORS ENCOUNTERED:\n{'-'*30}\n"
            for error in self.processing_stats['errors'][:10]:
                report_content += f"- {error}\n"
            
            if len(self.processing_stats['errors']) > 10:
                report_content += f"... and {len(self.processing_stats['errors']) - 10} more errors\n"
        
        # Add successful examples
        if self.processing_stats['successful_imports']:
            report_content += f"\nSAMPLE SUCCESSFUL IMPORTS:\n{'-'*30}\n"
            for success in self.processing_stats['successful_imports'][:5]:
                report_content += f"- Invoice {success['invoice_no']}, Employee {success['employee']}: "
                report_content += f"{success['hours']} hours, ${success['amount']}\n"
        
        # Success rate
        if total_processed + total_skipped > 0:
            success_rate = (total_processed / (total_processed + total_skipped)) * 100
            report_content += f"\nSUCCESS RATE: {success_rate:.2f}%\n"
        
        # Save report
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        self.logger.info(f"Import report saved to: {report_file}")
        return report_file
    
    def run_force_import(self, duplicate_file: str = None, missing_file: str = None) -> Dict[str, any]:
        """Main method to run the force import process"""
        
        self.logger.info("Starting force import process...")
        
        results = {
            'success': True,
            'duplicates_processed': 0,
            'missing_processed': 0,
            'errors': []
        }
        
        try:
            # Process duplicate file if provided
            if duplicate_file and os.path.exists(duplicate_file):
                self.logger.info(f"Processing duplicate file: {duplicate_file}")
                success = self.process_imports(duplicate_file, 'duplicate')
                if success:
                    self.logger.info("✅ Duplicate imports completed")
                else:
                    self.logger.error("❌ Duplicate imports failed")
                    results['success'] = False
            
            # Process missing file if provided
            if missing_file and os.path.exists(missing_file):
                self.logger.info(f"Processing missing file: {missing_file}")
                success = self.process_imports(missing_file, 'missing')
                if success:
                    self.logger.info("✅ Missing imports completed")
                else:
                    self.logger.error("❌ Missing imports failed")
                    results['success'] = False
            
            # Generate report
            report_file = self.generate_import_report()
            
            # Update results
            results['duplicates_processed'] = self.processing_stats['duplicates_processed']
            results['missing_processed'] = self.processing_stats['missing_processed']
            results['errors'] = self.processing_stats['errors']
            results['report_file'] = report_file
            
            # Print summary
            total_imported = results['duplicates_processed'] + results['missing_processed']
            print("\n" + "="*60)
            print("FORCE IMPORT COMPLETE!")
            print("="*60)
            print(f"Total records imported: {total_imported:,}")
            print(f"Processing report: {report_file}")
            
            if total_imported > 0:
                print(f"\n🔍 Verify imports in database:")
                print(f"SELECT COUNT(*) FROM invoice_details WHERE source_system = 'AUS' ")
                print(f"AND created_at >= '{datetime.now().strftime('%Y-%m-%d')}'::date;")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Fatal error in force import process: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            results['success'] = False
            results['errors'].append(str(e))
            return results


# Example usage
if __name__ == "__main__":
    processor = ForceImportProcessor()
    
    # Process files
    results = processor.run_force_import(
        duplicate_file='duplicate_analysis_20250624_135625.csv',
        missing_file='missing_analysis_reviewed.csv'  # Update this filename as needed
    )
    
    # Show some verification queries
    if results['duplicates_processed'] + results['missing_processed'] > 0:
        print("\n📊 Additional verification queries:")
        print("-- Check recent imports by invoice:")
        print("SELECT invoice_no, COUNT(*) as record_count")
        print("FROM invoice_details")
        print("WHERE source_system = 'AUS'")
        print(f"AND created_at >= '{datetime.now().strftime('%Y-%m-%d')}'::date")
        print("GROUP BY invoice_no")
        print("ORDER BY invoice_no;")
