"""
Force Import Processor - Fixed Version
Handles both duplicate analysis and missing records with proper column mapping
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
        
        # Set up logging with UTF-8 encoding
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        print("🔄 FORCE IMPORT PROCESSOR - FIXED VERSION")
        print("=" * 50)
    
    def detect_file_type(self, df: pd.DataFrame, file_path: str) -> str:
        """Detect whether file is duplicate analysis or missing/bulk review"""
        
        # Check column names to determine file type
        columns = list(df.columns)
        
        # Duplicate analysis files have these specific columns
        duplicate_indicators = ['Group_ID', 'Pattern_Type', 'Is_Reversal_Pair', 'Group_Size']
        
        # Missing/bulk review files have these columns
        bulk_indicators = ['Invoice_Number', 'Record_Count', 'Auto_Import', 'Recommended_Action']
        
        # Check for duplicate analysis columns
        if any(col in columns for col in duplicate_indicators):
            return 'duplicate'
        
        # Check for bulk review columns
        elif any(col in columns for col in bulk_indicators):
            return 'bulk_missing'
        
        # Check if it's raw missing analysis
        elif 'Missing_Type' in columns or 'Priority' in columns:
            return 'raw_missing'
        
        # Default assumption based on filename
        elif 'duplicate' in file_path.lower():
            return 'duplicate'
        else:
            return 'missing'
    
    def map_columns(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """Map columns to expected names based on file type"""
        
        if file_type == 'bulk_missing':
            # Map bulk review columns to expected format
            column_mapping = {
                'Invoice_Number': 'Invoice Number',
                'Auto_Import': 'Force_Import'
            }
            
            # Rename columns that exist
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # If we need to expand invoice-level to record-level
            # We'll need to load the original missing analysis file
            self.logger.info("Bulk review file detected - may need original detail file for record-level import")
        
        elif file_type == 'duplicate':
            # Duplicate analysis already has Force_Import column
            if 'Force_Import' not in df.columns:
                df['Force_Import'] = ''  # Add empty column for manual review
        
        return df
    
    def validate_and_prepare_file(self, file_path: str) -> Tuple[bool, pd.DataFrame, str]:
        """Validate file structure and prepare for processing"""
        
        try:
            # Read file with UTF-8 encoding
            df = pd.read_csv(file_path, encoding='utf-8')
            self.logger.info(f"Loaded {len(df)} records from {file_path}")
            
            # Detect file type
            file_type = self.detect_file_type(df, file_path)
            self.logger.info(f"Detected file type: {file_type}")
            
            # Map columns
            df = self.map_columns(df, file_type)
            
            # For bulk review files, we need the detail records
            if file_type == 'bulk_missing':
                # Check if this is an invoice-level file
                if 'Record_Count' in df.columns:
                    self.logger.warning("This appears to be an invoice-level review file.")
                    self.logger.warning("Please use the processed missing_analysis file with Force_Import flags.")
                    return False, pd.DataFrame(), file_type
            
            # Check for Force_Import column
            if 'Force_Import' not in df.columns and 'Auto_Import' not in df.columns:
                self.logger.error("No Force_Import or Auto_Import column found!")
                self.logger.info("Available columns: " + ", ".join(df.columns))
                return False, pd.DataFrame(), file_type
            
            # Normalize Force_Import values
            import_col = 'Force_Import' if 'Force_Import' in df.columns else 'Auto_Import'
            df['Force_Import'] = df[import_col].fillna('').astype(str).str.strip().str.upper()
            
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
            return False
    
    def insert_invoice_record(self, cursor, record: pd.Series, record_type: str) -> bool:
        """Insert a single invoice record into the database"""
        
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
            
            # Prepare values with proper null handling
            invoice_no = str(record.get('Invoice Number', ''))
            employee_id = str(record.get('Employee Number', '')) if pd.notna(record.get('Employee Number')) else ''
            
            # Insert the record
            cursor.execute("""
                INSERT INTO invoice_details (
                    invoice_no, source_system, work_date, employee_id, employee_name,
                    job_number, customer_number, po, post_description,
                    pay_description, hours_regular, rate_regular, amount_regular,
                    shift_in, shift_out, lunch_hours, bill_category,
                    created_at, notes
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                invoice_no,
                'AUS',
                record.get('Work Date'),
                employee_id,
                str(record.get('Employee Name', '')) if pd.notna(record.get('Employee Name')) else '',
                str(record.get('Job Number', '')) if pd.notna(record.get('Job Number')) else '',
                str(record.get('Customer Number', '')) if pd.notna(record.get('Customer Number')) else '',
                str(record.get('PO', '')) if pd.notna(record.get('PO')) else '',
                str(record.get('Post Description', '')) if pd.notna(record.get('Post Description')) else '',
                str(record.get('Pay Hours Description', '')) if pd.notna(record.get('Pay Hours Description')) else '',
                float(record.get('Hours', 0)) if pd.notna(record.get('Hours')) else 0,
                float(record.get('Bill Rate', 0)) if pd.notna(record.get('Bill Rate')) else None,
                float(record.get('Bill Amount', 0)) if pd.notna(record.get('Bill Amount')) else None,
                str(record.get('In Time', '')) if pd.notna(record.get('In Time')) else '',
                str(record.get('Out Time', '')) if pd.notna(record.get('Out Time')) else '',
                float(record.get('Lunch', 0)) if pd.notna(record.get('Lunch')) else None,
                int(record.get('Bill Cat Number', 0)) if pd.notna(record.get('Bill Cat Number')) else None,
                datetime.now(),
                f"Force imported - {record_type} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
            return False
    
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
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor() as cursor:
                batch_size = 100
                
                for start_idx in range(0, len(df_import), batch_size):
                    end_idx = min(start_idx + batch_size, len(df_import))
                    batch = df_import.iloc[start_idx:end_idx]
                    
                    batch_success = 0
                    batch_errors = 0
                    
                    for idx, record in batch.iterrows():
                        success = self.insert_invoice_record(cursor, record, file_type)
                        if success:
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
                    
                    # Commit batch
                    conn.commit()
                    self.logger.info(f"Batch {start_idx//batch_size + 1}: {batch_success} imported, {batch_errors} skipped")
        
        return True
    
    def generate_import_report(self) -> str:
        """Generate comprehensive import report"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Calculate totals
        total_processed = (self.processing_stats['duplicates_processed'] + 
                          self.processing_stats['missing_processed'])
        total_skipped = (self.processing_stats['duplicates_skipped'] + 
                        self.processing_stats['missing_skipped'])
        
        # Create report content
        report_content = f"""
FORCE IMPORT PROCESSING REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

PROCESSING SUMMARY:
{'-'*30}
Total records processed: {total_processed:,}
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
            report_content += f"""
VALIDATION FAILURES:
{'-'*30}
"""
            for failure in self.processing_stats['validation_failures'][:10]:
                report_content += f"- {failure['record']}: {failure['reason']} ({failure['type']})\n"
            
            if len(self.processing_stats['validation_failures']) > 10:
                report_content += f"... and {len(self.processing_stats['validation_failures']) - 10} more\n"
        
        # Add errors if any
        if self.processing_stats['errors']:
            report_content += f"""
ERRORS ENCOUNTERED:
{'-'*30}
"""
            for error in self.processing_stats['errors'][:5]:
                report_content += f"- {error}\n"
            
            if len(self.processing_stats['errors']) > 5:
                report_content += f"... and {len(self.processing_stats['errors']) - 5} more errors\n"
        
        # Success rate
        if total_processed + total_skipped > 0:
            success_rate = (total_processed / (total_processed + total_skipped)) * 100
            report_content += f"""
SUCCESS RATE: {success_rate:.2f}%
"""
        
        # Save report with UTF-8 encoding
        report_file = f'force_import_report_{timestamp}.txt'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        self.logger.info(f"Import report saved to: {report_file}")
        return report_file
    
    def run_force_import(self, duplicate_file: str = None, missing_file: str = None) -> Dict:
        """Run the complete force import process"""
        
        self.logger.info("Starting force import process...")
        
        results = {
            'success': True,
            'duplicate_file_processed': False,
            'missing_file_processed': False,
            'report_file': None
        }
        
        try:
            # Process duplicate imports if file provided
            if duplicate_file and os.path.exists(duplicate_file):
                self.logger.info(f"Processing duplicate file: {duplicate_file}")
                success = self.process_imports(duplicate_file, 'duplicate')
                if success:
                    results['duplicate_file_processed'] = True
                    self.logger.info("✅ Duplicate imports completed")
                else:
                    self.logger.error("❌ Duplicate imports failed")
                    results['success'] = False
            elif duplicate_file:
                self.logger.warning(f"Duplicate file not found: {duplicate_file}")
            
            # Process missing imports if file provided
            if missing_file and os.path.exists(missing_file):
                self.logger.info(f"Processing missing file: {missing_file}")
                success = self.process_imports(missing_file, 'missing')
                if success:
                    results['missing_file_processed'] = True
                    self.logger.info("✅ Missing imports completed")
                else:
                    self.logger.error("❌ Missing imports failed")
                    results['success'] = False
            elif missing_file:
                self.logger.warning(f"Missing file not found: {missing_file}")
            
            # Generate report
            report_file = self.generate_import_report()
            results['report_file'] = report_file
            
            # Final summary
            total_imported = (self.processing_stats['duplicates_processed'] + 
                            self.processing_stats['missing_processed'])
            
            print("\n" + "="*60)
            print("✅ FORCE IMPORT COMPLETE!")
            print("="*60)
            print(f"Total records imported: {total_imported:,}")
            print(f"Processing report: {report_file}")
            
            if total_imported > 0:
                print(f"\n🔍 Verify imports in database:")
                print(f"SELECT COUNT(*) FROM invoice_details WHERE source_system = 'AUS';")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Fatal error in force import process: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            results['success'] = False
            return results


def check_file_contents(filename: str):
    """Quick check of file contents and structure"""
    
    print(f"\n🔍 Checking file: {filename}")
    
    try:
        df = pd.read_csv(filename, encoding='utf-8')
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)[:10]}")  # First 10 columns
        
        # Check for key columns
        if 'Force_Import' in df.columns:
            force_import_counts = df['Force_Import'].fillna('').astype(str).str.upper().value_counts()
            print(f"\n   Force_Import values:")
            for value, count in force_import_counts.items():
                print(f"      {value}: {count}")
        
        if 'Auto_Import' in df.columns:
            auto_import_counts = df['Auto_Import'].fillna('').astype(str).str.upper().value_counts()
            print(f"\n   Auto_Import values:")
            for value, count in auto_import_counts.items():
                print(f"      {value}: {count}")
                
    except Exception as e:
        print(f"   Error reading file: {e}")


# Convenience function for command line usage
def process_reviewed_files(duplicate_file: str = None, missing_file: str = None):
    """
    Convenience function to process reviewed files
    
    Usage:
    - process_reviewed_files('duplicate_analysis_reviewed.csv')
    - process_reviewed_files(missing_file='missing_analysis_reviewed.csv')
    - process_reviewed_files('dup_file.csv', 'miss_file.csv')
    """
    processor = ForceImportProcessor()
    return processor.run_force_import(duplicate_file, missing_file)


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python force_import_processor.py <duplicate_file> [missing_file]")
        print("   or: python force_import_processor.py --missing <missing_file>")
        print("   or: python force_import_processor.py --check <file>")
        sys.exit(1)
    
    if sys.argv[1] == '--check' and len(sys.argv) >= 3:
        # Check file contents
        check_file_contents(sys.argv[2])
    elif sys.argv[1] == '--missing' and len(sys.argv) >= 3:
        # Only process missing file
        process_reviewed_files(missing_file=sys.argv[2])
    elif len(sys.argv) >= 3:
        # Process both files
        process_reviewed_files(sys.argv[1], sys.argv[2])
    else:
        # Process only duplicate file
        process_reviewed_files(sys.argv[1])
