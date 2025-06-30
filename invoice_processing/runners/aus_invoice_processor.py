"""
AUS Invoice Processor - Skip -ORG Invoices
Processes valid invoices and logs skipped ones for audit trail
"""

import pandas as pd
import psycopg2
from datetime import datetime
import logging
from typing import Dict, List, Tuple

class AUSInvoiceProcessor:
    def __init__(self, db_config: dict, log_file: str = 'aus_invoice_processing.log'):
        """
        Initialize processor with database config and logging
        
        Args:
            db_config: Database connection parameters
            log_file: Path to log file for audit trail
        """
        self.db_config = db_config
        self.stats = {
            'total_records': 0,
            'processed': 0,
            'skipped_org': 0,
            'skipped_no_date': 0,
            'errors': 0,
            'org_invoices': []  # Keep track of skipped -ORG invoices
        }
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()  # Also print to console
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def is_org_invoice(self, invoice_no: str) -> bool:
        """
        Check if invoice has -ORG suffix
        
        Python concept: String methods and boolean return
        """
        return str(invoice_no).upper().endswith('-ORG')
    
    def process_aus_file(self, file_path: str) -> Dict:
        """
        Process AUS invoice file, skipping -ORG invoices
        
        Returns:
            Dictionary with processing statistics
        """
        self.logger.info(f"Starting AUS invoice processing from: {file_path}")
        
        # Read the CSV file
        try:
            df = pd.read_csv(file_path)
            self.stats['total_records'] = len(df)
            self.logger.info(f"Loaded {len(df)} records from AUS file")
        except Exception as e:
            self.logger.error(f"Error reading file: {e}")
            raise
        
        # First, analyze -ORG invoices
        org_mask = df['Invoice Number'].apply(lambda x: self.is_org_invoice(x))
        org_invoices = df[org_mask]
        
        if len(org_invoices) > 0:
            self.logger.warning(f"Found {len(org_invoices)} records with -ORG suffix")
            
            # Log summary of -ORG invoices for audit trail
            org_summary = org_invoices.groupby('Invoice Number').agg({
                'Bill Amount': 'sum',
                'Hours': 'sum',
                'Employee Number': 'count'
            }).round(2)
            
            self.logger.info("Summary of skipped -ORG invoices:")
            for invoice_no, row in org_summary.iterrows():
                self.logger.info(
                    f"  {invoice_no}: ${row['Bill Amount']:,.2f} | "
                    f"{row['Hours']:.2f} hours | {row['Employee Number']} employees"
                )
                self.stats['org_invoices'].append({
                    'invoice_no': invoice_no,
                    'total_amount': row['Bill Amount'],
                    'total_hours': row['Hours'],
                    'employee_count': row['Employee Number']
                })
        
        # Filter out -ORG invoices
        valid_df = df[~org_mask].copy()
        self.stats['skipped_org'] = len(org_invoices)
        
        self.logger.info(f"Processing {len(valid_df)} valid invoices (excluded {len(org_invoices)} -ORG invoices)")
        
        # Process valid invoices
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cursor:
                for idx, row in valid_df.iterrows():
                    try:
                        self.process_invoice_row(cursor, row)
                        self.stats['processed'] += 1
                        
                        # Commit every 1000 records
                        if self.stats['processed'] % 1000 == 0:
                            conn.commit()
                            self.logger.info(f"Processed {self.stats['processed']} records...")
                    
                    except Exception as e:
                        self.stats['errors'] += 1
                        self.logger.error(f"Error processing row {idx}: {e}")
                        conn.rollback()
                
                # Final commit
                conn.commit()
        
        # Create summary report
        self.create_summary_report()
        
        return self.stats
    
    def process_invoice_row(self, cursor, row: pd.Series):
        """
        Process a single valid invoice row
        
        Python concepts:
        - Type hints with pandas Series
        - Database cursor operations
        - Data validation
        """
        # Validate work date
        work_date = row.get('Work Date')
        if pd.isna(work_date):
            self.stats['skipped_no_date'] += 1
            return
        
        # Insert into database
        cursor.execute("""
            INSERT INTO invoice_details (
                invoice_no,
                employee_number,
                employee_name,
                work_date,
                week_ending,
                job_number,
                customer_number,
                po_number,
                post_description,
                pay_description,
                hours,
                pay_rate,
                bill_rate,
                bill_amount,
                in_time,
                out_time,
                lunch_hours,
                bill_cat_number,
                source_system,
                processed_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            row.get('Invoice Number'),
            row.get('Employee Number'),
            row.get('Employee Name'),
            work_date,
            row.get('Week Ending'),
            row.get('Job Number'),
            row.get('Customer Number'),
            row.get('PO'),
            row.get('Post Description'),
            row.get('Pay Hours Description'),
            row.get('Hours'),
            row.get('Pay Rate'),
            row.get('Bill Rate'),
            row.get('Bill Amount'),
            row.get('In Time'),
            row.get('Out Time'),
            row.get('Lunch'),
            row.get('Bill Cat Number'),
            'AUS',
            datetime.now()
        ))
    
    def create_summary_report(self):
        """
        Create a comprehensive summary report
        
        Python concept: String formatting and file I/O
        """
        report = f"""
========================================================
AUS INVOICE PROCESSING SUMMARY
========================================================
Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

STATISTICS:
-----------
Total Records in File:     {self.stats['total_records']:,}
Successfully Processed:    {self.stats['processed']:,}
Skipped (-ORG invoices):   {self.stats['skipped_org']:,}
Skipped (no date):         {self.stats['skipped_no_date']:,}
Errors:                    {self.stats['errors']:,}

Success Rate: {(self.stats['processed'] / self.stats['total_records'] * 100):.2f}%

SKIPPED -ORG INVOICES SUMMARY:
------------------------------
Total Amount: ${sum(inv['total_amount'] for inv in self.stats['org_invoices']):,.2f}
Total Hours:  {sum(inv['total_hours'] for inv in self.stats['org_invoices']):,.2f}
"""
        
        # Save report
        report_file = f"aus_processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        # Also save detailed -ORG invoice list to CSV for audit
        if self.stats['org_invoices']:
            org_df = pd.DataFrame(self.stats['org_invoices'])
            org_df.to_csv(
                f"skipped_org_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                index=False
            )
        
        self.logger.info(f"Summary report saved to: {report_file}")
        print(report)

# Main execution function
def main():
    """
    Main function to run the AUS invoice processor
    
    Python concept: Main function pattern
    """
    # Database configuration
    db_config = {
        'host': 'your_host',
        'database': 'your_database',
        'user': 'your_user',
        'password': 'your_password',
        'port': 5432
    }
    
    # Create processor and run
    processor = AUSInvoiceProcessor(db_config)
    
    try:
        stats = processor.process_aus_file('invoice_details_aus.csv')
        
        # Print final summary
        print("\n" + "="*50)
        print("PROCESSING COMPLETE")
        print("="*50)
        print(f"✅ Successfully processed: {stats['processed']:,} records")
        print(f"⚠️  Skipped -ORG invoices: {stats['skipped_org']:,} records")
        print(f"❌ Errors encountered: {stats['errors']:,} records")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()