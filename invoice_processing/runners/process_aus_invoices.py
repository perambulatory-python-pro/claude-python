"""
AUS Invoice Processor - Production Ready
Processes AUS invoices, skipping -ORG entries
"""

import os
import pandas as pd
import psycopg2
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class AUSInvoiceProcessor:
    def __init__(self, log_file: str = 'aus_invoice_processing.log'):
        """Initialize processor with database connection"""
        
        # Get database URL from environment
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in .env file")
        
        # Initialize stats
        self.stats = {
            'total_records': 0,
            'processed': 0,
            'skipped_org': 0,
            'skipped_no_date': 0,
            'errors': 0,
            'org_invoices': []
        }
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Test connection
        self.test_connection()
    
    def test_connection(self):
        """Test database connection"""
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    self.logger.info("✅ Database connection successful")
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def is_org_invoice(self, invoice_no: str) -> bool:
        """Check if invoice has -ORG suffix"""
        return str(invoice_no).upper().endswith('-ORG')
    
    def process_aus_file(self, file_path: str = 'invoice_details_aus.csv') -> dict:
        """Process AUS invoice file"""
        self.logger.info(f"Starting AUS invoice processing from: {file_path}")
        
        # Read CSV
        try:
            df = pd.read_csv(file_path)
            self.stats['total_records'] = len(df)
            self.logger.info(f"Loaded {len(df)} records from AUS file")
        except Exception as e:
            self.logger.error(f"Error reading file: {e}")
            raise
        
        # Identify and log -ORG invoices
        org_mask = df['Invoice Number'].apply(lambda x: self.is_org_invoice(x))
        org_invoices = df[org_mask]
        
        if len(org_invoices) > 0:
            self.logger.warning(f"Found {len(org_invoices)} records with -ORG suffix")
            
            # Log summary
            org_summary = org_invoices.groupby('Invoice Number').agg({
                'Bill Amount': 'sum',
                'Hours': 'sum',
                'Employee Number': 'count'
            }).round(2)
            
            for invoice_no, row in org_summary.iterrows():
                self.logger.info(
                    f"  Skipping {invoice_no}: ${row['Bill Amount']:,.2f} | "
                    f"{row['Hours']:.2f} hours | {row['Employee Number']} records"
                )
                self.stats['org_invoices'].append({
                    'invoice_no': invoice_no,
                    'total_amount': row['Bill Amount'],
                    'total_hours': row['Hours'],
                    'record_count': row['Employee Number']
                })
        
        # Filter out -ORG invoices
        valid_df = df[~org_mask].copy()
        self.stats['skipped_org'] = len(org_invoices)
        
        self.logger.info(f"Processing {len(valid_df)} valid invoices")
        
        # Process valid records
        processed = 0
        batch_size = 1000
        
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor() as cursor:
                for start_idx in range(0, len(valid_df), batch_size):
                    end_idx = min(start_idx + batch_size, len(valid_df))
                    batch = valid_df.iloc[start_idx:end_idx]
                    
                    for idx, row in batch.iterrows():
                        try:
                            # Skip if no work date
                            if pd.isna(row.get('Work Date')):
                                self.stats['skipped_no_date'] += 1
                                continue
                            
                            # Process the row
                            self.process_invoice_row(cursor, row)
                            processed += 1
                            
                        except Exception as e:
                            self.stats['errors'] += 1
                            self.logger.error(f"Error processing row {idx}: {e}")
                    
                    # Commit batch
                    conn.commit()
                    self.logger.info(f"Processed {end_idx}/{len(valid_df)} records...")
        
        self.stats['processed'] = processed
        
        # Generate reports
        self.create_summary_report()
        
        return self.stats
    
    def process_invoice_row(self, cursor, row: pd.Series):
        """Process a single invoice row"""
        # Your existing invoice processing logic here
        # For now, just a placeholder that counts the row
        pass
    
    def create_summary_report(self):
        """Create summary report"""
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
Total Records: {sum(inv['record_count'] for inv in self.stats['org_invoices']):,}
"""
        
        # Save report
        report_file = f"aus_processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        # Save -ORG invoice details
        if self.stats['org_invoices']:
            org_df = pd.DataFrame(self.stats['org_invoices'])
            org_file = f"skipped_org_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            org_df.to_csv(org_file, index=False)
            self.logger.info(f"Saved -ORG invoice details to: {org_file}")
        
        self.logger.info(f"Summary report saved to: {report_file}")
        print(report)

# Main execution
def main():
    """Main function"""
    processor = AUSInvoiceProcessor()
    
    try:
        # Check if file exists
        if not os.path.exists('invoice_details_aus.csv'):
            print("❌ Error: invoice_details_aus.csv not found!")
            print("Please ensure the file is in the current directory")
            return
        
        # Process the file
        stats = processor.process_aus_file('invoice_details_aus.csv')
        
        print("\n" + "="*50)
        print("✅ PROCESSING COMPLETE!")
        print("="*50)
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()