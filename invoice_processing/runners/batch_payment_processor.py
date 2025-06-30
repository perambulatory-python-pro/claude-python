"""
Batch Payment Processor
Processes all Kaiser payment files (.xlsx and .msg) in a specified folder
Handles both Excel files and Outlook email files automatically

Key Features:
- Processes multiple files in one run
- Tracks success/failure for each file
- Skips already processed payments
- Generates detailed processing report
- Error handling and logging
"""

import os
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
import extract_msg
import traceback
from typing import Dict, List, Tuple

# Import your existing modules
from data_mapper_enhanced import EnhancedDataMapper
from database_manager_compatible import EnhancedDatabaseManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'batch_payment_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BatchPaymentProcessor:
    """
    Batch processor for Kaiser payment files
    """
    
    def __init__(self, db_manager: EnhancedDatabaseManager, data_mapper: EnhancedDataMapper):
        self.db = db_manager
        self.mapper = data_mapper
        self.processing_summary = {
            'excel': {'success': 0, 'failed': 0, 'skipped': 0, 'files': []},
            'email': {'success': 0, 'failed': 0, 'skipped': 0, 'files': []},
            'errors': []
        }
    
    def process_folder(self, folder_path: str) -> Dict:
        """
        Process all payment files in the specified folder
        
        Args:
            folder_path: Path to folder containing .xlsx and .msg files
            
        Returns:
            Processing summary dictionary
        """
        logger.info(f"Starting batch processing of folder: {folder_path}")
        
        if not os.path.exists(folder_path):
            raise ValueError(f"Folder not found: {folder_path}")
        
        # Get all relevant files
        excel_files = list(Path(folder_path).glob('*.xlsx')) + list(Path(folder_path).glob('*.xls'))
        msg_files = list(Path(folder_path).glob('*.msg'))
        
        logger.info(f"Found {len(excel_files)} Excel files and {len(msg_files)} email files")
        
        # Process Excel files
        for excel_file in excel_files:
            self._process_excel_file(excel_file)
        
        # Process Email files
        for msg_file in msg_files:
            self._process_msg_file(msg_file)
        
        # Generate summary report
        self._generate_summary_report()
        
        return self.processing_summary
    
    def _process_excel_file(self, file_path: Path) -> bool:
        """
        Process a single Excel payment file
        Enhanced to handle multiple payment IDs in a single file
        """
        logger.info(f"\nProcessing Excel file: {file_path.name}")
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Detect if it's a payment file
            file_type = self.mapper.auto_detect_file_type(file_path.name, df)
            
            if file_type != "KP_Payment_Excel":
                logger.warning(f"Not a payment file: {file_path.name} (detected as {file_type})")
                self.processing_summary['excel']['skipped'] += 1
                self.processing_summary['excel']['files'].append({
                    'file': file_path.name,
                    'status': 'skipped',
                    'reason': f'Not a payment file ({file_type})'
                })
                return False
            
            # Extract payment data (now handles multiple payments)
            master_result = self.mapper.extract_payment_master_data(df)
            
            # Check if multiple payments detected
            if master_result.get('multiple_payments', False):
                logger.info(f"Multiple payment IDs detected in {file_path.name}: {master_result['payment_ids']}")
                
                # Process each payment separately
                payments_data = self.mapper.process_multiple_payments(
                    master_result['dataframe'], 
                    master_result['payment_ids']
                )
                
                total_success = 0
                total_failed = 0
                total_skipped = 0
                processed_payments = []
                
                for master_data, detail_records in payments_data:
                    payment_id = master_data['payment_id']
                    
                    # Check if payment already exists
                    if self.db.check_payment_exists(payment_id):
                        logger.info(f"Payment {payment_id} already exists - skipping")
                        total_skipped += 1
                        processed_payments.append({
                            'payment_id': payment_id,
                            'status': 'skipped',
                            'reason': 'Already processed'
                        })
                        continue
                    
                    # Validate payment data
                    validation_results = self.mapper.validate_payment_data(master_data, detail_records)
                    
                    if not validation_results['is_valid']:
                        logger.error(f"Validation failed for payment {payment_id}: {validation_results['errors']}")
                        total_failed += 1
                        processed_payments.append({
                            'payment_id': payment_id,
                            'status': 'failed',
                            'reason': f"Validation: {validation_results['errors']}"
                        })
                        continue
                    
                    # Process payment
                    success = self.db.process_payment_remittance(master_data, detail_records)
                    
                    if success:
                        logger.info(f"Successfully processed payment {payment_id}")
                        total_success += 1
                        processed_payments.append({
                            'payment_id': payment_id,
                            'status': 'success',
                            'amount': master_data['payment_amount'],
                            'invoices': len(detail_records)
                        })
                    else:
                        logger.error(f"Database error processing payment {payment_id}")
                        total_failed += 1
                        processed_payments.append({
                            'payment_id': payment_id,
                            'status': 'failed',
                            'reason': 'Database error'
                        })
                
                # Update summary based on results
                if total_success > 0:
                    self.processing_summary['excel']['success'] += 1
                if total_failed > 0:
                    self.processing_summary['excel']['failed'] += 1
                if total_skipped > 0 and total_success == 0 and total_failed == 0:
                    self.processing_summary['excel']['skipped'] += 1
                
                # Create summary entry
                self.processing_summary['excel']['files'].append({
                    'file': file_path.name,
                    'status': 'multi-payment',
                    'total_payments': len(master_result['payment_ids']),
                    'success': total_success,
                    'failed': total_failed,
                    'skipped': total_skipped,
                    'payments': processed_payments
                })
                
                return total_success > 0
                
            else:
                # Single payment - process normally
                master_data = master_result  # It's already the master data for single payment
                detail_records = self.mapper.map_payment_details(df)
                
                # Check if payment already exists
                if self.db.check_payment_exists(master_data['payment_id']):
                    logger.info(f"Payment {master_data['payment_id']} already exists - skipping")
                    self.processing_summary['excel']['skipped'] += 1
                    self.processing_summary['excel']['files'].append({
                        'file': file_path.name,
                        'status': 'skipped',
                        'payment_id': master_data['payment_id'],
                        'reason': 'Already processed'
                    })
                    return False
                
                # Validate payment data
                validation_results = self.mapper.validate_payment_data(master_data, detail_records)
                
                if not validation_results['is_valid']:
                    logger.error(f"Validation failed for {file_path.name}: {validation_results['errors']}")
                    self.processing_summary['excel']['failed'] += 1
                    self.processing_summary['excel']['files'].append({
                        'file': file_path.name,
                        'status': 'failed',
                        'payment_id': master_data['payment_id'],
                        'reason': f"Validation: {validation_results['errors']}"
                    })
                    return False
                
                # Process payment
                success = self.db.process_payment_remittance(
                    master_data, 
                    detail_records
                )
                
                if success:
                    logger.info(f"Successfully processed payment {master_data['payment_id']} from {file_path.name}")
                    self.processing_summary['excel']['success'] += 1
                    self.processing_summary['excel']['files'].append({
                        'file': file_path.name,
                        'status': 'success',
                        'payment_id': master_data['payment_id'],
                        'amount': master_data['payment_amount'],
                        'invoices': len(detail_records)
                    })
                else:
                    self.processing_summary['excel']['failed'] += 1
                    self.processing_summary['excel']['files'].append({
                        'file': file_path.name,
                        'status': 'failed',
                        'payment_id': master_data['payment_id'],
                        'reason': 'Database error'
                    })
                
                return success
            
        except Exception as e:
            logger.error(f"Error processing Excel file {file_path.name}: {e}")
            logger.error(traceback.format_exc())
            self.processing_summary['excel']['failed'] += 1
            self.processing_summary['excel']['files'].append({
                'file': file_path.name,
                'status': 'error',
                'reason': str(e)
            })
            self.processing_summary['errors'].append(f"{file_path.name}: {str(e)}")
            return False
    
    def _process_msg_file(self, file_path: Path) -> bool:
        """
        Process a single Outlook .msg file
        """
        logger.info(f"\nProcessing email file: {file_path.name}")
        
        try:
            # Extract HTML content from .msg file
            msg = extract_msg.Message(str(file_path))
            
            # Get HTML content, handling both string and bytes
            html_content = msg.htmlBody or msg.body
            
            # Convert bytes to string if needed
            if isinstance(html_content, bytes):
                html_content = html_content.decode('utf-8', errors='ignore')
            
            if not html_content:
                logger.warning(f"No HTML content found in {file_path.name}")
                self.processing_summary['email']['skipped'] += 1
                self.processing_summary['email']['files'].append({
                    'file': file_path.name,
                    'status': 'skipped',
                    'reason': 'No HTML content'
                })
                return False
            
            # Check if it's a payment email
            if not self.mapper.detect_payment_email_html(html_content):
                logger.warning(f"Not a payment email: {file_path.name}")
                self.processing_summary['email']['skipped'] += 1
                self.processing_summary['email']['files'].append({
                    'file': file_path.name,
                    'status': 'skipped',
                    'reason': 'Not a payment email'
                })
                return False
            
            # Process payment email
            master_data, detail_records = self.mapper.process_payment_email_html(html_content)
            
            # Check if payment already exists
            if self.db.check_payment_exists(master_data['payment_id']):
                logger.info(f"Payment {master_data['payment_id']} already exists - skipping")
                self.processing_summary['email']['skipped'] += 1
                self.processing_summary['email']['files'].append({
                    'file': file_path.name,
                    'status': 'skipped',
                    'payment_id': master_data['payment_id'],
                    'reason': 'Already processed'
                })
                return False
            
            # Validate payment data
            validation_results = self.mapper.validate_payment_data(master_data, detail_records)
            
            if not validation_results['is_valid']:
                logger.error(f"Validation failed for {file_path.name}: {validation_results['errors']}")
                self.processing_summary['email']['failed'] += 1
                self.processing_summary['email']['files'].append({
                    'file': file_path.name,
                    'status': 'failed',
                    'payment_id': master_data['payment_id'],
                    'reason': f"Validation: {validation_results['errors']}"
                })
                return False
            
            # Process payment
            success = self.db.process_payment_remittance(
                master_data, 
                detail_records
            )
            
            if success:
                logger.info(f"Successfully processed payment {master_data['payment_id']} from {file_path.name}")
                self.processing_summary['email']['success'] += 1
                self.processing_summary['email']['files'].append({
                    'file': file_path.name,
                    'status': 'success',
                    'payment_id': master_data['payment_id'],
                    'amount': master_data['payment_amount'],
                    'invoices': len(detail_records)
                })
            else:
                self.processing_summary['email']['failed'] += 1
                self.processing_summary['email']['files'].append({
                    'file': file_path.name,
                    'status': 'failed',
                    'payment_id': master_data['payment_id'],
                    'reason': 'Database error'
                })
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing email file {file_path.name}: {e}")
            logger.error(traceback.format_exc())
            self.processing_summary['email']['failed'] += 1
            self.processing_summary['email']['files'].append({
                'file': file_path.name,
                'status': 'error',
                'reason': str(e)
            })
            self.processing_summary['errors'].append(f"{file_path.name}: {str(e)}")
            return False
    
    def _generate_summary_report(self):
        """
        Generate and save a detailed processing report
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report = f"""
Kaiser Payment Batch Processing Report
Generated: {timestamp}
{'='*60}

SUMMARY
-------
Excel Files:
  • Processed: {self.processing_summary['excel']['success'] + self.processing_summary['excel']['failed'] + self.processing_summary['excel']['skipped']}
  • Success: {self.processing_summary['excel']['success']}
  • Failed: {self.processing_summary['excel']['failed']}
  • Skipped: {self.processing_summary['excel']['skipped']}

Email Files (.msg):
  • Processed: {self.processing_summary['email']['success'] + self.processing_summary['email']['failed'] + self.processing_summary['email']['skipped']}
  • Success: {self.processing_summary['email']['success']}
  • Failed: {self.processing_summary['email']['failed']}
  • Skipped: {self.processing_summary['email']['skipped']}

Total Success Rate: {self._calculate_success_rate():.1f}%

{'='*60}
DETAILS
-------
"""
        
        # Add Excel file details
        if self.processing_summary['excel']['files']:
            report += "\nEXCEL FILES:\n"
            for file_info in self.processing_summary['excel']['files']:
                status_icon = '✓' if file_info['status'] == 'success' else '✗' if file_info['status'] == 'failed' else '○'
                report += f"{status_icon} {file_info['file']}\n"
                if file_info.get('payment_id'):
                    report += f"   Payment ID: {file_info['payment_id']}\n"
                if file_info.get('amount'):
                    report += f"   Amount: ${file_info['amount']:,.2f}\n"
                if file_info.get('invoices'):
                    report += f"   Invoices: {file_info['invoices']}\n"
                if file_info.get('reason'):
                    report += f"   Reason: {file_info['reason']}\n"
                report += "\n"
        
        # Add Email file details
        if self.processing_summary['email']['files']:
            report += "\nEMAIL FILES:\n"
            for file_info in self.processing_summary['email']['files']:
                status_icon = '✓' if file_info['status'] == 'success' else '✗' if file_info['status'] == 'failed' else '○'
                report += f"{status_icon} {file_info['file']}\n"
                if file_info.get('payment_id'):
                    report += f"   Payment ID: {file_info['payment_id']}\n"
                if file_info.get('amount'):
                    report += f"   Amount: ${file_info['amount']:,.2f}\n"
                if file_info.get('invoices'):
                    report += f"   Invoices: {file_info['invoices']}\n"
                if file_info.get('reason'):
                    report += f"   Reason: {file_info['reason']}\n"
                report += "\n"
        
        # Add errors section
        if self.processing_summary['errors']:
            report += f"\n{'='*60}\nERRORS\n-------\n"
            for error in self.processing_summary['errors']:
                report += f"• {error}\n"
        
        # Save report
        report_filename = f"payment_batch_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_filename, 'w', encoding='utf-8') as f:  # Fixed: specify UTF-8 encoding
            f.write(report)
        
        logger.info(f"\nReport saved to: {report_filename}")
        print(report)
    
    def _calculate_success_rate(self) -> float:
        """
        Calculate overall success rate
        """
        total_processed = (
            self.processing_summary['excel']['success'] + 
            self.processing_summary['excel']['failed'] +
            self.processing_summary['email']['success'] + 
            self.processing_summary['email']['failed']
        )
        
        if total_processed == 0:
            return 0.0
        
        total_success = (
            self.processing_summary['excel']['success'] + 
            self.processing_summary['email']['success']
        )
        
        return (total_success / total_processed) * 100


def main():
    """
    Main function to run batch processing
    """
    # Configuration
    PAYMENT_FOLDER = r"C:\Users\Brendon Jewell\Finance Ops Dropbox\Finance Ops Team Folder\DOMO Source Files\[DOMO] Accounts Receivable (AR)\Payment Notifications"
    
    print("Kaiser Payment Batch Processor")
    print("=" * 60)
    
    # Initialize components
    try:
        # Initialize database manager
        db_manager = EnhancedDatabaseManager()
        
        # Initialize data mapper
        data_mapper = EnhancedDataMapper()
        
        # Create batch processor
        processor = BatchPaymentProcessor(db_manager, data_mapper)
        
        # Get folder path from user if not set
        if PAYMENT_FOLDER == r"C:\path\to\your\payment\files":
            folder_path = input("Enter the path to your payment files folder: ").strip()
        else:
            folder_path = PAYMENT_FOLDER
            print(f"Using configured folder: {folder_path}")
        
        # Confirm before processing
        print(f"\nFolder: {folder_path}")
        
        # Count files
        if os.path.exists(folder_path):
            excel_count = len(list(Path(folder_path).glob('*.xlsx')) + list(Path(folder_path).glob('*.xls')))
            msg_count = len(list(Path(folder_path).glob('*.msg')))
            print(f"Found {excel_count} Excel files and {msg_count} email files")
            
            confirm = input("\nProceed with batch processing? (yes/no): ").strip().lower()
            
            if confirm == 'yes':
                # Process folder
                start_time = datetime.now()
                summary = processor.process_folder(folder_path)
                end_time = datetime.now()
                
                duration = (end_time - start_time).total_seconds()
                print(f"\n{'='*60}")
                print(f"Batch processing completed in {duration:.1f} seconds")
                print(f"Success rate: {processor._calculate_success_rate():.1f}%")
            else:
                print("Batch processing cancelled.")
        else:
            print(f"Error: Folder not found: {folder_path}")
    
    except Exception as e:
        print(f"Error: {e}")
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()