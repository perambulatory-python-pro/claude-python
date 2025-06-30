import pandas as pd
from datetime import datetime
import os
import numpy as np

class InvoiceMasterManager:
    def __init__(self, master_file_path="invoice_master.xlsx"):
        self.master_file_path = master_file_path
        self.backup_folder = "backups"
        self.log_messages = []
        
        # Create backups folder if it doesn't exist
        if not os.path.exists(self.backup_folder):
            os.makedirs(self.backup_folder)
    
    def log(self, message):
        """Add message to log and print it"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.log_messages.append(log_message)
        print(log_message)
    
    def create_backup(self):
        """Create a backup of the master file before processing"""
        if os.path.exists(self.master_file_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"invoice_master_backup_{timestamp}.xlsx"
            backup_path = os.path.join(self.backup_folder, backup_name)
            
            # Read and save backup
            master_df = pd.read_excel(self.master_file_path)
            master_df.to_excel(backup_path, index=False)
            self.log(f"Backup created: {backup_path}")
        else:
            self.log("No existing master file found - no backup needed")
    
    def load_master_file(self):
        """Load the master file or create a new one if it doesn't exist"""
        if os.path.exists(self.master_file_path):
            df = pd.read_excel(self.master_file_path)
            self.log(f"Loaded master file with {len(df)} existing records")
        else:
            # Create empty DataFrame with all required columns
            columns = [
                'EMID', 'NUID', 'SERVICE REQ\'D BY', 'Service Area', 'Post Name',
                'Invoice No.', 'Invoice From', 'Invoice To', 'Invoice Date',
                'EDI Date', 'Release Date', 'Add-On Date', 'Chartfield', 'Invoice Total',
                'Notes', 'Not Transmitted', 'Invoice No. History', 
                'Original EDI Date', 'Original Add-On Date', 'Original Release Date'
            ]
            df = pd.DataFrame(columns=columns)
            self.log("Created new master file - no existing records")
        
        return df
    
    def standardize_single_date(self, date_value):
        """
        Convert various date formats to ISO format (yyyy-mm-dd)
        Handles:
        - yyyymmdd format (e.g., 20250619)
        - Short date format (e.g., 6/19/2025, 06/19/2025)
        - Already formatted dates
        """
        if pd.isna(date_value):
            return None
        
        # Convert to string for processing
        date_str = str(date_value).strip()
        
        # Handle yyyymmdd format (8 digits)
        if len(date_str) == 8 and date_str.isdigit():
            try:
                # Parse yyyymmdd
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass
        
        # Handle various date formats using pandas
        try:
            # Try to parse with pandas (handles most common formats)
            parsed_date = pd.to_datetime(date_str, errors='coerce')
            if not pd.isna(parsed_date):
                return parsed_date.strftime('%Y-%m-%d')
        except:
            pass
        
        # If all else fails, return None
        return None
    
    def standardize_dates(self, df, date_columns):
        """Standardize date formats to yyyy-mm-dd"""
        for col in date_columns:
            if col in df.columns:
                # Apply the same date standardization logic from your original script
                df[col] = df[col].apply(self.standardize_single_date)
        return df
    
    def check_duplicates_in_input(self, df, file_type):
        """Check for duplicate Invoice No. in input file"""
        duplicates = df[df['Invoice No.'].duplicated(keep=False)]
        if not duplicates.empty:
            self.log(f"WARNING: {file_type} file contains duplicate Invoice No.:")
            for invoice_no in duplicates['Invoice No.'].unique():
                dup_rows = duplicates[duplicates['Invoice No.'] == invoice_no]
                self.log(f"  Invoice No. {invoice_no}: {len(dup_rows)} occurrences")
                
                # Check if rows are actually distinct
                dup_rows_no_invoice = dup_rows.drop('Invoice No.', axis=1)
                unique_rows = dup_rows_no_invoice.drop_duplicates()
                if len(unique_rows) < len(dup_rows):
                    self.log(f"    - Some rows are identical (will process only unique ones)")
                else:
                    self.log(f"    - All rows are distinct (will process first occurrence)")
        
        return duplicates
    
    def preserve_date_logic(self, master_row, new_date, date_col, original_date_col):
        """Handle date preservation logic"""
        existing_date = master_row.get(date_col)
        existing_original = master_row.get(original_date_col)
        
        if pd.isna(existing_date) or existing_date is None:
            # No existing date - use new date
            return new_date, existing_original
        elif existing_date != new_date and new_date is not None:
            # Different dates - preserve original and update current
            original_to_preserve = existing_original if pd.notna(existing_original) else existing_date
            return new_date, original_to_preserve
        else:
            # Same date or new date is null - keep existing
            return existing_date, existing_original
    
    def process_release_file(self, release_file_path):
        """Process weekly release file"""
        self.log(f"=== Processing Release File: {release_file_path} ===")
        
        # Create backup first
        self.create_backup()
        
        # Load files
        master_df = self.load_master_file()
        release_df = pd.read_excel(release_file_path)
        
        # Standardize dates - including the tracking dates
        date_columns = ['Invoice From', 'Invoice To', 'Invoice Date', 'Release Date']
        release_df = self.standardize_dates(release_df, date_columns)
        
        # Check for duplicates in input
        self.check_duplicates_in_input(release_df, "Release")
        
        # Remove duplicates, keeping first occurrence
        release_df = release_df.drop_duplicates(subset=['Invoice No.'], keep='first')
        
        updates_count = 0
        new_records_count = 0
        
        for _, release_row in release_df.iterrows():
            invoice_no = release_row['Invoice No.']
            
            # Check if invoice exists in master
            mask = master_df['Invoice No.'] == invoice_no
            
            if mask.any():
                # Update existing record
                idx = master_df.index[mask][0]
                
                # Handle Release Date preservation - get from input file
                input_release_date = release_row.get('Release Date')
                if pd.notna(input_release_date):
                    current_release, original_release = self.preserve_date_logic(
                        master_df.loc[idx], input_release_date, 'Release Date', 'Original Release Date'
                    )
                else:
                    # Fallback to existing dates if input doesn't have release date
                    current_release = master_df.loc[idx].get('Release Date')
                    original_release = master_df.loc[idx].get('Original Release Date')
                
                # Update common columns
                for col in release_df.columns:
                    if col in master_df.columns and col != 'Invoice No.':
                        master_df.loc[idx, col] = release_row[col]
                
                # Set release dates
                master_df.loc[idx, 'Release Date'] = current_release
                master_df.loc[idx, 'Original Release Date'] = original_release
                
                updates_count += 1
                self.log(f"Updated existing record: {invoice_no}")
                
            else:
                # Create new record
                new_row = release_row.copy()
                # Use release date from input file if available, otherwise use today
                if 'Release Date' not in new_row or pd.isna(new_row.get('Release Date')):
                    new_row['Release Date'] = datetime.now().strftime('%Y-%m-%d')
                
                # Add to master
                master_df = pd.concat([master_df, new_row.to_frame().T], ignore_index=True)
                new_records_count += 1
                self.log(f"Created new record: {invoice_no}")
        
        # Save updated master file
        master_df.to_excel(self.master_file_path, index=False)
        self.log(f"Release processing complete: {updates_count} updated, {new_records_count} new records")
        
        return master_df
    
    def process_addon_file(self, addon_file_path):
        """Process Add-On file"""
        self.log(f"=== Processing Add-On File: {addon_file_path} ===")
        
        # Create backup first
        self.create_backup()
        
        # Load files
        master_df = self.load_master_file()
        addon_df = pd.read_excel(addon_file_path)
        
        # Standardize dates - including the tracking dates
        date_columns = ['Invoice From', 'Invoice To', 'Invoice Date', 'Add-On Date']
        addon_df = self.standardize_dates(addon_df, date_columns)
        
        # Check for duplicates in input
        self.check_duplicates_in_input(addon_df, "Add-On")
        
        # Remove duplicates, keeping first occurrence
        addon_df = addon_df.drop_duplicates(subset=['Invoice No.'], keep='first')
        
        updates_count = 0
        new_records_count = 0
        history_links_count = 0
        
        for _, addon_row in addon_df.iterrows():
            invoice_no = addon_row['Invoice No.']
            
            # Check if invoice exists in master
            mask = master_df['Invoice No.'] == invoice_no
            
            if mask.any():
                # Update existing record
                idx = master_df.index[mask][0]
                
                # Handle Add-On Date preservation - get from input file
                input_addon_date = addon_row.get('Add-On Date')
                if pd.notna(input_addon_date):
                    current_addon, original_addon = self.preserve_date_logic(
                        master_df.loc[idx], input_addon_date, 'Add-On Date', 'Original Add-On Date'
                    )
                else:
                    # Fallback to existing dates if input doesn't have add-on date
                    current_addon = master_df.loc[idx].get('Add-On Date')
                    original_addon = master_df.loc[idx].get('Original Add-On Date')
                
                # Update common columns
                for col in addon_df.columns:
                    if col in master_df.columns and col != 'Invoice No.':
                        if col == 'Original invoice #':
                            # Map to Invoice No. History
                            master_df.loc[idx, 'Invoice No. History'] = addon_row[col]
                        else:
                            master_df.loc[idx, col] = addon_row[col]
                
                # Set add-on dates
                master_df.loc[idx, 'Add-On Date'] = current_addon
                master_df.loc[idx, 'Original Add-On Date'] = original_addon
                
                updates_count += 1
                self.log(f"Updated existing record: {invoice_no}")
                
            else:
                # Create new record
                new_row = addon_row.copy()
                # Use add-on date from input file if available, otherwise use today
                if 'Add-On Date' not in new_row or pd.isna(new_row.get('Add-On Date')):
                    new_row['Add-On Date'] = datetime.now().strftime('%Y-%m-%d')
                
                # Handle Invoice History mapping
                if 'Original invoice #' in addon_row and pd.notna(addon_row['Original invoice #']):
                    new_row['Invoice No. History'] = addon_row['Original invoice #']
                    new_row = new_row.drop('Original invoice #', errors='ignore')
                
                # Add to master
                master_df = pd.concat([master_df, new_row.to_frame().T], ignore_index=True)
                new_records_count += 1
                self.log(f"Created new record: {invoice_no}")
            
            # Handle bidirectional linking for invoice history
            if 'Original invoice #' in addon_row and pd.notna(addon_row['Original invoice #']):
                original_invoice = addon_row['Original invoice #']
                original_mask = master_df['Invoice No.'] == original_invoice
                
                if original_mask.any():
                    original_idx = master_df.index[original_mask][0]
                    master_df.loc[original_idx, 'Invoice No. History'] = invoice_no
                    history_links_count += 1
                    self.log(f"Linked invoices: {original_invoice} <-> {invoice_no}")
        
        # Save updated master file
        master_df.to_excel(self.master_file_path, index=False)
        self.log(f"Add-On processing complete: {updates_count} updated, {new_records_count} new records, {history_links_count} history links created")
        
        return master_df
    
    def process_edi_file(self, edi_file_path):
        """Process EDI upload file"""
        self.log(f"=== Processing EDI File: {edi_file_path} ===")
        
        # Create backup first
        self.create_backup()
        
        # Load files
        master_df = self.load_master_file()
        edi_df = pd.read_excel(edi_file_path)
        
        # Standardize dates - including the tracking dates
        date_columns = ['Invoice From', 'Invoice To', 'Invoice Date', 'EDI Date']
        edi_df = self.standardize_dates(edi_df, date_columns)
        
        # Check for duplicates in input
        self.check_duplicates_in_input(edi_df, "EDI")
        
        # Remove duplicates, keeping first occurrence
        edi_df = edi_df.drop_duplicates(subset=['Invoice No.'], keep='first')
        
        updates_count = 0
        new_records_count = 0
        
        for _, edi_row in edi_df.iterrows():
            invoice_no = edi_row['Invoice No.']
            
            # Check if invoice exists in master
            mask = master_df['Invoice No.'] == invoice_no
            
            if mask.any():
                # Update existing record
                idx = master_df.index[mask][0]
                
                # Handle EDI Date preservation - get from input file
                input_edi_date = edi_row.get('EDI Date')
                if pd.notna(input_edi_date):
                    current_edi, original_edi = self.preserve_date_logic(
                        master_df.loc[idx], input_edi_date, 'EDI Date', 'Original EDI Date'
                    )
                else:
                    # Fallback to existing dates if input doesn't have EDI date
                    current_edi = master_df.loc[idx].get('EDI Date')
                    original_edi = master_df.loc[idx].get('Original EDI Date')
                
                # Update common columns
                for col in edi_df.columns:
                    if col in master_df.columns and col != 'Invoice No.':
                        master_df.loc[idx, col] = edi_row[col]
                
                # Set EDI dates
                master_df.loc[idx, 'EDI Date'] = current_edi
                master_df.loc[idx, 'Original EDI Date'] = original_edi
                
                updates_count += 1
                self.log(f"Updated existing record: {invoice_no}")
                
            else:
                # Create new record
                new_row = edi_row.copy()
                # Use EDI date from input file if available, otherwise use today
                if 'EDI Date' not in new_row or pd.isna(new_row.get('EDI Date')):
                    new_row['EDI Date'] = datetime.now().strftime('%Y-%m-%d')
                
                # Add to master
                master_df = pd.concat([master_df, new_row.to_frame().T], ignore_index=True)
                new_records_count += 1
                self.log(f"Created new record: {invoice_no}")
        
        # Save updated master file
        master_df.to_excel(self.master_file_path, index=False)
        self.log(f"EDI processing complete: {updates_count} updated, {new_records_count} new records")
        
        return master_df
    
    def save_processing_log(self):
        """Save processing log to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"processing_log_{timestamp}.txt"
        
        with open(log_filename, 'w') as f:
            for message in self.log_messages:
                f.write(message + '\n')
        
        print(f"Processing log saved to: {log_filename}")

# Main execution functions
def process_release_file(release_file_path):
    """Standalone function to process release file"""
    manager = InvoiceMasterManager()
    manager.process_release_file(release_file_path)
    manager.save_processing_log()

def process_addon_file(addon_file_path):
    """Standalone function to process add-on file"""
    manager = InvoiceMasterManager()
    manager.process_addon_file(addon_file_path)
    manager.save_processing_log()

def process_edi_file(edi_file_path):
    """Standalone function to process EDI file"""
    manager = InvoiceMasterManager()
    manager.process_edi_file(edi_file_path)
    manager.save_processing_log()

# Example usage
if __name__ == "__main__":
    # Configuration - update these file names as needed
    RELEASE_FILE = "weekly_release.xlsx"
    ADDON_FILE = "weekly_addons.xlsx"
    EDI_FILE = "weekly_edi.xlsx"
    
    print("Invoice Master Database Upsert System")
    print("=====================================")
    print("Available functions:")
    print("1. process_release_file('filename.xlsx')")
    print("2. process_addon_file('filename.xlsx')")
    print("3. process_edi_file('filename.xlsx')")
    print()
    print("Example usage:")
    print(f"process_release_file('{RELEASE_FILE}')")
    print(f"process_addon_file('{ADDON_FILE}')")
    print(f"process_edi_file('{EDI_FILE}')")
    print()
    print("Uncomment the lines below to run automatically:")
    
    # Uncomment these lines to run specific processes
    # process_release_file(RELEASE_FILE)
    # process_addon_file(ADDON_FILE)
    # process_edi_file(EDI_FILE)