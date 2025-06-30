"""
Security Services Data Processing Script
========================================

This script processes multiple CSV files containing security hours data,
combines them in date descending order, and validates for duplicate records.

Key Learning Concepts:
1. File handling and path management
2. Pandas DataFrame operations
3. Data validation and quality checks
4. Error handling and logging
5. Date parsing and sorting

Author: Finance Operations Team
Purpose: Process security hours data for Kaiser healthcare contract
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import logging

# Set up logging to track our script's execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('security_data_processing.log'),
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger(__name__)

class SecurityDataProcessor:
    """
    A class to handle security hours data processing.
    
    Using a class helps us organize our code and maintain state
    (like keeping track of processed files, errors, etc.)
    """
    
    def __init__(self, input_folder="data", output_folder="output"):
        """
        Initialize the processor with folder paths.
        
        Args:
            input_folder (str): Folder containing CSV files to process
            output_folder (str): Folder to save processed files and reports
        """
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.processed_files = []
        self.errors = []
        
        # Create output folder if it doesn't exist
        self.output_folder.mkdir(exist_ok=True)
        
        logger.info(f"SecurityDataProcessor initialized")
        logger.info(f"Input folder: {self.input_folder}")
        logger.info(f"Output folder: {self.output_folder}")
    
    def find_csv_files(self):
        """
        Find all CSV files in the input folder.
        
        Returns:
            list: List of Path objects for CSV files
        """
        csv_files = list(self.input_folder.glob("*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files: {[f.name for f in csv_files]}")
        return csv_files
    
    def get_date_range_from_data(self, df, filename):
        """
        Extract the actual date range from the data itself.
        
        This is much more accurate than using file modification dates.
        We'll use the latest date in each file to determine sort order.
        
        Args:
            df (DataFrame): The loaded dataframe
            filename (str): Name of the file for logging
            
        Returns:
            tuple: (latest_date, earliest_date, date_range_string)
        """
        try:
            # Convert DateClockIn to datetime
            df['DateClockIn_parsed'] = pd.to_datetime(df['DateClockIn'], format='%m-%d-%Y', errors='coerce')
            
            # Get date range from the actual data
            latest_date = df['DateClockIn_parsed'].max()
            earliest_date = df['DateClockIn_parsed'].min()
            
            # Create a readable date range string
            date_range = f"{earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
            
            logger.info(f"{filename}: Data range {date_range}")
            
            # Return latest date for sorting (newest files first)
            return latest_date, earliest_date, date_range
            
        except Exception as e:
            logger.warning(f"Could not parse dates from {filename}: {e}")
            # Return very old date as fallback so file still gets processed
            return pd.Timestamp('1900-01-01'), pd.Timestamp('1900-01-01'), "Unknown date range"
    
    def load_and_validate_csv(self, file_path):
        """
        Load a CSV file and perform basic validation.
        
        Args:
            file_path (Path): Path to the CSV file
            
        Returns:
            tuple: (DataFrame, list of validation errors)
        """
        errors = []
        
        try:
            # Load the CSV file
            logger.info(f"Loading {file_path.name}...")
            df = pd.read_csv(file_path)
            
            # Basic validation checks
            if df.empty:
                errors.append(f"File {file_path.name} is empty")
                return None, errors
            
            # Check for required columns
            required_columns = ['ShiftId', 'DateClockIn', 'DateClockOut']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                errors.append(f"Missing required columns in {file_path.name}: {missing_columns}")
                return None, errors
            
            # Add source file column for tracking
            df['SourceFile'] = file_path.name
            
            logger.info(f"Successfully loaded {file_path.name}: {len(df)} records")
            return df, errors
            
        except Exception as e:
            error_msg = f"Error loading {file_path.name}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            return None, errors
    
    def check_duplicates(self, df):
        """
        Check for duplicate ShiftIds in the dataset.
        
        Args:
            df (DataFrame): Combined dataframe to check
            
        Returns:
            tuple: (bool has_duplicates, DataFrame duplicate_records)
        """
        logger.info("Checking for duplicate ShiftIds...")
        
        # Find duplicated ShiftIds
        duplicate_mask = df.duplicated(subset=['ShiftId'], keep=False)
        duplicate_records = df[duplicate_mask].copy()
        
        if not duplicate_records.empty:
            logger.warning(f"Found {len(duplicate_records)} duplicate records")
            
            # Sort duplicates by ShiftId for easier review
            duplicate_records = duplicate_records.sort_values('ShiftId')
            
            # Add a column showing which files the duplicates came from
            duplicate_summary = duplicate_records.groupby('ShiftId')['SourceFile'].apply(list).reset_index()
            duplicate_summary.columns = ['ShiftId', 'Files_Containing_Duplicate']
            
            logger.info(f"Duplicate ShiftIds found in: {duplicate_summary['Files_Containing_Duplicate'].tolist()}")
            
            return True, duplicate_records
        else:
            logger.info("No duplicate ShiftIds found - data quality check passed!")
            return False, pd.DataFrame()
    
    def combine_files_by_date(self, csv_files):
        """
        Combine multiple CSV files, sorted by actual data dates (newest first).
        
        Args:
            csv_files (list): List of Path objects for CSV files
            
        Returns:
            DataFrame: Combined dataframe sorted by date descending
        """
        all_dataframes = []
        file_info = []
        
        # Load each file and collect info for sorting
        for file_path in csv_files:
            df, errors = self.load_and_validate_csv(file_path)
            
            if df is not None:
                # Get actual date range from the data
                latest_date, earliest_date, date_range = self.get_date_range_from_data(df, file_path.name)
                
                file_info.append({
                    'file_path': file_path,
                    'dataframe': df,
                    'latest_date': latest_date,
                    'earliest_date': earliest_date,
                    'date_range': date_range,
                    'record_count': len(df)
                })
                self.processed_files.append(file_path.name)
            
            # Store any errors
            self.errors.extend(errors)
        
        if not file_info:
            logger.error("No valid files could be processed!")
            return pd.DataFrame()
        
        # Sort files by their latest date (newest first)
        file_info.sort(key=lambda x: x['latest_date'], reverse=True)
        
        logger.info("File processing order (newest data first):")
        for i, info in enumerate(file_info, 1):
            logger.info(f"  {i}. {info['file_path'].name}")
            logger.info(f"     Date range: {info['date_range']}")
            logger.info(f"     Records: {info['record_count']:,}")
        
        # Combine dataframes in the sorted order
        all_dataframes = [info['dataframe'] for info in file_info]
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        logger.info(f"Combined {len(file_info)} files into {len(combined_df):,} total records")
        
        return combined_df
    
    def save_results(self, combined_df, duplicate_records=None):
        """
        Save the processed results to files.
        
        Args:
            combined_df (DataFrame): Combined dataframe
            duplicate_records (DataFrame): Any duplicate records found
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save the combined dataset
        combined_filename = f"combined_security_hours_{timestamp}.csv"
        combined_path = self.output_folder / combined_filename
        combined_df.to_csv(combined_path, index=False)
        logger.info(f"Saved combined data to {combined_path}")
        
        # Save duplicate records if any exist
        if duplicate_records is not None and not duplicate_records.empty:
            duplicates_filename = f"duplicate_records_{timestamp}.csv"
            duplicates_path = self.output_folder / duplicates_filename
            duplicate_records.to_csv(duplicates_path, index=False)
            logger.info(f"Saved duplicate records to {duplicates_path}")
        
        # Save processing summary
        self.save_processing_summary(combined_df, duplicate_records, timestamp)
    
    def save_processing_summary(self, combined_df, duplicate_records, timestamp):
        """
        Create and save a summary report of the processing.
        
        Args:
            combined_df (DataFrame): Combined dataframe
            duplicate_records (DataFrame): Any duplicate records found
            timestamp (str): Timestamp for the report
        """
        summary_lines = [
            "SECURITY HOURS DATA PROCESSING SUMMARY",
            "=" * 50,
            f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "FILES PROCESSED:",
            "-" * 20
        ]
        
        for i, filename in enumerate(self.processed_files, 1):
            summary_lines.append(f"{i}. {filename}")
        
        summary_lines.extend([
            "",
            "RESULTS:",
            "-" * 20,
            f"Total records processed: {len(combined_df):,}",
            f"Unique ShiftIds: {combined_df['ShiftId'].nunique():,}",
        ])
        
        # Add date range info if we have DateClockIn data
        if 'DateClockIn' in combined_df.columns:
            try:
                combined_df['DateClockIn_parsed'] = pd.to_datetime(combined_df['DateClockIn'], format='%m-%d-%Y', errors='coerce')
                min_date = combined_df['DateClockIn_parsed'].min().strftime('%Y-%m-%d')
                max_date = combined_df['DateClockIn_parsed'].max().strftime('%Y-%m-%d')
                summary_lines.append(f"Date range: {min_date} to {max_date}")
            except:
                summary_lines.append(f"Date range: {combined_df['DateClockIn'].min()} to {combined_df['DateClockIn'].max()}")
        else:
            summary_lines.append("Date range: Could not determine")
        
        if duplicate_records is not None and not duplicate_records.empty:
            duplicate_shift_ids = duplicate_records['ShiftId'].nunique()
            summary_lines.extend([
                "",
                "⚠ DUPLICATES FOUND:",
                "-" * 20,
                f"Duplicate ShiftIds: {duplicate_shift_ids}",
                f"Total duplicate records: {len(duplicate_records)}",
                "Please review the duplicate_records_*.csv file"
            ])
        else:
            summary_lines.extend([
                "",
                "✓ NO DUPLICATES FOUND",
                "Data quality check passed!"
            ])
        
        if self.errors:
            summary_lines.extend([
                "",
                "ERRORS ENCOUNTERED:",
                "-" * 20
            ])
            for error in self.errors:
                summary_lines.append(f"✗ {error}")
        
        # Save summary to file
        summary_filename = f"processing_summary_{timestamp}.txt"
        summary_path = self.output_folder / summary_filename
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(summary_lines))
        
        # Also print summary to console
        logger.info("\n" + '\n'.join(summary_lines))
    
    def process_all_files(self):
        """
        Main method to process all CSV files in the input folder.
        
        This orchestrates the entire workflow:
        1. Find CSV files
        2. Combine them in date order
        3. Check for duplicates
        4. Save results
        """
        logger.info("Starting security hours data processing...")
        
        # Step 1: Find all CSV files
        csv_files = self.find_csv_files()
        if not csv_files:
            logger.error(f"No CSV files found in {self.input_folder}")
            return
        
        # Step 2: Combine files in date descending order
        combined_df = self.combine_files_by_date(csv_files)
        if combined_df.empty:
            logger.error("No data could be processed from the files")
            return
        
        # Step 3: Check for duplicates
        has_duplicates, duplicate_records = self.check_duplicates(combined_df)
        
        # Step 4: Save results
        self.save_results(
            combined_df, 
            duplicate_records if has_duplicates else None
        )
        
        logger.info("Processing completed successfully!")

# Main execution
if __name__ == "__main__":
    """
    This is the main entry point of our script.
    It only runs when the script is executed directly (not imported).
    """
    
    print("Security Hours Data Processor")
    print("=" * 40)
    print("This script will:")
    print("1. Find all CSV files in the 'data' folder")
    print("2. Combine them in date descending order")
    print("3. Check for duplicate ShiftIds")
    print("4. Save results and generate reports")
    print()
    
    # Create the processor and run it
    processor = SecurityDataProcessor(
        input_folder="data",    # Put your CSV files here
        output_folder="output"  # Results will be saved here
    )
    
    processor.process_all_files()