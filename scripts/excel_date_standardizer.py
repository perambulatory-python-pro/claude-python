import pandas as pd
from datetime import datetime
import re

def standardize_dates(date_value):
    """
    Convert various date formats to ISO format (yyyy-mm-dd)
    Handles:
    - yyyymmdd format (e.g., 20250619)
    - Short date format (e.g., 6/19/2025, 06/19/2025)
    - Already formatted dates
    """
    if pd.isna(date_value):
        return date_value
    
    # Convert to string for processing
    date_str = str(date_value).strip()
    
    # Handle yyyymmdd format (8 digits)
    if re.match(r'^\d{8}$', date_str):
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
    
    # If all else fails, return original value
    return date_value

def process_excel_file(input_file, output_file, date_columns):
    """
    Process an Excel file to standardize dates in multiple columns
    
    Parameters:
    - input_file: path to input Excel file
    - output_file: path to output Excel file
    - date_columns: list of column names containing dates
    """
    try:
        # Read Excel file with multiple fallback methods
        print(f"Reading file: {input_file}")
        
        # Try different engines to handle encoding issues
        for engine in ['openpyxl', 'xlrd']:
            try:
                df = pd.read_excel(input_file, engine=engine)
                print(f"Successfully read file using {engine} engine")
                break
            except Exception as e:
                print(f"Failed with {engine} engine: {e}")
                continue
        else:
            # If both engines fail, try with error handling
            df = pd.read_excel(input_file, engine='openpyxl', encoding_errors='ignore')
            print("Read file with encoding error handling")
        
        # Check if all columns exist
        missing_columns = [col for col in date_columns if col not in df.columns]
        if missing_columns:
            print(f"Warning: These date columns were not found: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            # Continue with only the columns that exist
            date_columns = [col for col in date_columns if col in df.columns]
        
        if not date_columns:
            print("No valid date columns found to process.")
            return False
        
        # Process each date column
        for date_column in date_columns:
            print(f"Processing column: {date_column}")
            
            # Create backup of original column (as string to preserve original format)
            df[f'{date_column}_original'] = df[date_column].astype(str)
            
            # Apply date standardization
            df[date_column] = df[date_column].apply(standardize_dates)
        
        # Save to new Excel file
        df.to_excel(output_file, index=False)
        
        print(f"Successfully processed {len(df)} rows")
        print(f"Output saved to: {output_file}")
        
        # Show sample of results for all processed columns
        print("\nSample of standardized dates:")
        sample_columns = []
        for col in date_columns:
            sample_columns.extend([f'{col}_original', col])
        
        sample_df = df[sample_columns].head(5)
        print(sample_df.to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return False

# Example usage for weekly data processing
def weekly_data_processor(input_folder, date_columns):
    """
    Process weekly data files automatically
    """
    import os
    from datetime import datetime
    
    # Get today's date for output filename
    today = datetime.now().strftime('%Y%m%d')
    
    # Look for Excel files in input folder
    excel_files = [f for f in os.listdir(input_folder) if f.endswith(('.xlsx', '.xls'))]
    
    for file in excel_files:
        input_path = os.path.join(input_folder, file)
        output_filename = f"standardized_{today}_{file}"
        output_path = os.path.join(input_folder, output_filename)
        
        print(f"Processing: {file}")
        process_excel_file(input_path, output_path, date_columns)
        print("-" * 50)

# Main execution
if __name__ == "__main__":
    # Configuration
    INPUT_FILE = "weekly_release_master.xlsx"  # Input file name
    OUTPUT_FILE = "standardized_data.xlsx"  # Output file name
    DATE_COLUMNS = ["Invoice Date", "Invoice From", "Invoice To", "Release Date"]  # All date columns
    
    # Process single file
    success = process_excel_file(INPUT_FILE, OUTPUT_FILE, DATE_COLUMNS)
    
    if success:
        print("\nDate standardization completed successfully!")
    else:
        print("\nThere was an error processing the file.")
    
    # Uncomment the line below for batch processing of weekly files
    # weekly_data_processor("path/to/your/weekly/data/folder", DATE_COLUMNS)