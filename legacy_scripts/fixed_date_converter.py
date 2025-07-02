"""
Fixed Date Converter for Enhanced Data Mapper
Handles multiple date formats including ISO timestamps

Python Learning Point: Robust date parsing needs to handle multiple input formats
The key is to try formats from most specific to least specific
"""

import pandas as pd
from datetime import datetime, date
from typing import Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class FixedDateConverter:
    """
    Robust date converter that handles multiple date formats
    """
    
    @staticmethod
    def convert_excel_date_robust(date_value: Any) -> Optional[date]:
        """
        Convert various date formats to Python date object
        
        Handles these formats:
        - ISO timestamps: "2025-01-01 00:00:00"
        - ISO dates: "2025-01-01"
        - Excel numeric dates: 45657
        - Short dates: "1/1/2025"
        - YearMonthDay: "20250101"
        - Already date objects
        
        Python Concept: Try/except with multiple format attempts
        """
        if pd.isna(date_value) or date_value is None or date_value == '':
            return None
        
        # If already a date object, return it
        if isinstance(date_value, date):
            return date_value
        
        # If it's a datetime object, extract the date
        if isinstance(date_value, datetime):
            return date_value.date()
        
        # Convert to string for processing
        date_str = str(date_value).strip()
        
        # Handle empty or 'nan' strings
        if not date_str or date_str.lower() in ['nan', 'none', 'null']:
            return None
        
        # Try different date formats in order of likelihood
        date_formats = [
            # ISO formats (most common in your data)
            '%Y-%m-%d %H:%M:%S',    # "2025-01-01 00:00:00"
            '%Y-%m-%d',             # "2025-01-01"
            
            # US formats
            '%m/%d/%Y',             # "1/1/2025"
            '%m/%d/%y',             # "1/1/25"
            
            # Compact format
            '%Y%m%d',               # "20250101"
            
            # European formats
            '%d/%m/%Y',             # "1/1/2025" (day first)
            '%d-%m-%Y',             # "1-1-2025"
            
            # Other ISO variants
            '%Y-%m-%dT%H:%M:%S',    # "2025-01-01T00:00:00"
            '%Y/%m/%d',             # "2025/1/1"
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                return parsed_date
            except ValueError:
                continue
        
        # Try pandas to_datetime as fallback (handles many formats)
        try:
            parsed_date = pd.to_datetime(date_str).date()
            return parsed_date
        except (ValueError, TypeError):
            pass
        
        # If it's a number, try treating as Excel serial date
        try:
            # Convert to float first
            numeric_value = float(date_str)
            
            # Excel dates start from 1900-01-01 (serial number 1)
            # But Excel incorrectly treats 1900 as a leap year, so we adjust
            if numeric_value >= 1:
                # Excel epoch is 1899-12-30 (not 1900-01-01)
                excel_epoch = datetime(1899, 12, 30)
                parsed_date = excel_epoch + pd.Timedelta(days=numeric_value)
                return parsed_date.date()
        except (ValueError, TypeError):
            pass
        
        # If all else fails, log the issue and return None
        logger.warning(f"Could not parse date value: '{date_value}' (type: {type(date_value)})")
        return None

# Function to patch your existing EnhancedDataMapper
def patch_enhanced_data_mapper():
    """
    Patch the existing EnhancedDataMapper with the robust date converter
    
    Usage:
    from fixed_date_converter import patch_enhanced_data_mapper
    patch_enhanced_data_mapper()
    """
    try:
        from invoice_processing.core.data_mapper_enhanced import EnhancedDataMapper
        
        # Replace the convert_excel_date method
        EnhancedDataMapper.convert_excel_date = staticmethod(FixedDateConverter.convert_excel_date_robust)
        
        print("✅ Successfully patched EnhancedDataMapper with robust date converter")
        return True
        
    except ImportError as e:
        print(f"❌ Could not import EnhancedDataMapper: {e}")
        return False
    except Exception as e:
        print(f"❌ Error patching EnhancedDataMapper: {e}")
        return False

# Enhanced Data Mapper with Fixed Date Conversion
class EnhancedDataMapperFixed:
    """
    Complete Enhanced Data Mapper with fixed date conversion
    Use this as a drop-in replacement for your existing mapper
    """
    
    # All your existing mappings (copied from your original)
    INVOICE_MAPPING = {
        'Invoice No.': 'invoice_no',
        'EMID': 'emid',
        'NUID': 'nuid', 
        'SERVICE REQ\'D BY': 'service_reqd_by',
        'Service Area': 'service_area',
        'Post Name': 'post_name',
        'Chartfield': 'chartfield',
        'Invoice From': 'invoice_from',
        'Invoice To': 'invoice_to', 
        'Invoice Date': 'invoice_date',
        'EDI Date': 'edi_date',
        'Release Date': 'release_date',
        'Add-On Date': 'add_on_date',
        'Invoice Total': 'invoice_total',
        'Not Transmitted': 'not_transmitted',
        'Original EDI Date': 'original_edi_date',
        'Original Add-On Date': 'original_add_on_date',
        'Original Release Date': 'original_release_date',
        'Invoice No. History': 'invoice_no_history',
        'Notes': 'notes'
    }
    
    BCI_DETAILS_MAPPING = {
        'Invoice_No': 'invoice_no',
        'Emp_No': 'employee_id',
        'Date': 'work_date',
        'Billed_Regular_Hours': 'hours_regular',
        'Billed_OT_Hours': 'hours_overtime',
        'Billed_Holiday_Hours': 'hours_holiday',
        'Billed_Total_Hours': 'hours_total',
        'Billed_Regular_Rate': 'rate_regular',
        'Billed_OT_Rate': 'rate_overtime',
        'Billed_Holiday_Rate': 'rate_holiday',
        'Billed_Regular_Wages': 'amount_regular',
        'Billed_OT_Wages': 'amount_overtime',
        'Billed_Holiday_Wages': 'amount_holiday',
        'Billed_Total_Wages': 'amount_total',
        'Location_Number': 'location_code',
        'Location': 'location_name',
        'Position_Number': 'position_code',
        'Position': 'position_description',
        'Customer_Number': 'customer_number',
        'Customer': 'customer_name',
        'Business_Unit': 'business_unit',
        'Shift_In': 'shift_in',
        'Shift_Out': 'shift_out',
        'Billing_Code': 'bill_category'
    }
    
    AUS_DETAILS_MAPPING = {
        'Invoice Number': 'invoice_no',
        'Employee Number': 'employee_id',
        'Employee Name': 'employee_name',
        'Work Date': 'work_date',
        'Hours': 'hours_regular',
        'Rate': 'rate_regular',
        'Amount': 'amount_regular',
        'Job Number': 'job_number',
        'Location': 'location_name',
        'Position': 'position_description'
    }
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Clean common pandas DataFrame issues before mapping"""
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Strip whitespace from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].astype(str).apply(lambda x: x.str.strip())
        
        # Replace 'nan' strings with actual NaN
        df = df.replace('nan', pd.NA)
        
        return df
    
    def convert_excel_date(self, date_value: Any) -> Optional[date]:
        """Use the robust date converter"""
        return FixedDateConverter.convert_excel_date_robust(date_value)
    
    def convert_to_decimal(self, value: Any):
        """Convert value to decimal, handling various formats"""
        try:
            from decimal import Decimal
            if pd.isna(value) or value is None:
                return Decimal('0')
            
            # Handle string values with commas
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if not value:
                    return Decimal('0')
            
            return Decimal(str(value))
        except:
            return Decimal('0')
    
    def map_aus_details(self, df: pd.DataFrame):
        """Map AUS details with fixed date conversion"""
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {'source_system': 'AUS'}
            
            # Map fields using AUS mapping
            for excel_col, db_col in self.AUS_DETAILS_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    
                    if 'date' in db_col.lower():
                        record[db_col] = self.convert_excel_date(value)  # ✅ FIXED DATE CONVERSION
                    elif db_col in ['hours_regular', 'rate_regular', 'amount_regular']:
                        record[db_col] = self.convert_to_decimal(value)
                    else:
                        record[db_col] = str(value) if not pd.isna(value) else None
            
            # For AUS, regular hours become total hours
            if record.get('hours_regular'):
                record['hours_total'] = record['hours_regular']
            
            # For AUS, regular amount becomes total amount
            if record.get('amount_regular'):
                record['amount_total'] = record['amount_regular']
            
            # Only include records with invoice numbers and employee IDs
            if record.get('invoice_no') and record.get('employee_id'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} AUS detail records with fixed date conversion")
        return mapped_records
    
    def map_bci_details(self, df: pd.DataFrame):
        """Map BCI details with fixed date conversion"""
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {'source_system': 'BCI'}
            
            # Map basic fields
            for excel_col, db_col in self.BCI_DETAILS_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    
                    if 'date' in db_col.lower():
                        record[db_col] = self.convert_excel_date(value)  # ✅ FIXED DATE CONVERSION
                    elif db_col in ['hours_regular', 'hours_overtime', 'hours_holiday', 
                                   'rate_regular', 'rate_overtime', 'rate_holiday',
                                   'amount_regular', 'amount_overtime', 'amount_holiday', 'amount_total']:
                        record[db_col] = self.convert_to_decimal(value)
                    else:
                        record[db_col] = str(value) if not pd.isna(value) else None
            
            # Build employee name from individual fields
            name_parts = []
            for name_field in ['Employee_First_Name', 'Employee_Last_Name', 'Employee_MI']:
                if name_field in row.index and not pd.isna(row[name_field]):
                    name_parts.append(str(row[name_field]).strip())
            
            if name_parts:
                record['employee_name'] = ' '.join(name_parts)
            
            # Calculate totals if needed
            if not record.get('hours_total'):
                total_hours = 0
                for hours_col in ['hours_regular', 'hours_overtime', 'hours_holiday']:
                    if record.get(hours_col):
                        total_hours += float(record[hours_col])
                record['hours_total'] = self.convert_to_decimal(total_hours) if total_hours > 0 else None
            
            # Only include records with invoice numbers and employee IDs
            if record.get('invoice_no') and record.get('employee_id'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} BCI detail records with fixed date conversion")
        return mapped_records

# Test function
def test_date_conversion():
    """Test the fixed date conversion with your problematic values"""
    converter = FixedDateConverter()
    
    test_dates = [
        "2025-01-01 00:00:00",
        "2024-12-06 00:00:00", 
        "2025-01-01",
        "1/1/2025",
        "20250101",
        45657,  # Excel serial date
        None,
        ""
    ]
    
    print("Testing date conversion:")
    for test_date in test_dates:
        result = converter.convert_excel_date_robust(test_date)
        print(f"  {test_date} → {result}")
    
    return True

if __name__ == "__main__":
    test_date_conversion()
