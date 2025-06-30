"""
Data Mapping Functions - Convert between Excel/Pandas and Database formats

Key Python Concepts:
- Dictionary mapping: Convert field names between systems
- Data type conversion: Ensure database compatibility  
- Data validation: Check for required fields and valid formats
- Pandas integration: Convert DataFrames to database records
"""

import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any
import logging
from decimal import Decimal
import numpy as np

logger = logging.getLogger(__name__)

class DataMapper:
    """
    Maps data between your Excel files and database format
    This handles all the field name conversions and data type transformations
    """
    
    # FIELD MAPPINGS - Excel column names to database column names
    
    INVOICE_MAPPING = {
        # Current Excel master file columns → Database columns
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
    
    # BCI Invoice Details mapping
    BCI_DETAILS_MAPPING = {
        'Invoice Number': 'invoice_no',
        'Employee Number': 'employee_id',
        'First': 'first_name',  # We'll combine First + Last + MI → employee_name
        'Last': 'last_name',
        'MI': 'middle_initial',
        'Work Date': 'work_date',
        'Hours': 'hours_regular',
        'OT Hours': 'hours_overtime', 
        'Holiday Hours': 'hours_holiday',
        'Billing Rate': 'rate_regular',
        'OT Rate': 'rate_overtime',
        'Holiday Rate': 'rate_holiday',
        'Regular Amount': 'amount_regular',
        'OT Amount': 'amount_overtime',
        'Holiday Amount': 'amount_holiday',
        'Total Amount': 'amount_total',
        'Location Code': 'location_code',
        'Location Name': 'location_name',
        'Building Code': 'building_code',
        'EMID': 'emid',
        'Position Code': 'position_code',
        'Position Description': 'position_description',
        'Job Number': 'job_number',
        'PO': 'po',
        'Customer Number': 'customer_number',
        'Customer Name': 'customer_name',
        'Business Unit': 'business_unit'
    }
    
    # AUS Invoice Details mapping (different structure)
    AUS_DETAILS_MAPPING = {
        'Invoice Number': 'invoice_no',
        'Employee Number': 'employee_id',
        'Employee Name': 'employee_name',  # Already combined in AUS
        'Work Date': 'work_date',
        'Hours': 'hours_regular',
        'Rate': 'rate_regular',
        'Amount': 'amount_regular',
        'Job Number': 'job_number',
        'Location': 'location_name',
        'Position': 'position_description'
        # Note: AUS has fewer fields than BCI
    }
    
    # Kaiser SCR Master mapping for building dimension
    KAISER_SCR_MAPPING = {
        'Building Code': 'building_code',
        'Building Name': 'building_name', 
        'GL LOC': 'emid',  # Kaiser uses GL LOC as EMID
        'Service Area': 'mc_service_area',
        'Region': 'region',
        'Building Address': 'address'
        # We'll derive business_unit from other fields
    }
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean common pandas DataFrame issues before mapping
        
        Python Concept: Static methods don't need 'self' - they're utility functions
        """
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Strip whitespace from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].astype(str).apply(lambda x: x.str.strip())
        
        # Replace 'nan' strings with actual NaN
        df = df.replace(['nan', 'NaN', 'NULL', ''], np.nan)
        
        logger.info(f"Cleaned DataFrame: {len(df)} rows remaining")
        return df
    
    @staticmethod
    def convert_excel_date(date_value: Any) -> Optional[date]:
        """
        Convert various Excel date formats to Python date objects
        
        Python Concept: Handles multiple input types gracefully
        """
        if pd.isna(date_value) or date_value is None:
            return None
            
        try:
            # If it's already a date, return it
            if isinstance(date_value, date):
                return date_value
            
            # If it's a datetime, extract the date part
            if isinstance(date_value, datetime):
                return date_value.date()
            
            # If it's a pandas timestamp
            if isinstance(date_value, pd.Timestamp):
                return date_value.date()
            
            # If it's a string, try to parse it
            if isinstance(date_value, str):
                # Handle common formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y%m%d', '%m-%d-%Y']:
                    try:
                        return datetime.strptime(date_value, fmt).date()
                    except ValueError:
                        continue
            
            # If it's a number (Excel date serial)
            if isinstance(date_value, (int, float)):
                # Excel date serial number (days since 1900-01-01)
                excel_epoch = datetime(1900, 1, 1)
                return (excel_epoch + pd.Timedelta(days=date_value-2)).date()
            
            logger.warning(f"Could not convert date value: {date_value}")
            return None
            
        except Exception as e:
            logger.warning(f"Date conversion error for {date_value}: {e}")
            return None
    
    @staticmethod
    def convert_to_decimal(value: Any) -> Optional[Decimal]:
        """
        Convert various numeric formats to Decimal (for database precision)
        """
        if pd.isna(value) or value is None:
            return None
            
        try:
            # Clean string values
            if isinstance(value, str):
                # Remove common formatting
                value = value.replace('$', '').replace(',', '').strip()
                if value == '':
                    return None
            
            return Decimal(str(value))
            
        except Exception as e:
            logger.warning(f"Decimal conversion error for {value}: {e}")
            return None
    
    def map_invoice_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convert Excel invoice DataFrame to database format
        
        Returns list of dictionaries ready for database insertion
        """
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {}
            
            # Map each field using our mapping dictionary
            for excel_col, db_col in self.INVOICE_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    
                    # Apply appropriate conversion based on column type
                    if 'date' in db_col.lower():
                        record[db_col] = self.convert_excel_date(value)
                    elif db_col == 'invoice_total':
                        record[db_col] = self.convert_to_decimal(value)
                    elif db_col == 'not_transmitted':
                        # Convert to boolean
                        record[db_col] = bool(value) if not pd.isna(value) else False
                    else:
                        # String fields
                        record[db_col] = str(value) if not pd.isna(value) else None
            
            # Only include records with invoice numbers
            if record.get('invoice_no'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} invoice records")
        return mapped_records
    
    def map_bci_details(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convert BCI invoice details DataFrame to database format
        """
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {'source_system': 'BCI'}  # Mark as BCI data
            
            # Map basic fields
            for excel_col, db_col in self.BCI_DETAILS_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    
                    if 'date' in db_col.lower():
                        record[db_col] = self.convert_excel_date(value)
                    elif db_col in ['hours_regular', 'hours_overtime', 'hours_holiday', 
                                   'rate_regular', 'rate_overtime', 'rate_holiday',
                                   'amount_regular', 'amount_overtime', 'amount_holiday', 'amount_total']:
                        record[db_col] = self.convert_to_decimal(value)
                    else:
                        record[db_col] = str(value) if not pd.isna(value) else None
            
            # Combine name fields for BCI (First + Last + MI → employee_name)
            if any(col in row.index for col in ['First', 'Last', 'MI']):
                name_parts = []
                if 'First' in row.index and not pd.isna(row['First']):
                    name_parts.append(str(row['First']).strip())
                if 'Last' in row.index and not pd.isna(row['Last']):
                    name_parts.append(str(row['Last']).strip())
                if 'MI' in row.index and not pd.isna(row['MI']):
                    name_parts.append(str(row['MI']).strip())
                
                record['employee_name'] = ' '.join(name_parts) if name_parts else None
            
            # Calculate total hours if not provided
            if not record.get('hours_total'):
                total_hours = Decimal('0')
                for hours_col in ['hours_regular', 'hours_overtime', 'hours_holiday']:
                    if record.get(hours_col):
                        total_hours += record[hours_col]
                record['hours_total'] = total_hours if total_hours > 0 else None
            
            # Only include records with invoice numbers and employee IDs
            if record.get('invoice_no') and record.get('employee_id'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} BCI detail records")
        return mapped_records
    
    def map_aus_details(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convert AUS invoice details DataFrame to database format
        """
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {'source_system': 'AUS'}  # Mark as AUS data
            
            # Map fields using AUS mapping
            for excel_col, db_col in self.AUS_DETAILS_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    
                    if 'date' in db_col.lower():
                        record[db_col] = self.convert_excel_date(value)
                    elif db_col in ['hours_regular', 'rate_regular', 'amount_regular']:
                        record[db_col] = self.convert_to_decimal(value)
                    else:
                        record[db_col] = str(value) if not pd.isna(value) else None
            
            # For AUS, regular hours become total hours (they don't separate OT/Holiday)
            if record.get('hours_regular'):
                record['hours_total'] = record['hours_regular']
            
            # For AUS, regular amount becomes total amount
            if record.get('amount_regular'):
                record['amount_total'] = record['amount_regular']
            
            # Only include records with invoice numbers and employee IDs
            if record.get('invoice_no') and record.get('employee_id'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} AUS detail records")
        return mapped_records
    
    def map_kaiser_scr_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convert Kaiser SCR master data to building dimension format
        """
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {}
            
            # Map basic fields
            for excel_col, db_col in self.KAISER_SCR_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    record[db_col] = str(value) if not pd.isna(value) else None
            
            # Derive business unit from GL DEPT or other logic
            if 'GL DEPT' in row.index and not pd.isna(row['GL DEPT']):
                record['business_unit'] = str(row['GL DEPT'])
            
            # Only include records with building codes
            if record.get('building_code'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} Kaiser SCR building records")
        return mapped_records
    
    @staticmethod
    def dataframe_to_dict_list(df: pd.DataFrame, mapping: Dict[str, str]) -> List[Dict]:
        """
        Generic function to convert any DataFrame using a field mapping
        
        Python Concept: Generic function that can be reused for different data types
        """
        records = []
        
        for _, row in df.iterrows():
            record = {}
            for excel_col, db_col in mapping.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    record[db_col] = str(value) if not pd.isna(value) else None
            records.append(record)
        
        return records