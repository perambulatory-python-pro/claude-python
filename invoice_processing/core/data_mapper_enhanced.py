"""
Enhanced Data Mapping Functions
Now includes proper handling for processing dates and business logic
Updated with new invoice_details column mappings and intelligent name parsing

Key Python Concepts:
- Function overloading: Different methods for different processing types
- Date injection: Adding processing dates to mapped data
- Data enrichment: Enhancing data with business context
- Flexible mapping: Handle files with or without date columns
- Intelligent parsing: Smart detection of company names vs individual names
"""

from turtle import st
import pandas as pd
from datetime import datetime, date, timedelta 
from typing import Dict, List, Optional, Union, Any, Tuple
from sqlalchemy import text
import logging
from decimal import Decimal
import numpy as np
import re
from bs4 import BeautifulSoup
import io

# Ensure pandas does not silently downcast types in future versions

pd.set_option('future.no_silent_downcasting', True)

logger = logging.getLogger(__name__)

class EnhancedDataMapper:
    """
    Enhanced data mapper that handles processing dates and business logic
    """
    
    def __init__(self):
        """Initialize the mapper with column mappings and patterns"""
        
        # Keep all existing field mappings from original DataMapper
        self.INVOICE_MAPPING = {
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
        
        # Updated BCI mapping based on new structure
        self.BCI_DETAILS_MAPPING = {
            'Business_Unit': 'business_unit',
            'Customer': 'customer_name',
            'Customer_Number': 'customer_number',
            'Customer_Number_Ext': 'customer_number_ext',
            'Address1': 'customer_ext_description',
            'Location': 'location_name',
            'Location_Number': 'location_code',
            'Position': 'position_description',
            'Position_Number': 'position_code',
            'Date': 'work_date',
            'Shift_In': 'shift_in',
            'Shift_Out': 'shift_out',
            'Emp_No': 'employee_id',
            'Employee_Last_Name': 'employee_name_last',
            'Employee_First_Name': 'employee_name_first',
            'Employee_MI': 'employee_middle_initial',
            'Billed_Regular_Hours': 'hours_regular',
            'Billed_Regular_Rate': 'rate_regular',
            'Billed_Regular_Wages': 'amount_regular',
            'Billed_OT_Hours': 'hours_overtime',
            'Billed_OT_Rate': 'rate_overtime',
            'Billed_OT_Wages': 'amount_overtime',
            'Billed_Holiday_Hours': 'hours_holiday',
            'Billed_Holiday_Rate': 'rate_holiday',
            'Billed_Holiday_Wages': 'amount_holiday',
            'Billed_Total_Hours': 'hours_total',
            'Billed_Total_Wages': 'amount_total',
            'Billing_Code': 'bill_category',
            'Weekending_Date': 'week_ending',
            'Invoice_No': 'invoice_no',
        }
        
        # Updated AUS mapping - note the removal of direct employee_name mapping
        self.AUS_DETAILS_MAPPING = {
            'Invoice Number': 'invoice_no',
            'Customer Number': 'customer_number',
            'Job Number': 'location_code',
            'Job Description': 'location_name',
            'Week Ending': 'week_ending',
            'Post Description': 'customer_ext_description',
            'Employee Number': 'employee_id',
            'Employee Name': '_temp_employee_name',  # Temporary for parsing
            'In Time': 'shift_in',
            'Out Time': 'shift_out',
            'Pay Hours Description': 'position_code',
            'Hours': 'hours_regular',
            'Bill Hours Description': 'position_description',
            'Bill Hours Qty': 'hours_total',
            'Bill Rate': 'rate_regular',
            'Bill Amount': 'amount_total',
            'Bill Cat Number': 'bill_category'
        }
        
        # Kaiser SCR Master mapping for building dimension
        self.KAISER_SCR_MAPPING = {
            'Building Code': 'building_code',
            'Building Name': 'building_name', 
            'GL LOC': 'emid',
            'Service Area': 'mc_service_area',
            'Region': 'region',
            'Building Address': 'address'
        }
        
        # Kaiser payment remittance details
        self.PAYMENT_MAPPING = {
            'Payment ID': 'payment_id',
            'Payment Date': 'payment_date', 
            'Payment Amount': 'payment_amount',
            'Invoice ID': 'invoice_no',
            'Invoice Date': 'invoice_date',
            'Gross Amount': 'gross_amount',
            'Discount': 'discount',
            'Net Amount': 'net_amount',
            'Payment Message': 'payment_message',
            'Vendor Name': 'vendor_name'
        }
        # Known company/service provider patterns
        self.COMPANY_PATTERNS = [
            r'LLC$', r'LLC\.?$', r'Llc$',
            r'INC$', r'INC\.?$', r'Inc$', r'Inc\.$',
            r'GROUP$', r'Group$',
            r'SERVICES?$', r'Services?$',
            r'CONSULTING$', r'Consulting$',
            r'CORPORATION$', r'Corporation$', r'Corp\.?$',
            r'COMPANY$', r'Company$', r'Co\.?$',
            r'ASSOCIATES?$', r'Associates?$',
            r'PARTNERS?$', r'Partners?$',
            r'SOLUTIONS?$', r'Solutions?$',
            r'STAFFING$', r'Staffing$',
            r'SECURITY$', r'Security$',
            # Specific known companies from your data
            r'^Skye Consulting Group',
            r'Special Buyback',
            r'Vacation.*Buyback',
            r'credit hours'
        ]
        
        # Compile regex patterns for efficiency
        self.company_regex = re.compile('|'.join(self.COMPANY_PATTERNS), re.IGNORECASE)
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Clean common pandas DataFrame issues before mapping"""
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Strip whitespace from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].astype(str).apply(lambda x: x.str.strip())
        
        # Replace 'nan' strings with actual NaN
        df = df.replace(['nan', 'NaN', 'NULL', ''], np.nan).infer_objects(copy=False)
        
        logger.info(f"Cleaned DataFrame: {len(df)} rows remaining")
        return df
    
    @staticmethod
    # Update your convert_excel_date method in EnhancedDataMapper class

    def convert_excel_date(self, excel_date):
        """
        Convert Excel date to string format YYYY-MM-DD
        Returns string or None, never a date object
        """
        try:
            if pd.isna(excel_date) or excel_date is None:
                return None
            
            # If it's already a string in the right format, return it
            if isinstance(excel_date, str):
                # Try to parse it to validate and standardize
                parsed = self.parse_date(excel_date)
                return parsed  # parse_date already returns string or None
            
            # If it's a datetime or date object
            if hasattr(excel_date, 'strftime'):
                return excel_date.strftime('%Y-%m-%d')
            
            # If it's a pandas Timestamp
            if hasattr(excel_date, 'date'):
                return excel_date.date().strftime('%Y-%m-%d')
            
            # If it's a number (Excel serial date)
            if isinstance(excel_date, (int, float)):
                # Excel dates start from 1900-01-01
                from datetime import datetime, timedelta
                
                # Handle the Excel date serial number
                if excel_date > 59:  # Excel incorrectly treats 1900 as leap year
                    excel_date -= 1
                
                # Convert to datetime and return as string
                base_date = datetime(1900, 1, 1)
                result_date = base_date + timedelta(days=int(excel_date) - 2)
                return result_date.strftime('%Y-%m-%d')
            
            return None
            
        except Exception as e:
            logger.warning(f"Error converting Excel date '{excel_date}': {e}")
            return None
    
    @staticmethod
    def convert_to_decimal(value: Any) -> Optional[Decimal]:
        """Convert various numeric formats to Decimal"""
        if pd.isna(value) or value is None:
            return None
            
        try:
            if isinstance(value, str):
                value = value.replace('$', '').replace(',', '').strip()
                if value == '':
                    return None
            
            return Decimal(str(value))
            
        except Exception as e:
            logger.warning(f"Decimal conversion error for {value}: {e}")
            return None
    
    def is_company_name(self, name: str) -> bool:
        """
        Detect if a name is likely a company/service provider
        """
        if pd.isna(name) or not name:
            return False
            
        name_str = str(name).strip()
        
        # Check against known patterns
        if self.company_regex.search(name_str):
            return True
            
        # Check for all caps with spaces (often company names)
        words = name_str.split()
        if len(words) > 2 and all(word.isupper() for word in words if len(word) > 2):
            return True
            
        # Check for special characters that indicate non-person entries
        if any(char in name_str for char in ['(', ')', '-credit', '@']):
            return True
            
        return False
    
    def parse_aus_employee_name(self, name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Parse AUS employee name into last, first, middle initial
        Returns: (last_name, first_name, middle_initial)
        """
        if pd.isna(name) or not name:
            return (None, None, None)
            
        name_str = str(name).strip()
        
        # Check if it's a company name - if so, put entire string in last_name
        if self.is_company_name(name_str):
            return (name_str, None, None)
        
        # Handle special formatting cases
        # Remove any Jr., Sr., III, etc. suffixes temporarily
        suffix_pattern = r'\s+(Jr\.?|Sr\.?|III|II|IV)$'
        suffix_match = re.search(suffix_pattern, name_str, re.IGNORECASE)
        suffix = ''
        if suffix_match:
            suffix = suffix_match.group(1)
            name_str = name_str[:suffix_match.start()]
        
        # Try different parsing patterns
        
        # Pattern 1: "LAST, FIRST [MI]" (most common)
        if ',' in name_str:
            parts = name_str.split(',', 1)
            last_name = parts[0].strip()
            
            if len(parts) > 1:
                remaining = parts[1].strip()
                # Split the remaining part by spaces
                name_parts = remaining.split()
                
                if len(name_parts) >= 2:
                    # Check if the last part is a middle initial (single letter or letter with period)
                    if len(name_parts[-1]) <= 2 and name_parts[-1].replace('.', '').isalpha():
                        first_name = ' '.join(name_parts[:-1])
                        middle_initial = name_parts[-1].replace('.', '')
                    else:
                        first_name = remaining
                        middle_initial = None
                else:
                    first_name = remaining
                    middle_initial = None
            else:
                first_name = None
                middle_initial = None
                
            # Re-append suffix to last name if it exists
            if suffix:
                last_name = f"{last_name} {suffix}"
                
            return (last_name, first_name, middle_initial)
        
        # Pattern 2: "FIRST LAST" (no comma)
        else:
            name_parts = name_str.split()
            
            if len(name_parts) == 0:
                return (None, None, None)
            elif len(name_parts) == 1:
                # Single name - assume it's last name
                return (name_parts[0], None, None)
            elif len(name_parts) == 2:
                # Two parts - assume "FIRST LAST"
                return (name_parts[1], name_parts[0], None)
            else:
                # Three or more parts
                # Check if last part is middle initial
                if len(name_parts[-1]) <= 2 and name_parts[-1].replace('.', '').isalpha():
                    # Pattern: "FIRST [MIDDLE NAMES] LAST MI"
                    return (name_parts[-2], ' '.join(name_parts[:-2]), name_parts[-1].replace('.', ''))
                else:
                    # Pattern: "FIRST [MIDDLE NAMES] LAST"
                    # Take first word as first name, rest as last name
                    return (' '.join(name_parts[1:]), name_parts[0], None)
    
    def map_invoice_data_with_processing_info(self, df: pd.DataFrame, 
                                             processing_type: str = None, 
                                             processing_date: date = None) -> List[Dict]:
        """
        Enhanced invoice mapping that handles processing dates
        
        Args:
            df: Input DataFrame
            processing_type: 'EDI', 'Release', or 'Add-On' (optional)
            processing_date: Date to use for processing (optional)
            
        Returns:
            List of dictionaries ready for database insertion
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
            
            # Handle special fields for Add-On processing
            if 'Original invoice #' in row.index and not pd.isna(row['Original invoice #']):
                record['original_invoice_no'] = str(row['Original invoice #'])
            
            # Only include records with invoice numbers
            if record.get('invoice_no'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} invoice records for {processing_type or 'general'} processing")
        return mapped_records
    
    # Convenience method that maintains backward compatibility
    def map_invoice_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Standard invoice mapping (maintains backward compatibility)
        """
        return self.map_invoice_data_with_processing_info(df)
    
    def map_bci_details(self, df: pd.DataFrame) -> List[Dict]:
        """Map BCI invoice details to database format with new column structure"""
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {'source_system': 'BCI'}
            
            # Map fields using BCI mapping
            for excel_col, db_col in self.BCI_DETAILS_MAPPING.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    
                    if db_col in ['work_date', 'week_ending']:
                        record[db_col] = self.convert_excel_date(value)
                    elif db_col in ['hours_regular', 'hours_overtime', 'hours_holiday', 'hours_total',
                                   'rate_regular', 'rate_overtime', 'rate_holiday',
                                   'amount_regular', 'amount_overtime', 'amount_holiday', 'amount_total']:
                        record[db_col] = self.convert_to_decimal(value)
                    elif db_col in ['shift_in', 'shift_out']:
                        record[db_col] = str(value) if pd.notna(value) else None
                    else:
                        record[db_col] = str(value).strip() if pd.notna(value) else None
            
            # Calculate totals if not provided
            if not record.get('hours_total') or record['hours_total'] == 0:
                total_hours = Decimal('0')
                for hours_col in ['hours_regular', 'hours_overtime', 'hours_holiday']:
                    if record.get(hours_col):
                        total_hours += record[hours_col]
                record['hours_total'] = total_hours if total_hours > 0 else None
            
            if not record.get('amount_total') or record['amount_total'] == 0:
                total_amount = Decimal('0')
                for amount_col in ['amount_regular', 'amount_overtime', 'amount_holiday']:
                    if record.get(amount_col):
                        total_amount += record[amount_col]
                record['amount_total'] = total_amount if total_amount > 0 else None
            
            # Only include records with invoice numbers:
            if record.get('invoice_no'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} BCI detail records")
        return mapped_records
    
    def map_aus_details(self, df: pd.DataFrame) -> List[Dict]:
        """Map AUS invoice details to database format with intelligent name parsing"""
        df = self.clean_dataframe(df.copy())
        mapped_records = []
        
        for _, row in df.iterrows():
            record = {'source_system': 'AUS'}
            
            # First pass - map all fields except employee name
            for excel_col, db_col in self.AUS_DETAILS_MAPPING.items():
                if excel_col in row.index and db_col != '_temp_employee_name':
                    value = row[excel_col]
                    
                    if db_col in ['work_date', 'week_ending']:
                        record[db_col] = self.convert_excel_date(value)
                    elif db_col in ['hours_regular', 'hours_total', 'rate_regular', 'amount_total']:
                        record[db_col] = self.convert_to_decimal(value)
                    elif db_col in ['shift_in', 'shift_out']:
                        record[db_col] = str(value) if pd.notna(value) else None
                    else:
                        record[db_col] = str(value).strip() if pd.notna(value) else None
            
            # Parse employee name
            if 'Employee Name' in row.index:
                last_name, first_name, middle_initial = self.parse_aus_employee_name(row['Employee Name'])
                record['employee_name_last'] = last_name
                record['employee_name_first'] = first_name
                record['employee_middle_initial'] = middle_initial
            else:
                record['employee_name_last'] = None
                record['employee_name_first'] = None
                record['employee_middle_initial'] = None
            
            # AUS typically only has regular hours, so set OT and holiday to 0
            record['hours_overtime'] = Decimal('0')
            record['hours_holiday'] = Decimal('0')
            record['rate_overtime'] = None
            record['rate_holiday'] = None
            record['amount_overtime'] = Decimal('0')
            record['amount_holiday'] = Decimal('0')
            
            # Use hours_regular as hours_total if not provided
            if not record.get('hours_total'):
                record['hours_total'] = record.get('hours_regular')
            
            # Only include records with invoice numbers
            if record.get('invoice_no'):
                mapped_records.append(record)
        
        logger.info(f"Mapped {len(mapped_records)} AUS detail records")
        return mapped_records
    
    def map_kaiser_scr_data(self, df: pd.DataFrame) -> List[Dict]:
        """Convert Kaiser SCR master data to building dimension format"""
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
    
    
    def extract_payment_master_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extract master payment record(s) from DataFrame
        Now handles multiple payment IDs in a single file
        Returns a dictionary with either single payment data or multiple payments
        """
        try:
            # Clean the dataframe first
            df = self.clean_dataframe(df.copy())
            
            # Get unique payment IDs
            unique_payment_ids = df['Payment ID'].dropna().unique()
            
            # Format payment IDs to preserve leading zeros
            formatted_payment_ids = []
            for pid in unique_payment_ids:
                if pd.isna(pid):
                    continue
                elif isinstance(pid, (int, float)):
                    formatted_pid = str(int(pid)).zfill(10)
                else:
                    formatted_pid = str(pid).strip()
                formatted_payment_ids.append(formatted_pid)
            
            logger.info(f"Found {len(formatted_payment_ids)} unique payment ID(s): {formatted_payment_ids}")
            
            # If multiple payment IDs found, return indicator for special handling
            if len(formatted_payment_ids) > 1:
                return {
                    'multiple_payments': True,
                    'payment_ids': formatted_payment_ids,
                    'dataframe': df  # Pass the dataframe for further processing
                }
            
            # Single payment ID - process normally
            first_row = df.iloc[0]
            payment_id = formatted_payment_ids[0] if formatted_payment_ids else ''
            
            # Extract payment date
            payment_date = self.convert_excel_date(first_row.get('Payment Date'))
            
            # Calculate total payment amount
            calculated_total = df['Net Amount'].sum() if 'Net Amount' in df.columns else 0
            stated_amount = self.convert_to_decimal(first_row.get('Payment Amount', 0))
            
            master_data = {
                'multiple_payments': False,
                'payment_id': payment_id,
                'payment_date': payment_date,
                'payment_amount': float(stated_amount) if stated_amount else 0.0,
                'calculated_amount': float(calculated_total),
                'vendor_name': str(first_row.get('Vendor Name', '')).strip() if 'Vendor Name' in df.columns else None
            }
            
            # Standardize the data
            master_data = self.standardize_payment_data(master_data)
            
            logger.info(f"Extracted single payment: ID={payment_id}, Amount=${master_data['payment_amount']:,.2f}")
            return master_data
            
        except Exception as e:
            logger.error(f"Error extracting payment master data: {e}")
            raise

    def process_multiple_payments(self, df: pd.DataFrame, payment_ids: list) -> list:
        """
        Process a DataFrame containing multiple payment IDs
        Returns a list of (master_data, detail_records) tuples
        """
        payments = []
        
        for payment_id in payment_ids:
            # Filter dataframe for this payment ID
            payment_df = df[df['Payment ID'].astype(str).str.strip() == payment_id.strip()].copy()
            
            if payment_df.empty:
                # Try numeric comparison if string comparison failed
                try:
                    numeric_id = int(payment_id.lstrip('0'))
                    payment_df = df[df['Payment ID'] == numeric_id].copy()
                except:
                    logger.warning(f"No records found for payment ID: {payment_id}")
                    continue
            
            logger.info(f"Processing payment {payment_id} with {len(payment_df)} records")
            
            # Extract master data for this payment
            first_row = payment_df.iloc[0]
            
            payment_date_obj = self.convert_excel_date(first_row.get('Payment Date'))
            payment_date = payment_date_obj.strftime('%Y-%m-%d') if payment_date_obj else None
            
            # Calculate totals for this payment only
            payment_total = payment_df['Net Amount'].sum() if 'Net Amount' in payment_df.columns else 0
            
            master_data = {
                'payment_id': payment_id,
                'payment_date': payment_date,
                'payment_amount': float(payment_total),  # Use calculated total for each payment
                'calculated_amount': float(payment_total),
                'vendor_name': str(first_row.get('Vendor Name', '')).strip() if 'Vendor Name' in payment_df.columns else None
            }
            
            # Standardize master data
            master_data = self.standardize_payment_data(master_data)
            
            # Map detail records for this payment
            detail_records = []
            for idx, row in payment_df.iterrows():
                try:
                    detail = {
                        'payment_id': payment_id,
                        'payment_date': self.parse_date(row.get('Payment Date', '')),
                        'invoice_date': self.parse_date(row.get('Invoice Date', '')),
                        'invoice_no': str(row.get('Invoice ID', '')),
                        'gross_amount': self.parse_amount(row.get('Gross Amount', 0)),
                        'discount': self.parse_amount(row.get('Discount', 0)),
                        'net_amount': self.parse_amount(row.get('Net Amount', 0)),
                        'payment_message': row.get('Payment Message', None)
                    }
                    
                    detail = self.standardize_payment_data(detail)
                    
                    if detail.get('invoice_no'):
                        detail_records.append(detail)
                        
                except Exception as e:
                    logger.error(f"Error processing row {idx} for payment {payment_id}: {e}")
                    continue
            
            payments.append((master_data, detail_records))
            logger.info(f"Payment {payment_id}: Amount=${master_data['payment_amount']:,.2f}, Details={len(detail_records)}")
        
        return payments

    def map_payment_details(self, df: pd.DataFrame) -> list:
        """
        Map payment details from DataFrame to database format
        Updated to preserve leading zeros in payment_id
        """
        try:
            # Clean the dataframe first
            df = self.clean_dataframe(df.copy())
            
            # Get payment_id from first row - with leading zeros preserved
            first_row = df.iloc[0] if len(df) > 0 else {}
            payment_id_raw = first_row.get('Payment ID', '')
            
            # Handle payment ID formatting
            if pd.isna(payment_id_raw):
                payment_id = ''
            elif isinstance(payment_id_raw, (int, float)):
                # Format with leading zeros
                payment_id = str(int(payment_id_raw)).zfill(10)
            else:
                payment_id = str(payment_id_raw).strip()
            
            details = []
            
            for idx, row in df.iterrows():
                try:
                    detail = {
                        'payment_id': payment_id,  # Use the formatted payment_id
                        'payment_date': self.parse_date(row.get('Payment Date', '')),
                        'invoice_date': self.parse_date(row.get('Invoice Date', '')),
                        'invoice_no': str(row.get('Invoice ID', '')),
                        'gross_amount': self.parse_amount(row.get('Gross Amount', 0)),
                        'discount': self.parse_amount(row.get('Discount', 0)),
                        'net_amount': self.parse_amount(row.get('Net Amount', 0)),
                        'payment_message': row.get('Payment Message', None)
                    }
                    
                    # Standardize the data
                    detail = self.standardize_payment_data(detail)
                    
                    # Skip if no invoice number
                    if detail.get('invoice_no'):
                        details.append(detail)
                    else:
                        logger.warning(f"Skipping row {idx}: No invoice number")
                        
                except Exception as e:
                    logger.error(f"Error processing row {idx}: {e}")
                    continue
            
            logger.info(f"Mapped {len(details)} payment details with payment_id: {payment_id}")
            return details
            
        except Exception as e:
            logger.error(f"Error mapping payment details: {e}")
            raise

    def validate_payment_data(self, master_data: Dict[str, Any], detail_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate payment data for consistency and business rules
        """
        validation_results = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'summary': {}
        }
        
        try:
            # Calculate totals from details
            detail_gross_total = sum(record.get('gross_amount', 0) for record in detail_records)
            detail_discount_total = sum(record.get('discount', 0) for record in detail_records)
            detail_net_total = sum(record.get('net_amount', 0) for record in detail_records)
            
            # Amount reconciliation check
            master_amount = master_data.get('payment_amount', 0)
            amount_diff = abs(master_amount - detail_net_total)
            
            if amount_diff > 0.01:  # Allow for small rounding differences
                validation_results['errors'].append(
                    f"Payment amount mismatch: Master=${master_amount:,.2f}, Details Total=${detail_net_total:,.2f}, Difference=${amount_diff:,.2f}"
                )
                validation_results['is_valid'] = False
            
            # Check for duplicate invoice numbers within payment
            invoice_numbers = [record['invoice_no'] for record in detail_records if record.get('invoice_no')]
            duplicate_invoices = [inv for inv in set(invoice_numbers) if invoice_numbers.count(inv) > 1]
            
            if duplicate_invoices:
                validation_results['warnings'].append(
                    f"Duplicate invoice numbers found: {', '.join(duplicate_invoices)}"
                )
            
            # Check for zero amounts
            zero_amount_invoices = [record['invoice_no'] for record in detail_records if record.get('net_amount', 0) == 0]
            if zero_amount_invoices:
                validation_results['warnings'].append(
                    f"Zero net amount invoices: {', '.join(zero_amount_invoices)}"
                )
            
            # Summary statistics
            validation_results['summary'] = {
                'master_payment_id': master_data.get('payment_id'),
                'master_amount': master_amount,
                'detail_count': len(detail_records),
                'detail_gross_total': detail_gross_total,
                'detail_discount_total': detail_discount_total,
                'detail_net_total': detail_net_total,
                'amount_variance': amount_diff,
                'unique_invoices': len(set(invoice_numbers)),
                'duplicate_invoice_count': len(duplicate_invoices)
            }
            
            logger.info(f"Payment validation completed: {'PASSED' if validation_results['is_valid'] else 'FAILED'}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error during payment validation: {e}")
            validation_results['errors'].append(f"Validation error: {e}")
            validation_results['is_valid'] = False
            return validation_results

    # This should be in data_mapper_enhanced.py, NOT in database_manager_compatible.py

    def standardize_html_payment_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize HTML email column names to match expected format
        """
        try:
            # Create a copy to avoid modifying original
            standardized_df = df.copy()
            
            # Define the exact mappings from HTML Email to database fields
            column_mapping = {
                # HTML Email Column → Database Field
                'Date': 'invoice_date',
                'Invoice ID': 'invoice_no',
                'Gross Amount': 'gross_amount',
                'Discount': 'discount',
                'Net Amount': 'net_amount',
                'Payment Message': 'payment_message',
                
                # Handle variations
                'invoice_date': 'invoice_date',
                'invoice_id': 'invoice_no',
                'gross_amount': 'gross_amount',
                'discount': 'discount',
                'net_amount': 'net_amount',
                'payment_message': 'payment_message'
            }
            
            # Apply mappings
            for old_name, new_name in column_mapping.items():
                if old_name in standardized_df.columns and old_name != new_name:
                    standardized_df = standardized_df.rename(columns={old_name: new_name})
                    logger.info(f"Mapped column: '{old_name}' → '{new_name}'")
            
            # Clean up amount columns
            amount_columns = ['gross_amount', 'net_amount', 'discount']
            for col in amount_columns:
                if col in standardized_df.columns:
                    standardized_df[col] = standardized_df[col].apply(self.clean_amount_value)
            
            # Clean up date columns - FIXED VERSION
            date_columns = ['invoice_date', 'payment_date']
            for col in date_columns:
                if col in standardized_df.columns:
                    standardized_df[col] = standardized_df[col].apply(
                        lambda x: self.parse_date(x) if pd.notna(x) else None
                    )
            
            logger.info(f"Standardized columns: {list(standardized_df.columns)}")
            return standardized_df
            
        except Exception as e:
            logger.error(f"Error standardizing HTML payment columns: {e}")
            raise
    
    def parse_amount(self, amount_str: str) -> float:
        """
        Parse amount string to float, handling various formats
        """
        if not amount_str:
            return 0.0
        
        try:
            # Remove common formatting characters
            clean_str = str(amount_str).replace('$', '').replace(',', '').strip()
            
            # Handle parentheses for negative numbers
            if clean_str.startswith('(') and clean_str.endswith(')'):
                clean_str = '-' + clean_str[1:-1]
            
            return float(clean_str) if clean_str else 0.0
        except:
            logger.warning(f"Could not parse amount: {amount_str}")
            return 0.0

    def clean_amount_value(self, value):
        """
        Clean and convert amount values to float
        Handles various formats like $1,234.56, (1234.56), etc.
        """
        try:
            # Handle None/NaN
            if pd.isna(value) or value is None:
                return 0.0
                
            # If already a number, just return as float
            if isinstance(value, (int, float)):
                return float(value)
                
            # Convert to string for processing
            value_str = str(value).strip()
            
            # Handle empty strings
            if not value_str:
                return 0.0
                
            # Remove currency symbols
            value_str = value_str.replace('$', '').replace('€', '').replace('£', '')
            
            # Remove commas
            value_str = value_str.replace(',', '')
            
            # Handle parentheses (negative values in accounting)
            if value_str.startswith('(') and value_str.endswith(')'):
                value_str = '-' + value_str[1:-1]
            
            # Remove any remaining spaces
            value_str = value_str.strip()
            
            # Convert to float
            return float(value_str)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error cleaning amount value '{value}': {e}")
            return 0.0
    
    def parse_date(self, date_str: str) -> str:
        """
        Parse date string to standard format YYYY-MM-DD
        """
        if not date_str:
            return None
        
        try:
            from datetime import datetime
            
            # Clean the date string - remove time component if present
            date_str = str(date_str).strip()
            
            # If it has "12:00:00 AM" or similar, remove it
            if ' 12:00:00 AM' in date_str:
                date_str = date_str.replace(' 12:00:00 AM', '')
            if ' 00:00:00' in date_str:
                date_str = date_str.replace(' 00:00:00', '')
            
            # Try common date formats
            date_formats = [
                '%m/%d/%Y',      # 6/27/2024
                '%m/%d/%y',      # 6/27/24
                '%Y-%m-%d',      # 2024-06-27
                '%d/%m/%Y',      # 27/6/2024
                '%m-%d-%Y',      # 06-27-2024
                '%Y/%m/%d',      # 2024/06/27
            ]
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Date parsing error: {e}")
            return None
    

    # In your EnhancedDataMapper class, update the map_kp_payment_excel method:

    def map_kp_payment_excel(self, df: pd.DataFrame) -> List[Dict]:
        """
        Map Kaiser Permanente payment Excel file to database format
        FIXED: Map Invoice ID to invoice_no (not invoice_id)
        """
        try:
            # Column mapping for Kaiser payment files
            # FIXED: Changed 'invoice_id' to 'invoice_no' to match database schema
            payment_mapping = {
                'Payment ID': 'payment_id',
                'Payment Date': 'payment_date', 
                'Payment Amount': 'payment_amount',
                'Invoice ID': 'invoice_no',  # FIXED: was 'invoice_id', now 'invoice_no'
                'Gross Amount': 'gross_amount',
                'Net Amount': 'net_amount',
                'Discount': 'discount',  # FIXED: Changed from 'Discount Amount' to match your Excel
                'Payment Message': 'payment_message'
            }
            
            # Clean and standardize column names
            df_cleaned = df.copy()
            df_cleaned.columns = df_cleaned.columns.str.strip()
            
            # Check for required columns
            required_cols = ['Payment ID', 'Invoice ID', 'Net Amount']
            missing_cols = [col for col in required_cols if col not in df_cleaned.columns]
            
            if missing_cols:
                logger.warning(f"Missing required columns: {missing_cols}")
                logger.info(f"Available columns: {list(df_cleaned.columns)}")
            
            # Map the data
            mapped_records = []
            
            for _, row in df_cleaned.iterrows():
                record = {}
                
                # Map each field
                for excel_col, db_field in payment_mapping.items():
                    if excel_col in row.index:
                        value = row[excel_col]
                        
                        if db_field in ['payment_date', 'invoice_date']:
                            # Handle date conversion
                            record[db_field] = self.convert_excel_date(value)
                        elif db_field in ['gross_amount', 'net_amount', 'discount', 'payment_amount']:
                            # Handle amount conversion
                            record[db_field] = self.clean_amount_value(value)
                        elif db_field == 'payment_id':
                            # Ensure payment ID has leading zeros (10 digits for Kaiser)
                            if pd.notna(value):
                                record[db_field] = str(value).zfill(10)
                            else:
                                record[db_field] = None
                        else:
                            # Regular string fields
                            record[db_field] = str(value) if pd.notna(value) else None
                
                # Set defaults if missing
                if 'vendor_name' not in record or not record['vendor_name']:
                    record['vendor_name'] = 'BLACKSTONE CONSULTING INC'
                
                # Ensure we have the payment_date from the first row if not in each row
                if 'payment_date' not in record and mapped_records:
                    record['payment_date'] = mapped_records[0].get('payment_date')
                
                # Only add records with required data
                # FIXED: Check for 'invoice_no' instead of 'invoice_id'
                if record.get('payment_id') and record.get('invoice_no'):
                    mapped_records.append(record)
                else:
                    logger.warning(f"Skipping record - missing required fields. Record: {record}")
            
            logger.info(f"Mapped {len(mapped_records)} Kaiser payment records")
            return mapped_records
            
        except Exception as e:
            logger.error(f"Error mapping Kaiser payment Excel: {e}")
            raise

    # Add this method to your EnhancedDataMapper class to handle date standardization

    def standardize_payment_data(self, data: Dict) -> Dict:
        """
        Standardize payment data to ensure consistent types
        Converts datetime.date objects to strings for database compatibility
        """
        standardized = data.copy()
        
        # Handle date fields - convert to string format
        date_fields = ['payment_date', 'invoice_date']
        for field in date_fields:
            if field in standardized and standardized[field] is not None:
                value = standardized[field]
                
                # If it's already a string in the right format, keep it
                if isinstance(value, str) and len(value) == 10 and value.count('-') == 2:
                    continue
                    
                # If it's a datetime.date object, convert to string
                elif hasattr(value, 'strftime'):
                    standardized[field] = value.strftime('%Y-%m-%d')
                    
                # If it's a pandas Timestamp
                elif hasattr(value, 'date'):
                    standardized[field] = value.date().strftime('%Y-%m-%d')
                    
                # Try parsing if it's a string in different format
                elif isinstance(value, str):
                    parsed = self.parse_date(value)
                    if parsed:
                        standardized[field] = parsed
                        
        # Handle amount fields - ensure they're floats
        amount_fields = ['payment_amount', 'gross_amount', 'discount', 'net_amount', 
                        'calculated_amount', 'discount_amount']
        for field in amount_fields:
            if field in standardized and standardized[field] is not None:
                try:
                    standardized[field] = float(standardized[field])
                except (ValueError, TypeError):
                    standardized[field] = 0.0
        
        # Ensure payment_id is a string with leading zeros
        if 'payment_id' in standardized and standardized['payment_id'] is not None:
            payment_id = standardized['payment_id']
            if isinstance(payment_id, (int, float)):
                standardized['payment_id'] = str(int(payment_id)).zfill(10)
            else:
                standardized['payment_id'] = str(payment_id).strip()
        
        # Standardize null/empty values
        for key, value in standardized.items():
            if pd.isna(value) or value == 'nan' or value == '':
                standardized[key] = None
        
        return standardized
    
    def detect_payment_email_html(self, content: str) -> bool:
        """
        Enhanced detection for Kaiser Permanente payment emails
        Based on the debug output showing 6/7 indicators consistently found
        """
        try:
            if isinstance(content, bytes):
                # Handle bytes content by decoding
                for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                    try:
                        content = content.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    content = content.decode('utf-8', errors='replace')
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for Kaiser Permanente specific indicators (based on debug output)
            content_text = content.lower()
            
            # Check for KP-specific patterns
            kp_indicators = [
                'payment id', 'vendor id', 'blackstone consulting', 
                'invoice id', 'gross amount', 'net amount', 'payment date'
            ]
            
            found_kp_indicators = sum(1 for indicator in kp_indicators if indicator in content_text)
            
            # Also check for table structure (should have 4 tables based on debug)
            tables = soup.find_all('table')
            
            logger.info(f"KP payment detection: {found_kp_indicators}/{len(kp_indicators)} indicators, {len(tables)} tables")
            
            # Need at least 5 KP indicators and 3+ tables for Kaiser emails
            return found_kp_indicators >= 5 and len(tables) >= 3
            
        except Exception as e:
            logger.warning(f"Error detecting payment email HTML: {e}")
            return False

    
    def parse_payment_email_html(self, html_content: str) -> pd.DataFrame:
        """
        Parse payment details from Kaiser HTML email
        Specifically handles the nested table structure in Kaiser emails
        """
        from bs4 import BeautifulSoup
        import re
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all tables with class="main"
            main_tables = soup.find_all('table', class_='main')
            logger.info(f"Found {len(main_tables)} main tables")
            
            # The payment details are in a nested table structure
            # Look for the table that contains the payment details headers
            detail_table = None
            
            for table in main_tables:
                # Check if this table contains the headers we're looking for
                headers = table.find_all('th')
                header_texts = [h.get_text(strip=True) for h in headers]
                
                # Check if this is the detail table by looking for specific headers
                if any('Invoice ID' in h for h in header_texts) and any('Net Amount' in h for h in header_texts):
                    detail_table = table
                    logger.info(f"Found detail table with headers: {header_texts}")
                    break
            
            if not detail_table:
                # Alternative: Look for nested tables
                for table in main_tables:
                    nested_tables = table.find_all('table', class_='main')
                    for nested in nested_tables:
                        headers = nested.find_all('th')
                        header_texts = [h.get_text(strip=True) for h in headers]
                        if any('Invoice ID' in h for h in header_texts):
                            detail_table = nested
                            logger.info("Found detail table as nested table")
                            break
                    if detail_table:
                        break
            
            if not detail_table:
                logger.error("Could not find payment detail table")
                raise ValueError("Payment detail table not found")
            
            # Parse the detail table
            data_rows = []
            
            # Get all rows
            rows = detail_table.find_all('tr')
            
            # Find header row and data rows
            header_row_idx = -1
            for i, row in enumerate(rows):
                if row.find('th'):
                    header_row_idx = i
                    break
            
            # Process data rows (after header)
            if header_row_idx >= 0:
                for row in rows[header_row_idx + 1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 5:  # Must have at least 5 cells
                        # Extract cell values
                        date_val = cells[0].get_text(strip=True)
                        invoice_id = cells[1].get_text(strip=True)
                        gross_amount = cells[2].get_text(strip=True)
                        discount = cells[3].get_text(strip=True)
                        net_amount = cells[4].get_text(strip=True)
                        payment_msg = cells[5].get_text(strip=True) if len(cells) > 5 else ''
                        
                        # Validate this is a data row (has invoice ID)
                        if invoice_id and invoice_id.strip():
                            data_rows.append({
                                'Date': date_val,
                                'Invoice ID': invoice_id,
                                'Gross Amount': gross_amount,
                                'Discount': discount,
                                'Net Amount': net_amount,
                                'Payment Message': payment_msg
                            })
                            logger.info(f"Found invoice row: ID={invoice_id}, Date={date_val}, Net={net_amount}")
            
            if not data_rows:
                logger.error("No data rows found in payment detail table")
                raise ValueError("No payment details found")
            
            # Create DataFrame
            df = pd.DataFrame(data_rows)
            logger.info(f"Successfully parsed {len(df)} payment rows")
            
            return df
            
        except Exception as e:
            logger.error(f"Error parsing payment email HTML: {e}")
            raise


    def extract_kaiser_payment_metadata(self, html_content: str) -> dict:
        """
        Extract payment header info from Kaiser email
        Updated to handle the actual HTML structure
        """
        from bs4 import BeautifulSoup
        import re
        
        try:
            # Handle bytes content
            if isinstance(html_content, bytes):
                html_content = html_content.decode('utf-8', errors='replace')
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Initialize with defaults
            payment_id = None
            payment_date = None
            payment_amount = None
            vendor_name = 'BLACKSTONE CONSULTING INC'
            vendor_id = None
            
            # Find all main tables
            main_tables = soup.find_all('table', class_='main')
            
            # Look for payment metadata in tables
            for table in main_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        header_cell = cells[0].get_text(strip=True).lower()
                        value_cell = cells[1].get_text(strip=True)
                        
                        if 'payment id' in header_cell:
                            payment_id = value_cell
                            logger.info(f"Found Payment ID: {payment_id}")
                        elif 'payment date' in header_cell:
                            # Parse the date
                            payment_date = self.parse_kaiser_date(value_cell)
                            logger.info(f"Found Payment Date: {payment_date}")
                        elif 'payment amount' in header_cell:
                            try:
                                payment_amount = float(value_cell.replace(',', ''))
                                logger.info(f"Found Payment Amount: ${payment_amount}")
                            except:
                                logger.warning(f"Could not parse payment amount: {value_cell}")
                        elif 'vendor id' in header_cell:
                            vendor_id = value_cell
                            logger.info(f"Found Vendor ID: {vendor_id}")
                        elif 'vendor' in header_cell and 'vendor name 2' not in header_cell:
                            if value_cell.strip():
                                vendor_name = value_cell
                                logger.info(f"Found Vendor Name: {vendor_name}")
            
            # Ensure we have required fields
            if not payment_id:
                logger.warning("Payment ID not found in metadata")
                payment_id = 'UNKNOWN'
            
            if not payment_date:
                logger.warning("Payment date not found, using today")
                from datetime import datetime
                payment_date = datetime.now().strftime('%Y-%m-%d')
            
            if payment_amount is None:
                logger.warning("Payment amount not found, will calculate from details")
                payment_amount = 0.0
            
            return {
                'payment_id': payment_id,
                'payment_date': payment_date,
                'payment_amount': payment_amount,
                'vendor_name': vendor_name,
                'vendor_id': vendor_id
            }
            
        except Exception as e:
            logger.error(f"Error extracting payment metadata: {e}")
            # Return defaults
            from datetime import datetime
            return {
                'payment_id': 'ERROR_' + datetime.now().strftime('%Y%m%d%H%M%S'),
                'payment_date': datetime.now().strftime('%Y-%m-%d'),
                'payment_amount': 0.0,
                'vendor_name': 'BLACKSTONE CONSULTING INC'
            }


    def parse_kaiser_date(self, date_str: str) -> str:
        """
        Parse Kaiser date format (e.g., "7/3/2025 12:00:00 AM") to YYYY-MM-DD
        """
        if not date_str:
            return None
        
        from datetime import datetime
        
        # Clean the date string
        date_str = str(date_str).strip()
        
        # Try different date formats
        date_formats = [
            '%m/%d/%Y %I:%M:%S %p',    # 7/3/2025 12:00:00 AM
            '%m/%d/%Y',                 # 7/3/2025
            '%Y-%m-%d',                 # 2025-07-03
            '%d/%m/%Y',                 # 03/07/2025
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    
    

    def process_payment_email_html(self, html_content: str) -> tuple:
        """
        Process complete Kaiser payment email HTML
        Returns master data and detail records properly mapped
        """
        try:
            logger.info("Processing Kaiser payment email...")
            
            # Step 1: Extract payment metadata
            master_data = self.extract_kaiser_payment_metadata(html_content)
            
            # Standardize master data
            if hasattr(self, 'standardize_payment_data'):
                master_data = self.standardize_payment_data(master_data)
            
            # Step 2: Parse payment details table
            df = self.parse_payment_email_html(html_content)
            
            # Step 3: Map to database format
            detail_records = []
            
            for idx, row in df.iterrows():
                try:
                    # Extract values from the DataFrame row
                    invoice_id = str(row.get('Invoice ID', ''))
                    
                    # Skip if no invoice ID
                    if not invoice_id or invoice_id == 'None':
                        logger.warning(f"Skipping row {idx}: No invoice ID")
                        continue
                    
                    record = {
                        # From master payment data
                        'payment_id': master_data['payment_id'],
                        'payment_date': master_data['payment_date'],
                        
                        # From detail row - map Invoice ID to invoice_no
                        'invoice_date': self.parse_date(row.get('Date', '')),
                        'invoice_no': invoice_id,  # This is the key mapping!
                        'gross_amount': self.parse_amount(row.get('Gross Amount', 0)),
                        'discount': self.parse_amount(row.get('Discount', 0)),
                        'net_amount': self.parse_amount(row.get('Net Amount', 0)),
                        'payment_message': row.get('Payment Message', None)
                    }
                    
                    # Standardize the record
                    if hasattr(self, 'standardize_payment_data'):
                        record = self.standardize_payment_data(record)
                    
                    # Validate required fields
                    if record.get('invoice_no') and record.get('payment_id'):
                        detail_records.append(record)
                        logger.debug(f"Added detail record: Invoice={record['invoice_no']}, Net=${record['net_amount']}")
                    else:
                        logger.warning(f"Skipping row {idx}: Missing required fields")
                        
                except Exception as e:
                    logger.error(f"Error processing row {idx}: {e}")
                    continue
            
            logger.info(f"Processed {len(detail_records)} payment detail records")
            
            if not detail_records:
                raise ValueError("No valid payment details could be extracted")
            
            return master_data, detail_records
            
        except Exception as e:
            logger.error(f"Error processing payment email HTML: {e}")
            raise

    def detect_file_type_from_filename(self, filename: str) -> str:
        """
        Auto-detect file type based on filename
        
        Args:
            filename: Name of the uploaded file
            
        Returns:
            String indicating file type: 'BCI Details', 'AUS Details', 'EDI', 'Release', 'Add-On', 'Kaiser', 'Payments' or 'Unknown'
        """
        filename_lower = filename.lower()
        
        payment_filename_patterns = [
            r'payment\d+_\d+\.xlsx?',  # Payment123456_20250623.xlsx
            r'.*payment.*\.xlsx?',      # Any file with "payment" in name
            r'.*remittance.*\.xlsx?',   # Any file with "remittance" in name
            r'kp.*payment.*\.xlsx?'     # Kaiser Permanente payment files
          ]
        
        for pattern in payment_filename_patterns:
            if re.match(pattern, filename_lower):
                logger.info(f"Payment file detected by filename pattern: {pattern}")
                return "KP_Payment_Excel"
        
        # Check for specific file naming patterns
        if 'tlm_bci' in filename_lower or filename_lower.startswith('bci'):
            return 'BCI Details'
        
        if 'aus_invoice' in filename_lower or filename_lower.startswith('aus'):
            return 'AUS Details'
        
        # Check for other common patterns
        if any(keyword in filename_lower for keyword in ['release', 'weekly_release']):
            return 'Release'
        
        if any(keyword in filename_lower for keyword in ['addon', 'add-on', 'add_on', 'weekly_addon']):
            return 'Add-On'
        
        if any(keyword in filename_lower for keyword in ['edi', 'weekly_edi']):
            return 'EDI'
        
        if any(keyword in filename_lower for keyword in ['kaiser', 'scr']):
            return 'Kaiser SCR Building Data'
        
        return 'Unknown'
    
    def detect_file_type_from_content(self, df: pd.DataFrame) -> str:
        """
        Auto-detect file type based on column names (fallback method)
        
        Returns:
            String indicating file type: 'EDI', 'Release', 'Add-On', 'BCI Details', 'AUS Details', 'Kaiser', or 'Unknown'
        """

        required_payment_columns = [
            'Payment ID', 'Payment Date', 'Payment Amount', 
            'Invoice ID', 'Gross Amount', 'Net Amount'
        ]
        
        # Clean column names and check
        df_columns = [col.strip() for col in df.columns]
        payment_columns_found = sum(1 for col in required_payment_columns if col in df_columns)
        
        if payment_columns_found >= 4:  # Need at least 4 out of 6 core columns
            logger.info(f"Payment file detected by column structure: {payment_columns_found}/6 columns found")
            return "KP_Payment_Excel"
        
        columns = [col.lower() for col in df.columns]
        
        # Check for AUS details FIRST (more specific check)
        if ('employee name' in columns and 'invoice number' in columns and 
            'first' not in columns and 'last' not in columns):
            return 'AUS Details'
        
        # Check for BCI details (has separate first/last name fields)
        if (any(col in columns for col in ['employee_last_name', 'employee_first_name', 'employee_mi']) and 
            any(col in columns for col in ['invoice_no', 'invoice number']) and
            any(col in columns for col in ['emp_no', 'employee number'])):
            return 'BCI Details'
        
        # Check for Kaiser SCR (check this BEFORE master invoices since it might have overlapping columns)
        if any(col in columns for col in ['building code', 'gl loc']) and 'service area' in columns:
            return 'Kaiser SCR Building Data'
        
        # Check for master invoice files (EDI/Release/Add-On)
        if 'invoice no.' in columns:
            # Look for hints about which type
            if any('release' in col.lower() for col in df.columns):
                return 'Release'
            elif any('add' in col.lower() and 'on' in col.lower() for col in df.columns):
                return 'Add-On'
            elif any('edi' in col.lower() for col in df.columns):
                return 'EDI'
            else:
                # Default to EDI for generic invoice files with 'Invoice No.'
                return 'EDI'
        
        # Fallback check for invoice number (different format)
        if 'invoice number' in columns:
            # If it has employee details, it's likely BCI/AUS (already checked above)
            # If it doesn't, it might be a master invoice file
            if not any(col in columns for col in ['employee number', 'employee name', 'first', 'last']):
                return 'EDI'
        
        return 'Unknown'
    
    def auto_detect_file_type(self, filename: str, df: pd.DataFrame = None, html_content: str = None) -> str:
        """
        Enhanced auto-detection that includes HTML email content
        """
        # NEW: Check for HTML email content first
        if html_content:
            if self.detect_payment_email_html(html_content):
                return "KP_Payment_HTML"
            else:
                return "Unknown_HTML"
        
        # Then try filename detection (more reliable for your specific files)
        filename_detection = self.detect_file_type_from_filename(filename)
        
        if filename_detection != 'Unknown':
            logger.info(f"File type detected from filename: {filename_detection}")
            return filename_detection
        
        # Fallback to content detection
        content_detection = self.detect_file_type_from_content(df)
        logger.info(f"File type detected from content: {content_detection}")
        
        return content_detection
    
    def convert_excel_date(self, date_value):
        """
        Convert various date formats to string format YYYY-MM-DD
        Handles multiple input formats robustly
        
        Args:
            date_value: Can be string, datetime, date, numeric Excel date, etc.
            
        Returns:
            str: Date in YYYY-MM-DD format, or None if invalid
        """
        if pd.isna(date_value) or date_value is None or date_value == '':
            return None
        
        # If already a string in the right format, return it
        if isinstance(date_value, str):
            date_str = date_value.strip()
            
            # Check if already in YYYY-MM-DD format
            if len(date_str) == 10 and date_str.count('-') == 2:
                try:
                    # Validate it's a real date
                    pd.to_datetime(date_str, format='%Y-%m-%d')
                    return date_str
                except:
                    pass
            
            # Handle empty or 'nan' strings
            if not date_str or date_str.lower() in ['nan', 'none', 'null']:
                return None
        
        # If it's a datetime or date object, convert to string
        if hasattr(date_value, 'strftime'):
            return date_value.strftime('%Y-%m-%d')
        
        # If it's a pandas Timestamp
        if hasattr(date_value, 'date'):
            return date_value.date().strftime('%Y-%m-%d')
        
        # Handle numeric Excel dates
        if isinstance(date_value, (int, float)):
            try:
                # Excel dates start from 1900-01-01
                # Excel incorrectly treats 1900 as leap year
                if date_value > 59:
                    date_value -= 1
                
                base_date = datetime(1900, 1, 1)
                result_date = base_date + timedelta(days=int(date_value) - 2)
                return result_date.strftime('%Y-%m-%d')
            except:
                pass
        
        # Try parsing string dates with multiple formats
        if isinstance(date_value, str):
            date_str = str(date_value).strip()
            
            # Common date formats to try
            date_formats = [
                '%Y-%m-%d %H:%M:%S',      # ISO with time
                '%Y-%m-%d',               # ISO date
                '%m/%d/%Y %I:%M:%S %p',   # 12/10/2024 12:00:00 AM
                '%m/%d/%Y',               # US format
                '%m/%d/%y',               # US short year
                '%d/%m/%Y',               # European format
                '%m-%d-%Y',               # Hyphenated US
                '%Y/%m/%d',               # Alternative ISO
                '%Y%m%d',                 # Compact format
                '%d-%b-%Y',               # 01-Jan-2024
                '%d-%b-%y',               # 01-Jan-24
                '%b %d, %Y',              # Jan 1, 2024
                '%B %d, %Y',              # January 1, 2024
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except:
                    continue
        
        # Try pandas parsing as last resort
        try:
            parsed = pd.to_datetime(date_value)
            if pd.notna(parsed):
                return parsed.strftime('%Y-%m-%d')
        except:
            pass
        
        # Log warning for unparseable dates
        logger.warning(f"Could not parse date value: '{date_value}' (type: {type(date_value)})")
        return None

    def validate_mapped_data(self, records: List[Dict]) -> Dict[str, List[str]]:
        """Validate mapped records and return any issues found"""
        issues = {
            'missing_invoice': [],
            'missing_employee': [],
            'invalid_amounts': [],
            'invalid_dates': []
        }
        
        for i, record in enumerate(records):
            # Check required fields
            if not record.get('invoice_no'):
                issues['missing_invoice'].append(f"Row {i+1}: No invoice number")
            
            if not record.get('employee_id') and not record.get('employee_name_last'):
                issues['missing_employee'].append(f"Row {i+1}: No employee ID or name")
            
            # Validate amounts
            if record.get('amount_total'):
                try:
                    if float(record['amount_total']) < 0:
                        issues['invalid_amounts'].append(f"Row {i+1}: Negative total amount")
                except:
                    issues['invalid_amounts'].append(f"Row {i+1}: Invalid total amount")
            
            # Validate dates
            if record.get('work_date'):
                try:
                    # Check if date is reasonable (not too far in past or future)
                    work_date = record['work_date']
                    if isinstance(work_date, date):
                        today = date.today()
                        days_diff = abs((today - work_date).days)
                        if days_diff > 365:  # More than a year
                            issues['invalid_dates'].append(
                                f"Row {i+1}: Work date {work_date} seems unusual"
                            )
                except:
                    pass
        
        return issues
    
    @staticmethod
    def dataframe_to_dict_list(df: pd.DataFrame, mapping: Dict[str, str]) -> List[Dict]:
        """Generic function to convert any DataFrame using a field mapping"""
        records = []
        
        for _, row in df.iterrows():
            record = {}
            for excel_col, db_col in mapping.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    record[db_col] = str(value) if not pd.isna(value) else None
            records.append(record)
        
        return records