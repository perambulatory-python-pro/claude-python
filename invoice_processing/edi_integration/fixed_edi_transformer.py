"""
Fixed EDI-Based Invoice Transformer
Properly handles building code lookups and business unit mapping
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from typing import Dict, List, Optional, Set, Tuple
from invoice_processing.core.unified_invoice_schema import UnifiedInvoiceDetail, PayType, standardize_pay_type


class FixedEDIBasedTransformer:
    def __init__(self, dimensions_file: str = "clean_dimensions.xlsx",
                 master_lookup_file: str = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx"):
        """
        Initialize transformer with clean dimension tables and master lookup
        
        Args:
            dimensions_file: Path to clean dimensions Excel file
            master_lookup_file: Path to master lookup file for additional mappings
        """
        self.dimensions_file = dimensions_file
        self.master_lookup_file = master_lookup_file
        self.load_dimensions()
        self.load_master_lookup()
        
        self.transformation_stats = {
            'bci_rows_processed': 0,
            'aus_rows_processed': 0,
            'invoices_matched': 0,
            'invoices_not_found': set(),
            'building_code_fallbacks': 0,
            'multiple_building_codes_flagged': [],
            'errors': [],
            'warnings': []
        }
    
    def load_dimensions(self):
        """Load all dimension tables"""
        print("Loading dimension tables...")
        
        # Load all dimensions
        self.service_area_dim = pd.read_excel(self.dimensions_file, sheet_name='Service_Area_Dim')
        self.building_dim = pd.read_excel(self.dimensions_file, sheet_name='Building_Dim')
        self.gl_dim = pd.read_excel(self.dimensions_file, sheet_name='GL_Combination_Dim')
        self.job_mapping_dim = pd.read_excel(self.dimensions_file, sheet_name='Job_Mapping_Dim')
        self.invoice_dim = pd.read_excel(self.dimensions_file, sheet_name='Invoice_Dim')
        
        # Create lookup dictionaries for fast access
        self.create_lookups()
        
        print(f"  ✓ Loaded {len(self.invoice_dim)} invoices")
        print(f"  ✓ Loaded {len(self.building_dim)} buildings")
        print(f"  ✓ Loaded {len(self.job_mapping_dim)} job mappings")
    
    def load_master_lookup(self):
        """Load master lookup table for Location/Job No to building code mapping"""
        print("Loading master lookup...")
        
        try:
            # Load with first row as header (after removing super headers)
            self.master_lookup_df = pd.read_excel(self.master_lookup_file, sheet_name='Master Lookup')
            print(f"  ✓ Loaded {len(self.master_lookup_df)} master lookup records")
            
            # Also load invoice details for fallback lookups
            if os.path.exists("invoice_details_bci.csv"):
                self.bci_details_df = pd.read_csv("invoice_details_bci.csv")
                # Create invoice to location lookup
                self.invoice_to_location = dict(zip(
                    self.bci_details_df['Invoice_No'].astype(str),
                    self.bci_details_df['Location_Number'].astype(str)
                ))
            else:
                self.invoice_to_location = {}
                
            if os.path.exists("invoice_details_aus.csv"):
                self.aus_details_df = pd.read_csv("invoice_details_aus.csv")
                # Create invoice to job lookup
                self.invoice_to_job = dict(zip(
                    self.aus_details_df['Invoice Number'].astype(str),
                    self.aus_details_df['Job Number'].astype(str)
                ))
            else:
                self.invoice_to_job = {}
                
        except Exception as e:
            print(f"  ⚠️  Error loading master lookup: {e}")
            self.master_lookup_df = pd.DataFrame()
            self.invoice_to_location = {}
            self.invoice_to_job = {}
    
    def create_lookups(self):
        """Create lookup dictionaries for performance"""
        # Invoice lookup (string keys for revision support)
        self.invoice_dim['INVOICE_NUMBER'] = self.invoice_dim['INVOICE_NUMBER'].astype(str)
        self.invoice_lookup = self.invoice_dim.set_index('INVOICE_NUMBER').to_dict('index')
        
        # Building lookup with renamed business_unit field
        self.building_lookup = {}
        for _, row in self.building_dim.iterrows():
            building_code = row['BUILDING_CODE']
            self.building_lookup[building_code] = {
                'EMID': row['EMID'],
                'SERVICE_AREA': row['SERVICE_AREA'],
                'BLDG_BUSINESS_UNIT': row.get('BUSINESS_UNIT'),  # Renamed!
                'BUILDING_NAME': row.get('BUILDING_NAME'),
                'ADDRESS': row.get('ADDRESS')
            }
        
        # LOC to building lookup (via kp_loc_ref)
        self.loc_to_building = {}
        # Load the building reference to get kp_loc_ref
        try:
            building_ref = pd.read_excel("emid_job_bu_table.xlsx", sheet_name="buildings")
            for _, row in building_ref.iterrows():
                if pd.notna(row.get('kp_loc_ref')):
                    loc_ref = str(int(row['kp_loc_ref']))
                    self.loc_to_building[loc_ref] = row['building_code']
        except Exception as e:
            print(f"  ⚠️  Could not load kp_loc_ref mapping: {e}")
        
        # Job to EMID lookup (for AUS)
        self.job_to_emid = {}
        for _, row in self.job_mapping_dim.iterrows():
            job_code = str(row['JOB_CODE'])
            if job_code not in self.job_to_emid:
                self.job_to_emid[job_code] = []
            self.job_to_emid[job_code].append({
                'EMID': row['EMID'],
                'JOB_TYPE': row['JOB_TYPE']
            })
        
        # Service area lookup with regions
        self.service_area_lookup = {}
        # Load EMID reference for regions
        try:
            emid_ref = pd.read_excel("emid_job_bu_table.xlsx", sheet_name="emid_job_code")
            for _, row in emid_ref.iterrows():
                emid = row['emid']
                self.service_area_lookup[emid] = {
                    'MC SERVICE AREA': row['mc_service_area'],
                    'BUILDING_REGION': row.get('region'),  # This becomes building_region
                    'JOB_CODE': row.get('job_code'),
                    'DESCRIPTION': row.get('description')
                }
        except Exception as e:
            print(f"  ⚠️  Could not enhance service area lookup: {e}")
    
    def get_invoice_dimensions(self, invoice_number: str) -> Optional[Dict]:
        """
        Get all dimensional data for an invoice from EDI
        Now properly extracts BUILDING_CODE from the dimension table
        """
        invoice_str = str(invoice_number).strip()
    
    # Direct lookup
        if invoice_str in self.invoice_lookup:
            edi_data = self.invoice_lookup[invoice_str]
            
            result = {
                'EMID': edi_data.get('EMID'),
                'SERVICE_AREA': edi_data.get('SERVICE_AREA'),
                'BUILDING_CODE': edi_data.get('BUILDING_CODE'),  # It's already BUILDING_CODE in the dimension!
                'GL_BU': edi_data.get('GL_BU'),
                'GL_LOC': edi_data.get('GL_LOC'),
                'GL_DEPT': edi_data.get('GL_DEPT'),
                'GL_ACCT': edi_data.get('GL_ACCT'),
                'ONELINK_REGION': edi_data.get('REGION'),
                'ONELINK_STATUS': edi_data.get('ONELINK_STATUS'),
                'PAID_DATE': edi_data.get('PAID_DATE')
            }
            
            # If no building code, try LOC lookup
            if not result['BUILDING_CODE'] and result.get('GL_LOC'):
                loc_str = str(result['GL_LOC'])
                if loc_str in self.loc_to_building:
                    result['BUILDING_CODE'] = self.loc_to_building[loc_str]
                    self.transformation_stats['warnings'].append(
                        f"Used LOC {loc_str} to find building {result['BUILDING_CODE']}"
                    )
            
            self.transformation_stats['invoices_matched'] += 1
            return result
        
        # Try without revision letter
        base_invoice = invoice_str.rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        if base_invoice != invoice_str and base_invoice in self.invoice_lookup:
            self.transformation_stats['invoices_matched'] += 1
            self.transformation_stats['warnings'].append(
                f"Invoice {invoice_str} matched to {base_invoice} (revision stripped)"
            )
            return self.get_invoice_dimensions(base_invoice)
        
        # Not found
        self.transformation_stats['invoices_not_found'].add(invoice_str)
        return None
    
    def get_bci_building_code_fallback(self, invoice_number: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Fallback logic for BCI building code lookup through Master Lookup
        Returns: (building_code, warning_message)
        """
        # Get Location_Number from invoice details
        location_number = self.invoice_to_location.get(str(invoice_number))
        if not location_number:
            return None, f"No Location_Number found for invoice {invoice_number}"
        
        # Look up in Master Lookup
        matches = self.master_lookup_df[
            self.master_lookup_df['Location/Job No'] == location_number
        ]
        
        if len(matches) == 0:
            return None, f"Location_Number {location_number} not found in Master Lookup"
        
        # Get unique building codes
        unique_building_codes = matches['building_code'].dropna().unique()
        
        if len(unique_building_codes) == 1:
            self.transformation_stats['building_code_fallbacks'] += 1
            return unique_building_codes[0], None
        
        elif len(unique_building_codes) > 1:
            # Multiple building codes - need EMID to disambiguate
            invoice_dims = self.get_invoice_dimensions(invoice_number)
            if invoice_dims and invoice_dims.get('EMID'):
                emid = invoice_dims['EMID']
                
                # Filter matches by EMID
                emid_matches = matches[matches['EMID'] == emid]
                emid_building_codes = emid_matches['building_code'].dropna().unique()
                
                if len(emid_building_codes) == 1:
                    self.transformation_stats['building_code_fallbacks'] += 1
                    return emid_building_codes[0], None
                else:
                    warning = f"Multiple building codes for Location {location_number} + EMID {emid}: {emid_building_codes}"
                    self.transformation_stats['multiple_building_codes_flagged'].append({
                        'invoice': invoice_number,
                        'location': location_number,
                        'emid': emid,
                        'building_codes': list(emid_building_codes)
                    })
                    return None, warning
            else:
                warning = f"Multiple building codes for Location {location_number}, no EMID available: {unique_building_codes}"
                self.transformation_stats['multiple_building_codes_flagged'].append({
                    'invoice': invoice_number,
                    'location': location_number,
                    'building_codes': list(unique_building_codes)
                })
                return None, warning
        
        return None, f"No building codes found for Location {location_number}"
    
    def get_aus_building_code(self, invoice_number: str, job_number: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get building code for AUS invoice through Master Lookup
        Returns: (building_code, warning_message)
        """
        if not job_number:
            job_number = self.invoice_to_job.get(str(invoice_number))
            if not job_number:
                return None, f"No Job Number found for invoice {invoice_number}"
        
        # Look up in Master Lookup
        matches = self.master_lookup_df[
            self.master_lookup_df['Location/Job No'] == job_number
        ]
        
        if len(matches) == 0:
            return None, f"Job Number {job_number} not found in Master Lookup"
        
        # Get unique building codes
        unique_building_codes = matches['building_code'].dropna().unique()
        
        if len(unique_building_codes) == 1:
            return unique_building_codes[0], None
        
        elif len(unique_building_codes) > 1:
            # Multiple building codes - need EMID to disambiguate
            invoice_dims = self.get_invoice_dimensions(invoice_number)
            if invoice_dims and invoice_dims.get('EMID'):
                emid = invoice_dims['EMID']
                
                # Filter matches by EMID
                emid_matches = matches[matches['EMID'] == emid]
                emid_building_codes = emid_matches['building_code'].dropna().unique()
                
                if len(emid_building_codes) == 1:
                    return emid_building_codes[0], None
                else:
                    warning = f"Multiple building codes for Job {job_number} + EMID {emid}"
                    self.transformation_stats['multiple_building_codes_flagged'].append({
                        'invoice': invoice_number,
                        'job': job_number,
                        'emid': emid,
                        'building_codes': list(emid_building_codes)
                    })
                    return None, warning
            else:
                warning = f"Multiple building codes for Job {job_number}, no EMID available"
                return None, warning
        
        return None, f"No building codes found for Job {job_number}"
    
    def transform_bci_row(self, row: pd.Series, row_idx: int, source_file: str) -> Optional[UnifiedInvoiceDetail]:
        """Enhanced BCI transformation with proper building code and business unit lookups"""
        try:
            # Get invoice dimensions from EDI
            invoice_no = str(row['Invoice_No'])
            invoice_dims = self.get_invoice_dimensions(invoice_no)
            
            # Initialize all dimensional fields
            emid = None
            service_area = None
            building_code = None
            bldg_business_unit = None
            building_region = None
            onelink_region = None
            job_code = None
            
            if invoice_dims:
                # Primary data from EDI
                emid = invoice_dims['EMID']
                service_area = invoice_dims['SERVICE_AREA']
                building_code = invoice_dims.get('BUILDING_CODE')
                onelink_region = invoice_dims.get('ONELINK_REGION')
                
                # If no building code from EDI, try fallback
                if not building_code:
                    building_code, warning = self.get_bci_building_code_fallback(invoice_no)
                    if warning:
                        self.transformation_stats['warnings'].append(warning)
            else:
                # No EDI match - try fallback for building code
                building_code, warning = self.get_bci_building_code_fallback(invoice_no)
                if warning:
                    self.transformation_stats['warnings'].append(warning)
            
            # Now get additional data based on what we found
            if building_code and building_code in self.building_lookup:
                building_info = self.building_lookup[building_code]
                bldg_business_unit = building_info.get('BLDG_BUSINESS_UNIT')
                if not emid:
                    emid = building_info.get('EMID')
                if not service_area:
                    service_area = building_info.get('SERVICE_AREA')
            
            # Get EMID-based data
            if emid and emid in self.service_area_lookup:
                emid_info = self.service_area_lookup[emid]
                building_region = emid_info.get('BUILDING_REGION')
                job_code = emid_info.get('JOB_CODE')
            
            # Parse Customer field for job_code if we don't have it
            if not job_code and pd.notna(row.get('Customer')):
                customer_str = str(row['Customer'])
                if ':' in customer_str:
                    job_code = customer_str.split(':', 1)[1].strip()
            
            # Determine pay type (same as before)
            if pd.notna(row.get('Billed_Holiday_Hours', 0)) and row.get('Billed_Holiday_Hours', 0) > 0:
                pay_type = PayType.HOLIDAY
                hours = row['Billed_Holiday_Hours']
                rate = row.get(' Billed_Holiday_Rate ', 0)
                amount = row.get(' Billed_Holiday_Wages ', 0)
            elif pd.notna(row.get('Billed_OT_Hours', 0)) and row.get('Billed_OT_Hours', 0) > 0:
                pay_type = PayType.OVERTIME
                hours = row['Billed_OT_Hours']
                rate = row.get(' Billed_OT_Rate ', 0)
                amount = row.get(' Billed_OT_Wages ', 0)
            else:
                pay_type = PayType.REGULAR
                hours = row.get('Billed_Regular_Hours', 0)
                rate = row.get(' Billed_Regular_Rate ', 0)
                amount = row.get(' Billed_Regular_Wages ', 0)
            
            # Parse dates
            work_date = pd.to_datetime(row['Date']).date()
            week_ending = pd.to_datetime(row['Weekending_Date']).date()
            
            # Create unique line ID
            emp_no = row.get('Emp_No', 'UNKNOWN')
            invoice_line_id = f"{invoice_no}_{emp_no}_{work_date}_{pay_type.value}"
            
            return UnifiedInvoiceDetail(
                # Core identifiers
                invoice_number=invoice_no,
                invoice_line_id=invoice_line_id,
                source_system='BCI',
                work_date=work_date,
                week_ending_date=week_ending,
                pay_type=pay_type,
                hours_quantity=float(hours) if pd.notna(hours) else 0,
                bill_rate=float(rate) if pd.notna(rate) else 0,
                bill_amount=float(amount) if pd.notna(amount) else 0,
                created_timestamp=datetime.now(),
                source_file=source_file,
                source_row_number=row_idx,
                
                # Organizational hierarchy - properly populated!
                emid=emid,
                business_unit=bldg_business_unit,  # This is now the building's business unit
                mc_service_area=service_area,
                building_code=building_code,
                location_name=row.get('Location'),
                location_number=row.get('Location_Number'),
                job_code=job_code,
                
                # Additional regional data (these need to be added to UnifiedInvoiceDetail)
                # building_region=building_region,
                # onelink_region=onelink_region,
                
                # Customer info
                customer_name=row.get('Customer'),
                customer_number=str(row.get('Customer_Number', '')),
                
                # Position info
                position_description=row.get('Position'),
                position_number=row.get('Position_Number'),
                
                # Employee info
                employee_number=int(emp_no) if pd.notna(emp_no) and str(emp_no) != 'UNKNOWN' else None,
                employee_first_name=row.get('Employee_First_Name'),
                employee_last_name=row.get('Employee_Last_Name'),
                employee_middle_initial=row.get('Employee_MI'),
                
                # Time and attendance
                shift_start_time=row.get('Shift_In'),
                shift_end_time=row.get('Shift_Out'),
                
                # Pay info
                pay_rate=float(rate) if pd.notna(rate) else 0,
                
                # Billing codes
                billing_code=str(row.get('Billing_Code', ''))
            )
            
        except Exception as e:
            self.transformation_stats['errors'].append(
                f"BCI row {row_idx}: {str(e)}"
            )
            return None
    
    def transform_aus_row(self, row: pd.Series, row_idx: int, source_file: str) -> Optional[UnifiedInvoiceDetail]:
        """Enhanced AUS transformation with proper building code and business unit lookups"""
        try:
            # Get invoice dimensions from EDI
            invoice_no = str(row['Invoice Number'])
            job_number = str(row.get('Job Number', ''))
            invoice_dims = self.get_invoice_dimensions(invoice_no)
            
            # Initialize all dimensional fields
            emid = None
            service_area = None
            building_code = None
            bldg_business_unit = None
            building_region = None
            onelink_region = None
            job_code = None
            
            if invoice_dims:
                # Primary data from EDI
                emid = invoice_dims['EMID']
                service_area = invoice_dims['SERVICE_AREA']
                building_code = invoice_dims.get('BUILDING_CODE')
                onelink_region = invoice_dims.get('ONELINK_REGION')
                
                # If no building code from EDI, try job lookup
                if not building_code:
                    building_code, warning = self.get_aus_building_code(invoice_no, job_number)
                    if warning:
                        self.transformation_stats['warnings'].append(warning)
            else:
                # No EDI match - try job lookup for building code
                building_code, warning = self.get_aus_building_code(invoice_no, job_number)
                if warning:
                    self.transformation_stats['warnings'].append(warning)
            
            # Get additional data based on what we found
            if building_code and building_code in self.building_lookup:
                building_info = self.building_lookup[building_code]
                bldg_business_unit = building_info.get('BLDG_BUSINESS_UNIT')
                if not emid:
                    emid = building_info.get('EMID')
                if not service_area:
                    service_area = building_info.get('SERVICE_AREA')
            
            # Get EMID-based data
            if emid and emid in self.service_area_lookup:
                emid_info = self.service_area_lookup[emid]
                building_region = emid_info.get('BUILDING_REGION')
                job_code = emid_info.get('JOB_CODE')
            
            # Parse dates
            work_date = pd.to_datetime(row['Work Date']).date()
            week_ending = pd.to_datetime(row['Week Ending']).date()
            
            # Determine pay type
            pay_desc = row.get('Pay Hours Description', '')
            pay_type = standardize_pay_type(pay_desc)
            
            # Create unique line ID
            emp_no = row.get('Employee Number', 'UNKNOWN')
            invoice_line_id = f"{invoice_no}_{emp_no}_{work_date}_{pay_type.value}"
            
            return UnifiedInvoiceDetail(
                # Core identifiers
                invoice_number=invoice_no,
                invoice_line_id=invoice_line_id,
                source_system='AUS',
                work_date=work_date,
                week_ending_date=week_ending,
                pay_type=pay_type,
                hours_quantity=float(row.get('Hours', 0)) if pd.notna(row.get('Hours')) else 0,
                bill_rate=float(row.get('Bill Rate', 0)) if pd.notna(row.get('Bill Rate')) else 0,
                bill_amount=float(row.get('Bill Amount', 0)) if pd.notna(row.get('Bill Amount')) else 0,
                created_timestamp=datetime.now(),
                source_file=source_file,
                source_row_number=row_idx,
                
                # Organizational hierarchy - properly populated!
                emid=emid,
                business_unit=bldg_business_unit,  # Building's business unit
                mc_service_area=service_area,
                building_code=building_code,
                job_number=job_number,
                job_code=job_code,
                
                # Additional regional data (these need to be added to UnifiedInvoiceDetail)
                # building_region=building_region,
                # onelink_region=onelink_region,
                
                # Customer info
                customer_number=str(row.get('Customer Number', '')),
                customer_po=row.get('PO'),
                
                # Position info
                position_description=row.get('Post Description'),
                
                # Employee info
                employee_number=int(emp_no) if pd.notna(emp_no) and str(emp_no) != 'UNKNOWN' else None,
                employee_full_name=row.get('Employee Name'),
                
                # Time and attendance
                shift_start_time=row.get('In Time'),
                shift_end_time=row.get('Out Time'),
                lunch_minutes=float(row.get('Lunch', 0)) * 60 if pd.notna(row.get('Lunch')) else None,
                
                # Pay info
                pay_description=pay_desc,
                pay_rate=float(row.get('Pay Rate')) if pd.notna(row.get('Pay Rate')) else None,
                
                # Billing codes
                bill_category_number=int(row.get('Bill Cat Number')) if pd.notna(row.get('Bill Cat Number')) else None
            )
            
        except Exception as e:
            self.transformation_stats['errors'].append(
                f"AUS row {row_idx}: {str(e)}"
            )
            return None
    
    def transform_bci_file(self, filepath: str) -> pd.DataFrame:
        """Transform entire BCI file using fixed logic"""
        print(f"Transforming BCI file: {filepath}")
        
        # Reset specific stats
        self.transformation_stats['bci_rows_processed'] = 0
        
        # Read file
        bci_df = pd.read_excel(filepath) if filepath.endswith('.xlsx') else pd.read_csv(filepath)
        
        # Transform each row
        unified_records = []
        for idx, row in bci_df.iterrows():
            unified = self.transform_bci_row(row, idx, filepath)
            if unified:
                unified_records.append(unified.to_dict())
                self.transformation_stats['bci_rows_processed'] += 1
        
        # Create DataFrame
        result_df = pd.DataFrame(unified_records)
        print(f"  ✓ Transformed {len(result_df)} BCI records")
        
        return result_df
    
    def transform_aus_file(self, filepath: str) -> pd.DataFrame:
        """Transform entire AUS file using fixed logic"""
        print(f"Transforming AUS file: {filepath}")
        
        # Reset specific stats
        self.transformation_stats['aus_rows_processed'] = 0
        
        # Read file
        aus_df = pd.read_csv(filepath) if filepath.endswith('.csv') else pd.read_excel(filepath)
        
        # Transform each row
        unified_records = []
        for idx, row in aus_df.iterrows():
            unified = self.transform_aus_row(row, idx, filepath)
            if unified:
                unified_records.append(unified.to_dict())
                self.transformation_stats['aus_rows_processed'] += 1
        
        # Create DataFrame
        result_df = pd.DataFrame(unified_records)
        print(f"  ✓ Transformed {len(result_df)} AUS records")
        
        return result_df
    
    def generate_transformation_report(self) -> pd.DataFrame:
        """Generate a detailed transformation report"""
        report_data = {
            'Metric': [],
            'Value': []
        }
        
        # Basic stats
        report_data['Metric'].extend([
            'BCI Rows Processed',
            'AUS Rows Processed',
            'Invoices Matched to EDI',
            'Invoices Not Found in EDI',
            'Building Code Fallbacks Used',
            'Multiple Building Codes Flagged',
            'Total Errors',
            'Total Warnings'
        ])
        
        report_data['Value'].extend([
            self.transformation_stats['bci_rows_processed'],
            self.transformation_stats['aus_rows_processed'],
            self.transformation_stats['invoices_matched'],
            len(self.transformation_stats['invoices_not_found']),
            self.transformation_stats['building_code_fallbacks'],
            len(self.transformation_stats['multiple_building_codes_flagged']),
            len(self.transformation_stats['errors']),
            len(self.transformation_stats['warnings'])
        ])
        
        return pd.DataFrame(report_data)


def reprocess_with_fixed_transformer():
    """Reprocess the data with the fixed transformer"""
    
    print("REPROCESSING WITH FIXED DIMENSION LOOKUPS")
    print("=" * 60)
    
    # Check if dimensions exist
    if not os.path.exists("clean_dimensions.xlsx"):
        print("❌ clean_dimensions.xlsx not found. Run the main transformation first.")
        return
    
    print("\n1. Initializing fixed transformer...")
    transformer = FixedEDIBasedTransformer("clean_dimensions.xlsx")
    
    print("\n2. Transforming BCI invoice details...")
    bci_unified = transformer.transform_bci_file("invoice_details_bci.csv")
    print(f"   ✓ Transformed {len(bci_unified)} BCI records")
    
    print("\n3. Transforming AUS invoice details...")
    aus_unified = transformer.transform_aus_file("invoice_details_aus.csv")
    print(f"   ✓ Transformed {len(aus_unified)} AUS records")
    
    print("\n4. Combining data...")
    unified_df = pd.concat([bci_unified, aus_unified], ignore_index=True)
    print(f"   ✓ Combined {len(unified_df)} total records")
    
    # Check field population
    print("\n5. Checking field population...")
    for field in ['business_unit', 'job_code', 'emid', 'building_code']:
        if field in unified_df.columns:
            filled = unified_df[field].notna().sum()
            print(f"   {field}: {filled}/{len(unified_df)} filled ({filled/len(unified_df)*100:.1f}%)")
    
    # Check for flagged multiple building codes
    if transformer.transformation_stats['multiple_building_codes_flagged']:
        print(f"\n⚠️  Multiple building codes flagged for investigation:")
        for flag in transformer.transformation_stats['multiple_building_codes_flagged'][:5]:
            print(f"   Invoice: {flag['invoice']}, Location/Job: {flag.get('location', flag.get('job'))}")
            print(f"   Building codes: {flag['building_codes']}")
    
    # Export
    print("\n6. Exporting fixed data...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # CSV export
    csv_file = f"unified_invoice_data_fixed_{timestamp}.csv"
    unified_df.to_csv(csv_file, index=False)
    print(f"   ✓ Exported to {csv_file}")
    
    # Try Excel export with multiple sheets
    try:
        excel_file = f"unified_invoice_data_fixed_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main data
            unified_df.to_excel(writer, sheet_name='Unified_Invoice_Details', index=False)
            
            # Field population summary
            summary_data = {
                'Field': ['business_unit', 'job_code', 'emid', 'building_code'],
                'Records_Filled': [],
                'Percentage': []
            }
            
            for field in summary_data['Field']:
                if field in unified_df.columns:
                    filled = unified_df[field].notna().sum()
                    summary_data['Records_Filled'].append(filled)
                    summary_data['Percentage'].append(f"{filled/len(unified_df)*100:.1f}%")
                else:
                    summary_data['Records_Filled'].append(0)
                    summary_data['Percentage'].append("0.0%")
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Field_Population_Summary', index=False)
            
            # Transformation report
            report_df = transformer.generate_transformation_report()
            report_df.to_excel(writer, sheet_name='Transformation_Report', index=False)
            
            # Multiple building codes for investigation
            if transformer.transformation_stats['multiple_building_codes_flagged']:
                flags_df = pd.DataFrame(transformer.transformation_stats['multiple_building_codes_flagged'])
                flags_df.to_excel(writer, sheet_name='Multiple_Building_Codes', index=False)
        
        print(f"   ✓ Also exported to {excel_file}")
    except:
        print("   ⚠️ Excel export failed, but CSV is available")
    
    print("\n" + "="*60)
    print("✅ REPROCESSING COMPLETE!")
    print("=" * 60)
    
    return unified_df


if __name__ == "__main__":
    # Note for user about updating unified schema
    print("\n" + "="*60)
    print("IMPORTANT: Before running this script...")
    print("=" * 60)
    print("\n1. Update unified_invoice_schema.py to add these fields to UnifiedInvoiceDetail:")
    print("   - building_region: Optional[str] = None")
    print("   - onelink_region: Optional[str] = None")
    print("\n2. Then run: python fixed_edi_transformer.py")
    print("\nThis will reprocess all data with the fixed lookup logic.")
    print("=" * 60)
    
    # Uncomment to run
    unified_df = reprocess_with_fixed_transformer()