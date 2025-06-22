"""
EDI-Based Invoice Transformer
Uses EDI data as source of truth for dimensional assignments
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from unified_invoice_schema import UnifiedInvoiceDetail, PayType, standardize_pay_type


class EDIBasedTransformer:
    def __init__(self, dimensions_file: str = "clean_dimensions.xlsx"):
        """
        Initialize transformer with clean dimension tables
        
        Args:
            dimensions_file: Path to clean dimensions Excel file
        """
        self.dimensions_file = dimensions_file
        self.load_dimensions()
        
        self.transformation_stats = {
            'bci_rows_processed': 0,
            'aus_rows_processed': 0,
            'invoices_matched': 0,
            'invoices_not_found': set(),
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
    
    def create_lookups(self):
        """Create lookup dictionaries for performance"""
        # Invoice lookup (string keys for revision support)
        self.invoice_dim['INVOICE_NUMBER'] = self.invoice_dim['INVOICE_NUMBER'].astype(str)
        self.invoice_lookup = self.invoice_dim.set_index('INVOICE_NUMBER').to_dict('index')
        
        # Building lookup
        self.building_lookup = self.building_dim.set_index('BUILDING_CODE').to_dict('index')
        
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
        
        # Service area lookup
        self.service_area_lookup = self.service_area_dim.set_index('EMID').to_dict('index')
    
    def get_invoice_dimensions(self, invoice_number: str) -> Optional[Dict]:
        """
        Get all dimensional data for an invoice from EDI
        
        Args:
            invoice_number: Invoice number (handles revisions)
            
        Returns:
            Dictionary with EMID, building, GL info, or None
        """
        invoice_str = str(invoice_number).strip()
        
        # Direct lookup
        if invoice_str in self.invoice_lookup:
            self.transformation_stats['invoices_matched'] += 1
            return self.invoice_lookup[invoice_str]
        
        # Try without revision letter
        base_invoice = invoice_str.rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        if base_invoice != invoice_str and base_invoice in self.invoice_lookup:
            self.transformation_stats['invoices_matched'] += 1
            self.transformation_stats['warnings'].append(
                f"Invoice {invoice_str} matched to {base_invoice} (revision stripped)"
            )
            return self.invoice_lookup[base_invoice]
        
        # Not found
        self.transformation_stats['invoices_not_found'].add(invoice_str)
        return None
    
    def transform_bci_row(self, row: pd.Series, row_idx: int, source_file: str) -> Optional[UnifiedInvoiceDetail]:
        """Transform a single BCI row using EDI dimensions"""
        try:
            # Get invoice dimensions from EDI
            invoice_no = str(row['Invoice_No'])
            invoice_dims = self.get_invoice_dimensions(invoice_no)
            
            if not invoice_dims:
                # Invoice not in EDI - use fallback logic
                self.transformation_stats['warnings'].append(
                    f"BCI Invoice {invoice_no} not found in EDI"
                )
                # Create minimal record
                emid = None
                service_area = None
                building_code = None
                business_unit = None
            else:
                # Use EDI dimensions
                emid = invoice_dims['EMID']
                service_area = invoice_dims['SERVICE_AREA']
                building_code = invoice_dims.get('BUILDING_CODE')
                
                # Get business unit from building dimension
                if building_code and building_code in self.building_lookup:
                    business_unit = self.building_lookup[building_code].get('BUSINESS_UNIT')
                else:
                    business_unit = None
            
            # Determine pay type based on which hours field has data
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
                
                # Organizational hierarchy from EDI
                emid=emid,
                business_unit=business_unit,
                mc_service_area=service_area,
                building_code=building_code,
                location_name=row.get('Location'),
                location_number=row.get('Location_Number'),
                
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
                work_date=work_date,
                week_ending_date=week_ending,
                shift_start_time=row.get('Shift_In'),
                shift_end_time=row.get('Shift_Out'),
                
                # Pay info
                pay_type=pay_type,
                hours_quantity=float(hours) if pd.notna(hours) else 0,
                bill_rate=float(rate) if pd.notna(rate) else 0,
                bill_amount=float(amount) if pd.notna(amount) else 0,
                
                # Billing codes
                billing_code=str(row.get('Billing_Code', '')),
                
                # Audit fields
                created_timestamp=datetime.now(),
                source_file=source_file,
                source_row_number=row_idx
            )
            
        except Exception as e:
            self.transformation_stats['errors'].append(
                f"BCI row {row_idx}: {str(e)}"
            )
            return None
    
    def transform_aus_row(self, row: pd.Series, row_idx: int, source_file: str) -> Optional[UnifiedInvoiceDetail]:
        """Transform a single AUS row using EDI dimensions"""
        try:
            # Get invoice dimensions from EDI
            invoice_no = str(row['Invoice Number'])
            invoice_dims = self.get_invoice_dimensions(invoice_no)
            
            # Also try to get EMID from job number
            job_number = str(row.get('Job Number', ''))
            job_emids = self.job_to_emid.get(job_number, [])
            
            if not invoice_dims:
                # Invoice not in EDI - try job mapping
                if job_emids:
                    # Use first EMID from job mapping
                    emid = job_emids[0]['EMID']
                    if emid in self.service_area_lookup:
                        service_area = self.service_area_lookup[emid]['MC SERVICE AREA']
                    else:
                        service_area = None
                    
                    self.transformation_stats['warnings'].append(
                        f"AUS Invoice {invoice_no} not in EDI, using job {job_number} → EMID {emid}"
                    )
                else:
                    # No mapping found
                    self.transformation_stats['warnings'].append(
                        f"AUS Invoice {invoice_no} not found in EDI, job {job_number} not mapped"
                    )
                    emid = None
                    service_area = None
                
                building_code = None
                business_unit = None
            else:
                # Use EDI dimensions
                emid = invoice_dims['EMID']
                service_area = invoice_dims['SERVICE_AREA']
                building_code = invoice_dims.get('BUILDING_CODE')
                
                # Validate against job mapping
                if job_emids:
                    job_emid_list = [j['EMID'] for j in job_emids]
                    if emid not in job_emid_list:
                        self.transformation_stats['warnings'].append(
                            f"Invoice {invoice_no}: EDI EMID {emid} differs from job mapping {job_emid_list}"
                        )
                
                # Get business unit
                if building_code and building_code in self.building_lookup:
                    business_unit = self.building_lookup[building_code].get('BUSINESS_UNIT')
                else:
                    business_unit = None
            
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
                
                # Organizational hierarchy
                emid=emid,
                business_unit=business_unit,
                mc_service_area=service_area,
                building_code=building_code,
                job_number=job_number,
                
                # Customer info
                customer_number=str(row.get('Customer Number', '')),
                customer_po=row.get('PO'),
                
                # Position info
                position_description=row.get('Post Description'),
                
                # Employee info
                employee_number=int(emp_no) if pd.notna(emp_no) and str(emp_no) != 'UNKNOWN' else None,
                employee_full_name=row.get('Employee Name'),
                
                # Time and attendance
                work_date=work_date,
                week_ending_date=week_ending,
                shift_start_time=row.get('In Time'),
                shift_end_time=row.get('Out Time'),
                lunch_minutes=float(row.get('Lunch', 0)) * 60 if pd.notna(row.get('Lunch')) else None,
                
                # Pay info
                pay_type=pay_type,
                pay_description=pay_desc,
                hours_quantity=float(row.get('Hours', 0)) if pd.notna(row.get('Hours')) else 0,
                pay_rate=float(row.get('Pay Rate')) if pd.notna(row.get('Pay Rate')) else None,
                bill_rate=float(row.get('Bill Rate', 0)) if pd.notna(row.get('Bill Rate')) else 0,
                bill_amount=float(row.get('Bill Amount', 0)) if pd.notna(row.get('Bill Amount')) else 0,
                
                # Billing codes
                bill_category_number=int(row.get('Bill Cat Number')) if pd.notna(row.get('Bill Cat Number')) else None,
                
                # Audit fields
                created_timestamp=datetime.now(),
                source_file=source_file,
                source_row_number=row_idx
            )
            
        except Exception as e:
            self.transformation_stats['errors'].append(
                f"AUS row {row_idx}: {str(e)}"
            )
            return None
    
    def transform_bci_file(self, filepath: str) -> pd.DataFrame:
        """Transform entire BCI file using EDI dimensions"""
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
        """Transform entire AUS file using EDI dimensions"""
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
            'Total Errors',
            'Total Warnings'
        ])
        
        report_data['Value'].extend([
            self.transformation_stats['bci_rows_processed'],
            self.transformation_stats['aus_rows_processed'],
            self.transformation_stats['invoices_matched'],
            len(self.transformation_stats['invoices_not_found']),
            len(self.transformation_stats['errors']),
            len(self.transformation_stats['warnings'])
        ])
        
        return pd.DataFrame(report_data)


# Example usage
if __name__ == "__main__":
    # First create dimension tables
    from dimension_table_creator import DimensionTableCreator
    
    print("Step 1: Creating dimension tables...")
    creator = DimensionTableCreator()
    creator.export_all_dimensions("clean_dimensions.xlsx")
    
    print("\nStep 2: Transforming invoice details using EDI dimensions...")
    transformer = EDIBasedTransformer("clean_dimensions.xlsx")
    
    # Transform files
    if os.path.exists("invoice_details_bci.csv"):
        bci_unified = transformer.transform_bci_file("invoice_details_bci.csv")
    
    if os.path.exists("invoice_details_aus.csv"):
        aus_unified = transformer.transform_aus_file("invoice_details_aus.csv")
    
    # Generate report
    report = transformer.generate_transformation_report()
    print("\nTransformation Report:")
    print(report.to_string(index=False))
    
    # Show sample of unmatched invoices
    if transformer.transformation_stats['invoices_not_found']:
        print(f"\nSample of invoices not found in EDI:")
        for inv in list(transformer.transformation_stats['invoices_not_found'])[:10]:
            print(f"  - {inv}")
