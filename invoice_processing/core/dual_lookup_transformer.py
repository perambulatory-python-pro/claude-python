"""
Dual Lookup Transformer
Handles both EDI lookups (for historical) and Location/Job lookups (for new invoices)
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from typing import Dict, List, Optional, Set, Tuple
from invoice_processing.core.unified_invoice_schema import UnifiedInvoiceDetail, PayType, standardize_pay_type


class DualLookupTransformer:
    def __init__(self, dimensions_file: str = "clean_dimensions.xlsx",
                 master_lookup_file: str = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx",
                 emid_file: str = "emid_job_bu_table.xlsx"):
        """
        Initialize transformer with all lookup sources
        """
        self.dimensions_file = dimensions_file
        self.master_lookup_file = master_lookup_file
        self.emid_file = emid_file
        
        self.load_dimensions()
        self.load_master_lookup()
        self.load_reference_data()
        
        self.transformation_stats = {
            'bci_rows_processed': 0,
            'aus_rows_processed': 0,
            'invoices_matched_edi': 0,
            'invoices_matched_fallback': 0,
            'invoices_not_found': set(),
            'building_codes_from_edi': 0,
            'building_codes_from_location': 0,
            'building_codes_from_job': 0,
            'building_codes_not_found': 0,
            'multiple_building_codes_flagged': [],
            'errors': [],
            'warnings': []
        }
    
    def load_dimensions(self):
        """Load dimension tables from EDI-based processing"""
        print("Loading dimension tables...")
        
        try:
            self.invoice_dim = pd.read_excel(self.dimensions_file, sheet_name='Invoice_Dim')
            self.building_dim = pd.read_excel(self.dimensions_file, sheet_name='Building_Dim')
            self.service_area_dim = pd.read_excel(self.dimensions_file, sheet_name='Service_Area_Dim')
            self.job_mapping_dim = pd.read_excel(self.dimensions_file, sheet_name='Job_Mapping_Dim')
            
            # Create lookup dictionaries
            self.create_edi_lookups()
            
            print(f"  ✓ Loaded {len(self.invoice_dim)} EDI invoices")
            print(f"  ✓ Loaded {len(self.building_dim)} buildings")
        except Exception as e:
            print(f"  ⚠️  Error loading dimensions: {e}")
            self.invoice_dim = pd.DataFrame()
            self.building_dim = pd.DataFrame()
    
    def load_master_lookup(self):
        """Load master lookup for Location/Job No to building code mapping"""
        print("Loading master lookup...")
        
        try:
            self.master_lookup_df = pd.read_excel(self.master_lookup_file, sheet_name='Master Lookup',header=0)
            print(f"  ✓ Loaded {len(self.master_lookup_df)} master lookup records")
        except Exception as e:
            print(f"  ⚠️  Error loading master lookup: {e}")
            self.master_lookup_df = pd.DataFrame()
    
    def load_reference_data(self):
        """Load EMID and building reference data"""
        print("Loading reference data...")
        
        try:
            # Load EMID reference
            self.emid_ref = pd.read_excel(self.emid_file, sheet_name="emid_job_code")
            self.building_ref = pd.read_excel(self.emid_file, sheet_name="buildings")
            
            # Create additional lookups
            self.create_reference_lookups()
            
            print(f"  ✓ Loaded {len(self.emid_ref)} EMID mappings")
            print(f"  ✓ Loaded {len(self.building_ref)} building references")
        except Exception as e:
            print(f"  ⚠️  Error loading reference data: {e}")
    
    def create_edi_lookups(self):
        """Create lookups from EDI-based dimensions"""
        # Invoice lookup (handle NaN properly)
        self.invoice_dim['INVOICE_NUMBER'] = self.invoice_dim['INVOICE_NUMBER'].astype(str)
        self.edi_invoice_lookup = {}
        
        for _, row in self.invoice_dim.iterrows():
            invoice_no = row['INVOICE_NUMBER']
            self.edi_invoice_lookup[invoice_no] = {
                k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()
            }
        
        # Building lookup
        self.building_lookup = {}
        for _, row in self.building_dim.iterrows():
            building_code = row['BUILDING_CODE']
            self.building_lookup[building_code] = {
                'EMID': row.get('EMID'),
                'SERVICE_AREA': row.get('SERVICE_AREA'),
                'BLDG_BUSINESS_UNIT': row.get('BUSINESS_UNIT')
            }
    
    def create_reference_lookups(self):
        """Create lookups from reference files"""
        # EMID to job code
        self.emid_to_job_code = {}
        for _, row in self.emid_ref.iterrows():
            self.emid_to_job_code[row['emid']] = {
                'job_code': row.get('job_code'),
                'description': row.get('description'),
                'region': row.get('region')
            }
        
        # Building reference with business unit
        self.building_ref_lookup = {}
        for _, row in self.building_ref.iterrows():
            building_code = row['building_code']
            self.building_ref_lookup[building_code] = {
                'emid': row.get('emid'),
                'mc_service_area': row.get('mc_service_area'),
                'business_unit': row.get('business_unit'),
                'kp_loc_ref': row.get('kp_loc_ref')
            }
        
        # LOC to building lookup
        self.loc_to_building = {}
        for _, row in self.building_ref.iterrows():
            if pd.notna(row.get('kp_loc_ref')):
                loc_ref = str(int(row['kp_loc_ref']))
                self.loc_to_building[loc_ref] = row['building_code']
    
    def get_building_code_for_bci(self, invoice_no: str, location_number: str) -> Tuple[Optional[str], Optional[str], str]:
        """
        Get building code for BCI invoice
        Returns: (building_code, emid, source)
        """
        # 1. First try EDI lookup
        if invoice_no in self.edi_invoice_lookup:
            edi_data = self.edi_invoice_lookup[invoice_no]
            building_code = edi_data.get('BUILDING_CODE')
            if building_code:
                self.transformation_stats['building_codes_from_edi'] += 1
                return building_code, edi_data.get('EMID'), 'EDI'
        
        # 2. If not in EDI or no building code, use Location_Number
        if location_number:
            # Look in Master Lookup
            matches = self.master_lookup_df[
                self.master_lookup_df['Location/Job No'].astype(str).str.strip() == str(location_number).strip()
            ]
            
            if len(matches) > 0:
                unique_buildings = matches['building_code'].dropna().unique()
                
                if len(unique_buildings) == 1:
                    self.transformation_stats['building_codes_from_location'] += 1
                    # Get EMID from the match
                    emid = matches.iloc[0].get('EMID')
                    return unique_buildings[0], emid, 'Location'
                
                elif len(unique_buildings) > 1:
                    # Try to disambiguate with EMID from EDI if available
                    if invoice_no in self.edi_invoice_lookup:
                        edi_emid = self.edi_invoice_lookup[invoice_no].get('EMID')
                        if edi_emid:
                            emid_matches = matches[matches['EMID'] == edi_emid]
                            emid_buildings = emid_matches['building_code'].dropna().unique()
                            if len(emid_buildings) == 1:
                                self.transformation_stats['building_codes_from_location'] += 1
                                return emid_buildings[0], edi_emid, 'Location+EMID'
                    
                    # Log multiple buildings
                    self.transformation_stats['multiple_building_codes_flagged'].append({
                        'invoice': invoice_no,
                        'location': location_number,
                        'building_codes': list(unique_buildings)
                    })
        
        self.transformation_stats['building_codes_not_found'] += 1
        return None, None, 'NotFound'
    
    def get_building_code_for_aus(self, invoice_no: str, job_number: str) -> Tuple[Optional[str], Optional[str], str]:
        """
        Get building code for AUS invoice
        Returns: (building_code, emid, source)
        """
        # 1. First try EDI lookup
        if invoice_no in self.edi_invoice_lookup:
            edi_data = self.edi_invoice_lookup[invoice_no]
            building_code = edi_data.get('BUILDING_CODE')
            if building_code:
                self.transformation_stats['building_codes_from_edi'] += 1
                return building_code, edi_data.get('EMID'), 'EDI'
        
        # 2. If not in EDI or no building code, use Job Number
        if job_number and self.master_lookup_df is not None and not self.master_lookup_df.empty:
            # FIX: Add string conversion and stripping
            job_number_clean = str(job_number).strip()
            
            # Look in Master Lookup with proper string handling
            matches = self.master_lookup_df[
                self.master_lookup_df['Location/Job No'].astype(str).str.strip() == job_number_clean
            ]
            
            if len(matches) > 0:
                unique_buildings = matches['building_code'].dropna().unique()
                
                if len(unique_buildings) == 1:
                    self.transformation_stats['building_codes_from_job'] += 1
                    emid = matches.iloc[0].get('EMID')
                    return unique_buildings[0], emid, 'Job'
                
                elif len(unique_buildings) > 1:
                    # Multiple buildings logic...
                    self.transformation_stats['multiple_building_codes_flagged'].append({
                        'invoice': invoice_no,
                        'job': job_number,
                        'building_codes': list(unique_buildings)
                    })
        
        self.transformation_stats['building_codes_not_found'] += 1
        return None, None, 'NotFound'
    
    def get_dimensional_data(self, building_code: str, emid: str = None) -> Dict:
        """Get all dimensional data for a building code"""
        result = {
            'bldg_business_unit': None,
            'mc_service_area': None,
            'job_code': None,
            'building_region': None
        }
        
        # First check our dimension tables
        if building_code in self.building_lookup:
            dim_data = self.building_lookup[building_code]
            result['bldg_business_unit'] = dim_data.get('BLDG_BUSINESS_UNIT')
            result['mc_service_area'] = dim_data.get('SERVICE_AREA')
            if not emid:
                emid = dim_data.get('EMID')
        
        # Also check reference data
        if building_code in self.building_ref_lookup:
            ref_data = self.building_ref_lookup[building_code]
            if not result['bldg_business_unit']:
                result['bldg_business_unit'] = ref_data.get('business_unit')
            if not result['mc_service_area']:
                result['mc_service_area'] = ref_data.get('mc_service_area')
            if not emid:
                emid = ref_data.get('emid')
        
        # Get EMID-based data
        if emid and emid in self.emid_to_job_code:
            emid_data = self.emid_to_job_code[emid]
            result['job_code'] = emid_data.get('job_code')
            result['building_region'] = emid_data.get('region')
        
        return result
    
    def transform_bci_row(self, row: pd.Series, row_idx: int, source_file: str) -> Optional[UnifiedInvoiceDetail]:
        """Transform a single BCI row with dual lookup strategy"""
        try:
            invoice_no = str(row['Invoice_No'])
            location_number = str(row.get('Location_Number', ''))
            
            # Get building code and EMID
            building_code, emid, source = self.get_building_code_for_bci(invoice_no, location_number)
            
            # Get dimensional data
            if building_code:
                dims = self.get_dimensional_data(building_code, emid)
            else:
                dims = {
                    'bldg_business_unit': None,
                    'mc_service_area': None,
                    'job_code': None,
                    'building_region': None
                }
            
            # Try to get job_code from Customer field if not found
            if not dims['job_code'] and pd.notna(row.get('Customer')):
                customer_str = str(row['Customer'])
                if ':' in customer_str:
                    dims['job_code'] = customer_str.split(':', 1)[1].strip()
            
            # Get onelink_region from EDI if available
            onelink_region = None
            if invoice_no in self.edi_invoice_lookup:
                onelink_region = self.edi_invoice_lookup[invoice_no].get('REGION')
            
            # Determine pay type
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
                
                # Organizational hierarchy
                emid=emid,
                business_unit=dims['bldg_business_unit'],
                mc_service_area=dims['mc_service_area'],
                building_code=building_code,
                location_name=row.get('Location'),
                location_number=location_number,
                job_code=dims['job_code'],
                
                # Regional data
                building_region=dims['building_region'],
                onelink_region=onelink_region,
                
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
        """Transform a single AUS row with dual lookup strategy"""
        try:
            invoice_no = str(row['Invoice Number'])
            job_number = str(row.get('Job Number', ''))
            
            # DEBUG: Count how many times we try to look up jobs
            if not hasattr(self, 'aus_job_lookup_attempts'):
                self.aus_job_lookup_attempts = 0
                self.aus_job_lookup_success = 0

            self.aus_job_lookup_attempts += 1

            building_code, emid, source = self.get_building_code_for_aus(invoice_no, job_number)

            if source == 'Job':
                self.aus_job_lookup_success += 1

            # Get building code and EMID
            building_code, emid, source = self.get_building_code_for_aus(invoice_no, job_number)
            
            # Get dimensional data
            if building_code:
                dims = self.get_dimensional_data(building_code, emid)
            else:
                dims = {
                    'bldg_business_unit': None,
                    'mc_service_area': None,
                    'job_code': None,
                    'building_region': None
                }
            
            # Get onelink_region from EDI if available
            onelink_region = None
            if invoice_no in self.edi_invoice_lookup:
                onelink_region = self.edi_invoice_lookup[invoice_no].get('REGION')
            
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
                
                # Organizational hierarchy
                emid=emid,
                business_unit=dims['bldg_business_unit'],
                mc_service_area=dims['mc_service_area'],
                building_code=building_code,
                job_number=job_number,
                job_code=dims['job_code'],
                
                # Regional data
                building_region=dims['building_region'],
                onelink_region=onelink_region,
                
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
        """Transform entire BCI file"""
        print(f"Transforming BCI file: {filepath}")
        
        self.transformation_stats['bci_rows_processed'] = 0
        
        bci_df = pd.read_excel(filepath) if filepath.endswith('.xlsx') else pd.read_csv(filepath)
        
        unified_records = []
        for idx, row in bci_df.iterrows():
            unified = self.transform_bci_row(row, idx, filepath)
            if unified:
                unified_records.append(unified.to_dict())
                self.transformation_stats['bci_rows_processed'] += 1
        
        result_df = pd.DataFrame(unified_records)
        print(f"  ✓ Transformed {len(result_df)} BCI records")
        
        return result_df
    
    def transform_aus_file(self, filepath: str) -> pd.DataFrame:
        """Transform entire AUS file"""
        print(f"Transforming AUS file: {filepath}")
        
        self.transformation_stats['aus_rows_processed'] = 0
        
        aus_df = pd.read_csv(filepath) if filepath.endswith('.csv') else pd.read_excel(filepath)
        
        unified_records = []
        for idx, row in aus_df.iterrows():
            unified = self.transform_aus_row(row, idx, filepath)
            if unified:
                unified_records.append(unified.to_dict())
                self.transformation_stats['aus_rows_processed'] += 1
        
        result_df = pd.DataFrame(unified_records)
        print(f"  ✓ Transformed {len(result_df)} AUS records")

        if hasattr(self, 'aus_job_lookup_attempts'):
            print(f"\nDEBUG: AUS Job Lookup Stats:")
            print(f"  Attempts: {self.aus_job_lookup_attempts}")
            print(f"  Success: {self.aus_job_lookup_success}")
            print(f"  Master lookup loaded: {hasattr(self, 'master_lookup_df') and not self.master_lookup_df.empty}")
        
        return result_df
    
    def generate_detailed_report(self) -> pd.DataFrame:
        """Generate detailed transformation report"""
        report_data = {
            'Metric': [
                'BCI Rows Processed',
                'AUS Rows Processed',
                'Building Codes from EDI',
                'Building Codes from Location (BCI)',
                'Building Codes from Job (AUS)',
                'Building Codes Not Found',
                'Multiple Building Codes Flagged',
                'Total Errors'
            ],
            'Value': [
                self.transformation_stats['bci_rows_processed'],
                self.transformation_stats['aus_rows_processed'],
                self.transformation_stats['building_codes_from_edi'],
                self.transformation_stats['building_codes_from_location'],
                self.transformation_stats['building_codes_from_job'],
                self.transformation_stats['building_codes_not_found'],
                len(self.transformation_stats['multiple_building_codes_flagged']),
                len(self.transformation_stats['errors'])
            ]
        }
        
        return pd.DataFrame(report_data)


def run_dual_lookup_transformation():
    """Run the dual lookup transformation"""
    
    print("DUAL LOOKUP TRANSFORMATION")
    print("=" * 60)
    print("Using both EDI (historical) and Location/Job (new) lookups")
    print("=" * 60)
    
    # Initialize transformer
    transformer = DualLookupTransformer()
    
    # Transform files
    print("\n1. Transforming BCI invoice details...")
    bci_unified = transformer.transform_bci_file("invoice_details_bci.csv")
    
    print("\n2. Transforming AUS invoice details...")
    aus_unified = transformer.transform_aus_file("invoice_details_aus.csv")
    
    print("\n3. Combining data...")
    unified_df = pd.concat([bci_unified, aus_unified], ignore_index=True)
    print(f"   ✓ Combined {len(unified_df)} total records")
    
    # Check field population
    print("\n4. Field Population Results:")
    for field in ['business_unit', 'job_code', 'emid', 'building_code']:
        if field in unified_df.columns:
            filled = unified_df[field].notna().sum()
            print(f"   {field}: {filled}/{len(unified_df)} filled ({filled/len(unified_df)*100:.1f}%)")
    
    # Show lookup source breakdown
    print("\n5. Building Code Source Breakdown:")
    report_df = transformer.generate_detailed_report()
    print(report_df.to_string(index=False))
    
    # Export
    print("\n6. Exporting results...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    csv_file = f"unified_invoice_dual_lookup_{timestamp}.csv"
    unified_df.to_csv(csv_file, index=False)
    print(f"   ✓ Exported to {csv_file}")
    
    # Excel export with detailed report
    excel_file = f"unified_invoice_dual_lookup_{timestamp}.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        unified_df.to_excel(writer, sheet_name='Unified_Invoice_Details', index=False)
        report_df.to_excel(writer, sheet_name='Lookup_Source_Report', index=False)
        
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
        
        # Multiple building codes
        if transformer.transformation_stats['multiple_building_codes_flagged']:
            flags_df = pd.DataFrame(transformer.transformation_stats['multiple_building_codes_flagged'])
            flags_df.to_excel(writer, sheet_name='Multiple_Building_Codes', index=False)

        # Records without building codes
        no_building_df = unified_df[unified_df['building_code'].isna()].copy()
        if len(no_building_df) > 0:
            # Select key columns for investigation
            investigation_cols = ['invoice_number', 'source_system', 'location_number', 
                                'job_number', 'work_date', 'employee_number', 'bill_amount']
            # Only include columns that exist
            cols_to_export = [col for col in investigation_cols if col in no_building_df.columns]
            no_building_summary = no_building_df[cols_to_export]
            no_building_summary.to_excel(writer, sheet_name='No_Building_Code', index=False)

        # Export transformation errors
        if transformer.transformation_stats['errors']:
            errors_df = pd.DataFrame({
                'Error': transformer.transformation_stats['errors']
            })
            errors_df.to_excel(writer, sheet_name='Transformation_Errors', index=False)
        
    
    print(f"   ✓ Detailed report exported to {excel_file}")
    
    print("\n" + "="*60)
    print("✅ DUAL LOOKUP TRANSFORMATION COMPLETE!")
    print("=" * 60)
    
    return unified_df


if __name__ == "__main__":
    unified_df = run_dual_lookup_transformation()
