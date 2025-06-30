"""
Invoice Details Transformer
Converts BCI and AUS formats to unified schema for internal analysis
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import numpy as np
from invoice_processing.core.unified_invoice_schema import UnifiedInvoiceDetail, PayType, standardize_pay_type


class InvoiceTransformer:
    def __init__(self, emid_mapping_path: str = "emid_job_bu_table.xlsx",
                 master_lookup_path: str = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx"):
        """
        Initialize transformer with reference data
        
        Args:
            emid_mapping_path: Path to EMID/Building reference file
            master_lookup_path: Path to master validation lookup file
        """
        # Use enhanced lookup manager instead of basic reference loading
        from invoice_processing.core.enhanced_lookup_manager import EnhancedLookupManager
        self.lookup_manager = EnhancedLookupManager(emid_mapping_path, master_lookup_path)
        
        self.transformation_stats = {
            'bci_rows_processed': 0,
            'aus_rows_processed': 0,
            'errors': [],
            'warnings': [],
            'unmapped_aus_jobs': set(),
            'unmapped_bci_locations': set()
        }

    
    def transform_bci_row(self, row: pd.Series, row_idx: int, source_file: str) -> Optional[UnifiedInvoiceDetail]:
        """Transform a single BCI row to unified format"""
        try:
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
            
            # Parse work date
            work_date = pd.to_datetime(row['Date']).date()
            week_ending = pd.to_datetime(row['Weekending_Date']).date()
            
            # Look up building info using enhanced manager
            location_number = row.get('Location_Number', '')
            location_name = row.get('Location', '')
            building_info = self.lookup_building_info(
                location_number=location_number,
                location_name=location_name
            )
            
            # Get complete dimensions if we found a building
            if building_info.get('building_code'):
                complete_info = self.lookup_manager.get_complete_dimensions(building_info['building_code'])
                building_info.update(complete_info)
            
            # Create unique line ID
            emp_no = row.get('Emp_No', 'UNKNOWN')
            invoice_no_str = str(row['Invoice_No'])  # Handle as string for revisions
            invoice_line_id = f"{invoice_no_str}_{emp_no}_{work_date}_{pay_type.value}"
            
            return UnifiedInvoiceDetail(
                # Core identifiers
                invoice_number=invoice_no_str,  # Now stored as string
                invoice_line_id=invoice_line_id,
                source_system='BCI',
                
                # Organizational hierarchy
                emid=building_info.get('emid'),
                business_unit=row.get('Business_Unit'),
                mc_service_area=building_info.get('mc_service_area'),
                building_code=building_info.get('building_code'),
                location_name=row.get('Location'),
                location_number=location_number,
                
                # Customer info
                customer_name=row.get('Customer'),
                customer_number=str(row.get('Customer_Number', '')),
                
                # Position info
                position_description=row.get('Position'),
                position_number=row.get('Position_Number'),
                
                # Employee info
                employee_number=int(emp_no) if pd.notna(emp_no) and emp_no != 'UNKNOWN' else None,
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
        """Transform a single AUS row to unified format"""
        try:
            # Parse dates
            work_date = pd.to_datetime(row['Work Date']).date()
            week_ending = pd.to_datetime(row['Week Ending']).date()
            
            # Determine pay type
            pay_desc = row.get('Pay Hours Description', '')
            pay_type = standardize_pay_type(pay_desc)
            
            # Extract job code from Job Number and get full mapping
            job_number = str(row.get('Job Number', ''))
            job_info = self.lookup_job_info(job_number)
            
            # Get building code from job info
            building_code = job_info.get('building_code')
            
            # Get complete dimensions if we found a building code
            if building_code:
                complete_info = self.lookup_manager.get_complete_dimensions(building_code)
                job_info.update(complete_info)
            
            # Create unique line ID
            emp_no = row.get('Employee Number', 'UNKNOWN')
            invoice_no_str = str(row['Invoice Number'])  # Handle as string for revisions
            invoice_line_id = f"{invoice_no_str}_{emp_no}_{work_date}_{pay_type.value}"
            
            return UnifiedInvoiceDetail(
                # Core identifiers
                invoice_number=invoice_no_str,  # Now stored as string
                invoice_line_id=invoice_line_id,
                source_system='AUS',
                
                # Organizational hierarchy - now fully populated from master lookup
                emid=job_info.get('emid'),
                business_unit=job_info.get('business_unit'),  # From consolidated lookup
                mc_service_area=job_info.get('mc_service_area'),
                building_code=job_info.get('building_code'),
                location_name=job_info.get('location_name'),  # From master lookup
                job_code=job_info.get('job_code'),  # From EMID lookup
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
    
    def lookup_building_info(self, location_number: Optional[str] = None, 
                           location_name: Optional[str] = None) -> Dict:
        """Look up building information using enhanced lookup manager"""
        result = self.lookup_manager.lookup_bci_location_info(location_number, location_name)
        
        if not result and location_number:
            self.transformation_stats['unmapped_bci_locations'].add(location_number)
        
        return result
    
    def lookup_job_info(self, job_number: str) -> Dict:
        """Look up job information using enhanced lookup manager"""
        result = self.lookup_manager.lookup_aus_job_info(job_number)
        
        if not result:
            self.transformation_stats['unmapped_aus_jobs'].add(job_number)
        
        return result
    
    def transform_bci_file(self, filepath: str) -> pd.DataFrame:
        """Transform entire BCI file to unified format"""
        print(f"Transforming BCI file: {filepath}")
        
        # Read BCI file
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
        print(f"Transformed {len(result_df)} BCI records")
        
        return result_df
    
    def transform_aus_file(self, filepath: str) -> pd.DataFrame:
        """Transform entire AUS file to unified format"""
        print(f"Transforming AUS file: {filepath}")
        
        # Read AUS file
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
        print(f"Transformed {len(result_df)} AUS records")
        
        return result_df
    
    def combine_and_analyze(self, bci_file: str, aus_file: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Transform both files and combine for analysis
        
        Returns:
            Tuple of (combined_df, analysis_stats)
        """
        # Reset stats
        self.transformation_stats = {
            'bci_rows_processed': 0,
            'aus_rows_processed': 0,
            'errors': [],
            'warnings': []
        }
        
        # Transform both files
        bci_unified = self.transform_bci_file(bci_file)
        aus_unified = self.transform_aus_file(aus_file)
        
        # Combine
        combined_df = pd.concat([bci_unified, aus_unified], ignore_index=True)
        
        # Analysis stats
        analysis_stats = {
            'total_records': len(combined_df),
            'bci_records': len(bci_unified),
            'aus_records': len(aus_unified),
            'unique_invoices': combined_df['invoice_number'].nunique(),
            'unique_employees': combined_df['employee_number'].nunique(),
            'date_range': {
                'start': combined_df['work_date'].min(),
                'end': combined_df['work_date'].max()
            },
            'pay_type_distribution': combined_df['pay_type'].value_counts().to_dict(),
            'total_hours': combined_df['hours_quantity'].sum(),
            'total_billing': combined_df['bill_amount'].sum(),
            'transformation_errors': len(self.transformation_stats['errors']),
            'unmapped_records': {
                'aus_jobs': len(self.transformation_stats['unmapped_aus_jobs']),
                'bci_locations': len(self.transformation_stats['unmapped_bci_locations'])
            }
        }
        
        # Log unmapped records if any
        if self.transformation_stats['unmapped_aus_jobs']:
            print(f"\nWarning: {len(self.transformation_stats['unmapped_aus_jobs'])} unmapped AUS job numbers:")
            for job in sorted(list(self.transformation_stats['unmapped_aus_jobs']))[:10]:
                print(f"  - {job}")
            if len(self.transformation_stats['unmapped_aus_jobs']) > 10:
                print(f"  ... and {len(self.transformation_stats['unmapped_aus_jobs']) - 10} more")
        
        if self.transformation_stats['unmapped_bci_locations']:
            print(f"\nWarning: {len(self.transformation_stats['unmapped_bci_locations'])} unmapped BCI locations:")
            for loc in sorted(list(self.transformation_stats['unmapped_bci_locations']))[:10]:
                print(f"  - {loc}")
            if len(self.transformation_stats['unmapped_bci_locations']) > 10:
                print(f"  ... and {len(self.transformation_stats['unmapped_bci_locations']) - 10} more")
        
        return combined_df, analysis_stats
    
    def export_unified_data(self, unified_df: pd.DataFrame, output_path: str):
        """Export unified data to Excel with formatting"""
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Main data sheet
            unified_df.to_excel(writer, sheet_name='Unified_Invoice_Details', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Records',
                    'BCI Records',
                    'AUS Records',
                    'Unique Invoices',
                    'Unique Employees',
                    'Total Hours',
                    'Total Billing'
                ],
                'Value': [
                    len(unified_df),
                    len(unified_df[unified_df['source_system'] == 'BCI']),
                    len(unified_df[unified_df['source_system'] == 'AUS']),
                    unified_df['invoice_number'].nunique(),
                    unified_df['employee_number'].nunique(),
                    f"{unified_df['hours_quantity'].sum():,.2f}",
                    f"${unified_df['bill_amount'].sum():,.2f}"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Errors sheet if any
            if self.transformation_stats['errors']:
                error_df = pd.DataFrame({
                    'Error': self.transformation_stats['errors']
                })
                error_df.to_excel(writer, sheet_name='Transformation_Errors', index=False)
        
        print(f"Unified data exported to: {output_path}")

# Example usage
if __name__ == "__main__":
    # Initialize transformer
    transformer = InvoiceTransformer("emid_job_bu_table.xlsx")
    
    # Transform and combine files
    combined_df, stats = transformer.combine_and_analyze(
        bci_file="invoice_details_bci.csv",
        aus_file="invoice_details_aus.csv"
    )
    
    # Display analysis
    print("\n=== TRANSFORMATION COMPLETE ===")
    print(f"Total records: {stats['total_records']:,}")
    print(f"BCI records: {stats['bci_records']:,}")
    print(f"AUS records: {stats['aus_records']:,}")
    print(f"Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")
    print(f"Total hours: {stats['total_hours']:,.2f}")
    print(f"Total billing: ${stats['total_billing']:,.2f}")
    
    print("\nPay Type Distribution:")
    for pay_type, count in stats['pay_type_distribution'].items():
        print(f"  {pay_type}: {count:,}")
    
    # Export to Excel
    transformer.export_unified_data(combined_df, "unified_invoice_analysis.xlsx")
