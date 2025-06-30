"""
Fix Business Unit and Job Code Lookups in Transformer
Updates the EDI-based transformer to properly populate these fields
"""

import pandas as pd
import os
from datetime import datetime
from invoice_processing.edi_integration.edi_based_transformer import EDIBasedTransformer
from invoice_processing.core.unified_invoice_schema import UnifiedInvoiceDetail, PayType, standardize_pay_type


class FixedEDIBasedTransformer(EDIBasedTransformer):
    """
    Fixed version that properly populates business_unit and job_code
    """
    
    def create_lookups(self):
        """Enhanced lookups including job_code"""
        # Call parent method first
        super().create_lookups()
        
        # Add EMID to job_code lookup
        self.emid_to_job_code = {}
        emid_groups = self.job_mapping_dim[self.job_mapping_dim['JOB_TYPE'] == 'EMID_REF'].groupby('EMID')
        for emid, group in emid_groups:
            # Get the first job_code for this EMID
            job_codes = group['JOB_CODE'].tolist()
            if job_codes:
                self.emid_to_job_code[emid] = job_codes[0]
        
        print(f"  ✓ Created EMID to job_code lookup with {len(self.emid_to_job_code)} entries")
    
    def transform_bci_row(self, row: pd.Series, row_idx: int, source_file: str) -> UnifiedInvoiceDetail:
        """Enhanced BCI transformation with proper lookups"""
        try:
            # Get invoice dimensions from EDI
            invoice_no = str(row['Invoice_No'])
            invoice_dims = self.get_invoice_dimensions(invoice_no)
            
            if not invoice_dims:
                # Invoice not in EDI - use fallback logic
                self.transformation_stats['warnings'].append(
                    f"BCI Invoice {invoice_no} not found in EDI"
                )
                emid = None
                service_area = None
                building_code = None
                business_unit = None
                job_code = None
            else:
                # Use EDI dimensions
                emid = invoice_dims['EMID']
                service_area = invoice_dims['SERVICE_AREA']
                building_code = invoice_dims.get('BUILDING_CODE')
                
                # Look up business unit from building dimension
                if building_code and building_code in self.building_lookup:
                    building_info = self.building_lookup[building_code]
                    business_unit = building_info.get('BUSINESS_UNIT')
                    
                    # If still None, try to get from EMID's service area
                    if not business_unit and emid in self.service_area_lookup:
                        # Some EMIDs might have a default business unit
                        business_unit = None  # We don't have this mapping
                else:
                    business_unit = None
                
                # Look up job_code from EMID
                if emid and emid in self.emid_to_job_code:
                    job_code = self.emid_to_job_code[emid]
                else:
                    job_code = None
            
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
                
                # Organizational hierarchy - NOW WITH job_code!
                emid=emid,
                business_unit=business_unit,
                mc_service_area=service_area,
                building_code=building_code,
                location_name=row.get('Location'),
                location_number=row.get('Location_Number'),
                job_code=job_code,  # Now populated!
                
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
    
    def transform_aus_row(self, row: pd.Series, row_idx: int, source_file: str) -> UnifiedInvoiceDetail:
        """Enhanced AUS transformation with proper lookups"""
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
                    
                    # Get job_code
                    job_code = self.emid_to_job_code.get(emid)
                    
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
                    job_code = None
                
                building_code = None
                business_unit = None
            else:
                # Use EDI dimensions
                emid = invoice_dims['EMID']
                service_area = invoice_dims['SERVICE_AREA']
                building_code = invoice_dims.get('BUILDING_CODE')
                
                # Get job_code
                job_code = self.emid_to_job_code.get(emid)
                
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
                work_date=work_date,
                week_ending_date=week_ending,
                pay_type=pay_type,
                hours_quantity=float(row.get('Hours', 0)) if pd.notna(row.get('Hours')) else 0,
                bill_rate=float(row.get('Bill Rate', 0)) if pd.notna(row.get('Bill Rate')) else 0,
                bill_amount=float(row.get('Bill Amount', 0)) if pd.notna(row.get('Bill Amount')) else 0,
                created_timestamp=datetime.now(),
                source_file=source_file,
                source_row_number=row_idx,
                
                # Organizational hierarchy - NOW WITH job_code!
                emid=emid,
                business_unit=business_unit,
                mc_service_area=service_area,
                building_code=building_code,
                job_number=job_number,
                job_code=job_code,  # Now populated!
                
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
    
    # Export
    print("\n6. Exporting fixed data...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # CSV export
    csv_file = f"unified_invoice_data_fixed_{timestamp}.csv"
    unified_df.to_csv(csv_file, index=False)
    print(f"   ✓ Exported to {csv_file}")
    
    # Try Excel export
    try:
        excel_file = f"unified_invoice_data_fixed_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            unified_df.to_excel(writer, sheet_name='Unified_Invoice_Details', index=False)
            
            # Add summary
            summary_data = {
                'Field': ['business_unit', 'job_code', 'emid', 'building_code'],
                'Records_Filled': [
                    unified_df['business_unit'].notna().sum(),
                    unified_df['job_code'].notna().sum(),
                    unified_df['emid'].notna().sum(),
                    unified_df['building_code'].notna().sum()
                ],
                'Percentage': [
                    f"{unified_df['business_unit'].notna().sum()/len(unified_df)*100:.1f}%",
                    f"{unified_df['job_code'].notna().sum()/len(unified_df)*100:.1f}%",
                    f"{unified_df['emid'].notna().sum()/len(unified_df)*100:.1f}%",
                    f"{unified_df['building_code'].notna().sum()/len(unified_df)*100:.1f}%"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Field_Population_Summary', index=False)
        
        print(f"   ✓ Also exported to {excel_file}")
    except:
        print("   ⚠️ Excel export failed, but CSV is available")
    
    print("\n" + "="*60)
    print("✅ REPROCESSING COMPLETE!")
    print("=" * 60)
    
    return unified_df


if __name__ == "__main__":
    unified_df = reprocess_with_fixed_transformer()
