"""
Dimension Table Creator
Creates clean, normalized dimension tables from EDI and reference data
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from typing import Dict, List, Tuple


class DimensionTableCreator:
    def __init__(self, 
                 edi_file: str = "all_edi_2025.xlsx",
                 master_lookup_file: str = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx",
                 emid_file: str = "emid_job_bu_table.xlsx"):
        """
        Initialize with all data sources
        
        Args:
            edi_file: Path to EDI data (source of truth)
            master_lookup_file: Path to master lookup (for AUS mappings)
            emid_file: Path to existing EMID reference
        """
        self.edi_file = edi_file
        self.master_lookup_file = master_lookup_file
        self.emid_file = emid_file
        
        print("Dimension Table Creator")
        print("=" * 60)
        print(f"Loading data sources...")
        
        self.load_all_data()
        
    def load_all_data(self):
        """Load all data sources"""
        # Load EDI data (source of truth)
        print(f"  Loading EDI data from {self.edi_file}...")
        self.edi_df = pd.read_excel(self.edi_file, sheet_name="All 2025_EDI")
        print(f"    ✓ Loaded {len(self.edi_df)} EDI records")
        
        # Load master lookup
        print(f"  Loading master lookup from {self.master_lookup_file}...")
        try:
            self.master_lookup_df = pd.read_excel(self.master_lookup_file, sheet_name="Master Lookup", header=1)
            
            # Debug: Show actual column names
            print(f"    Master lookup columns: {list(self.master_lookup_df.columns[:5])}...")
            
            # Find the actual Tina column name (might have extra spaces or characters)
            tina_columns = [col for col in self.master_lookup_df.columns if 'Tina' in str(col) or 'Building Code' in str(col)]
            if tina_columns:
                print(f"    Found Tina/Building columns: {tina_columns}")
                # Use the first matching column
                self.tina_column_name = tina_columns[0]
            else:
                # Fallback - check column X (index 23)
                if len(self.master_lookup_df.columns) > 23:
                    self.tina_column_name = self.master_lookup_df.columns[23]
                    print(f"    Using column X as building code: '{self.tina_column_name}'")
                else:
                    self.tina_column_name = None
                    print(f"    WARNING: Could not find Tina Building Code column!")
            
            print(f"    ✓ Loaded {len(self.master_lookup_df)} lookup records")
        except Exception as e:
            print(f"    ERROR loading master lookup: {e}")
            self.master_lookup_df = pd.DataFrame()
            self.tina_column_name = None
        
        # Load existing EMID reference
        print(f"  Loading EMID reference from {self.emid_file}...")
        self.emid_ref_df = pd.read_excel(self.emid_file, sheet_name="emid_job_code")
        self.building_ref_df = pd.read_excel(self.emid_file, sheet_name="buildings")
        print(f"    ✓ Loaded {len(self.emid_ref_df)} EMID records and {len(self.building_ref_df)} building records")
    
    def create_service_area_dimension(self) -> pd.DataFrame:
        """
        Create Service Area dimension table
        One row per EMID with its service area and region
        """
        print("\n📊 Creating Service Area Dimension...")
        
        # Get unique EMID combinations from EDI
        service_area_dim = self.edi_df[['EMID', 'MC SERVICE AREA', 'REGION']].drop_duplicates()
        
        # Sort by EMID
        service_area_dim = service_area_dim.sort_values('EMID')
        
        # Add description from EMID reference if available
        emid_desc = self.emid_ref_df[['emid', 'description']].drop_duplicates()
        emid_desc.columns = ['EMID', 'DESCRIPTION']
        
        service_area_dim = service_area_dim.merge(emid_desc, on='EMID', how='left')
        
        # Add metadata
        service_area_dim['CREATED_DATE'] = datetime.now()
        service_area_dim['SOURCE'] = 'EDI_2025'
        
        # Verify 1:1 relationship
        if service_area_dim['EMID'].duplicated().any():
            print("  ⚠️  Warning: Found EMIDs with multiple service areas!")
            duplicates = service_area_dim[service_area_dim['EMID'].duplicated(keep=False)]
            print(duplicates)
        else:
            print(f"  ✓ Created {len(service_area_dim)} service areas (1:1 with EMID)")
        
        return service_area_dim
    
    def create_building_dimension(self) -> pd.DataFrame:
        """
        Create Building dimension table
        One row per building with its EMID assignment
        """
        print("\n🏢 Creating Building Dimension...")
        
        # Get unique building-EMID combinations from EDI
        building_from_edi = self.edi_df[['KP bldg', 'EMID', 'MC SERVICE AREA']].dropna(subset=['KP bldg'])
        building_from_edi = building_from_edi.drop_duplicates(subset=['KP bldg'])
        building_from_edi.columns = ['BUILDING_CODE', 'EMID', 'SERVICE_AREA']
        
        # Get additional building info from reference file
        building_ref = self.building_ref_df[['building_code', 'business_unit']].drop_duplicates()
        building_ref.columns = ['BUILDING_CODE', 'BUSINESS_UNIT']
        
        # Merge
        building_dim = building_from_edi.merge(building_ref, on='BUILDING_CODE', how='left')
        
        # Get building names from master lookup where available
        if hasattr(self, 'tina_column_name') and self.tina_column_name:
            try:
                master_buildings = self.master_lookup_df[[self.tina_column_name, 'Location/Job Name', 'Address on file']].dropna(subset=[self.tina_column_name])
                master_buildings = master_buildings.drop_duplicates(subset=[self.tina_column_name])
                master_buildings.columns = ['BUILDING_CODE', 'BUILDING_NAME', 'ADDRESS']
                
                building_dim = building_dim.merge(master_buildings, on='BUILDING_CODE', how='left')
            except KeyError as e:
                print(f"  ⚠️  Warning: Could not merge building names: {e}")
        else:
            print(f"  ⚠️  Warning: Tina column not found, skipping building names")
        
        # Sort and add metadata
        building_dim = building_dim.sort_values(['EMID', 'BUILDING_CODE'])
        building_dim['CREATED_DATE'] = datetime.now()
        building_dim['SOURCE'] = 'EDI_2025'
        
        print(f"  ✓ Created {len(building_dim)} building records")
        
        # Show distribution
        buildings_per_emid = building_dim.groupby('EMID').size()
        print(f"  📊 Buildings per EMID: Min={buildings_per_emid.min()}, Max={buildings_per_emid.max()}, Avg={buildings_per_emid.mean():.1f}")
        
        return building_dim
    
    def create_gl_combination_dimension(self) -> pd.DataFrame:
        """
        Create GL Combination dimension table
        One row per unique GL string combination
        """
        print("\n💰 Creating GL Combination Dimension...")
        
        # Get unique GL combinations from EDI
        gl_columns = ['KP bldg', 'GL BU', 'LOC', 'DEPT', 'ACCT', 'DEPT DESC']
        gl_combos = self.edi_df[gl_columns].dropna(subset=['KP bldg'])
        
        # Create unique GL combination ID
        gl_combos['GL_COMBINATION_ID'] = (
            gl_combos['KP bldg'].astype(str) + '_' +
            gl_combos['GL BU'].astype(str) + '_' +
            gl_combos['LOC'].astype(str) + '_' +
            gl_combos['DEPT'].astype(str)
        )
        
        # Remove duplicates
        gl_combos = gl_combos.drop_duplicates(subset=['GL_COMBINATION_ID'])
        
        # Rename columns
        gl_combos.columns = ['BUILDING_CODE', 'GL_BU', 'GL_LOC', 'GL_DEPT', 
                             'GL_ACCT', 'DEPT_DESC', 'GL_COMBINATION_ID']
        
        # Reorder columns
        gl_dim = gl_combos[['GL_COMBINATION_ID', 'BUILDING_CODE', 'GL_BU', 
                           'GL_LOC', 'GL_DEPT', 'GL_ACCT', 'DEPT_DESC']]
        
        # Add metadata
        gl_dim['CREATED_DATE'] = datetime.now()
        gl_dim['SOURCE'] = 'EDI_2025'
        
        print(f"  ✓ Created {len(gl_dim)} GL combination records")
        
        # Show buildings with multiple GL combos
        gl_per_building = gl_dim.groupby('BUILDING_CODE').size()
        multi_gl_buildings = gl_per_building[gl_per_building > 1]
        print(f"  📊 {len(multi_gl_buildings)} buildings have multiple GL combinations")
        
        return gl_dim
    
    def create_job_mapping_dimension(self) -> pd.DataFrame:
        """
        Create Job Code mapping table
        Maps various job codes (including AUS) to EMIDs
        """
        print("\n🔗 Creating Job Code Mapping Dimension...")
        
        # Start with EMID reference job codes
        job_mappings = []
        
        # Add EMID reference mappings
        for _, row in self.emid_ref_df.iterrows():
            job_mappings.append({
                'JOB_CODE': row['job_code'],
                'EMID': row['emid'],
                'JOB_TYPE': 'EMID_REF',
                'DESCRIPTION': row.get('description', '')
            })
        
        # Add AUS job mappings from master lookup
        # First, create building to EMID lookup from our building dimension
        building_dim = self.create_building_dimension()
        building_to_emid = dict(zip(building_dim['BUILDING_CODE'], building_dim['EMID']))
        
        # Process master lookup
        if hasattr(self, 'tina_column_name') and self.tina_column_name:
            try:
                aus_mappings = self.master_lookup_df[['Location/Job No', self.tina_column_name]].dropna()
                
                for _, row in aus_mappings.iterrows():
                    job_no = str(row['Location/Job No']).strip()
                    building_code = str(row[self.tina_column_name]).strip()
                    
                    if building_code in building_to_emid:
                        job_mappings.append({
                            'JOB_CODE': job_no,
                            'EMID': building_to_emid[building_code],
                            'JOB_TYPE': 'AUS',
                            'DESCRIPTION': f'AUS Job → Building {building_code}'
                        })
            except Exception as e:
                print(f"  ⚠️  Warning: Error processing AUS mappings: {e}")
        else:
            print(f"  ⚠️  Warning: Skipping AUS mappings - Tina column not found")
        
        # Create DataFrame
        job_mapping_dim = pd.DataFrame(job_mappings)
        
        # Remove exact duplicates
        job_mapping_dim = job_mapping_dim.drop_duplicates()
        
        # Sort
        job_mapping_dim = job_mapping_dim.sort_values(['JOB_CODE', 'EMID'])
        
        # Add metadata
        job_mapping_dim['CREATED_DATE'] = datetime.now()
        job_mapping_dim['SOURCE'] = 'COMBINED'
        
        print(f"  ✓ Created {len(job_mapping_dim)} job code mappings")
        
        # Check for jobs with multiple EMIDs
        multi_emid_jobs = job_mapping_dim.groupby('JOB_CODE')['EMID'].nunique()
        multi_emid_jobs = multi_emid_jobs[multi_emid_jobs > 1]
        
        if len(multi_emid_jobs) > 0:
            print(f"  ⚠️  {len(multi_emid_jobs)} job codes map to multiple EMIDs")
            print("     This is expected for some transitional job codes")
        
        return job_mapping_dim
    
    def create_invoice_dimension(self) -> pd.DataFrame:
        """
        Create Invoice dimension table
        One row per invoice with all dimensional assignments
        """
        print("\n📄 Creating Invoice Dimension...")
        
        # First, check what columns we actually have
        print("  Available columns in EDI data:")
        invoice_total_col = None
        for col in self.edi_df.columns:
            if 'Invoice Total' in str(col):
                invoice_total_col = col
                print(f"    Found invoice total column: '{col}'")
                break
        
        # Select key invoice fields - use actual column names
        invoice_cols = ['Invoice No.', 'EMID', 'MC SERVICE AREA', 'KP bldg',
                       'GL BU', 'LOC', 'DEPT', 'ACCT', 'Invoice From', 
                       'Invoice To', 'Invoice Date', 'EDI Date', 'ONELINK STATUS', 'PAID DATE']
        
        # Add invoice total column if found
        if invoice_total_col:
            invoice_cols.append(invoice_total_col)
        
        # Check which columns exist
        available_cols = [col for col in invoice_cols if col in self.edi_df.columns]
        missing_cols = [col for col in invoice_cols if col not in self.edi_df.columns]
        
        if missing_cols:
            print(f"  ⚠️  Missing columns: {missing_cols}")
        
        invoice_dim = self.edi_df[available_cols].copy()
        
        # Create GL combination ID
        invoice_dim['GL_COMBINATION_ID'] = (
            invoice_dim['KP bldg'].astype(str) + '_' +
            invoice_dim['GL BU'].astype(str) + '_' +
            invoice_dim['LOC'].astype(str) + '_' +
            invoice_dim['DEPT'].astype(str)
        )
        
        # Rename columns - build dynamically based on what we have
        rename_dict = {
            'Invoice No.': 'INVOICE_NUMBER',
            'EMID': 'EMID',
            'MC SERVICE AREA': 'SERVICE_AREA',
            'KP bldg': 'BUILDING_CODE',
            'GL BU': 'GL_BU',
            'LOC': 'GL_LOC',
            'DEPT': 'GL_DEPT',
            'ACCT': 'GL_ACCT',
            'Invoice From': 'INVOICE_FROM',
            'Invoice To': 'INVOICE_TO',
            'Invoice Date': 'INVOICE_DATE',
            'EDI Date': 'EDI_DATE',
            'ONELINK STATUS': 'ONELINK_STATUS',
            'PAID DATE': 'PAID_DATE'
        }
        
        if invoice_total_col:
            rename_dict[invoice_total_col] = 'INVOICE_TOTAL'
        
        invoice_dim = invoice_dim.rename(columns=rename_dict)
        
        # Add GL_COMBINATION_ID to final columns
        final_cols = list(rename_dict.values()) + ['GL_COMBINATION_ID']
        invoice_dim = invoice_dim[final_cols]
        
        # Convert invoice number to string to handle revisions
        invoice_dim['INVOICE_NUMBER'] = invoice_dim['INVOICE_NUMBER'].astype(str)
        
        # Sort
        invoice_dim = invoice_dim.sort_values('INVOICE_NUMBER')
        
        print(f"  ✓ Created {len(invoice_dim)} invoice records")
        
        return invoice_dim
    
    def export_all_dimensions(self, output_file: str = "clean_dimensions.xlsx"):
        """
        Export all dimension tables to Excel
        """
        print(f"\n💾 Exporting dimension tables to {output_file}...")
        
        # Create all dimensions first
        service_area_dim = self.create_service_area_dimension()
        building_dim = self.create_building_dimension()
        gl_dim = self.create_gl_combination_dimension()
        job_mapping_dim = self.create_job_mapping_dimension()
        invoice_dim = self.create_invoice_dimension()
        
        # Create summary data
        summary_data = {
            'Dimension Table': [
                'Service Area',
                'Building', 
                'GL Combination',
                'Job Mapping',
                'Invoice'
            ],
            'Record Count': [
                len(service_area_dim),
                len(building_dim),
                len(gl_dim),
                len(job_mapping_dim),
                len(invoice_dim)
            ],
            'Primary Key': [
                'EMID',
                'BUILDING_CODE',
                'GL_COMBINATION_ID',
                'JOB_CODE + EMID',
                'INVOICE_NUMBER'
            ],
            'Description': [
                'One row per EMID/Service Area',
                'One row per building with EMID assignment',
                'One row per unique GL coding combination',
                'Maps job codes to EMIDs (handles duplicates)',
                'One row per invoice with all dimensions'
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        
        # Write to Excel with explicit engine
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
                # Write summary first
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Write dimension tables
                service_area_dim.to_excel(writer, sheet_name='Service_Area_Dim', index=False)
                building_dim.to_excel(writer, sheet_name='Building_Dim', index=False)
                gl_dim.to_excel(writer, sheet_name='GL_Combination_Dim', index=False)
                job_mapping_dim.to_excel(writer, sheet_name='Job_Mapping_Dim', index=False)
                invoice_dim.to_excel(writer, sheet_name='Invoice_Dim', index=False)
                
                # Get the workbook and ensure sheets are visible
                workbook = writer.book
                for sheet in workbook.worksheets:
                    sheet.sheet_state = 'visible'
                
                # Format the summary sheet
                worksheet = writer.sheets['Summary']
                for column_cells in worksheet.columns:
                    length = max(len(str(cell.value or '')) for cell in column_cells)
                    worksheet.column_dimensions[column_cells[0].column_letter].width = min(length + 2, 50)
            
            print(f"  ✓ Successfully exported all dimension tables")
            
        except Exception as e:
            print(f"  ❌ Error exporting to Excel: {e}")
            print(f"  Attempting alternative export method...")
            
            # Alternative: Export each sheet separately
            try:
                # Remove existing file if it exists
                if os.path.exists(output_file):
                    os.remove(output_file)
                
                # Write each sheet individually
                with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Append other sheets
                with pd.ExcelWriter(output_file, engine='openpyxl', mode='a') as writer:
                    service_area_dim.to_excel(writer, sheet_name='Service_Area_Dim', index=False)
                    building_dim.to_excel(writer, sheet_name='Building_Dim', index=False)
                    gl_dim.to_excel(writer, sheet_name='GL_Combination_Dim', index=False)
                    job_mapping_dim.to_excel(writer, sheet_name='Job_Mapping_Dim', index=False)
                    invoice_dim.to_excel(writer, sheet_name='Invoice_Dim', index=False)
                
                print(f"  ✓ Successfully exported using alternative method")
                
            except Exception as e2:
                print(f"  ❌ Alternative export also failed: {e2}")
                # Last resort: export as CSV files
                print(f"  Exporting as CSV files instead...")
                summary_df.to_csv('dimensions_summary.csv', index=False)
                service_area_dim.to_csv('service_area_dim.csv', index=False)
                building_dim.to_csv('building_dim.csv', index=False)
                gl_dim.to_csv('gl_combination_dim.csv', index=False)
                job_mapping_dim.to_csv('job_mapping_dim.csv', index=False)
                invoice_dim.to_csv('invoice_dim.csv', index=False)
                print(f"  ✓ Exported as CSV files")
        
        return {
            'service_area': service_area_dim,
            'building': building_dim,
            'gl_combination': gl_dim,
            'job_mapping': job_mapping_dim,
            'invoice': invoice_dim
        }
    
    def validate_dimensions(self):
        """
        Validate the integrity of dimension tables
        """
        print("\n🔍 Validating Dimension Integrity...")
        
        dimensions = self.export_all_dimensions("clean_dimensions_temp.xlsx")
        
        # Validate Service Area
        print("\n  Service Area Dimension:")
        emid_counts = dimensions['service_area']['EMID'].value_counts()
        if (emid_counts > 1).any():
            print("    ❌ Found duplicate EMIDs!")
        else:
            print("    ✓ All EMIDs are unique")
        
        # Validate Building
        print("\n  Building Dimension:")
        orphan_buildings = set(dimensions['building']['EMID']) - set(dimensions['service_area']['EMID'])
        if orphan_buildings:
            print(f"    ❌ Found {len(orphan_buildings)} buildings with invalid EMIDs")
        else:
            print("    ✓ All buildings have valid EMIDs")
        
        # Validate GL Combinations
        print("\n  GL Combination Dimension:")
        orphan_gl = set(dimensions['gl_combination']['BUILDING_CODE']) - set(dimensions['building']['BUILDING_CODE'])
        if orphan_gl:
            print(f"    ❌ Found {len(orphan_gl)} GL combinations with invalid buildings")
        else:
            print("    ✓ All GL combinations have valid buildings")
        
        # Validate Invoices
        print("\n  Invoice Dimension:")
        orphan_invoices = set(dimensions['invoice']['EMID']) - set(dimensions['service_area']['EMID'])
        if orphan_invoices:
            print(f"    ❌ Found {len(orphan_invoices)} invoices with invalid EMIDs")
        else:
            print("    ✓ All invoices have valid EMIDs")
        
        # Clean up temp file
        os.remove("clean_dimensions_temp.xlsx")
        
        return dimensions


# Example usage
if __name__ == "__main__":
    # Create dimension tables
    creator = DimensionTableCreator()
    
    # Export all dimensions
    dimensions = creator.export_all_dimensions("clean_dimensions.xlsx")
    
    # Validate
    creator.validate_dimensions()
    
    print("\n✅ Dimension table creation complete!")
    print("   Check 'clean_dimensions.xlsx' for results")
