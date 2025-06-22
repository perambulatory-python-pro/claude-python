"""
Reconciliation Report Generator
Compares old lookup approach vs EDI-based approach
"""

import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple


class ReconciliationReporter:
    def __init__(self,
                 edi_file: str = "all_edi_2025.xlsx",
                 old_emid_file: str = "emid_job_bu_table.xlsx",
                 master_lookup_file: str = "2025_Master Lookup_Validation Location with GL Reference_V3.xlsx",
                 dimensions_file: str = "clean_dimensions.xlsx"):
        """
        Initialize reconciliation reporter
        
        Args:
            edi_file: EDI data (new source of truth)
            old_emid_file: Old EMID reference file
            master_lookup_file: Master lookup file
            dimensions_file: New clean dimensions
        """
        self.edi_file = edi_file
        self.old_emid_file = old_emid_file
        self.master_lookup_file = master_lookup_file
        self.dimensions_file = dimensions_file
        
        print("Reconciliation Report Generator")
        print("=" * 60)
        
        self.load_all_data()
    
    def load_all_data(self):
        """Load all data sources for comparison"""
        # Load EDI data
        print("Loading data sources...")
        self.edi_df = pd.read_excel(self.edi_file, sheet_name="All 2025_EDI")
        
        # Load old reference data
        self.old_emid_df = pd.read_excel(self.old_emid_file, sheet_name="emid_job_code")
        self.old_building_df = pd.read_excel(self.old_emid_file, sheet_name="buildings")
        
        # Load master lookup
        self.master_lookup_df = pd.read_excel(self.master_lookup_file, sheet_name="Master Lookup", header=1)
        
        # Load new dimensions
        self.new_job_mapping = pd.read_excel(self.dimensions_file, sheet_name="Job_Mapping_Dim")
        self.new_building_dim = pd.read_excel(self.dimensions_file, sheet_name="Building_Dim")
        
        print(f"  ✓ Data loaded successfully")
    
    def compare_job_code_mappings(self) -> pd.DataFrame:
        """Compare old job code mappings with new approach"""
        print("\n📊 Comparing Job Code Mappings...")
        
        comparisons = []
        
        # Check each job code in old system
        for _, old_row in self.old_emid_df.iterrows():
            job_code = old_row['job_code']
            old_emid = old_row['emid']
            
            # Find in new system
            new_mappings = self.new_job_mapping[self.new_job_mapping['JOB_CODE'] == job_code]
            
            if len(new_mappings) == 0:
                comparisons.append({
                    'JOB_CODE': job_code,
                    'OLD_EMID': old_emid,
                    'NEW_EMID': 'NOT_FOUND',
                    'STATUS': 'MISSING',
                    'NOTES': 'Job code not in new mapping'
                })
            elif len(new_mappings) == 1:
                new_emid = new_mappings.iloc[0]['EMID']
                if old_emid == new_emid:
                    status = 'MATCH'
                    notes = 'Same EMID'
                else:
                    status = 'DIFFERENT'
                    notes = f'Changed from {old_emid} to {new_emid}'
                
                comparisons.append({
                    'JOB_CODE': job_code,
                    'OLD_EMID': old_emid,
                    'NEW_EMID': new_emid,
                    'STATUS': status,
                    'NOTES': notes
                })
            else:
                # Multiple mappings in new system
                new_emids = list(new_mappings['EMID'].unique())
                status = 'MULTIPLE'
                notes = f'Maps to {len(new_emids)} EMIDs: {", ".join(new_emids)}'
                
                comparisons.append({
                    'JOB_CODE': job_code,
                    'OLD_EMID': old_emid,
                    'NEW_EMID': ','.join(new_emids),
                    'STATUS': status,
                    'NOTES': notes
                })
        
        comparison_df = pd.DataFrame(comparisons)
        
        # Summary statistics
        print(f"\n  Job Code Mapping Summary:")
        print(f"    Total job codes in old system: {len(self.old_emid_df)}")
        print(f"    Matching EMIDs: {len(comparison_df[comparison_df['STATUS'] == 'MATCH'])}")
        print(f"    Different EMIDs: {len(comparison_df[comparison_df['STATUS'] == 'DIFFERENT'])}")
        print(f"    Multiple EMIDs (expected): {len(comparison_df[comparison_df['STATUS'] == 'MULTIPLE'])}")
        print(f"    Missing in new system: {len(comparison_df[comparison_df['STATUS'] == 'MISSING'])}")
        
        return comparison_df
    
    def analyze_duplicate_resolution(self) -> pd.DataFrame:
        """Analyze how duplicate job codes were resolved"""
        print("\n🔍 Analyzing Duplicate Resolution...")
        
        # Find job codes with multiple EMIDs in old system
        job_code_counts = self.old_emid_df['job_code'].value_counts()
        duplicate_jobs = job_code_counts[job_code_counts > 1].index
        
        resolution_analysis = []
        
        for job_code in duplicate_jobs:
            old_records = self.old_emid_df[self.old_emid_df['job_code'] == job_code]
            old_emids = list(old_records['emid'].values)
            
            # Check EDI to see which EMID is actually used
            job_invoices = []
            
            # For each EMID, count invoices in EDI
            emid_invoice_counts = {}
            for emid in old_emids:
                invoice_count = len(self.edi_df[self.edi_df['EMID'] == emid])
                emid_invoice_counts[emid] = invoice_count
            
            # Determine which EMID "won"
            if emid_invoice_counts:
                winning_emid = max(emid_invoice_counts, key=emid_invoice_counts.get)
                total_invoices = sum(emid_invoice_counts.values())
            else:
                winning_emid = 'NONE'
                total_invoices = 0
            
            resolution_analysis.append({
                'JOB_CODE': job_code,
                'DUPLICATE_EMIDS': ', '.join(old_emids),
                'EMID_COUNT': len(old_emids),
                'WINNING_EMID': winning_emid,
                'TOTAL_INVOICES': total_invoices,
                'INVOICE_DISTRIBUTION': str(emid_invoice_counts)
            })
        
        resolution_df = pd.DataFrame(resolution_analysis)
        
        print(f"  Found {len(duplicate_jobs)} job codes with duplicates")
        print(f"  These were resolved based on actual invoice usage in EDI")
        
        return resolution_df
    
    def compare_aus_mappings(self) -> pd.DataFrame:
        """Compare AUS job number mappings"""
        print("\n🔄 Comparing AUS Job Mappings...")
        
        # First, find the actual column names
        job_col = None
        building_col = None
        
        for col in self.master_lookup_df.columns:
            if 'Location/Job No' in str(col) or col == 'Location/Job No':
                job_col = col
            if 'Tina' in str(col) or 'Building Code' in str(col):
                building_col = col
        
        if not job_col or not building_col:
            print(f"  ⚠️  Could not find required columns")
            print(f"     Job column: {job_col}")
            print(f"     Building column: {building_col}")
            return pd.DataFrame()
        
        # Get AUS mappings from master lookup
        aus_mappings = self.master_lookup_df[[job_col, building_col]].dropna()
        aus_mappings.columns = ['JOB_NUMBER', 'BUILDING_CODE']
        
        comparisons = []
        
        for _, row in aus_mappings.iterrows():
            job_no = str(row['JOB_NUMBER']).strip()
            building_code = str(row['BUILDING_CODE']).strip()
            
            # Find building in new dimensions
            building_info = self.new_building_dim[self.new_building_dim['BUILDING_CODE'] == building_code]
            
            if len(building_info) > 0:
                emid = building_info.iloc[0]['EMID']
                service_area = building_info.iloc[0]['SERVICE_AREA']
                
                # Check if this job is in new mapping
                job_in_new = self.new_job_mapping[self.new_job_mapping['JOB_CODE'] == job_no]
                
                if len(job_in_new) > 0:
                    status = 'MAPPED'
                    notes = f'→ {building_code} → {emid} ({service_area})'
                else:
                    status = 'NOT_IN_MAPPING'
                    notes = f'Building found but job not in mapping table'
            else:
                status = 'BUILDING_NOT_FOUND'
                notes = f'Building {building_code} not in EDI data'
                emid = 'UNKNOWN'
                service_area = 'UNKNOWN'
            
            comparisons.append({
                'AUS_JOB': job_no,
                'BUILDING_CODE': building_code,
                'EMID': emid,
                'SERVICE_AREA': service_area,
                'STATUS': status,
                'NOTES': notes
            })
        
        comparison_df = pd.DataFrame(comparisons)
        
        # Summary
        print(f"  Total AUS jobs in master lookup: {len(aus_mappings)}")
        if len(comparison_df) > 0:
            print(f"  Successfully mapped: {len(comparison_df[comparison_df['STATUS'] == 'MAPPED'])}")
            print(f"  Building not found: {len(comparison_df[comparison_df['STATUS'] == 'BUILDING_NOT_FOUND'])}")
        
        return comparison_df
    
    def analyze_invoice_coverage(self) -> Dict:
        """Analyze invoice coverage in EDI vs detail files"""
        print("\n📋 Analyzing Invoice Coverage...")
        
        # Get unique invoices from EDI
        edi_invoices = set(self.edi_df['Invoice No.'].astype(str))
        
        # Load sample invoice details if available
        coverage_stats = {
            'edi_total_invoices': len(edi_invoices),
            'edi_unique_emids': self.edi_df['EMID'].nunique(),
            'edi_unique_buildings': self.edi_df['KP bldg'].nunique(),
            'edi_date_range': {
                'start': pd.to_datetime(self.edi_df['Invoice Date']).min(),
                'end': pd.to_datetime(self.edi_df['Invoice Date']).max()
            }
        }
        
        print(f"  EDI contains {len(edi_invoices)} unique invoices")
        print(f"  Date range: {coverage_stats['edi_date_range']['start'].date()} to {coverage_stats['edi_date_range']['end'].date()}")
        print(f"  Unique EMIDs: {coverage_stats['edi_unique_emids']}")
        print(f"  Unique Buildings: {coverage_stats['edi_unique_buildings']}")
        
        return coverage_stats
    
    def generate_visual_report(self, output_file: str = "reconciliation_report.png"):
        """Generate visual reconciliation report"""
        print(f"\n📊 Generating visual report...")
        
        # Set up the plot
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('EDI-Based Transformation Reconciliation Report', fontsize=16)
        
        # 1. Job Code Mapping Status
        ax1 = axes[0, 0]
        job_comparison = self.compare_job_code_mappings()
        status_counts = job_comparison['STATUS'].value_counts()
        
        colors = {'MATCH': 'green', 'DIFFERENT': 'orange', 'MULTIPLE': 'blue', 'MISSING': 'red'}
        ax1.pie(status_counts.values, labels=status_counts.index, autopct='%1.1f%%',
                colors=[colors.get(x, 'gray') for x in status_counts.index])
        ax1.set_title('Job Code Mapping Comparison')
        
        # 2. Duplicate Resolution Analysis
        ax2 = axes[0, 1]
        dup_resolution = self.analyze_duplicate_resolution()
        if len(dup_resolution) > 0:
            emid_counts = dup_resolution['EMID_COUNT'].value_counts().sort_index()
            ax2.bar(emid_counts.index.astype(str), emid_counts.values)
            ax2.set_xlabel('Number of EMIDs per Job Code')
            ax2.set_ylabel('Count of Job Codes')
            ax2.set_title('Duplicate Job Code Distribution')
        else:
            ax2.text(0.5, 0.5, 'No duplicates found', ha='center', va='center')
            ax2.set_title('Duplicate Job Code Distribution')
        
        # 3. EMID Invoice Distribution
        ax3 = axes[1, 0]
        emid_invoice_counts = self.edi_df.groupby('EMID').size().sort_values(ascending=False).head(10)
        ax3.barh(emid_invoice_counts.index, emid_invoice_counts.values)
        ax3.set_xlabel('Number of Invoices')
        ax3.set_ylabel('EMID')
        ax3.set_title('Top 10 EMIDs by Invoice Count')
        
        # 4. Building GL Combination Complexity
        ax4 = axes[1, 1]
        gl_complexity = self.edi_df.groupby('KP bldg')[['GL BU', 'LOC', 'DEPT']].apply(
            lambda x: len(x.drop_duplicates())
        ).sort_values(ascending=False).head(10)
        
        ax4.barh(gl_complexity.index, gl_complexity.values)
        ax4.set_xlabel('Number of GL Combinations')
        ax4.set_ylabel('Building Code')
        ax4.set_title('Top 10 Buildings by GL Complexity')
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"  ✓ Visual report saved to {output_file}")
        
        plt.close()
    
    def export_full_report(self, output_file: str = "reconciliation_report.xlsx"):
        """Export comprehensive reconciliation report"""
        print(f"\n💾 Exporting full reconciliation report to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Job code comparison
            job_comparison = self.compare_job_code_mappings()
            job_comparison.to_excel(writer, sheet_name='Job_Code_Comparison', index=False)
            
            # Duplicate resolution
            dup_resolution = self.analyze_duplicate_resolution()
            if len(dup_resolution) > 0:
                dup_resolution.to_excel(writer, sheet_name='Duplicate_Resolution', index=False)
            
            # AUS mappings
            aus_comparison = self.compare_aus_mappings()
            aus_comparison.to_excel(writer, sheet_name='AUS_Mappings', index=False)
            
            # Coverage statistics
            coverage_stats = self.analyze_invoice_coverage()
            coverage_df = pd.DataFrame([coverage_stats])
            coverage_df.to_excel(writer, sheet_name='Coverage_Stats', index=False)
            
            # Summary sheet
            summary_data = {
                'Reconciliation Area': [
                    'Job Code Mappings',
                    'Duplicate Job Codes', 
                    'AUS Job Mappings',
                    'Invoice Coverage'
                ],
                'Key Finding': [
                    f"{len(job_comparison[job_comparison['STATUS'] == 'MATCH'])} of {len(job_comparison)} job codes match",
                    f"{len(dup_resolution)} duplicate job codes resolved by EDI usage",
                    f"{len(aus_comparison[aus_comparison['STATUS'] == 'MAPPED'])} of {len(aus_comparison)} AUS jobs mapped",
                    f"{coverage_stats['edi_total_invoices']} invoices in EDI spanning {coverage_stats['edi_unique_emids']} EMIDs"
                ],
                'Recommendation': [
                    'Use EDI-based approach for accurate EMID assignment',
                    'Clean up job code table to remove unused duplicates',
                    'Validate unmapped AUS jobs with billing team',
                    'Process all invoices through EDI-based transformer'
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format summary sheet
            worksheet = writer.sheets['Summary']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"  ✓ Full report exported successfully")


# Main execution
def run_complete_reconciliation():
    """Run complete reconciliation process"""
    print("COMPLETE RECONCILIATION PROCESS")
    print("=" * 60)
    
    # Step 1: Create dimension tables
    print("\n📊 Step 1: Creating Clean Dimension Tables")
    from dimension_table_creator import DimensionTableCreator
    
    creator = DimensionTableCreator()
    dimensions = creator.export_all_dimensions("clean_dimensions.xlsx")
    creator.validate_dimensions()
    
    # Step 2: Run reconciliation analysis
    print("\n🔍 Step 2: Running Reconciliation Analysis")
    reporter = ReconciliationReporter()
    
    # Generate reports
    reporter.generate_visual_report()
    reporter.export_full_report()
    
    # Step 3: Test new transformer
    print("\n🔄 Step 3: Testing EDI-Based Transformer")
    from edi_based_transformer import EDIBasedTransformer
    
    transformer = EDIBasedTransformer("clean_dimensions.xlsx")
    
    # Show transformation stats
    print("\n📈 Key Findings:")
    print(f"  ✓ Created {len(dimensions['service_area'])} clean service areas")
    print(f"  ✓ Created {len(dimensions['building'])} building mappings")
    print(f"  ✓ Created {len(dimensions['job_mapping'])} job code mappings")
    print(f"  ✓ Linked {len(dimensions['invoice'])} invoices to dimensions")
    
    print("\n✅ Reconciliation complete!")
    print("\n📁 Files created:")
    print("  - clean_dimensions.xlsx (normalized dimension tables)")
    print("  - reconciliation_report.xlsx (detailed analysis)")
    print("  - reconciliation_report.png (visual summary)")
    
    return dimensions, reporter, transformer


if __name__ == "__main__":
    dimensions, reporter, transformer = run_complete_reconciliation()
