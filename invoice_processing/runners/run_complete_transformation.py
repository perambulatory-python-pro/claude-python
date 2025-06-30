"""
Complete EDI-Based Transformation Runner
Executes the full process from dimension creation to final unified data
"""

import pandas as pd
import os
from datetime import datetime


def run_complete_transformation():
    """
    Run the complete transformation process:
    1. Create clean dimension tables
    2. Generate reconciliation report
    3. Transform invoice details using EDI dimensions
    4. Export unified data
    """
    
    print("=" * 70)
    print("EDI-BASED INVOICE TRANSFORMATION - COMPLETE PROCESS")
    print("=" * 70)
    print()
    
    # Check required files
    required_files = {
        'all_edi_2025.xlsx': 'EDI invoice data',
        'emid_job_bu_table.xlsx': 'EMID reference data',
        '2025_Master Lookup_Validation Location with GL Reference_V3.xlsx': 'Master lookup',
        'invoice_details_bci.csv': 'BCI invoice details',
        'invoice_details_aus.csv': 'AUS invoice details'
    }
    
    print("📋 Checking required files...")
    missing_files = []
    for file, description in required_files.items():
        if os.path.exists(file):
            print(f"  ✓ {file} ({description})")
        else:
            print(f"  ❌ {file} ({description}) - MISSING")
            missing_files.append(file)
    
    if missing_files:
        print(f"\n❌ Cannot proceed - missing {len(missing_files)} required files")
        return
    
    print("\n✅ All required files found!")
    
    try:
        # Step 1: Create dimension tables
        print("\n" + "="*60)
        print("STEP 1: CREATING CLEAN DIMENSION TABLES")
        print("="*60)
        
        from invoice_processing.edi_integration.dimension_table_creator import DimensionTableCreator
        
        creator = DimensionTableCreator()
        dimensions = creator.export_all_dimensions("clean_dimensions.xlsx")
        creator.validate_dimensions()
        
        # Step 2: Generate reconciliation report
        print("\n" + "="*60)
        print("STEP 2: GENERATING RECONCILIATION REPORT")
        print("="*60)
        
        from invoice_processing.edi_integration.reconciliation_report import ReconciliationReporter
        
        reporter = ReconciliationReporter()
        reporter.generate_visual_report()
        reporter.export_full_report()
        
        # Step 3: Transform invoice details
        print("\n" + "="*60)
        print("STEP 3: TRANSFORMING INVOICE DETAILS WITH EDI DIMENSIONS")
        print("="*60)
        
        from invoice_processing.edi_integration.edi_based_transformer import EDIBasedTransformer
        
        transformer = EDIBasedTransformer("clean_dimensions.xlsx")
        
        # Transform BCI
        print("\n🔄 Transforming BCI invoice details...")
        bci_unified = transformer.transform_bci_file("invoice_details_bci.csv")
        
        # Transform AUS
        print("\n🔄 Transforming AUS invoice details...")
        aus_unified = transformer.transform_aus_file("invoice_details_aus.csv")
        
        # Combine data
        print("\n📊 Combining transformed data...")
        unified_df = pd.concat([bci_unified, aus_unified], ignore_index=True)
        
        # Step 4: Generate comprehensive output
        print("\n" + "="*60)
        print("STEP 4: GENERATING FINAL OUTPUT")
        print("="*60)
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"unified_invoice_data_edi_based_{timestamp}.xlsx"
        
        print(f"\n💾 Exporting unified data to {output_file}...")
        
        # Clean up date columns before processing
        if 'work_date' in unified_df.columns:
            # Remove any NaT or invalid dates
            unified_df = unified_df[unified_df['work_date'].notna()]
            print(f"  Cleaned data: {len(unified_df)} records with valid dates")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main data
            unified_df.to_excel(writer, sheet_name='Unified_Invoice_Details', index=False)
            
            # Calculate date range safely
            if len(unified_df) > 0 and 'work_date' in unified_df.columns:
                valid_dates = unified_df['work_date'].dropna()
                if len(valid_dates) > 0:
                    date_min = valid_dates.min()
                    date_max = valid_dates.max()
                else:
                    date_min = 'N/A'
                    date_max = 'N/A'
            else:
                date_min = 'N/A'
                date_max = 'N/A'
            
            # Summary statistics
            summary_stats = {
                'Metric': [
                    'Total Records',
                    'BCI Records',
                    'AUS Records',
                    'Records with EMID',
                    'Records with Building Code',
                    'Unique Invoices',
                    'Unique EMIDs',
                    'Unique Employees',
                    'Total Hours',
                    'Total Billing',
                    'Date Range Start',
                    'Date Range End'
                ],
                'Value': [
                    len(unified_df),
                    len(unified_df[unified_df['source_system'] == 'BCI']),
                    len(unified_df[unified_df['source_system'] == 'AUS']),
                    len(unified_df[unified_df['emid'].notna()]),
                    len(unified_df[unified_df['building_code'].notna()]),
                    unified_df['invoice_number'].nunique(),
                    unified_df['emid'].nunique(),
                    unified_df['employee_number'].nunique(),
                    f"{unified_df['hours_quantity'].sum():,.2f}",
                    f"${unified_df['bill_amount'].sum():,.2f}",
                    str(date_min),
                    str(date_max)
                ]
            }
            
            summary_df = pd.DataFrame(summary_stats)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Transformation report
            report_df = transformer.generate_transformation_report()
            report_df.to_excel(writer, sheet_name='Transformation_Report', index=False)
            
            # EMID coverage analysis
            emid_coverage = unified_df.groupby(['source_system', 'emid']).size().reset_index(name='record_count')
            emid_coverage = emid_coverage.pivot(index='emid', columns='source_system', values='record_count').fillna(0)
            emid_coverage['Total'] = emid_coverage.sum(axis=1)
            emid_coverage.to_excel(writer, sheet_name='EMID_Coverage')
            
            # Unmatched invoices
            if transformer.transformation_stats['invoices_not_found']:
                unmatched_df = pd.DataFrame({
                    'Invoice_Number': sorted(list(transformer.transformation_stats['invoices_not_found']))
                })
                unmatched_df.to_excel(writer, sheet_name='Unmatched_Invoices', index=False)
        
        print(f"  ✓ Unified data exported successfully!")
        
        # Display final summary
        print("\n" + "="*60)
        print("TRANSFORMATION COMPLETE - SUMMARY")
        print("="*60)
        
        print(f"\n📊 Processing Results:")
        print(f"  • Total records processed: {len(unified_df):,}")
        print(f"  • BCI records: {len(unified_df[unified_df['source_system'] == 'BCI']):,}")
        print(f"  • AUS records: {len(unified_df[unified_df['source_system'] == 'AUS']):,}")
        
        print(f"\n🎯 Dimensional Coverage:")
        emid_coverage_pct = (len(unified_df[unified_df['emid'].notna()]) / len(unified_df)) * 100
        building_coverage_pct = (len(unified_df[unified_df['building_code'].notna()]) / len(unified_df)) * 100
        print(f"  • Records with EMID: {emid_coverage_pct:.1f}%")
        print(f"  • Records with Building Code: {building_coverage_pct:.1f}%")
        
        print(f"\n💰 Financial Summary:")
        print(f"  • Total hours: {unified_df['hours_quantity'].sum():,.2f}")
        print(f"  • Total billing: ${unified_df['bill_amount'].sum():,.2f}")
        
        if transformer.transformation_stats['invoices_not_found']:
            print(f"\n⚠️  Invoices not found in EDI: {len(transformer.transformation_stats['invoices_not_found'])}")
            print("    (See 'Unmatched_Invoices' sheet for details)")
        
        print(f"\n📁 Output Files Created:")
        print(f"  1. clean_dimensions.xlsx - Normalized dimension tables")
        print(f"  2. reconciliation_report.xlsx - Detailed mapping analysis")
        print(f"  3. reconciliation_report.png - Visual summary")
        print(f"  4. {output_file} - Final unified invoice data")
        
        print("\n✅ All processing complete!")
        
        return {
            'dimensions': dimensions,
            'unified_data': unified_df,
            'output_file': output_file,
            'stats': transformer.transformation_stats
        }
        
    except Exception as e:
        print(f"\n❌ Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def quick_analysis(unified_df):
    """
    Perform quick analysis on the unified data
    """
    print("\n" + "="*60)
    print("QUICK ANALYSIS")
    print("="*60)
    
    # Top EMIDs by volume
    print("\n📊 Top 10 EMIDs by Record Count:")
    top_emids = unified_df['emid'].value_counts().head(10)
    for emid, count in top_emids.items():
        if pd.notna(emid):
            service_area_rows = unified_df[unified_df['emid'] == emid]['mc_service_area']
            service_area = service_area_rows.iloc[0] if len(service_area_rows) > 0 and pd.notna(service_area_rows.iloc[0]) else 'Unknown'
        else:
            service_area = 'Unknown'
        print(f"  {emid} ({service_area}): {count:,} records")
    
    # Pay type distribution
    print("\n💵 Pay Type Distribution:")
    pay_types = unified_df['pay_type'].value_counts()
    for pay_type, count in pay_types.items():
        pct = (count / len(unified_df)) * 100
        print(f"  {pay_type}: {count:,} records ({pct:.1f}%)")
    
    # Source system comparison
    print("\n🔄 Source System Comparison:")
    for source in ['BCI', 'AUS']:
        source_df = unified_df[unified_df['source_system'] == source]
        if len(source_df) > 0:
            print(f"\n  {source}:")
            print(f"    Records: {len(source_df):,}")
            print(f"    Hours: {source_df['hours_quantity'].sum():,.2f}")
            print(f"    Billing: ${source_df['bill_amount'].sum():,.2f}")
            
            # Calculate average rate safely
            non_zero_rates = source_df[source_df['bill_rate'] > 0]['bill_rate']
            if len(non_zero_rates) > 0:
                print(f"    Avg Rate: ${non_zero_rates.mean():.2f}")
            else:
                print(f"    Avg Rate: N/A")


if __name__ == "__main__":
    # Run the complete transformation
    result = run_complete_transformation()
    
    if result:
        # Perform quick analysis
        quick_analysis(result['unified_data'])
        
        print("\n" + "="*60)
        print("🎉 SUCCESS! Your invoice data has been transformed using")
        print("   the EDI-based dimensional model.")
        print("="*60)