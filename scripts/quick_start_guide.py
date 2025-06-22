"""
Quick Start Guide - Interactive Walkthrough
A simplified, interactive guide to get you started
"""

import pandas as pd
import os
from datetime import datetime


def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60)


def print_step(number, title):
    """Print a step header"""
    print(f"\n{'─'*50}")
    print(f"STEP {number}: {title}")
    print('─'*50)


def wait_for_user():
    """Wait for user to press Enter"""
    input("\nPress Enter to continue...")


def main():
    """Main interactive walkthrough"""
    print_header("INVOICE DATA UNIFICATION - QUICK START GUIDE")
    print("\nWelcome! This guide will walk you through validating and")
    print("processing your invoice data step by step.")
    
    wait_for_user()
    
    # STEP 1: Check Prerequisites
    print_step(1, "Checking Prerequisites")
    
    print("\nChecking for required files...")
    required_files = {
        'emid_job_bu_table.xlsx': 'EMID/Building reference',
        '2025_Master Lookup_Validation Location with GL Reference_V3.xlsx': 'Master lookup from billing manager',
        'invoice_details_bci.csv': 'BCI invoice sample',
        'invoice_details_aus.csv': 'AUS invoice sample'
    }
    
    files_found = []
    files_missing = []
    
    for filename, description in required_files.items():
        if os.path.exists(filename):
            files_found.append(f"✅ {filename} ({description})")
        else:
            files_missing.append(f"❌ {filename} ({description})")
    
    print("\nFiles found:")
    for f in files_found:
        print(f"  {f}")
    
    if files_missing:
        print("\nFiles missing:")
        for f in files_missing:
            print(f"  {f}")
        print("\n⚠️  Please ensure all required files are in the current directory.")
        return
    
    print("\n✅ All required files found!")
    wait_for_user()
    
    # STEP 2: Quick Validation
    print_step(2, "Quick Master Lookup Validation")
    
    print("\nLet's check your master lookup coverage...")
    
    try:
        # Import and initialize
        from enhanced_lookup_manager import EnhancedLookupManager
        
        lookup_manager = EnhancedLookupManager()
        
        print(f"\n📊 Master Lookup Statistics:")
        print(f"   • AUS job mappings: {len(lookup_manager.aus_job_lookup)}")
        print(f"   • Consolidated building codes: {len(lookup_manager.consolidated_building_lookup)}")
        print(f"   • EMID mappings: {len(lookup_manager.emid_lookup)}")
        
        # Test a few lookups
        print("\n🧪 Testing sample lookups:")
        test_jobs = ['207168', '281084T']
        for job in test_jobs:
            result = lookup_manager.lookup_aus_job_info(job)
            if result:
                print(f"   • Job {job} → Building {result.get('building_code')} ✅")
            else:
                print(f"   • Job {job} → Not found ❌")
        
    except Exception as e:
        print(f"\n❌ Error during validation: {str(e)}")
        return
    
    wait_for_user()
    
    # STEP 3: Process Sample Data
    print_step(3, "Process Sample Data")
    
    print("\nWould you like to process a small sample (100 records) or full files?")
    print("1. Small sample (recommended for first run)")
    print("2. Full files")
    
    choice = input("\nEnter your choice (1 or 2): ").strip()
    
    try:
        from invoice_transformer import InvoiceTransformer
        
        transformer = InvoiceTransformer()
        
        if choice == '1':
            # Process samples
            print("\n🔄 Processing 100-record samples...")
            
            # Read samples
            bci_sample = pd.read_csv('invoice_details_bci.csv', nrows=100)
            aus_sample = pd.read_csv('invoice_details_aus.csv', nrows=100)
            
            print(f"   • Read {len(bci_sample)} BCI records")
            print(f"   • Read {len(aus_sample)} AUS records")
            
            # Save samples for processing
            bci_sample.to_csv('bci_sample.csv', index=False)
            aus_sample.to_csv('aus_sample.csv', index=False)
            
            # Transform
            print("\n🔄 Transforming data...")
            combined_df, stats = transformer.combine_and_analyze('bci_sample.csv', 'aus_sample.csv')
            
            # Clean up sample files
            os.remove('bci_sample.csv')
            os.remove('aus_sample.csv')
            
        else:
            # Process full files
            print("\n🔄 Processing full files (this may take a minute)...")
            combined_df, stats = transformer.combine_and_analyze(
                'invoice_details_bci.csv',
                'invoice_details_aus.csv'
            )
        
        # Show results
        print("\n✨ Processing Complete!")
        print(f"\n📊 Results:")
        print(f"   • Total records processed: {stats['total_records']:,}")
        print(f"   • BCI records: {stats['bci_records']:,}")
        print(f"   • AUS records: {stats['aus_records']:,}")
        print(f"   • Total hours: {stats['total_hours']:,.2f}")
        print(f"   • Total billing: ${stats['total_billing']:,.2f}")
        
        # Check for invoice revisions
        print("\n🔍 Checking for invoice revisions...")
        revision_invoices = combined_df[
            combined_df['invoice_number'].str.contains(r'[A-Za-z]+$', regex=True, na=False)
        ]
        
        if len(revision_invoices) > 0:
            print(f"   • Found {len(revision_invoices)} invoices with revision letters")
            print("   • Examples:", revision_invoices['invoice_number'].head(3).tolist())
        else:
            print("   • No revision invoices found in this sample")
        
        # Save output
        output_file = f"unified_quickstart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        print(f"\n💾 Saving results to {output_file}...")
        transformer.export_unified_data(combined_df, output_file)
        print("   ✅ Saved successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    wait_for_user()
    
    # STEP 4: Review Results
    print_step(4, "Review Results & Next Steps")
    
    print("\n📋 What we've accomplished:")
    print("   ✅ Validated master lookup mappings")
    print("   ✅ Transformed BCI and AUS data to unified format")
    print("   ✅ Applied dimensional data (building codes, EMID, etc.)")
    print("   ✅ Exported results to Excel")
    
    print("\n🎯 Your next steps:")
    print("\n1. Review the exported files:")
    print("   • consolidated_dimensions.xlsx - Your reference data")
    print(f"   • {output_file} - Your unified invoice data")
    
    print("\n2. Check for data quality issues:")
    if transformer.transformation_stats['unmapped_aus_jobs']:
        print(f"   • Add mappings for {len(transformer.transformation_stats['unmapped_aus_jobs'])} AUS jobs")
    if transformer.transformation_stats['unmapped_bci_locations']:
        print(f"   • Add mappings for {len(transformer.transformation_stats['unmapped_bci_locations'])} BCI locations")
    
    print("\n3. Once mappings are complete:")
    print("   • Process all 2025 historical files")
    print("   • Set up weekly processing routine")
    print("   • Build analytics on the unified data")
    
    print("\n📚 For detailed analysis, run:")
    print("   python validation_walkthrough.py")
    
    print("\n✅ Quick start complete! You're ready to process invoice data.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user.")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
