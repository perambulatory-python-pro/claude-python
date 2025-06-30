"""
Updated AUS processing with smart duplicate detection AND invoice validation
Now includes the critical check that invoice numbers must exist in main invoices table
"""

import pandas as pd
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
load_dotenv()

print("🔧 Starting SMART AUS processing with invoice validation...")

# Apply the date fix
from fixed_date_converter import patch_enhanced_data_mapper
patch_enhanced_data_mapper()
print("✅ Date conversion fixed!")

# Use the smart processing function
from smart_duplicate_handler import process_file_with_smart_duplicates

# Load your file
aus_file = 'AUS_Invoice.xlsx'
print(f"📂 Loading {aus_file}...")
aus_df = pd.read_excel(aus_file)
print(f"✅ Loaded {len(aus_df):,} rows")

# Process with smart duplicate detection AND invoice validation
print("🧠 Processing with smart validation and duplicate detection...")
result = process_file_with_smart_duplicates(
    df=aus_df, 
    source_system='AUS', 
    database_url=os.getenv('DATABASE_URL')
)

# Show comprehensive results
print(f"\n📊 COMPREHENSIVE PROCESSING RESULTS:")
print(f"   Total input records: {result['total_input_records']:,}")

# Invoice validation results
print(f"\n🔍 INVOICE VALIDATION:")
print(f"   ✅ Valid invoice records: {result['valid_invoice_records']:,}")
print(f"   🚨 Invalid invoice records: {result['invalid_invoice_records']:,}")

if result['missing_invoices']:
    print(f"   📋 Missing invoices ({len(result['missing_invoices'])} unique):")
    for missing_invoice in result['missing_invoices'][:10]:  # Show first 10
        print(f"     - {missing_invoice}")
    if len(result['missing_invoices']) > 10:
        print(f"     ... and {len(result['missing_invoices']) - 10} more")

# Duplicate detection results
print(f"\n🔄 DUPLICATE DETECTION:")
print(f"   ✅ New records: {result['new_records']:,}")
print(f"   🔄 Duplicate records: {result['duplicate_records']:,}")

# Final insertion results
print(f"\n💾 INSERTION RESULTS:")
print(f"   ✅ Successfully inserted: {result['inserted']:,}")
print(f"   ⏱️ Processing time: {result['processing_time']:.1f} seconds")

# Detailed duplicate analysis
if result['duplicate_records'] > 0:
    print(f"\n🔍 DUPLICATE ANALYSIS:")
    analysis = result['duplicate_analysis']
    print(f"   Total duplicates found: {analysis['total']}")
    print(f"   Invoices with duplicates: {len(analysis['by_invoice'])}")
    
    # Show top duplicate invoices
    top_duplicates = sorted(analysis['by_invoice'].items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"   Top invoices with duplicates:")
    for invoice, count in top_duplicates:
        print(f"     {invoice}: {count} duplicates")

# Error reporting
if result['errors']:
    print(f"\n⚠️ ERRORS/WARNINGS:")
    for error in result['errors']:
        print(f"   - {error}")

# Success/failure summary
if result['success']:
    print(f"\n🎉 PROCESSING COMPLETED SUCCESSFULLY!")
    
    # Calculate efficiency metrics
    total_input = result['total_input_records']
    total_inserted = result['inserted']
    efficiency = (total_inserted / total_input * 100) if total_input > 0 else 0
    
    print(f"   📈 Efficiency: {efficiency:.1f}% of input records inserted")
    
    if result['invalid_invoice_records'] > 0:
        print(f"   🚨 DATA INTEGRITY ISSUE: {result['invalid_invoice_records']} records had invalid invoice numbers")
        print(f"      → These invoices need to be processed in the main invoices table first")
    
    if result['duplicate_records'] > 0:
        print(f"   🔄 DUPLICATE PREVENTION: {result['duplicate_records']} duplicate records safely skipped")
else:
    print(f"\n❌ PROCESSING FAILED!")
    print(f"   Check errors above for details")

print("\n🏁 Smart processing complete!")

# Optional: Generate a summary report
if result['success']:
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"aus_processing_report_{timestamp}.txt"
    
    with open(report_file, 'w') as f:
        f.write(f"AUS Processing Report - {pd.Timestamp.now()}\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Input: {result['total_input_records']:,} records\n")
        f.write(f"Valid invoices: {result['valid_invoice_records']:,}\n")
        f.write(f"Invalid invoices: {result['invalid_invoice_records']:,}\n")
        f.write(f"New records: {result['new_records']:,}\n")
        f.write(f"Duplicates: {result['duplicate_records']:,}\n")
        f.write(f"Inserted: {result['inserted']:,}\n")
        f.write(f"Processing time: {result['processing_time']:.1f} seconds\n\n")
        
        if result['missing_invoices']:
            f.write("Missing Invoice Numbers:\n")
            for invoice in result['missing_invoices']:
                f.write(f"  {invoice}\n")
        
        if result['errors']:
            f.write("\nErrors/Warnings:\n")
            for error in result['errors']:
                f.write(f"  {error}\n")
    
    print(f"📄 Detailed report saved to: {report_file}")
