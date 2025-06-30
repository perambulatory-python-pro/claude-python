"""
Updated AUS processing with smart duplicate detection
"""

import pandas as pd
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
load_dotenv()

print("🔧 Starting SMART AUS processing...")

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

# Process with smart duplicate detection
print("🧠 Processing with smart duplicate detection...")
result = process_file_with_smart_duplicates(
    df=aus_df, 
    source_system='AUS', 
    database_url=os.getenv('DATABASE_URL')
)

# Show results
print(f"\n📊 SMART PROCESSING RESULTS:")
print(f"   Total input records: {result['total_input_records']:,}")
print(f"   New records: {result['new_records']:,}")
print(f"   Duplicate records: {result['duplicate_records']:,}")
print(f"   Successfully inserted: {result['inserted']:,}")
print(f"   Processing time: {result['processing_time']:.1f} seconds")

if result['duplicate_records'] > 0:
    print(f"\n🔍 DUPLICATE ANALYSIS:")
    analysis = result['duplicate_analysis']
    print(f"   Total duplicates found: {analysis['total']}")
    print(f"   Invoices with duplicates: {len(analysis['by_invoice'])}")
    
    # Show top duplicate invoices
    top_duplicates = sorted(analysis['by_invoice'].items(), key=lambda x: x[1], reverse=True)[:5]
    for invoice, count in top_duplicates:
        print(f"     {invoice}: {count} duplicates")

print("🏁 Smart processing complete!")