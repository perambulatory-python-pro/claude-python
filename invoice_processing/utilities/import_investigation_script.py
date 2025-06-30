"""
Force Import Investigation Script
Analyzes why records weren't imported
"""

import pandas as pd
import os
from datetime import datetime

def investigate_import_results(duplicate_file: str, report_file: str = None):
    """Investigate why so few records were imported"""
    
    print("🔍 FORCE IMPORT INVESTIGATION")
    print("=" * 60)
    
    # Load the duplicate analysis file
    df = pd.read_csv(duplicate_file, encoding='utf-8')
    print(f"\nTotal records in file: {len(df):,}")
    
    # Check Force_Import column
    print("\n1. Force_Import Column Analysis:")
    print("-" * 40)
    
    if 'Force_Import' in df.columns:
        # Normalize the values to see what's there
        df['Force_Import_Clean'] = df['Force_Import'].fillna('EMPTY').astype(str).str.strip().str.upper()
        
        # Count different values
        force_import_counts = df['Force_Import_Clean'].value_counts()
        print("\nForce_Import values found:")
        for value, count in force_import_counts.items():
            print(f"  '{value}': {count:,} records")
        
        # Check specifically for TRUE values
        true_count = len(df[df['Force_Import_Clean'] == 'TRUE'])
        false_count = len(df[df['Force_Import_Clean'] == 'FALSE'])
        other_count = len(df[~df['Force_Import_Clean'].isin(['TRUE', 'FALSE'])])
        
        print(f"\nSummary:")
        print(f"  Marked TRUE (to import): {true_count:,}")
        print(f"  Marked FALSE (to skip): {false_count:,}")
        print(f"  Other/Empty (not processed): {other_count:,}")
        
        # Show sample of non-TRUE values
        if other_count > 0:
            print(f"\nSample of records not marked TRUE or FALSE:")
            other_df = df[~df['Force_Import_Clean'].isin(['TRUE', 'FALSE'])]
            print(other_df[['Invoice Number', 'Employee Number', 'Force_Import', 'Force_Import_Clean']].head(10))
    else:
        print("ERROR: Force_Import column not found!")
        print(f"Available columns: {list(df.columns)}")
    
    # Check for existing records that might have been skipped
    print("\n2. Duplicate Check Analysis:")
    print("-" * 40)
    
    # Group by invoice to see patterns
    true_df = df[df.get('Force_Import_Clean', '') == 'TRUE']
    if len(true_df) > 0:
        invoice_groups = true_df.groupby('Invoice Number').size()
        print(f"  Unique invoices marked TRUE: {len(invoice_groups)}")
        print(f"  Average records per invoice: {invoice_groups.mean():.1f}")
        print(f"  Max records in one invoice: {invoice_groups.max()}")
    
    # Analyze the duplicate patterns
    print("\n3. Duplicate Pattern Analysis:")
    print("-" * 40)
    
    if 'Pattern_Type' in df.columns:
        pattern_counts = df['Pattern_Type'].value_counts()
        print("\nDuplicate patterns found:")
        for pattern, count in pattern_counts.items():
            print(f"  {pattern}: {count:,} records")
    
    if 'Is_Reversal_Pair' in df.columns:
        reversal_counts = df['Is_Reversal_Pair'].value_counts()
        print("\nReversal pairs:")
        for is_reversal, count in reversal_counts.items():
            print(f"  {is_reversal}: {count:,} records")
    
    # Check if there's a report file to analyze
    if report_file and os.path.exists(report_file):
        print(f"\n4. Import Report Analysis:")
        print("-" * 40)
        print(f"Reading report: {report_file}")
        with open(report_file, 'r') as f:
            report_content = f.read()
            # Look for validation failures
            if 'VALIDATION FAILURES' in report_content:
                print("\n⚠️  Validation failures found in report!")
                print("Check the report file for details about why records were skipped.")
    
    # Recommendations
    print("\n5. RECOMMENDATIONS:")
    print("-" * 40)
    print("1. Most likely cause: Records not marked with Force_Import = 'TRUE'")
    print("2. To import more records:")
    print("   - Open the duplicate_analysis CSV file")
    print("   - Review records and set Force_Import = TRUE for ones to import")
    print("   - Common candidates: reversal pairs, wage adjustments")
    print("3. Check for existing records - duplicates already in database won't import")
    print("4. Review the import report for specific validation failures")
    
    return df

def check_database_duplicates():
    """Check how many records might already exist in database"""
    
    print("\n6. Database Duplicate Check Query:")
    print("-" * 40)
    print("Run this query to see how many AUS records already exist:")
    print("""
SELECT 
    invoice_no, 
    COUNT(*) as record_count,
    MIN(created_at) as first_imported,
    MAX(created_at) as last_imported
FROM invoice_details
WHERE source_system = 'AUS'
GROUP BY invoice_no
HAVING COUNT(*) > 1
ORDER BY record_count DESC
LIMIT 20;
""")

if __name__ == "__main__":
    # Investigate the import
    duplicate_file = 'duplicate_analysis_20250624_135625.csv'
    report_file = 'force_import_report_20250624_204930.txt'
    
    df = investigate_import_results(duplicate_file, report_file)
    check_database_duplicates()
    
    # Additional analysis
    print("\n7. Quick Fix to Import More Records:")
    print("-" * 40)
    print("If you want to import ALL duplicates (be careful!):")
    print("1. Create a new file with all Force_Import = TRUE:")
    print("   df['Force_Import'] = 'TRUE'")
    print("   df.to_csv('duplicate_analysis_all_true.csv', index=False)")
    print("2. Run the import again with the new file")
    print("\nOr selectively mark records:")
    print("- Reversal pairs (adjustments) are usually safe to import")
    print("- Exact duplicates might need only one copy imported")
