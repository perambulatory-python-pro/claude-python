"""
Trimble Data Processing Diagnostics
Analyze why 27 CSV records became 23 database records

Python Learning Concepts:
1. Data Quality Analysis - Finding duplicates and data issues
2. Comparative Analysis - CSV vs Database reconciliation  
3. Debugging Techniques - Systematic investigation
4. Data Profiling - Understanding data characteristics
"""

import pandas as pd
import numpy as np
from collections import Counter
from capital_project_db_manager import CapitalProjectDBManager

def analyze_trimble_processing_discrepancy():
    """
    Investigate why 27 CSV records became 23 database records
    """
    print("🔍 TRIMBLE DATA PROCESSING DIAGNOSTICS")
    print("=" * 50)
    
    # Read the original CSV
    csv_file = 'Blackstone Consulting DPs 06.24.2025 07 00 51 AM.csv'
    original_df = pd.read_csv(csv_file)
    
    print(f"📄 Original CSV Analysis:")
    print(f"   Total records: {len(original_df)}")
    print(f"   Columns: {list(original_df.columns)}")
    
    # Check for duplicates in the key field (Vendor Reference/Invoice Number)
    invoice_col = 'Vendor Reference/Invoice Number'
    if invoice_col in original_df.columns:
        invoice_values = original_df[invoice_col]
        
        print(f"\n🔑 Invoice Number Analysis:")
        print(f"   Unique invoice numbers: {invoice_values.nunique()}")
        print(f"   Total records: {len(invoice_values)}")
        print(f"   Missing/null values: {invoice_values.isna().sum()}")
        
        # Find duplicates
        duplicates = invoice_values[invoice_values.duplicated(keep=False)]
        if len(duplicates) > 0:
            print(f"   🚨 FOUND {len(duplicates)} duplicate invoice numbers!")
            
            # Show duplicate details
            duplicate_numbers = duplicates.unique()
            print(f"\n📋 Duplicate Invoice Numbers:")
            for dup_num in duplicate_numbers:
                dup_records = original_df[original_df[invoice_col] == dup_num]
                print(f"\n   Invoice {dup_num} appears {len(dup_records)} times:")
                
                # Show key differences between duplicate records
                key_cols = ['Current Step', 'Status', 'Step Date Created', 'Project Number']
                available_cols = [col for col in key_cols if col in dup_records.columns]
                
                for i, (idx, row) in enumerate(dup_records.iterrows()):
                    print(f"     Record {i+1}: ", end="")
                    details = []
                    for col in available_cols:
                        details.append(f"{col}={row[col]}")
                    print(" | ".join(details))
        else:
            print("   ✅ No duplicate invoice numbers found")
    
    # Check for empty/null key values
    print(f"\n🔍 Data Quality Checks:")
    for col in original_df.columns:
        null_count = original_df[col].isna().sum()
        if null_count > 0:
            print(f"   {col}: {null_count} null values")
    
    # Read what actually made it to the database
    manager = CapitalProjectDBManager()
    
    try:
        db_query = """
            SELECT 
                invoice_number,
                current_step,
                current_step_date,
                status,
                project_number,
                trimble_date_created,
                last_updated
            FROM capital_project_trimble_tracking
            ORDER BY invoice_number
        """
        
        db_df = pd.read_sql(db_query, manager.engine)
        
        print(f"\n💾 Database Analysis:")
        print(f"   Records in database: {len(db_df)}")
        print(f"   Unique invoice numbers: {db_df['invoice_number'].nunique()}")
        
        # Compare CSV vs Database
        csv_invoice_numbers = set(original_df[invoice_col].dropna().astype(str))
        db_invoice_numbers = set(db_df['invoice_number'].astype(str))
        
        missing_in_db = csv_invoice_numbers - db_invoice_numbers
        extra_in_db = db_invoice_numbers - csv_invoice_numbers
        
        print(f"\n🔄 Reconciliation:")
        print(f"   CSV unique invoices: {len(csv_invoice_numbers)}")
        print(f"   DB unique invoices: {len(db_invoice_numbers)}")
        
        if missing_in_db:
            print(f"   🚨 Missing from DB: {len(missing_in_db)} invoices")
            print(f"      {list(missing_in_db)}")
        
        if extra_in_db:
            print(f"   ⚠️  Extra in DB: {len(extra_in_db)} invoices")
            print(f"      {list(extra_in_db)}")
        
        if not missing_in_db and not extra_in_db:
            print("   ✅ Perfect match between CSV and database!")
    
    except Exception as e:
        print(f"   ❌ Error reading database: {e}")
    
    finally:
        manager.close()

def reprocess_with_detailed_logging():
    """
    Reprocess the Trimble file with detailed logging to see what happens
    """
    print("\n🔄 REPROCESSING WITH DETAILED LOGGING")
    print("=" * 50)
    
    manager = CapitalProjectDBManager()
    
    try:
        # Clear existing data for clean test (optional)
        print("📋 Current database state before reprocessing...")
        
        # Process with detailed tracking
        csv_file = 'Blackstone Consulting DPs 06.24.2025 07 00 51 AM.csv'
        
        # Read and analyze step by step
        df = pd.read_csv(csv_file)
        print(f"   Loaded {len(df)} records from CSV")
        
        # Apply column mapping
        column_mapping = {
            'Vendor Reference/Invoice Number': 'invoice_number',
            'Date Created': 'trimble_date_created',
            'Current Step': 'current_step',
            'Status': 'status',
            'Step Date Created': 'current_step_date',
            'Project Number': 'project_number',
            'Payment Reference': 'payment_reference',
            'OneLink Voucher ID': 'onelink_voucher_id'
        }
        
        df_renamed = df.rename(columns=column_mapping)
        print(f"   Renamed columns successfully")
        
        # Convert dates
        date_columns = ['trimble_date_created', 'current_step_date']
        for col in date_columns:
            if col in df_renamed.columns:
                df_renamed[col] = pd.to_datetime(df_renamed[col], errors='coerce')
        
        # Check for issues after transformation
        invoice_numbers = df_renamed['invoice_number'].dropna()
        print(f"   Non-null invoice numbers: {len(invoice_numbers)}")
        print(f"   Unique invoice numbers: {invoice_numbers.nunique()}")
        
        # Count duplicates in transformed data
        duplicates_after_transform = invoice_numbers[invoice_numbers.duplicated()]
        if len(duplicates_after_transform) > 0:
            print(f"   🚨 {len(duplicates_after_transform)} duplicates found after transformation")
            print(f"   Duplicate invoice numbers: {duplicates_after_transform.tolist()}")
        
        print(f"\n📊 Processing Results Summary:")
        print(f"   Original CSV records: {len(df)}")
        print(f"   Records after cleaning: {len(df_renamed)}")
        print(f"   Records with valid invoice numbers: {len(invoice_numbers)}")
        print(f"   Unique invoice numbers: {invoice_numbers.nunique()}")
        print(f"   Expected database records: {invoice_numbers.nunique()}")
        
    except Exception as e:
        print(f"   ❌ Error during analysis: {e}")
    
    finally:
        manager.close()

if __name__ == "__main__":
    analyze_trimble_processing_discrepancy()
    print("\n" + "="*50)
    reprocess_with_detailed_logging()
