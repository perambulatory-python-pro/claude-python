"""
AUS Invoice Reconciliation - Find Missing Records
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def reconcile_aus_records():
    """Detailed reconciliation of AUS invoice records"""
    
    print("🔍 AUS INVOICE RECONCILIATION ANALYSIS")
    print("=" * 70)
    
    # 1. Load original file
    print("\n1. Loading original AUS file...")
    df_original = pd.read_csv('invoice_details_aus.csv')
    print(f"   Total records in file: {len(df_original):,}")
    
    # 2. Identify -ORG invoices
    org_mask = df_original['Invoice Number'].str.contains('-ORG', na=False)
    df_org = df_original[org_mask]
    df_non_org = df_original[~org_mask]
    
    print(f"\n2. -ORG Invoice Analysis:")
    print(f"   Records with -ORG suffix: {len(df_org):,}")
    print(f"   Records without -ORG suffix: {len(df_non_org):,}")
    
    # 3. Check what's in the database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()
    
    # Get all AUS invoice numbers from database
    cur.execute("""
        SELECT invoice_no, COUNT(*) as record_count
        FROM invoice_details
        WHERE source_system = 'AUS'
        GROUP BY invoice_no
        ORDER BY invoice_no;
    """)
    
    db_invoices = {}
    for invoice_no, count in cur.fetchall():
        db_invoices[invoice_no] = count
    
    print(f"\n3. Database Analysis:")
    print(f"   Total AUS records in database: {sum(db_invoices.values()):,}")
    print(f"   Unique invoice numbers in database: {len(db_invoices):,}")
    
    # 4. Analyze missing records
    print(f"\n4. Missing Records Analysis:")
    
    # Group original file by invoice number
    file_invoice_groups = df_non_org.groupby('Invoice Number').size()
    
    missing_records = []
    found_records = []
    
    for invoice_no, expected_count in file_invoice_groups.items():
        invoice_str = str(invoice_no)
        
        if invoice_str in db_invoices:
            db_count = db_invoices[invoice_str]
            if db_count != expected_count:
                # Partial load
                missing_records.extend(
                    df_non_org[df_non_org['Invoice Number'] == invoice_no].to_dict('records')
                )
                print(f"   ⚠️  Invoice {invoice_no}: Expected {expected_count} records, found {db_count}")
        else:
            # Completely missing
            missing_records.extend(
                df_non_org[df_non_org['Invoice Number'] == invoice_no].to_dict('records')
            )
            found_records.append({
                'invoice_no': invoice_no,
                'expected_records': expected_count,
                'found_records': 0,
                'status': 'MISSING'
            })
    
    print(f"\n   Total missing records: {len(missing_records):,}")
    
    # 5. Check for duplicates in original file
    print(f"\n5. Duplicate Analysis in Original File:")
    
    # Check for exact duplicates (all columns)
    exact_duplicates = df_non_org[df_non_org.duplicated(keep=False)]
    print(f"   Exact duplicate records: {len(exact_duplicates):,}")
    
    # Check for key-based duplicates
    key_columns = ['Invoice Number', 'Employee Number', 'Work Date', 'Hours']
    key_duplicates = df_non_org[df_non_org.duplicated(subset=key_columns, keep=False)]
    print(f"   Duplicate by key fields: {len(key_duplicates):,}")
    
    # 6. Analyze missing records by category
    if missing_records:
        missing_df = pd.DataFrame(missing_records)
        
        print(f"\n6. Missing Records Breakdown:")
        
        # By invoice
        missing_by_invoice = missing_df.groupby('Invoice Number').agg({
            'Bill Amount': ['count', 'sum'],
            'Hours': 'sum'
        }).round(2)
        
        print(f"   Invoices with missing records: {len(missing_by_invoice)}")
        print(f"   Total missing amount: ${missing_df['Bill Amount'].sum():,.2f}")
        print(f"   Total missing hours: {missing_df['Hours'].sum():,.2f}")
        
        # Check for patterns
        print(f"\n   Checking for patterns in missing records...")
        
        # No work date?
        no_date = missing_df[missing_df['Work Date'].isna()]
        print(f"   - Records with no work date: {len(no_date):,}")
        
        # Zero hours?
        zero_hours = missing_df[missing_df['Hours'] == 0]
        print(f"   - Records with zero hours: {len(zero_hours):,}")
        
        # Negative amounts?
        negative = missing_df[missing_df['Bill Amount'] < 0]
        print(f"   - Records with negative amounts: {len(negative):,}")
        
        # Save detailed analysis
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save missing records
        missing_file = f'missing_aus_records_{timestamp}.csv'
        missing_df.to_csv(missing_file, index=False)
        print(f"\n   ✓ Missing records saved to: {missing_file}")
        
        # Save duplicate analysis
        if len(exact_duplicates) > 0:
            dup_file = f'duplicate_aus_records_{timestamp}.csv'
            exact_duplicates.to_csv(dup_file, index=False)
            print(f"   ✓ Duplicate records saved to: {dup_file}")
        
        # Create summary report
        summary = f"""
AUS INVOICE RECONCILIATION SUMMARY
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}

FILE ANALYSIS:
- Total records in file: {len(df_original):,}
- ORG invoice records: {len(df_org):,}
- Non-ORG records: {len(df_non_org):,}

DATABASE ANALYSIS:
- Records in database: {sum(db_invoices.values()):,}
- Unique invoices in DB: {len(db_invoices):,}

MISSING RECORDS:
- Total missing: {len(missing_records):,}
- Missing amount: ${missing_df['Bill Amount'].sum():,.2f}
- Missing hours: {missing_df['Hours'].sum():,.2f}

POTENTIAL ISSUES:
- No work date: {len(no_date):,}
- Zero hours: {len(zero_hours):,}
- Negative amounts: {len(negative):,}
- Exact duplicates: {len(exact_duplicates):,}

RECONCILIATION:
Original file: {len(df_original):,}
Less ORG: -{len(df_org):,}
Expected: {len(df_non_org):,}
In Database: {sum(db_invoices.values()):,}
Gap: {len(df_non_org) - sum(db_invoices.values()):,}
"""
        
        summary_file = f'aus_reconciliation_summary_{timestamp}.txt'
        with open(summary_file, 'w') as f:
            f.write(summary)
        
        print(f"   ✓ Summary report saved to: {summary_file}")
    
    else:
        print("\n   ✅ No missing records found!")
    
    cur.close()
    conn.close()
    
    # 7. Return key metrics
    return {
        'original_total': len(df_original),
        'org_records': len(df_org),
        'expected_non_org': len(df_non_org),
        'in_database': sum(db_invoices.values()),
        'missing_count': len(missing_records),
        'exact_duplicates': len(exact_duplicates)
    }

# Run the reconciliation
if __name__ == "__main__":
    metrics = reconcile_aus_records()
    
    print("\n" + "="*70)
    print("RECONCILIATION COMPLETE")
    print("="*70)
    print(f"Gap to investigate: {metrics['expected_non_org'] - metrics['in_database']:,} records")