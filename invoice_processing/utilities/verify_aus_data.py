"""
Verify AUS Invoice Data in Neon Database - Fixed Column Names
"""

import os
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def verify_aus_data():
    """Comprehensive verification of AUS data in database"""
    
    print("🔍 VERIFYING AUS DATA IN DATABASE")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # 1. First, let's see what columns we actually have
        print("\n1. Checking actual table structure...")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'invoice_details'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        if columns:
            print("   ✅ Table 'invoice_details' columns:")
            for col_name, col_type in columns:
                print(f"      - {col_name}: {col_type}")
        else:
            print("   ❌ Table 'invoice_details' not found!")
            return
        
        # Store column names for reference
        column_names = [col[0] for col in columns]
        
        # 2. Count total records
        print("\n2. Counting records...")
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT invoice_no) as unique_invoices,
                COUNT(CASE WHEN source_system = 'AUS' THEN 1 END) as aus_records,
                COUNT(CASE WHEN source_system = 'BCI' THEN 1 END) as bci_records
            FROM invoice_details;
        """)
        
        result = cur.fetchone()
        if result:
            total, unique_inv, aus_count, bci_count = result
            print(f"   Total records: {total:,}")
            print(f"   Unique invoices: {unique_inv:,}")
            print(f"   AUS records: {aus_count:,}")
            print(f"   BCI records: {bci_count:,}")
        
        # 3. Check date range for AUS data
        print("\n3. Checking AUS data date range...")
        cur.execute("""
            SELECT 
                MIN(work_date) as earliest_date,
                MAX(work_date) as latest_date,
                COUNT(DISTINCT work_date) as unique_dates
            FROM invoice_details
            WHERE source_system = 'AUS';
        """)
        
        result = cur.fetchone()
        if result and result[0]:
            min_date, max_date, unique_dates = result
            print(f"   Date range: {min_date} to {max_date}")
            print(f"   Unique work dates: {unique_dates}")
        
        # 4. Sample AUS invoices
        print("\n4. Sample AUS invoices...")
        cur.execute("""
            SELECT DISTINCT invoice_no
            FROM invoice_details
            WHERE source_system = 'AUS'
            ORDER BY invoice_no DESC
            LIMIT 10;
        """)
        
        sample_invoices = cur.fetchall()
        if sample_invoices:
            print("   Recent AUS invoice numbers:")
            for (invoice_no,) in sample_invoices:
                print(f"      - {invoice_no}")
        
        # 5. Check for any -ORG invoices (should be none)
        print("\n5. Checking for -ORG invoices...")
        cur.execute("""
            SELECT COUNT(*)
            FROM invoice_details
            WHERE source_system = 'AUS' 
            AND invoice_no LIKE '%-ORG';
        """)
        
        org_count = cur.fetchone()[0]
        if org_count and org_count > 0:
            print(f"   ⚠️  Found {org_count} -ORG invoices")
        else:
            print("   ✅ No -ORG invoices found (as expected)")
        
        # 6. Summary statistics (using actual column names)
        print("\n6. AUS data summary statistics...")
        
        # Build query based on actual columns
        stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT invoice_no) as unique_invoices
        """
        
        # Add optional columns if they exist
        if 'employee_name' in column_names:
            stats_query += ", COUNT(DISTINCT employee_name) as unique_employees"
        if 'job_number' in column_names:
            stats_query += ", COUNT(DISTINCT job_number) as unique_jobs"
        if 'hours' in column_names:
            stats_query += ", SUM(hours) as total_hours"
        if 'bill_amount' in column_names:
            stats_query += ", SUM(bill_amount) as total_billing"
        if 'bill_rate' in column_names:
            stats_query += ", AVG(bill_rate) as avg_bill_rate"
        
        stats_query += """
            FROM invoice_details
            WHERE source_system = 'AUS';
        """
        
        cur.execute(stats_query)
        stats = cur.fetchone()
        
        if stats:
            print(f"   Total AUS records: {stats[0]:,}")
            print(f"   Unique invoices: {stats[1]:,}")
            if len(stats) > 2 and stats[2] is not None:
                print(f"   Unique employees: {stats[2]:,}")
            if len(stats) > 3 and stats[3] is not None:
                print(f"   Unique job numbers: {stats[3]:,}")
            if len(stats) > 4 and stats[4] is not None:
                print(f"   Total hours: {stats[4]:,.2f}")
            if len(stats) > 5 and stats[5] is not None:
                print(f"   Total billing: ${stats[5]:,.2f}")
            if len(stats) > 6 and stats[6] is not None:
                print(f"   Average bill rate: ${stats[6]:.2f}")
        
        # 7. Recent entries with actual columns
        print("\n7. Most recent AUS entries...")
        
        # Build query with available columns
        select_cols = ['invoice_no']
        if 'employee_name' in column_names:
            select_cols.append('employee_name')
        if 'work_date' in column_names:
            select_cols.append('work_date')
        if 'hours' in column_names:
            select_cols.append('hours')
        if 'bill_amount' in column_names:
            select_cols.append('bill_amount')
        
        recent_query = f"""
            SELECT {', '.join(select_cols)}
            FROM invoice_details
            WHERE source_system = 'AUS'
            ORDER BY 
                {'created_timestamp' if 'created_timestamp' in column_names else 'invoice_no'} DESC
            LIMIT 5;
        """
        
        cur.execute(recent_query)
        recent = cur.fetchall()
        
        if recent:
            print("   Latest processed records:")
            headers = [col.replace('_', ' ').title() for col in select_cols]
            print(f"   {' | '.join(headers)}")
            print("   " + "-"*70)
            for row in recent:
                print(f"   {' | '.join(str(val) for val in row)}")
        
        # Close connection
        cur.close()
        conn.close()
        
        print("\n" + "="*60)
        print("✅ VERIFICATION COMPLETE!")
        print("=" * 60)
        
        return column_names
        
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
        return []

# Run verification and show what to do next
if __name__ == "__main__":
    columns = verify_aus_data()
    
    if columns:
        print("\n📊 KEY FINDINGS:")
        print("The invoice_details table was created but with simplified column names.")
        print("This means we need to check our processor's INSERT statement.")
        print(f"\nActual columns in database: {', '.join(columns[:10])}")
        if len(columns) > 10:
            print(f"... and {len(columns) - 10} more columns")