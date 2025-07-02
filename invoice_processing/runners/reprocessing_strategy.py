"""
File Reprocessing Strategy
Identify and safely reprocess files that were processed with aggressive duplicate detection
"""

import pandas as pd
from datetime import datetime, timedelta
from database.database_manager_compatible import CompatibleEnhancedDatabaseManager

def analyze_recent_processing():
    """
    Analyze recent database inserts to identify files that may need reprocessing
    """
    print("🔍 ANALYZING RECENT PROCESSING FOR REPROCESSING NEEDS")
    print("=" * 70)
    
    try:
        db = CompatibleEnhancedDatabaseManager()
        
        # Get recent invoice details with creation dates
        print("\n1. Checking recent invoice detail inserts...")
        
        recent_details_df = db.execute_custom_query("""
            SELECT 
                source_system,
                DATE(created_at) as insert_date,
                COUNT(*) as record_count,
                COUNT(DISTINCT invoice_no) as unique_invoices,
                COUNT(DISTINCT employee_id) as unique_employees,
                MIN(created_at) as first_insert,
                MAX(created_at) as last_insert
            FROM invoice_details 
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY source_system, DATE(created_at)
            ORDER BY insert_date DESC, source_system
        """)
        
        if len(recent_details_df) > 0:
            print(f"   📊 Recent processing activity (last 7 days):")
            print(f"   {'Date':<12} {'System':<6} {'Records':<8} {'Invoices':<9} {'Employees':<9}")
            print(f"   {'-'*50}")
            
            for _, row in recent_details_df.iterrows():
                print(f"   {row['insert_date']:<12} {row['source_system']:<6} {row['record_count']:>7,} {row['unique_invoices']:>8,} {row['unique_employees']:>8,}")
        else:
            print("   📋 No recent processing activity found")
        
        # Check for suspicious patterns (low employee-to-record ratios might indicate missed duplicates)
        print(f"\n2. Analyzing processing patterns for potential missed records...")
        
        if len(recent_details_df) > 0:
            for _, row in recent_details_df.iterrows():
                records_per_employee = row['record_count'] / row['unique_employees'] if row['unique_employees'] > 0 else 0
                
                # Flag potentially problematic processing
                if records_per_employee < 5:  # Very low ratio might indicate aggressive duplicate removal
                    print(f"   ⚠️  {row['insert_date']} {row['source_system']}: Only {records_per_employee:.1f} records per employee")
                    print(f"      This might indicate aggressive duplicate detection")
                elif records_per_employee > 50:  # Very high ratio might indicate normal processing
                    print(f"   ✅ {row['insert_date']} {row['source_system']}: {records_per_employee:.1f} records per employee (normal)")
        
        return recent_details_df
        
    except Exception as e:
        print(f"❌ Error analyzing recent processing: {e}")
        return pd.DataFrame()

def check_specific_processing_session():
    """
    Check the specific processing session that had 937 duplicates
    """
    print(f"\n3. Checking for the specific session with 937 duplicates...")
    
    try:
        db = CompatibleEnhancedDatabaseManager()
        
        # Look for BCI records inserted recently
        bci_recent = db.execute_custom_query("""
            SELECT 
                DATE(created_at) as insert_date,
                COUNT(*) as total_records,
                COUNT(DISTINCT invoice_no) as unique_invoices,
                COUNT(DISTINCT employee_id) as unique_employees,
                COUNT(DISTINCT CONCAT(invoice_no, '-', employee_id, '-', work_date)) as unique_combinations
            FROM invoice_details 
            WHERE source_system = 'BCI'
              AND created_at >= NOW() - INTERVAL '2 days'
            GROUP BY DATE(created_at)
            ORDER BY insert_date DESC
        """)
        
        if len(bci_recent) > 0:
            print(f"   📊 Recent BCI processing:")
            for _, row in bci_recent.iterrows():
                total = row['total_records']
                combinations = row['unique_combinations']
                potential_duplicates = total - combinations
                
                print(f"   📅 {row['insert_date']}: {total:,} records, {combinations:,} unique combinations")
                
                if total == 26121:  # This matches your recent processing
                    print(f"      🎯 This appears to be your recent processing session!")
                    print(f"      📋 {potential_duplicates:,} potential legitimate records may have been missed")
                    print(f"      💡 This session should be reprocessed with new duplicate logic")
                    
                    return row['insert_date']
        
        return None
        
    except Exception as e:
        print(f"❌ Error checking specific session: {e}")
        return None

def recommend_reprocessing_approach():
    """
    Recommend the safest approach for reprocessing
    """
    print(f"\n💡 REPROCESSING RECOMMENDATIONS")
    print("=" * 50)
    
    print(f"🎯 **Safest Reprocessing Approach:**")
    print(f"")
    print(f"**Option A: Fresh Reprocessing (Recommended)**")
    print(f"1. Keep existing database records (don't delete anything)")
    print(f"2. Update database manager with new duplicate detection")
    print(f"3. Reprocess the same source files (TLM_BCI.xlsx, etc.)")
    print(f"4. New logic will only insert truly new records")
    print(f"5. Database constraints will prevent any actual duplicates")
    print(f"")
    print(f"**Option B: Targeted Cleanup (Advanced)**")
    print(f"1. Identify records inserted with old logic") 
    print(f"2. Delete only those records from a specific time range")
    print(f"3. Reprocess the source files")
    print(f"4. More complex but faster")
    print(f"")
    print(f"🔧 **Preparation Steps:**")
    print(f"1. Create the database unique constraint first")
    print(f"2. Update the database manager with new duplicate logic")
    print(f"3. Test with a small file first")
    print(f"4. Then reprocess your main files")
    print(f"")
    print(f"⚠️  **Important Notes:**")
    print(f"• The new logic is additive - it won't create true duplicates")
    print(f"• Database constraints provide safety net")
    print(f"• You should see ~900+ additional records inserted on reprocessing")
    print(f"• Original 26,121 records will remain unchanged")

def create_reprocessing_checklist():
    """
    Create a step-by-step checklist for reprocessing
    """
    print(f"\n📋 REPROCESSING CHECKLIST")
    print("=" * 50)
    
    checklist = [
        "☐ 1. Backup current database (optional but recommended)",
        "☐ 2. Create unique constraint on invoice_details table",
        "☐ 3. Update database_manager_compatible.py with new duplicate logic",
        "☐ 4. Restart Streamlit application",
        "☐ 5. Test with a small sample file first",
        "☐ 6. Reprocess TLM_BCI.xlsx (expect ~900+ additional records)",
        "☐ 7. Reprocess any other files processed with old logic",
        "☐ 8. Verify record counts and data integrity",
        "☐ 9. Document new baseline record counts"
    ]
    
    for item in checklist:
        print(f"   {item}")
    
    print(f"\n🎯 **Expected Outcome:**")
    print(f"• Total records will increase by the number of legitimate records")
    print(f"• No true duplicates will be created (database prevents this)")
    print(f"• Processing will be faster going forward")
    print(f"• More accurate data representation")

if __name__ == "__main__":
    recent_data = analyze_recent_processing()
    problematic_date = check_specific_processing_session()
    recommend_reprocessing_approach()
    create_reprocessing_checklist()
    
    print(f"\n🚀 NEXT IMMEDIATE STEPS:")
    print(f"1. Run the database constraint creation SQL")
    print(f"2. Update your database manager with Option 2 code")
    print(f"3. Reprocess your source files - they're safe to reprocess!")
    print(f"4. The new logic will add missing records without creating duplicates")