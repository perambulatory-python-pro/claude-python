"""
Quick fix for the dashboard query column reference issue
"""

def get_corrected_dashboard_query():
    """
    Returns the corrected dashboard query with proper column names
    """
    return """
        SELECT 
            cpi.capital_project_number,
            cpi.invoice_number,
            i.chartfield,
            i.release_date,
            i.edi_date,
            i.add_on_date,
            i.invoice_total,
            i.service_area,
            i.post_name,
            tt.current_step,
            tt.current_step_date,
            tt.previous_step,
            tt.previous_step_date,
            tt.status as trimble_status,
            tt.project_number as trimble_project_number,
            CASE 
                WHEN ie.invoice_number IS NOT NULL THEN 'Emailed'
                WHEN i.release_date IS NOT NULL THEN 'Ready to Email'
                ELSE 'Pending Release'
            END as email_status,
            ie.emailed_date,
            nl.sent_date as notification_sent_date
        FROM capital_project_invoices cpi
        JOIN invoices i ON cpi.invoice_number = i.invoice_no
        LEFT JOIN capital_project_trimble_tracking tt 
            ON cpi.invoice_number = tt.invoice_number
        LEFT JOIN capital_project_invoices_emailed ie 
            ON cpi.invoice_number = ie.invoice_number
        LEFT JOIN notification_log nl 
            ON cpi.invoice_number = nl.invoice_number 
            AND nl.notification_type = 'release_notification'
        ORDER BY i.release_date DESC NULLS LAST, cpi.capital_project_number
    """

# Test the query directly
if __name__ == "__main__":
    from capital_project_db_manager import CapitalProjectDBManager
    import pandas as pd
    
    manager = CapitalProjectDBManager()
    
    try:
        print("🔧 Testing corrected dashboard query...")
        
        query = get_corrected_dashboard_query()
        df = pd.read_sql(query, manager.engine)
        
        print(f"✅ SUCCESS! Query returned {len(df)} records")
        print(f"Columns: {list(df.columns)}")
        
        if len(df) > 0:
            print("\nFirst few records:")
            print(df.head())
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
    
    finally:
        manager.close()
