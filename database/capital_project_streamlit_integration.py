"""
Capital Project Streamlit Integration
Add capital project tracking to your existing invoice management app

Python Learning Concepts:
1. Streamlit App Extensions - Adding new pages to existing apps
2. File Upload Handling - Processing CSV files through web interface
3. Data Visualization - Creating dashboards and status displays
4. Real-time Updates - Refreshing data and showing progress
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
from sqlalchemy import text
import io
from database.capital_project_db_manager import CapitalProjectDBManager

def add_capital_project_pages():
    """
    Add capital project pages to your existing Streamlit invoice app
    Call this function from your main app
    """
    
    # Add to your existing page selection
    capital_pages = [
        "📊 Capital Project Dashboard",
        "📁 Process Trimble File", 
        "🔍 Capital Project Search",
        "📈 Capital Project Analytics",
        "📧 Email Notifications"
    ]
    
    return capital_pages

# Fixed Capital Project Dashboard function
def render_capital_project_dashboard():
    """Capital Project Dashboard using actual view columns"""
    st.header("📊 Capital Project Dashboard")
    st.write("Overview of all capital project invoices and their status")
    
    db = st.session_state.enhanced_db_manager
    
    # Add filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        date_range = st.selectbox(
            "Date Range",
            ["Last 30 Days", "Last 90 Days", "This Year", "All Time"],
            index=3  # Default to All Time
        )
    
    with col2:
        project_search = st.text_input("Search Project/Invoice/EMID")
    
    with col3:
        min_amount = st.number_input("Minimum Invoice Amount", value=0.0, min_value=0.0)
    
    # Calculate date filter
    if date_range == "Last 30 Days":
        date_filter = datetime.now() - timedelta(days=30)
    elif date_range == "Last 90 Days":
        date_filter = datetime.now() - timedelta(days=90)
    elif date_range == "This Year":
        date_filter = datetime(datetime.now().year, 1, 1)
    else:
        date_filter = None
    
    try:
        # Query using actual column names from the capital_projects view
        query = """
        SELECT 
            cap_project_no,
            emid,
            job_code,
            region,
            mc_service_area,
            invoice_no,
            invoice_date,
            invoice_total,
            edi_date,
            release_date,
            current_step,
            status,
            payment_reference,
            onelink_voucher_id,
            add_on_date,
            original_edi_date
        FROM capital_projects
        WHERE 1=1
        """
        
        params = []
        
        if date_filter:
            query += " AND invoice_date >= %s"
            params.append(date_filter)
        
        if project_search:
            query += """ AND (cap_project_no ILIKE %s 
                         OR invoice_no ILIKE %s 
                         OR emid ILIKE %s
                         OR mc_service_area ILIKE %s)"""
            search_param = f"%{project_search}%"
            params.extend([search_param, search_param, search_param, search_param])
        
        if min_amount > 0:
            query += " AND invoice_total >= %s"
            params.append(min_amount)
        
        query += " ORDER BY invoice_date DESC"
        
        # Execute query
        df = db.execute_custom_query(query, params)
        
        if not df.empty:
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Capital Projects", len(df))
            with col2:
                total_amount = df['invoice_total'].sum()
                st.metric("Total Amount", f"${float(total_amount):,.2f}")
            with col3:
                avg_amount = df['invoice_total'].mean()
                st.metric("Avg Invoice", f"${float(avg_amount):,.2f}")
            with col4:
                unique_projects = df['cap_project_no'].nunique()
                st.metric("Unique Projects", unique_projects)
            
            st.markdown("---")
            
            # Additional metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                regions = df['region'].value_counts()
                st.write("**Projects by Region:**")
                for region, count in regions.items():
                    if pd.notna(region):
                        st.write(f"- {region}: {count}")
            
            with col2:
                statuses = df['status'].value_counts()
                st.write("**Projects by Status:**")
                for status, count in statuses.items():
                    if pd.notna(status):
                        st.write(f"- {status}: {count}")
            
            with col3:
                st.write("**Processing Status:**")
                edi_count = df['edi_date'].notna().sum()
                released_count = df['release_date'].notna().sum()
                st.write(f"- EDI Submitted: {edi_count}")
                st.write(f"- Released: {released_count}")
            
            st.markdown("---")
            
            # Format and display data
            display_df = df.copy()
            
            # Format columns
            display_df['invoice_total'] = display_df['invoice_total'].apply(
                lambda x: f"${float(x):,.2f}" if pd.notna(x) else ""
            )
            
            # Format dates
            date_cols = ['invoice_date', 'edi_date', 'release_date', 'add_on_date', 'original_edi_date']
            for col in date_cols:
                if col in display_df.columns:
                    display_df[col] = pd.to_datetime(display_df[col]).dt.strftime('%Y-%m-%d')
                    display_df[col] = display_df[col].fillna('')
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'cap_project_no': 'Capital Project #',
                'emid': 'EMID',
                'job_code': 'Job Code',
                'region': 'Region',
                'mc_service_area': 'Service Area',
                'invoice_no': 'Invoice #',
                'invoice_date': 'Invoice Date',
                'invoice_total': 'Amount',
                'edi_date': 'EDI Date',
                'release_date': 'Release Date',
                'current_step': 'Current Step',
                'status': 'Status',
                'payment_reference': 'Payment Ref',
                'onelink_voucher_id': 'OneLink ID',
                'add_on_date': 'Add-On Date',
                'original_edi_date': 'Original EDI Date'
            })
            
            # Select columns to display (you can adjust this based on what's most important)
            display_columns = [
                'Capital Project #', 'Invoice #', 'Amount', 'Invoice Date',
                'EMID', 'Service Area', 'Region', 'Status', 
                'EDI Date', 'Release Date'
            ]
            
            # Only show columns that exist
            available_display_cols = [col for col in display_columns if col in display_df.columns]
            
            st.dataframe(
                display_df[available_display_cols],
                use_container_width=True,
                hide_index=True
            )
            
            # Export button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Capital Projects Data",
                data=csv,
                file_name=f"capital_projects_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
        else:
            st.info("No capital projects found matching your filters.")
            
            # Check if there's any data at all
            try:
                count_query = "SELECT COUNT(*) as count FROM capital_projects"
                count_result = db.execute_custom_query(count_query)
                total_count = count_result['count'].iloc[0] if not count_result.empty else 0
                
                if total_count > 0:
                    st.write(f"Note: There are {total_count} total capital projects. Try adjusting your filters.")
                else:
                    st.warning("No capital projects found. Capital projects are identified by 'CAP' in the chartfield.")
                    st.info("Process invoices with 'CAP' in their chartfield to see them here.")
            except:
                pass
                
    except Exception as e:
        st.error(f"Error loading capital projects: {e}")
        import traceback
        with st.expander("Error Details"):
            st.text(traceback.format_exc())

def render_process_trimble_file():
    """
    Page for processing Trimble CSV files
    """
    st.header("📁 Process Trimble File")
    st.markdown("Upload and process Trimble Direct Pay CSV files")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Choose Trimble CSV file",
        type=['csv'],
        help="Upload the Direct Pay CSV file from Trimble"
    )
    
    if uploaded_file is not None:
        # Show file details
        file_details = {
            "Filename": uploaded_file.name,
            "File size": f"{uploaded_file.size:,} bytes",
            "File type": uploaded_file.type
        }
        
        st.write("📋 File Details:")
        for key, value in file_details.items():
            st.write(f"- **{key}**: {value}")
        
        # Preview the file
        try:
            df = pd.read_csv(uploaded_file)
            st.write(f"📊 File contains {len(df)} records with {len(df.columns)} columns")
            
            with st.expander("Preview Data (first 5 rows)"):
                st.dataframe(df.head(), use_container_width=True)
            
            # Validate required columns
            required_columns = [
                'Vendor Reference/Invoice Number',
                'Current Step', 
                'Status',
                'Project Number'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"Missing required columns: {missing_columns}")
                st.stop()
            else:
                st.success("✅ All required columns found")
            
            # Process button
            if st.button("🚀 Process Trimble File", type="primary"):
                
                # Save uploaded file temporarily
                temp_filename = f"temp_trimble_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                
                with open(temp_filename, 'wb') as f:
                    f.write(uploaded_file.getvalue())
                
                # Process with progress bar
                with st.spinner("Processing Trimble file..."):
                    manager = CapitalProjectDBManager()
                    
                    try:
                        results = manager.process_trimble_csv(temp_filename)
                        
                        # Show results
                        st.success("✅ Processing completed!")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Inserted", results.get('inserted', 0))
                        with col2:
                            st.metric("Updated", results.get('updated', 0))
                        with col3:
                            st.metric("Step Changes", results.get('step_changes', 0))
                        with col4:
                            st.metric("Errors", results.get('errors', 0))
                        
                        # Show failed invoices if any
                        if results.get('failed_invoices'):
                            st.warning("⚠️ Some invoices failed to process:")
                            for invoice in results['failed_invoices']:
                                st.write(f"- {invoice}")
                        
                        # Refresh data cache
                        st.cache_data.clear()
                        
                        st.info("💡 Go to the Capital Project Dashboard to see updated data")
                        
                    except Exception as e:
                        st.error(f"Processing failed: {e}")
                    
                    finally:
                        manager.close()
                        # Clean up temp file
                        import os
                        if os.path.exists(temp_filename):
                            os.remove(temp_filename)
        
        except Exception as e:
            st.error(f"Error reading file: {e}")

def render_capital_project_search():
    """
    Search and filter capital projects
    """
    st.header("🔍 Capital Project Search")
    st.markdown("Search for specific capital projects and invoices")
    
    manager = CapitalProjectDBManager()
    
    try:
        # Search options
        search_type = st.radio(
            "Search by:",
            ["Invoice Number", "Capital Project Number", "Trimble Status", "Date Range"]
        )
        
        if search_type == "Invoice Number":
            search_term = st.text_input("Enter Invoice Number")
            if search_term:
                query = """
                    SELECT * FROM capital_project_invoices cpi
                    JOIN invoices i ON cpi.invoice_number = i.invoice_no
                    LEFT JOIN capital_project_trimble_tracking tt ON cpi.invoice_number = tt.invoice_number
                    WHERE cpi.invoice_number ILIKE %s
                """
                results = pd.read_sql(query, manager.engine, params=[f'%{search_term}%'])
        
        elif search_type == "Capital Project Number":
            search_term = st.text_input("Enter Capital Project Number")
            if search_term:
                query = """
                    SELECT * FROM capital_project_invoices cpi
                    JOIN invoices i ON cpi.invoice_number = i.invoice_no
                    LEFT JOIN capital_project_trimble_tracking tt ON cpi.invoice_number = tt.invoice_number
                    WHERE cpi.capital_project_number ILIKE %s
                """
                results = pd.read_sql(query, manager.engine, params=[f'%{search_term}%'])
        
        elif search_type == "Trimble Status":
            status_options = pd.read_sql(
                "SELECT DISTINCT status FROM capital_project_trimble_tracking WHERE status IS NOT NULL",
                manager.engine
            )['status'].tolist()
            
            selected_status = st.selectbox("Select Status", status_options)
            if selected_status:
                query = """
                    SELECT * FROM capital_project_invoices cpi
                    JOIN invoices i ON cpi.invoice_number = i.invoice_no
                    JOIN capital_project_trimble_tracking tt ON cpi.invoice_number = tt.invoice_number
                    WHERE tt.status = %s
                """
                results = pd.read_sql(query, manager.engine, params=[selected_status])
        
        elif search_type == "Date Range":
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")
            
            if start_date and end_date:
                query = """
                    SELECT * FROM capital_project_invoices cpi
                    JOIN invoices i ON cpi.invoice_number = i.invoice_no
                    LEFT JOIN capital_project_trimble_tracking tt ON cpi.invoice_number = tt.invoice_number
                    WHERE i.release_date BETWEEN %s AND %s
                """
                results = pd.read_sql(query, manager.engine, params=[start_date, end_date])
        
        # Display results
        if 'results' in locals() and len(results) > 0:
            st.subheader(f"📋 Search Results ({len(results)} found)")
            st.dataframe(results, use_container_width=True, hide_index=True)
            
            # Download results
            csv_data = results.to_csv(index=False)
            st.download_button(
                label="📥 Download Search Results",
                data=csv_data,
                file_name=f"search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        elif 'search_term' in locals() or 'selected_status' in locals() or ('start_date' in locals() and 'end_date' in locals()):
            st.info("No results found for your search criteria")
    
    except Exception as e:
        st.error(f"Search error: {e}")
    
    finally:
        manager.close()

# Example of how to integrate into your existing app
def integrate_with_existing_app():
    """
    Example of how to add these pages to your existing invoice_app.py
    """
    example_code = '''
    # Add this to your existing invoice_app.py
    
    # Import the capital project functions
    from capital_project_streamlit_integration import (
        add_capital_project_pages,
        render_capital_project_dashboard,
        render_process_trimble_file,
        render_capital_project_search
    )
    
    # Add capital project pages to your existing page list
    pages = [
        "🏠 Home",
        "📄 File Converter", 
        "📊 Release Processing",
        "➕ Add-On Processing", 
        "📤 EDI Processing",
        "👀 Master Data View",
        "📝 Processing Logs"
    ] + add_capital_project_pages()  # Add capital project pages
    
    # Add the page routing
    elif page == "📊 Capital Project Dashboard":
        render_capital_project_dashboard()
    elif page == "📁 Process Trimble File":
        render_process_trimble_file()
    elif page == "🔍 Capital Project Search":
        render_capital_project_search()
    '''
    
    st.code(example_code, language='python')

if __name__ == "__main__":
    # Demo the capital project pages
    st.set_page_config(page_title="Capital Project Manager", layout="wide")
    
    pages = [
        "📊 Capital Project Dashboard",
        "📁 Process Trimble File",
        "🔍 Capital Project Search",
        "🔧 Integration Example"
    ]
    
    page = st.sidebar.selectbox("Choose a page", pages)
    
    if page == "📊 Capital Project Dashboard":
        render_capital_project_dashboard()
    elif page == "📁 Process Trimble File":
        render_process_trimble_file()
    elif page == "🔍 Capital Project Search":
        render_capital_project_search()
    elif page == "🔧 Integration Example":
        st.header("🔧 Integration with Existing App")
        integrate_with_existing_app()
