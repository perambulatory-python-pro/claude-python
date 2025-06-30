"""
Database-Powered Invoice Management System
This replaces your Excel-based Streamlit app with database operations

Key Python Concepts:
- Dependency injection: Pass database manager to functions
- Error handling: Graceful handling of database errors
- Session state: Streamlit's way of maintaining state between interactions
- Caching: @st.cache_data for performance optimization
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

# Import our database components
from database_manager import DatabaseManager
from data_mapper import DataMapper

# Load environment variables
load_dotenv()

# Configure page
st.set_page_config(
    page_title="Invoice Management System (Database)",
    page_icon="🗄️",
    layout="wide"
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize session state
if 'db_manager' not in st.session_state:
    try:
        st.session_state.db_manager = DatabaseManager()
        st.session_state.data_mapper = DataMapper()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()

if 'processing_logs' not in st.session_state:
    st.session_state.processing_logs = []

def add_log(message: str):
    """Add message to processing logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    st.session_state.processing_logs.append(log_entry)
    logger.info(message)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_table_stats():
    """Get database statistics (cached for performance)"""
    try:
        return st.session_state.db_manager.get_table_stats()
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return {}

@st.cache_data(ttl=60)  # Cache for 1 minute  
def get_cached_invoices(filters_str: str = ""):
    """Get invoices with caching (filters_str is for cache invalidation)"""
    try:
        # Convert filters string back to dict if needed
        filters = eval(filters_str) if filters_str else None
        return st.session_state.db_manager.get_invoices(filters=filters)
    except Exception as e:
        logger.error(f"Error getting invoices: {e}")
        return pd.DataFrame()

# Title and description
st.title("🗄️ Invoice Management System (Database-Powered)")
st.markdown("**✨ Now with database storage for improved performance and scalability!**")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["🏠 Dashboard", "📊 Master Data View", "📤 File Upload & Processing", 
     "🔍 Advanced Search", "📈 Analytics", "📝 Processing Logs", "⚙️ Database Tools"]
)

# Dashboard Page
if page == "🏠 Dashboard":
    st.header("📊 Dashboard")
    
    # Database status
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("🔗 Database Status")
        if st.session_state.db_manager.test_connection():
            st.success("✅ Connected")
        else:
            st.error("❌ Connection Failed")
    
    with col2:
        st.subheader("📈 Quick Stats")
        stats = get_cached_table_stats()
        if stats:
            st.metric("Total Invoices", f"{stats.get('invoices', 0):,}")
            st.metric("Invoice Details", f"{stats.get('invoice_details', 0):,}")
            st.metric("Buildings", f"{stats.get('building_dimension', 0):,}")
        else:
            st.warning("Unable to load statistics")
    
    with col3:
        st.subheader("🚀 Quick Actions")
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()  # Clear all cached data
            st.success("Data refreshed!")
            st.rerun()
        
        if st.button("📊 View All Invoices"):
            st.session_state['nav_to'] = "📊 Master Data View"
            st.rerun()
    
    # Recent activity summary
    st.subheader("📋 System Overview")
    
    if stats:
        total_invoices = stats.get('invoices', 0)
        total_details = stats.get('invoice_details', 0)
        
        if total_invoices > 0:
            avg_details_per_invoice = total_details / total_invoices
            st.info(f"""
            **System Summary:**
            - Total Invoices: {total_invoices:,}
            - Total Detail Records: {total_details:,}  
            - Average Details per Invoice: {avg_details_per_invoice:.1f}
            - Buildings in System: {stats.get('building_dimension', 0):,}
            """)
        else:
            st.info("Database is ready for data import.")

# Master Data View Page  
elif page == "📊 Master Data View":
    st.header("📊 Master Invoice Data")
    
    # Filters
    st.subheader("🔍 Filters")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Get unique service areas for filter
        try:
            all_invoices = get_cached_invoices()
            if not all_invoices.empty:
                service_areas = ['All'] + sorted(all_invoices['service_area'].dropna().unique().tolist())
                selected_area = st.selectbox("Service Area", service_areas)
            else:
                selected_area = 'All'
                st.warning("No invoice data found")
        except Exception as e:
            st.error(f"Error loading service areas: {e}")
            selected_area = 'All'
    
    with col2:
        # Invoice number search
        search_invoice = st.text_input("🔍 Search Invoice No.", placeholder="Enter invoice number...")
    
    with col3:
        # Date range filter
        date_from = st.date_input("From Date")
    
    with col4:
        date_to = st.date_input("To Date")
    
    # Apply filters and display data
    try:
        # Build filters dictionary
        filters = {}
        if selected_area != 'All':
            filters['service_area'] = selected_area
        
        # Get filtered data
        if search_invoice:
            # Use search function for invoice number
            filtered_df = st.session_state.db_manager.search_invoices(search_invoice)
        else:
            # Use filters
            filters_str = str(filters) if filters else ""
            filtered_df = get_cached_invoices(filters_str)
        
        # Apply date filters if specified
        if not filtered_df.empty and (date_from or date_to):
            if 'invoice_date' in filtered_df.columns:
                filtered_df['invoice_date'] = pd.to_datetime(filtered_df['invoice_date'])
                if date_from:
                    filtered_df = filtered_df[filtered_df['invoice_date'] >= pd.Timestamp(date_from)]
                if date_to:
                    filtered_df = filtered_df[filtered_df['invoice_date'] <= pd.Timestamp(date_to)]
        
        # Display results
        if not filtered_df.empty:
            st.subheader(f"📋 Results ({len(filtered_df):,} records)")
            
            # Display summary metrics
            if 'invoice_total' in filtered_df.columns:
                total_amount = filtered_df['invoice_total'].sum()
                avg_amount = filtered_df['invoice_total'].mean()
                
                metric_col1, metric_col2, metric_col3 = st.columns(3)
                with metric_col1:
                    st.metric("Total Amount", f"${total_amount:,.2f}")
                with metric_col2:
                    st.metric("Average Amount", f"${avg_amount:,.2f}")
                with metric_col3:
                    st.metric("Record Count", f"{len(filtered_df):,}")
            
            # Display data table
            st.dataframe(
                filtered_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Download button
            csv_data = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv_data,
                file_name=f"invoice_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No invoices found matching your criteria.")
            
    except Exception as e:
        st.error(f"Error retrieving invoice data: {e}")
        logger.error(f"Invoice data retrieval error: {e}")

# File Upload & Processing Page
elif page == "📤 File Upload & Processing":
    st.header("📤 File Upload & Processing")
    
    # Choose processing type
    processing_type = st.selectbox(
        "Select processing type:",
        ["Invoice Master Update", "BCI Invoice Details", "AUS Invoice Details", "Kaiser SCR Building Data"]
    )
    
    # File upload
    uploaded_file = st.file_uploader(
        f"Upload {processing_type} file",
        type=['xlsx', 'xls', 'csv'],
        help="Upload Excel or CSV files"
    )
    
    if uploaded_file is not None:
        try:
            # Read the uploaded file
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.success(f"File loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Show preview
            with st.expander("📋 Preview Data"):
                st.dataframe(df.head(10))
                st.write(f"**Columns:** {', '.join(df.columns.tolist())}")
            
            # Process the data
            if st.button("🚀 Process Data", type="primary"):
                with st.spinner("Processing data..."):
                    try:
                        mapper = st.session_state.data_mapper
                        db = st.session_state.db_manager
                        
                        if processing_type == "Invoice Master Update":
                            # Map and upsert invoice data
                            mapped_data = mapper.map_invoice_data(df)
                            result = db.upsert_invoices(mapped_data)
                            
                            st.success(f"""
                            ✅ Invoice processing completed!
                            - Inserted: {result['inserted']} new invoices
                            - Updated: {result['updated']} existing invoices
                            """)
                            add_log(f"Processed {len(mapped_data)} invoice records: {result}")
                        
                        elif processing_type == "BCI Invoice Details":
                            # Map and insert BCI details
                            mapped_data = mapper.map_bci_details(df)
                            result = db.bulk_insert_invoice_details(mapped_data)
                            
                            st.success(f"""
                            ✅ BCI details processing completed!
                            - Inserted: {result:,} detail records
                            """)
                            add_log(f"Processed {result} BCI detail records")
                        
                        elif processing_type == "AUS Invoice Details":
                            # Map and insert AUS details
                            mapped_data = mapper.map_aus_details(df)
                            result = db.bulk_insert_invoice_details(mapped_data)
                            
                            st.success(f"""
                            ✅ AUS details processing completed!
                            - Inserted: {result:,} detail records  
                            """)
                            add_log(f"Processed {result} AUS detail records")
                        
                        elif processing_type == "Kaiser SCR Building Data":
                            # Map and upsert building data
                            mapped_data = mapper.map_kaiser_scr_data(df)
                            result = db.upsert_building_dimension(mapped_data)
                            
                            st.success(f"""
                            ✅ Building data processing completed!
                            - Processed: {result:,} building records
                            """)
                            add_log(f"Processed {result} building dimension records")
                        
                        # Clear cache to show updated data
                        st.cache_data.clear()
                        
                    except Exception as e:
                        st.error(f"Processing failed: {e}")
                        add_log(f"Processing error: {e}")
                        logger.error(f"File processing error: {e}")
        
        except Exception as e:
            st.error(f"Error reading file: {e}")

# Advanced Search Page
elif page == "🔍 Advanced Search":
    st.header("🔍 Advanced Search")
    
    search_type = st.selectbox("Search Type", ["Invoices", "Invoice Details"])
    
    if search_type == "Invoices":
        st.subheader("🔍 Invoice Search")
        
        col1, col2 = st.columns(2)
        with col1:
            search_term = st.text_input("Invoice Number (partial match)")
            emid_search = st.text_input("EMID")
        with col2:
            service_area_search = st.text_input("Service Area")
            amount_min = st.number_input("Minimum Amount", min_value=0.0)
        
        if st.button("🔍 Search"):
            try:
                # Build custom query
                query_parts = ["SELECT * FROM invoices WHERE 1=1"]
                
                if search_term:
                    query_parts.append(f"AND invoice_no ILIKE '%{search_term}%'")
                if emid_search:
                    query_parts.append(f"AND emid = '{emid_search}'")
                if service_area_search:
                    query_parts.append(f"AND service_area ILIKE '%{service_area_search}%'")
                if amount_min > 0:
                    query_parts.append(f"AND invoice_total >= {amount_min}")
                
                query = " ".join(query_parts)
                results = st.session_state.db_manager.execute_custom_query(query)
                
                if not results.empty:
                    st.success(f"Found {len(results)} matching invoices")
                    st.dataframe(results, use_container_width=True)
                else:
                    st.warning("No matching invoices found")
                    
            except Exception as e:
                st.error(f"Search error: {e}")

# Analytics Page
elif page == "📈 Analytics":
    st.header("📈 Analytics Dashboard")
    
    try:
        # Get summary statistics
        stats_query = """
        SELECT 
            COUNT(*) as total_invoices,
            SUM(invoice_total) as total_amount,
            AVG(invoice_total) as avg_amount,
            service_area,
            COUNT(*) as area_count
        FROM invoices 
        WHERE invoice_total IS NOT NULL
        GROUP BY service_area
        ORDER BY area_count DESC
        """
        
        analytics_data = st.session_state.db_manager.execute_custom_query(stats_query)
        
        if not analytics_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📊 By Service Area")
                st.dataframe(analytics_data)
            
            with col2:
                st.subheader("💰 Financial Summary")
                total_amount = analytics_data['total_amount'].sum()
                total_invoices = analytics_data['area_count'].sum()
                st.metric("Total Amount", f"${total_amount:,.2f}")
                st.metric("Total Invoices", f"{total_invoices:,}")
        
    except Exception as e:
        st.error(f"Analytics error: {e}")

# Processing Logs Page
elif page == "📝 Processing Logs":
    st.header("📝 Processing Logs")
    
    if st.session_state.processing_logs:
        st.subheader("Current Session Logs")
        for log in st.session_state.processing_logs:
            st.text(log)
        
        if st.button("Clear Session Logs"):
            st.session_state.processing_logs = []
            st.success("Logs cleared!")
    else:
        st.info("No processing logs for this session yet.")

# Database Tools Page
elif page == "⚙️ Database Tools":
    st.header("⚙️ Database Tools")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Table Statistics")
        if st.button("🔄 Refresh Stats"):
            stats = get_cached_table_stats()
            for table, count in stats.items():
                st.metric(table.replace('_', ' ').title(), f"{count:,}")
    
    with col2:
        st.subheader("🔧 Maintenance")
        if st.button("🧹 Clear Cache"):
            st.cache_data.clear()
            st.success("Cache cleared!")
        
        if st.button("🔍 Test Connection"):
            if st.session_state.db_manager.test_connection():
                st.success("✅ Database connection successful")
            else:
                st.error("❌ Database connection failed")

# Footer
st.markdown("---")
st.markdown("*Invoice Management System - Database-Powered Edition*")

# Handle navigation from other pages
if 'nav_to' in st.session_state:
    del st.session_state['nav_to']