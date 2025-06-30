"""
Enhanced Database-Powered Invoice Management System
Addresses all 4 identified issues:
1. Date input prompting for all processing types
2. Proper date population logic
3. "Not Transmitted" business rules
4. Optional date filtering (no required From Date)

Key Python Concepts:
- Conditional UI rendering: Show/hide elements based on state
- Date handling: Optional vs required date inputs
- Business logic integration: Complex processing rules
- State management: Preserve user inputs across interactions
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime, date
from dotenv import load_dotenv
import logging

# Import our enhanced database components
from database_manager_enhanced import EnhancedDatabaseManager
from data_mapper import DataMapper

# Load environment variables
load_dotenv()

# Configure page
st.set_page_config(
    page_title="Invoice Management System (Enhanced)",
    page_icon="🗄️",
    layout="wide"
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize session state
if 'enhanced_db_manager' not in st.session_state:
    try:
        st.session_state.enhanced_db_manager = EnhancedDatabaseManager()
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
        return st.session_state.enhanced_db_manager.get_table_stats()
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return {}

@st.cache_data(ttl=60)  # Cache for 1 minute  
def get_cached_invoices(filters_str: str = ""):
    """Get invoices with caching (filters_str is for cache invalidation)"""
    try:
        # Convert filters string back to dict if needed
        filters = eval(filters_str) if filters_str else None
        return st.session_state.enhanced_db_manager.get_invoices(filters=filters)
    except Exception as e:
        logger.error(f"Error getting invoices: {e}")
        return pd.DataFrame()

def process_file_with_date_logic(uploaded_file, processing_type: str, processing_date: date):
    """
    Process uploaded file with proper date logic and business rules
    
    Args:
        uploaded_file: Streamlit uploaded file object
        processing_type: 'EDI', 'Release', or 'Add-On'
        processing_date: Date to use for processing
    """
    try:
        # Read the uploaded file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        add_log(f"Loaded {len(df)} rows from {uploaded_file.name}")
        
        # Map data to database format
        mapper = st.session_state.data_mapper
        mapped_data = mapper.map_invoice_data(df)
        
        if not mapped_data:
            st.error("No valid data found in file")
            return
        
        # Process with enhanced business logic
        db = st.session_state.enhanced_db_manager
        result = db.upsert_invoices_with_business_logic(
            mapped_data, 
            processing_type, 
            processing_date
        )
        
        # Handle invoice history linking for Add-On files
        if processing_type == 'Add-On':
            # Check for "Original invoice #" field in original data
            original_df = df.copy()
            if 'Original invoice #' in original_df.columns:
                # Add history linking data to mapped data
                for i, mapped_record in enumerate(mapped_data):
                    if i < len(original_df):
                        original_invoice = original_df.iloc[i].get('Original invoice #')
                        if pd.notna(original_invoice):
                            mapped_record['original_invoice_no'] = str(original_invoice)
                
                # Process history linking
                history_links = db.process_invoice_history_linking(mapped_data)
                st.info(f"Created {history_links} invoice history links")
                add_log(f"Created {history_links} invoice history links")
        
        # Show results
        st.success(f"""
        ✅ {processing_type} processing completed!
        - Date used: {processing_date}
        - Inserted: {result['inserted']} new invoices
        - Updated: {result['updated']} existing invoices
        """)
        
        add_log(f"{processing_type} processing: {result['inserted']} inserted, {result['updated']} updated")
        
        # Clear cache to show updated data
        st.cache_data.clear()
        
    except Exception as e:
        st.error(f"Processing failed: {e}")
        add_log(f"Processing error: {e}")
        logger.error(f"File processing error: {e}")

def render_date_input_section(processing_type: str) -> date:
    """
    Render date input section for processing
    
    Args:
        processing_type: 'EDI', 'Release', or 'Add-On'
        
    Returns:
        Selected date
    """
    st.subheader(f"📅 {processing_type} Date")
    
    # Default to today's date
    default_date = date.today()
    
    # Date input with clear labeling
    if processing_type == 'EDI':
        processing_date = st.date_input(
            f"EDI Date (when invoices were submitted)",
            value=default_date,
            help="This date will be populated in the EDI Date field for all invoices"
        )
    elif processing_type == 'Release':
        processing_date = st.date_input(
            f"Release Date (when invoices were released)",
            value=default_date,
            help="This date will be populated in the Release Date field for all invoices"
        )
    elif processing_type == 'Add-On':
        processing_date = st.date_input(
            f"Add-On Date (when add-on invoices were submitted)",
            value=default_date,
            help="This date will be populated in the Add-On Date field for all invoices"
        )
    
    # Show date preservation logic explanation
    with st.expander("ℹ️ Date Logic Explanation"):
        st.markdown(f"""
        **{processing_type} Date Processing Logic:**
        
        **For New Invoices:**
        - {processing_type} Date will be set to: `{processing_date}`
        - Not Transmitted: `{'True (held for validation)' if processing_type == 'EDI' else 'Unchanged'}`
        
        **For Existing Invoices:**
        - If no existing {processing_type} Date: Set to `{processing_date}`
        - If existing {processing_type} Date differs: Move existing to Original {processing_type} Date, set new date to `{processing_date}`
        - If Original {processing_type} Date already exists: Preserve original, update current to `{processing_date}`
        
        {'**Not Transmitted Logic (EDI only):**' if processing_type == 'EDI' else ''}
        {f'- New EDI submissions: Set to True (held for validation)' if processing_type == 'EDI' else ''}
        {f'- Updated EDI submissions: Set to False (transmitted after release)' if processing_type == 'EDI' else ''}
        """)
    
    return processing_date

# Title and description
st.title("🗄️ Invoice Management System (Enhanced)")
st.markdown("**✨ Now with proper date handling and business logic!**")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["🏠 Dashboard", "📊 Master Data View", "📤 File Upload & Processing", 
     "🚀 Quick Process", "🔍 Advanced Search", "📝 Processing Logs"]
)

# Dashboard Page
if page == "🏠 Dashboard":
    st.header("📊 Dashboard")
    
    # Database status
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("🔗 Database Status")
        if st.session_state.enhanced_db_manager.test_connection():
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
    
    with col3:
        st.subheader("🚀 Quick Actions")
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.success("Data refreshed!")
            st.rerun()

# Master Data View Page (FIXED: Optional date filtering)
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
        # FIXED: Optional date range filter (no required dates)
        use_date_filter = st.checkbox("📅 Filter by Date Range")
        if use_date_filter:
            date_from = st.date_input("From Date (optional)")
        else:
            date_from = None
    
    with col4:
        if use_date_filter:
            date_to = st.date_input("To Date (optional)")
        else:
            date_to = None
    
    # Apply filters and display data
    try:
        # Build filters dictionary
        filters = {}
        if selected_area != 'All':
            filters['service_area'] = selected_area
        
        # Get filtered data
        if search_invoice:
            # Use search function for invoice number
            filtered_df = st.session_state.enhanced_db_manager.search_invoices(search_invoice)
        else:
            # Use filters
            filters_str = str(filters) if filters else ""
            filtered_df = get_cached_invoices(filters_str)
        
        # Apply date filters if specified and checkbox is checked
        if not filtered_df.empty and use_date_filter and (date_from or date_to):
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
            st.info("No invoices found matching your criteria.")
            
    except Exception as e:
        st.error(f"Error retrieving invoice data: {e}")
        logger.error(f"Invoice data retrieval error: {e}")

# File Upload & Processing Page (FIXED: Always prompt for dates)
elif page == "📤 File Upload & Processing":
    st.header("📤 File Upload & Processing")
    
    # Choose processing type
    processing_type = st.selectbox(
        "Select processing type:",
        ["EDI", "Release", "Add-On", "BCI Invoice Details", "AUS Invoice Details", "Kaiser SCR Building Data"]
    )
    
    # FIXED: Always show date input for EDI/Release/Add-On
    processing_date = None
    if processing_type in ['EDI', 'Release', 'Add-On']:
        processing_date = render_date_input_section(processing_type)
    
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
            
            # FIXED: Process with proper date logic
            if st.button("🚀 Process Data", type="primary"):
                with st.spinner("Processing data..."):
                    if processing_type in ['EDI', 'Release', 'Add-On']:
                        # Use enhanced processing with date logic
                        process_file_with_date_logic(uploaded_file, processing_type, processing_date)
                    
                    elif processing_type == "BCI Invoice Details":
                        # Handle BCI invoice details
                        mapper = st.session_state.data_mapper
                        mapped_data = mapper.map_bci_details(df)
                        result = st.session_state.enhanced_db_manager.bulk_insert_invoice_details(mapped_data)
                        
                        st.success(f"✅ BCI details processing completed! Inserted: {result:,} detail records")
                        add_log(f"Processed {result} BCI detail records")
                    
                    elif processing_type == "AUS Invoice Details":
                        # Handle AUS invoice details
                        mapper = st.session_state.data_mapper
                        mapped_data = mapper.map_aus_details(df)
                        result = st.session_state.enhanced_db_manager.bulk_insert_invoice_details(mapped_data)
                        
                        st.success(f"✅ AUS details processing completed! Inserted: {result:,} detail records")
                        add_log(f"Processed {result} AUS detail records")
                    
                    elif processing_type == "Kaiser SCR Building Data":
                        # Handle building data (no date logic needed)
                        mapper = st.session_state.data_mapper
                        mapped_data = mapper.map_kaiser_scr_data(df)
                        # Note: This would need a building dimension upsert method
                        st.info("Kaiser SCR processing not yet implemented in enhanced manager")
        
        except Exception as e:
            st.error(f"Error reading file: {e}")

# Quick Process Page (FIXED: Always prompt for dates)
elif page == "🚀 Quick Process":
    st.header("🚀 Quick Process")
    st.markdown("Process standard weekly files with proper date handling")
    
    # Date input section for each file type
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("📅 Release Date")
        release_date = st.date_input("Release Date", value=date.today(), key="release_date")
        release_file_exists = os.path.exists("weekly_release.xlsx")
        st.write(f"File: {'✅ Found' if release_file_exists else '❌ Not Found'} (weekly_release.xlsx)")
    
    with col2:
        st.subheader("📅 Add-On Date")
        addon_date = st.date_input("Add-On Date", value=date.today(), key="addon_date")
        addon_file_exists = os.path.exists("weekly_addons.xlsx")
        st.write(f"File: {'✅ Found' if addon_file_exists else '❌ Not Found'} (weekly_addons.xlsx)")
    
    with col3:
        st.subheader("📅 EDI Date")
        edi_date = st.date_input("EDI Date", value=date.today(), key="edi_date")
        edi_file_exists = os.path.exists("weekly_edi.xlsx")
        st.write(f"File: {'✅ Found' if edi_file_exists else '❌ Not Found'} (weekly_edi.xlsx)")
    
    # Process All button with date logic
    if st.button("🔄 Process All 3 Files (Release → Add-On → EDI)", type="primary"):
        files_to_process = [
            ("weekly_release.xlsx", "Release", release_date),
            ("weekly_addons.xlsx", "Add-On", addon_date), 
            ("weekly_edi.xlsx", "EDI", edi_date)
        ]
        
        processed_count = 0
        
        for filename, file_type, processing_date in files_to_process:
            if os.path.exists(filename):
                with st.spinner(f"Processing {filename} with {file_type} date {processing_date}..."):
                    try:
                        # Read file
                        df = pd.read_excel(filename)
                        
                        # Map data
                        mapper = st.session_state.data_mapper
                        mapped_data = mapper.map_invoice_data(df)
                        
                        # Process with business logic
                        db = st.session_state.enhanced_db_manager
                        result = db.upsert_invoices_with_business_logic(
                            mapped_data, 
                            file_type, 
                            processing_date
                        )
                        
                        st.success(f"✅ {file_type} file processed! (Date: {processing_date})")
                        add_log(f"{file_type} processing with date {processing_date}: {result}")
                        processed_count += 1
                        
                    except Exception as e:
                        st.error(f"❌ Error processing {filename}: {e}")
                        add_log(f"Error processing {filename}: {e}")
            else:
                st.warning(f"⚠️ {filename} not found")
        
        if processed_count > 0:
            st.success(f"🎉 Processed {processed_count} files successfully!")
            st.cache_data.clear()  # Refresh data
        else:
            st.error("❌ No files were processed")

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
                results = st.session_state.enhanced_db_manager.execute_custom_query(query)
                
                if not results.empty:
                    st.success(f"Found {len(results)} matching invoices")
                    st.dataframe(results, use_container_width=True)
                else:
                    st.warning("No matching invoices found")
                    
            except Exception as e:
                st.error(f"Search error: {e}")

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

# Footer
st.markdown("---")
st.markdown("*Invoice Management System - Enhanced with Business Logic*")
