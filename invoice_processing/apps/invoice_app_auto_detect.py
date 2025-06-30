"""
Enhanced Streamlit App with Auto File Type Detection
Automatically detects AUS_Invoice.xlsx and TLM_BCI.xlsx files

Key Features:
- Auto-detects file types from filename patterns
- Smart processing type selection
- Handles your specific file naming conventions
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime, date, timedelta
from sqlalchemy import text
from typing import Dict, List
from dotenv import load_dotenv
import logging

# Import our enhanced database components
from database_manager_compatible import CompatibleEnhancedDatabaseManager
from data_mapper_enhanced import EnhancedDataMapper

# NEW: Import smart processing components
from fixed_date_converter import patch_enhanced_data_mapper
from smart_duplicate_handler import process_file_with_smart_duplicates

# Import the capital project functions
from capital_project_streamlit_integration import (
    add_capital_project_pages,
    render_capital_project_dashboard,
    render_process_trimble_file,
    render_capital_project_search
)

def check_email_dependencies():
    """Check if required dependencies for email processing are available"""
    missing_deps = []
    
    try:
        import extract_msg
    except ImportError:
        missing_deps.append("extract-msg (for Outlook .msg files)")
    
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        missing_deps.append("beautifulsoup4 (for HTML parsing)")
    
    return missing_deps

# Check dependencies on startup
missing_deps = check_email_dependencies()
if missing_deps:
    st.sidebar.warning("⚠️ **Optional Dependencies Missing:**")
    for dep in missing_deps:
        st.sidebar.write(f"• {dep}")
    st.sidebar.info("Install with: `pip install extract-msg beautifulsoup4`")

# Apply the date fix when the app starts
patch_enhanced_data_mapper()

# Load environment variables
load_dotenv()

# Configure page
st.set_page_config(
    page_title="Invoice Lifecycle Management System",
    page_icon="🤖",
    layout="wide"
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize session state
if 'enhanced_db_manager' not in st.session_state:
    try:
        st.session_state.enhanced_db_manager = CompatibleEnhancedDatabaseManager()
        st.session_state.data_mapper = EnhancedDataMapper()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()

if 'processing_logs' not in st.session_state:
    st.session_state.processing_logs = []

def safe_dataframe_display(df, num_rows=10):
    """
    Safely prepare a DataFrame for Streamlit display by converting problematic columns
    """
    # Create a copy to avoid modifying the original
    display_df = df.head(num_rows).copy()
    
    # Convert object columns that might have mixed types to strings
    for col in display_df.columns:
        if display_df[col].dtype == 'object':
            try:
                # Convert to string to ensure consistent type
                display_df[col] = display_df[col].astype(str)
                # Replace 'nan' strings with empty strings for cleaner display
                display_df[col] = display_df[col].replace('nan', '')
            except:
                pass
    
    return display_df

def process_aus_file_smart_streamlit(uploaded_file):
    """Smart AUS file processing with validation"""
    try:
    # Read the file
        if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
        else:
                df = pd.read_excel(uploaded_file)
            
        st.write(f"📊 Processing {len(df):,} AUS records from {uploaded_file.name}")
            
            # Process with smart validation
        with st.spinner("🧠 Processing with smart validation..."):
                result = process_file_with_smart_duplicates(
                    df=df, 
                    source_system='AUS',
                    database_url=os.getenv('DATABASE_URL')
                )
        
        # Display results in Streamlit
        display_smart_processing_results_with_export(result, "AUS", uploaded_file.name)
        
    except Exception as e:
        st.error(f"❌ Error processing AUS file: {e}")
        add_log(f"Error processing AUS file: {e}")

def process_bci_file_smart_streamlit(uploaded_file):
    """Smart BCI file processing with validation"""
    try:
        # Read the file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        st.write(f"📊 Processing {len(df):,} BCI records from {uploaded_file.name}")
        
        # Process with smart validation
        with st.spinner("🧠 Processing with smart validation..."):
            result = process_file_with_smart_duplicates(
                df=df, 
                source_system='BCI',
                database_url=os.getenv('DATABASE_URL')
            )
            
        # Display results in Streamlit
        display_smart_processing_results_with_export(result, "BCI", uploaded_file.name)
        
    except Exception as e:
        st.error(f"❌ Error processing BCI file: {e}")
        add_log(f"Error processing BCI file: {e}")

def display_smart_processing_results_with_export(result, file_type, original_filename):
    """Display smart processing results - fixed for new data structure"""
    if result.get('success', False):
        st.success(f"✅ {file_type} Processing Completed!")
        
        # Main metrics - handle missing fields gracefully
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Input", f"{result.get('total_records', 0):,}")
        with col2:
            st.metric("✅ Inserted", f"{result.get('inserted', 0):,}")
        with col3:  
            st.metric("⚠️ Missing Invoices", f"{result.get('missing_invoice_count', 0):,}")
        with col4:
            st.metric("❌ Other Errors", f"{len(result.get('error_records', [])):,}")
        
        # Processing efficiency
        total_input = result.get('total_records', 0)
        total_inserted = result.get('inserted', 0)
        efficiency = (total_inserted / total_input * 100) if total_input > 0 else 0
        
        st.info(f"📈 **Processing Efficiency:** {efficiency:.1f}% of input records were inserted")
        
        # Show warnings for missing invoices
        if result.get('missing_invoice_count', 0) > 0:
            st.warning(f"⚠️ **Data Integrity Issue:** {result['missing_invoice_count']} records had invoice numbers not found in main invoices table")
            
            with st.expander("📋 View Missing Invoice Numbers", expanded=True):
                st.write("**These invoice numbers were in the detail file but not in your main invoices table:**")
                
                missing_invoices = result.get('missing_invoices', [])
                
                # Display in columns for better readability
                cols = st.columns(3)
                for i, invoice in enumerate(missing_invoices[:15]):
                    cols[i % 3].write(f"• {invoice}")
                
                if len(missing_invoices) > 15:
                    st.write(f"... and {len(missing_invoices) - 15} more")
                
                st.info("💡 **Tip:** Process these invoices through Weekly Release/EDI first, then re-upload this detail file.")
                
                # Direct download button
                if missing_invoices:
                    missing_df = pd.DataFrame({'Missing_Invoice_Numbers': missing_invoices})
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"missing_invoices_{file_type}_{timestamp}.csv"
                    
                    csv = missing_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Missing Invoice Numbers",
                        data=csv,
                        file_name=filename,
                        mime="text/csv",
                        key="download_missing_inv"
                    )
                
                # Also download full records if available
                missing_records = result.get('missing_invoice_records', [])
                if missing_records:
                    missing_records_df = pd.DataFrame(missing_records)
                    missing_records_csv = missing_records_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Full Records with Missing Invoices",
                        data=missing_records_csv,
                        file_name=f"records_missing_invoices_{file_type}_{timestamp}.csv",
                        mime="text/csv",
                        key="download_missing_records"
                    )
        
        # Show other errors if any
        error_records = result.get('error_records', [])
        if error_records:
            with st.expander(f"❌ Other Errors ({len(error_records)} records)"):
                st.error("These records failed due to data quality issues.")
                
                # Show first few errors
                for i, record in enumerate(error_records[:5]):
                    error_msg = record.get('error', 'Unknown error')
                    st.write(f"• {error_msg}")
                
                if len(error_records) > 5:
                    st.write(f"... and {len(error_records) - 5} more")
                
                # Download error records
                error_df = pd.DataFrame(error_records)
                error_csv = error_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Error Records",
                    data=error_csv,
                    file_name=f"error_records_{file_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_errors"
                )
    else:
        st.error(f"❌ {file_type} processing failed. Check the logs for details.")

def display_validation_results(validation_results: Dict):
    """
    Display validation results in a formatted way with download option
    """
    # DEBUG: Check what we received
    #st.write("🔍 DEBUG: display_validation_results called")
    #st.write(f"DEBUG: validation_results type: {type(validation_results)}")
    #st.write(f"DEBUG: validation_results keys: {list(validation_results.keys())}")
    #st.write(f"DEBUG: total_invoices_checked: {validation_results.get('total_invoices_checked', 'NOT FOUND')}")
    
    st.markdown("#### Validation Summary")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Invoices", validation_results['total_invoices_checked'])
    with col2:
        st.metric("✅ Matching", validation_results['matching_invoices'])
    with col3:
        st.metric("❌ Mismatched", validation_results['mismatched_invoices'])
    
    # Show discrepancies if any
    if validation_results['mismatched_invoices'] > 0:
        st.warning(f"Found {validation_results['mismatched_invoices']} invoices with total mismatches (tolerance: 0.5%)")
        
        with st.expander("📊 Discrepancy Details"):
            disc_df = pd.DataFrame(validation_results['discrepancies'][:20])
            
            # Format currency columns
            currency_cols = ['invoice_total', 'detail_total', 'discrepancy']
            for col in currency_cols:
                if col in disc_df.columns:
                    disc_df[col] = disc_df[col].apply(lambda x: f"${x:,.2f}")
            
            if 'discrepancy_pct' in disc_df.columns:
                disc_df['discrepancy_pct'] = disc_df['discrepancy_pct'].apply(lambda x: f"{x:.1f}%")
            
            st.dataframe(disc_df)
    
    # DEBUG: Check session state
    #st.write("🔍 DEBUG: Checking session state for db manager")
    #st.write(f"DEBUG: enhanced_db_manager exists: {'enhanced_db_manager' in st.session_state}")
    #st.write(f"DEBUG: db_manager exists: {'db_manager' in st.session_state}")
    
    # Export section with debugging
    st.markdown("---")
    st.markdown("### Export Options")
    
    # Method 1: Using a form to prevent rerun issues
    with st.form("export_validation_form"):
        st.write("Click below to generate export file:")
        export_clicked = st.form_submit_button("📥 Generate Export File")
        
        if export_clicked:
            #st.write("✅ DEBUG: Export button clicked!")
            
            try:
                # Try to get db manager
                db = st.session_state.get('enhanced_db_manager') or st.session_state.get('db_manager')
                #st.write(f"DEBUG: Database manager found: {db is not None}")
                
                if db:
                    #st.write("DEBUG: Calling export_validation_results method...")
                    export_file = db.export_validation_results(validation_results)
                    #st.write(f"DEBUG: export_file returned: {export_file}")
                    
                    if export_file:
                        if os.path.exists(export_file):
                            #st.write(f"✅ DEBUG: File exists!")
                            #st.write(f"DEBUG: File path: {export_file}")
                            #st.write(f"DEBUG: File size: {os.path.getsize(export_file)} bytes")
                            
                            # Store in session state
                            st.session_state['last_export_file'] = export_file
                            st.session_state['export_ready'] = True
                            st.success("Export file generated successfully!")
                        else:
                            st.error(f"❌ File not found at path: {export_file}")
                    else:
                        st.error("❌ export_validation_results returned None")
                else:
                    st.error("❌ Database manager not found in session state")
                    #st.write("DEBUG: Available session state keys:", list(st.session_state.keys()))
                    
            except Exception as e:
                st.error(f"❌ Exception occurred: {str(e)}")
                #st.write("DEBUG: Exception type:", type(e).__name__)
                import traceback
                st.text("Traceback:")
                st.text(traceback.format_exc())
    
    # Check if we have a file ready to download (outside the form)
    if st.session_state.get('export_ready', False) and st.session_state.get('last_export_file'):
        #st.write("📎 DEBUG: Export file is ready for download")
        export_file = st.session_state['last_export_file']
        
        if os.path.exists(export_file):
            with open(export_file, 'rb') as f:
                file_bytes = f.read()
            
            # Determine file type
            is_excel = export_file.endswith('.xlsx')
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if is_excel else "text/csv"
            
            st.download_button(
                label="⬇️ Download Validation Report",
                data=file_bytes,
                file_name=os.path.basename(export_file),
                mime=mime_type,
                key=f"download_validation_{int(datetime.now().timestamp())}"
            )
            
            # Don't clear the export_ready flag immediately
        else:
            st.error(f"File no longer exists: {export_file}")
    
    # Show validation report
    with st.expander("📋 Validation Report"):
        db = st.session_state.get('enhanced_db_manager') or st.session_state.get('db_manager')
        if db and hasattr(db, 'get_validation_summary_report'):
            report = db.get_validation_summary_report(validation_results)
            st.text(report)
        else:
            st.write("Summary report not available")
    
    if validation_results.get('total_discrepancy_amount'):
        if abs(validation_results['total_discrepancy_amount']) <= validation_results['total_invoices_checked']:
            st.info(f"💰 Total Discrepancy: ${validation_results['total_discrepancy_amount']:,.2f} (within rounding tolerance)")
        else:
            st.error(f"💰 Total Discrepancy: ${validation_results['total_discrepancy_amount']:,.2f}")
    else:
        st.success("✅ All invoice totals match within tolerance!")

        # Direct approach for the validation export
        st.markdown("---")
        st.markdown("### Direct Export (No Button Click)")

        # Generate export file immediately
        try:
            db = st.session_state.get('enhanced_db_manager')
            if db and validation_results.get('total_invoices_checked', 0) > 0:
                export_file = db.export_validation_results(validation_results)
                if export_file and os.path.exists(export_file):
                    with open(export_file, 'rb') as f:
                        file_data = f.read()
                    
                    st.download_button(
                        "📥 Download Validation Results (Direct)",
                        data=file_data,
                        file_name=f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"direct_dl_{int(datetime.now().timestamp())}"
                    )
                    st.success("✅ Export file ready for download!")
        except Exception as e:
            st.error(f"Direct export error: {str(e)}")
            import traceback
            st.text(traceback.format_exc())

def add_log(message: str):
    """Add message to processing logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    st.session_state.processing_logs.append(log_entry)
    logger.info(message)

@st.cache_data(ttl=300)
def get_cached_table_stats():
    """Get database statistics (cached for performance)"""
    try:
        return st.session_state.enhanced_db_manager.get_table_stats()
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return {}

def process_file_with_auto_detection(uploaded_file):
    """
    Process uploaded file with automatic type detection and appropriate handling
    """
    try:
        # Read the uploaded file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        add_log(f"Loaded {len(df)} rows from {uploaded_file.name}")
        
        # Auto-detect file type
        mapper = st.session_state.data_mapper
        detected_type = mapper.auto_detect_file_type(uploaded_file.name, df)
        
        st.info(f"🤖 **Auto-detected file type:** {detected_type}")
        
        # Show preview with safe display
        with st.expander("📋 Preview Data"):
            # Use the safe display function
            display_df = safe_dataframe_display(df, 10)
            st.dataframe(display_df)
            st.write(f"**Columns:** {', '.join(df.columns.tolist())}")
        
        # Handle based on detected type
        if detected_type == "BCI Details":
            return process_bci_details(df, uploaded_file.name)
        elif detected_type == "AUS Details":
            return process_aus_details(df, uploaded_file.name)
        elif detected_type in ["EDI", "Release", "Add-On"]:
            return process_invoice_master(df, uploaded_file.name, detected_type)
        elif detected_type == "Kaiser SCR Building Data":
            return process_kaiser_scr(df, uploaded_file.name)
        elif detected_type == "KP_Payment_Excel":  # ADD THIS LINE
            return process_kp_payment_excel(df, uploaded_file.name)  # ADD THIS LINE
        else:
            st.warning(f"⚠️ Could not auto-detect file type. Please select manually.")
            return False
        
    except Exception as e:
        st.error(f"Error processing file: {e}")
        add_log(f"Processing error: {e}")
        return False

def process_eml_file(uploaded_file) -> bool:
    """
    Process .eml email files containing payment remittance data
    """
    try:
        import email
        from email import policy
        
        st.info("📧 Processing .eml email file...")
        
        # Read the .eml file
        file_content = uploaded_file.read()
        
        # Parse the email
        if isinstance(file_content, bytes):
            msg = email.message_from_bytes(file_content, policy=policy.default)
        else:
            msg = email.message_from_string(file_content, policy=policy.default)
        
        # Show email metadata
        with st.expander("📧 Email Information"):
            st.write(f"**From:** {msg.get('From', 'Unknown')}")
            st.write(f"**Subject:** {msg.get('Subject', 'No Subject')}")
            st.write(f"**Date:** {msg.get('Date', 'Unknown')}")
        
        # Extract HTML content
        html_content = None
        text_content = None
        
        if msg.is_multipart():
            # Look for HTML part in multipart email
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/html":
                    html_content = part.get_content()
                    break
                elif content_type == "text/plain" and not text_content:
                    text_content = part.get_content()
        else:
            # Single part email
            content_type = msg.get_content_type()
            if content_type == "text/html":
                html_content = msg.get_content()
            elif content_type == "text/plain":
                text_content = msg.get_content()
        
        # Try HTML first, fall back to text
        if html_content:
            st.success("✅ Found HTML content in email")
            
            # Check if it contains payment data
            mapper = st.session_state.data_mapper
            if mapper.detect_payment_email_html(html_content):
                st.success("💰 Payment data detected in email HTML")
                return process_kp_payment_html(html_content, uploaded_file.name)
            else:
                st.warning("⚠️ No payment data detected in HTML content")
                
                # Show preview of HTML content
                with st.expander("🔍 HTML Content Preview"):
                    preview = html_content[:500]
                    st.text(preview)
                    if len(html_content) > 500:
                        st.write("... (truncated)")
                
                return False
        
        elif text_content:
            st.warning("📄 Only plain text content found - payment processing requires HTML tables")
            
            # Show preview of text content
            with st.expander("📄 Text Content Preview"):
                preview = text_content[:500]
                st.text(preview)
                if len(text_content) > 500:
                    st.write("... (truncated)")
            
            st.info("💡 **Tip:** Make sure your email client is set to receive HTML emails.")
            return False
        
        else:
            st.error("❌ No content found in email file")
            return False
        
    except Exception as e:
        st.error(f"Error processing .eml file: {e}")
        add_log(f"EML processing error: {e}")
        return False

def process_bci_details(df: pd.DataFrame, filename: str):
    """
    Process BCI details file with fast upload and post-validation
    """
    st.subheader("🏢 Processing BCI Invoice Details")
    
    # Initialize session state for this upload
    if 'bci_upload_complete' not in st.session_state:
        st.session_state.bci_upload_complete = False
    if 'bci_upload_results' not in st.session_state:
        st.session_state.bci_upload_results = None
    
    try:
        mapper = st.session_state.data_mapper
        
        # Map the data
        with st.spinner("Mapping BCI data..."):
            mapped_data = mapper.map_bci_details(df)
        
        if not mapped_data:
            st.error("No valid BCI data found in file")
            return False
        
        st.success(f"✅ Mapped {len(mapped_data)} records from {filename}")
        
        # Add source system identifier
        for record in mapped_data:
            record['source_system'] = 'BCI'
        
        # Show preview
        with st.expander("📋 Preview Mapped Data (First Record)"):
            if mapped_data:
                sample = mapped_data[0]
                cols = st.columns(2)
                for i, (key, value) in enumerate(sample.items()):
                    with cols[i % 2]:
                        display_value = str(value) if value is not None else ''
                        st.write(f"**{key}:** {display_value}")
        
        # Check if upload was already completed
        if st.session_state.bci_upload_complete and st.session_state.bci_upload_results:
            # Show previous upload results
            st.info("✅ Upload already completed. Showing previous results:")
            upload_results = st.session_state.bci_upload_results
            
            # Display results with downloads
            display_smart_processing_results_with_export(upload_results, filename, "BCI")
            
            st.success(f"""
            ✅ **BCI Upload Completed!**
            - **File:** {filename}
            - **Inserted:** {upload_results['inserted']:,} records
            - **Failed:** {upload_results['failed']:,} records (likely duplicates)
            """)
        else:
            # Process button
            if st.button("🚀 Upload Now", type="primary", key="bci_upload"):
                # Phase 1: Fast upload
                st.markdown("### 📤 Phase 1: Upload Source File")
                
                # Create progress bar and status text
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Define progress callback
                def update_progress(progress, message):
                    progress_bar.progress(progress)
                    status_text.text(message)
                
                # Upload with progress tracking
                db = st.session_state.enhanced_db_manager
                upload_results = db.bulk_insert_invoice_details_fast_validated(
                    mapped_data, 
                    progress_callback=update_progress
                )
                
                # Clear progress indicators
                progress_bar.empty()
                status_text.empty()
                
                # Store results in session state
                st.session_state.bci_upload_complete = True
                st.session_state.bci_upload_results = upload_results
                
                # Display results with downloads
                display_smart_processing_results_with_export(upload_results, "BCI", filename)
                
                add_log(f"BCI processing: {upload_results['inserted']} inserted from {filename}")
                
                # Force rerun to show validation button
                st.rerun()
        
        # Phase 2: Validation (show this if upload is complete)
        if st.session_state.bci_upload_complete:  # or aus_upload_complete for AUS
            st.markdown("### 🔍 Phase 2: Invoice Total Validation")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔍 Run Validation Check", type="primary", key="bci_validate"):
                    with st.spinner("Validating invoice totals..."):
                        db = st.session_state.enhanced_db_manager
                        
                        # Get the invoice numbers that were just uploaded
                        uploaded_invoices = []
                        if hasattr(st.session_state, 'bci_upload_results') and st.session_state.bci_upload_results:
                            uploaded_invoices = st.session_state.bci_upload_results.get('inserted_invoice_numbers', [])
                        
                        if uploaded_invoices:
                            # Only validate the invoices that were just uploaded
                            st.info(f"Validating {len(uploaded_invoices)} unique invoices from this upload...")
                            validation_results = db.validate_specific_invoice_totals(uploaded_invoices)
                        else:
                            st.warning("No invoice numbers found from the upload to validate.")
                            validation_results = {
                                'total_invoices_checked': 0,
                                'matching_invoices': 0,
                                'mismatched_invoices': 0,
                                'discrepancies': [],
                                'validation_timestamp': datetime.now()
                            }
                        
                        # Store validation results in session state
                        st.session_state.bci_validation_results = validation_results
                        
                        # Force a rerun to display results outside the button
                        st.rerun()
            
            with col2:
                if st.button("🔄 Reset and Upload New File", key="bci_reset"):
                    # Clear session state
                    st.session_state.bci_upload_complete = False
                    st.session_state.bci_upload_results = None
                    if 'bci_validation_results' in st.session_state:
                        del st.session_state.bci_validation_results
                    st.rerun()
            
            # DISPLAY VALIDATION RESULTS OUTSIDE THE BUTTON CLICK
            # This is the key change!
            if 'bci_validation_results' in st.session_state and st.session_state.bci_validation_results:
                display_validation_results(st.session_state.bci_validation_results)
        
        return st.session_state.bci_upload_complete
        
    except Exception as e:
        st.error(f"BCI processing failed: {e}")
        add_log(f"BCI processing error: {e}")
        return False
    
def process_aus_details(df: pd.DataFrame, filename: str):
    """
    Process AUS details file with fast upload and post-validation
    """
    st.subheader("🏢 Processing AUS Invoice Details")
    
    # Initialize session state for this upload
    if 'aus_upload_complete' not in st.session_state:
        st.session_state.aus_upload_complete = False
    if 'aus_upload_results' not in st.session_state:
        st.session_state.aus_upload_results = None
    
    try:
        mapper = st.session_state.data_mapper
        
        # Map the data
        with st.spinner("Mapping AUS data..."):
            mapped_data = mapper.map_aus_details(df)
        
        if not mapped_data:
            st.error("No valid AUS data found in file")
            return False
        
        st.success(f"✅ Mapped {len(mapped_data)} records from {filename}")
        
        # Add source system identifier
        for record in mapped_data:
            record['source_system'] = 'AUS'
        
        # Show preview
        with st.expander("📋 Preview Mapped Data (First Record)"):
            if mapped_data:
                sample = mapped_data[0]
                cols = st.columns(2)
                for i, (key, value) in enumerate(sample.items()):
                    with cols[i % 2]:
                        display_value = str(value) if value is not None else ''
                        st.write(f"**{key}:** {display_value}")
        
        # Check if upload was already completed
        if st.session_state.aus_upload_complete and st.session_state.aus_upload_results:
            # Show previous upload results
            st.info("✅ Upload already completed. Showing previous results:")
            upload_results = st.session_state.aus_upload_results
            
            # Display results with downloads
            display_smart_processing_results_with_export(upload_results, filename, "AUS")
            
            st.success(f"""
            ✅ **AUS Upload Completed!**
            - **File:** {filename}
            - **Inserted:** {upload_results['inserted']:,} records
            - **Failed:** {upload_results['failed']:,} records
            """)
        else:
            # Process button
            if st.button("🚀 Upload Now", type="primary", key="aus_upload"):
                # Phase 1: Fast upload
                st.markdown("### 📤 Phase 1: Upload Source File")
                
                # Create progress bar and status text
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Define progress callback
                def update_progress(progress, message):
                    progress_bar.progress(progress)
                    status_text.text(message)
                
                # Upload with progress tracking
                db = st.session_state.enhanced_db_manager
                upload_results = db.bulk_insert_invoice_details_fast_validated(
                    mapped_data, 
                    progress_callback=update_progress
                )
                
                # Clear progress indicators
                progress_bar.empty()
                status_text.empty()
                
                # Store results in session state
                st.session_state.aus_upload_complete = True
                st.session_state.aus_upload_results = upload_results
                
                # Display results with downloads
                display_smart_processing_results_with_export(upload_results, "AUS", filename)
                
                add_log(f"AUS processing: {upload_results['inserted']} inserted from {filename}")
                
                # Force rerun to show validation button
                st.rerun()
        
        # Phase 2: Validation (show this if upload is complete)
        if st.session_state.aus_upload_complete:  # or aus_upload_complete for AUS
            st.markdown("### 🔍 Phase 2: Invoice Total Validation")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔍 Run Validation Check", type="primary", key="aus_validate"):
                    with st.spinner("Validating invoice totals..."):
                        db = st.session_state.enhanced_db_manager
                        
                        # Get the invoice numbers that were just uploaded
                        uploaded_invoices = []
                        if hasattr(st.session_state, 'aus_upload_results') and st.session_state.aus_upload_results:
                            uploaded_invoices = st.session_state.aus_upload_results.get('inserted_invoice_numbers', [])
                        
                        if uploaded_invoices:
                            # Only validate the invoices that were just uploaded
                            st.info(f"Validating {len(uploaded_invoices)} unique invoices from this upload...")
                            validation_results = db.validate_specific_invoice_totals(uploaded_invoices)
                        else:
                            st.warning("No invoice numbers found from the upload to validate.")
                            validation_results = {
                                'total_invoices_checked': 0,
                                'matching_invoices': 0,
                                'mismatched_invoices': 0,
                                'discrepancies': [],
                                'validation_timestamp': datetime.now()
                            }
                        
                        # Store validation results in session state
                        st.session_state.aus_validation_results = validation_results
                        
                        # Force a rerun to display results outside the button
                        st.rerun()
            
            with col2:
                if st.button("🔄 Reset and Upload New File", key="aus_reset"):
                    # Clear session state
                    st.session_state.aus_upload_complete = False
                    st.session_state.aus_upload_results = None
                    if 'aus_validation_results' in st.session_state:
                        del st.session_state.aus_validation_results
                    st.rerun()
            
            # DISPLAY VALIDATION RESULTS OUTSIDE THE BUTTON CLICK
            # This is the key change!
            if 'aus_validation_results' in st.session_state and st.session_state.aus_validation_results:
                display_validation_results(st.session_state.aus_validation_results)
        
        return st.session_state.aus_upload_complete
        
    except Exception as e:
        st.error(f"AUS processing failed: {e}")
        add_log(f"AUS processing error: {e}")
        return False

def process_invoice_master(df: pd.DataFrame, filename: str, processing_type: str):
    """Process invoice master files (EDI/Release/Add-On) with date prompting"""
    st.subheader(f"📄 Processing {processing_type} File")
    
    # ALWAYS prompt for processing date
    st.markdown("### 📅 Processing Date Required")
    
    if processing_type == 'EDI':
        processing_date = st.date_input(
            f"EDI Date (when invoices were submitted)",
            value=date.today(),
            help="This date will be populated in the EDI Date field for all invoices"
        )
    elif processing_type == 'Release':
        processing_date = st.date_input(
            f"Release Date (when invoices were released)",
            value=date.today(),
            help="This date will be populated in the Release Date field for all invoices"
        )
    elif processing_type == 'Add-On':
        processing_date = st.date_input(
            f"Add-On Date (when add-on invoices were submitted)",
            value=date.today(),
            help="This date will be populated in the Add-On Date field for all invoices"
        )
    
    # Show business logic explanation
    with st.expander("ℹ️ Business Logic Applied"):
        st.markdown(f"""
        **{processing_type} Processing Logic:**
        
        - **Processing Date:** `{processing_date}`
        - **New Invoices:** {processing_type} Date set to processing date
        - **Existing Invoices:** Date preservation logic applied
        {'- **Not Transmitted:** New EDI = True, Updates = False' if processing_type == 'EDI' else ''}
        {'- **History Linking:** Automatic bidirectional linking' if processing_type == 'Add-On' else ''}
        """)
    
    if st.button(f"🚀 Process {processing_type} File", type="primary"):
        with st.spinner(f"Processing {processing_type} data..."):
            try:
                mapper = st.session_state.data_mapper
                mapped_data = mapper.map_invoice_data_with_processing_info(df, processing_type, processing_date)
                
                if not mapped_data:
                    st.error("No valid invoice data found in file")
                    return False
                
                db = st.session_state.enhanced_db_manager
                result = db.upsert_invoices_with_business_logic(
                    mapped_data,
                    processing_type,
                    processing_date
                )
                
                # Handle invoice history linking for Add-On files
                history_links = 0
                if processing_type == 'Add-On':
                    # Check for "Original invoice #" field
                    original_df = df.copy()
                    if 'Original invoice #' in original_df.columns:
                        for i, mapped_record in enumerate(mapped_data):
                            if i < len(original_df):
                                original_invoice = original_df.iloc[i].get('Original invoice #')
                                if pd.notna(original_invoice):
                                    mapped_record['original_invoice_no'] = str(original_invoice)
                        
                        history_links = db.process_invoice_history_linking(mapped_data)
                
                st.success(f"""
                ✅ {processing_type} processing completed!
                - File: {filename}
                - Date: {processing_date}
                - Inserted: {result['inserted']} new invoices
                - Updated: {result['updated']} existing invoices
                {f'- History Links: {history_links}' if history_links > 0 else ''}
                """)
                
                add_log(f"{processing_type} processing: {result} from {filename}")
                st.cache_data.clear()  # Refresh cached data
                return True
                
            except Exception as e:
                st.error(f"{processing_type} processing failed: {e}")
                add_log(f"{processing_type} processing error: {e}")
                return False
    
    return None  # User hasn't clicked process yet

def process_kaiser_scr(df: pd.DataFrame, filename: str):
    """Process Kaiser SCR building data"""
    st.subheader("🏢 Processing Kaiser SCR Building Data")
    
    try:
        mapper = st.session_state.data_mapper
        mapped_data = mapper.map_kaiser_scr_data(df)
        
        if not mapped_data:
            st.error("No valid building data found in file")
            return False
        
        # Note: Would need to implement building dimension upsert in compatible manager
        st.info(f"Would process {len(mapped_data)} building records (feature pending)")
        add_log(f"Kaiser SCR processing: {len(mapped_data)} records from {filename}")
        return True
        
    except Exception as e:
        st.error(f"Kaiser SCR processing failed: {e}")
        add_log(f"Kaiser SCR processing error: {e}")
        return False
    
def process_kp_payment_excel(df: pd.DataFrame, filename: str):
    """Process Kaiser Permanente payment Excel file"""
    st.subheader("💰 Processing Kaiser Permanente Payment")
    
    try:
        mapper = st.session_state.data_mapper
        mapped_data = mapper.map_kp_payment_excel(df)
        
        if not mapped_data:
            st.error("No valid payment data found in file")
            return False
        
        # Show payment summary
        st.markdown("### 📋 Payment Summary")
        
        # Get payment metadata from first record
        first_record = mapped_data[0]
        payment_id = first_record.get('payment_id')
        payment_date = first_record.get('payment_date')
        
        # Calculate total amount
        total_amount = sum(float(record.get('net_amount', 0)) for record in mapped_data)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Payment ID", payment_id)
        with col2:
            st.metric("Payment Date", payment_date)
        with col3:
            st.metric("Total Amount", f"${total_amount:,.2f}")
        
        # Show detail records preview
        with st.expander("📋 Invoice Details Preview"):
            preview_df = pd.DataFrame(mapped_data[:10])  # Show first 10
            st.dataframe(preview_df)
            
            if len(mapped_data) > 10:
                st.info(f"Showing first 10 of {len(mapped_data)} records")
        
        # Check if payment already exists
        db = st.session_state.enhanced_db_manager
        payment_exists = db.check_payment_exists(payment_id)
        
        if payment_exists:
            st.error("⚠️ **Payment Already Processed**")
            existing_summary = db.get_payment_summary(payment_id)
            st.json(existing_summary)
            return False
        
        # Process to database
        if st.button("💾 Save Payment to Database", type="primary"):
            with st.spinner("Saving payment data..."):
                
                # Insert master record
                master_success = db.insert_payment_master(
                    payment_id=payment_id,
                    payment_date=payment_date,
                    payment_amount=total_amount,
                    vendor_name=first_record.get('vendor_name', 'BLACKSTONE CONSULTING INC'),
                    source_file=filename
                )
                
                if master_success:
                    # Insert detail records
                    details_success = db.insert_payment_details_batch(mapped_data)
                    
                    if details_success:
                        st.success(f"✅ **Successfully processed payment {payment_id}!**")
                        st.success(f"💾 Saved {len(mapped_data)} invoice detail records")
                        
                        add_log(f"Processed Kaiser payment: {payment_id} with {len(mapped_data)} details")
                        return True
                    else:
                        st.error("❌ Failed to save payment details")
                        return False
                else:
                    st.error("❌ Failed to save payment master record")
                    return False
        
        return True
        
    except Exception as e:
        st.error(f"❌ Payment processing failed: {e}")
        add_log(f"Payment processing error: {e}")
        return False


def execute_payment_processing(master_data: dict, detail_records: list, 
                             filename: str, db_manager) -> bool:
    """
    Execute the actual payment processing with progress tracking
    Following your existing progress tracking patterns
    """
    
    # Create progress indicators (following your existing pattern)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def progress_callback(progress: float, message: str):
        progress_bar.progress(progress)
        status_text.text(message)
    
    try:
        with st.spinner("Processing payment remittance..."):
            # Process the payment (using your existing database manager)
            results = db_manager.process_payment_remittance(
                master_data, 
                detail_records,
                progress_callback
            )
        
        # Clear progress indicators (following your existing pattern)
        progress_bar.empty()
        status_text.empty()
        
        if results['success']:
            # Success display (following your existing success patterns)
            st.success("🎉 **Payment Processing Completed Successfully!**")
            
            # Display results (following your existing metrics pattern)
            final_summary = results.get('final_summary', {})
            detail_results = results.get('detail_results', {})
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Payment ID", results['payment_id'])
            with col2:
                st.metric("Details Inserted", detail_results.get('inserted', 0))
            with col3:
                st.metric("Success Rate", "100%" if detail_results.get('success', False) else "Partial")
            
            # Export options (following your existing download patterns)
            st.markdown("### 📥 Export Options")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("📄 Download Payment Summary"):
                    summary_data = {
                        'Payment ID': [results['payment_id']],
                        'Payment Date': [final_summary.get('payment_date', '')],
                        'Payment Amount': [final_summary.get('payment_amount', 0)],
                        'Detail Count': [final_summary.get('detail_count', 0)],
                        'Total Net': [final_summary.get('total_net', 0)]
                    }
                    summary_df = pd.DataFrame(summary_data)
                    csv = summary_df.to_csv(index=False)
                    st.download_button(
                        "Download Summary CSV",
                        csv,
                        file_name=f"payment_summary_{results['payment_id']}.csv",
                        mime="text/csv"
                    )
            
            with col2:
                if st.button("📋 Download All Details"):
                    details = db_manager.get_payment_details_for_export(results['payment_id'])
                    if details:
                        details_df = pd.DataFrame(details)
                        csv = details_df.to_csv(index=False)
                        st.download_button(
                            "Download Details CSV",
                            csv,
                            file_name=f"payment_details_{results['payment_id']}.csv",
                            mime="text/csv"
                        )
            
            # Log successful processing (using your existing logging)
            add_log(f"Payment processing success: {results['payment_id']} from {filename}")
            return True
            
        else:
            # Error handling (following your existing error patterns)
            st.error(f"❌ **Payment Processing Failed**")
            st.error(f"Error: {results.get('error', 'Unknown error')}")
            
            if 'existing_payment' in results:
                st.info("This payment was previously processed. See details above.")
            
            add_log(f"Payment processing failed: {results.get('error')} for {filename}")
            return False
            
    except Exception as e:
        # Cleanup and error handling (following your existing error patterns)
        progress_bar.empty()
        status_text.empty()
        st.error(f"💥 **Processing Error:** {e}")
        add_log(f"Payment processing exception: {e}")
        return False

def process_eml_file(uploaded_file) -> bool:
    """
    Process .eml email files containing payment remittance data
    Fixed to properly handle encoding
    """
    try:
        import email
        from email import policy
        
        st.info("📧 Processing .eml email file...")
        
        # Read the .eml file
        file_content = uploaded_file.read()
        
        # Parse the email
        msg = email.message_from_bytes(file_content, policy=policy.default)
        
        # Show email metadata
        with st.expander("📧 Email Information"):
            st.write(f"**From:** {msg.get('From', 'Unknown')}")
            st.write(f"**Subject:** {msg.get('Subject', 'No Subject')}")
            st.write(f"**Date:** {msg.get('Date', 'Unknown')}")
        
        # Extract HTML content
        html_content = None
        text_content = None
        
        if msg.is_multipart():
            # Look for HTML part in multipart email
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/html":
                    html_content = part.get_content()
                    # FIX: Handle bytes
                    if isinstance(html_content, bytes):
                        html_content = html_content.decode('utf-8', errors='replace')
                elif content_type == "text/plain" and not text_content:
                    text_content = part.get_content()
                    if isinstance(text_content, bytes):
                        text_content = text_content.decode('utf-8', errors='replace')
        else:
            # Single part email
            content_type = msg.get_content_type()
            if content_type == "text/html":
                html_content = msg.get_content()
                if isinstance(html_content, bytes):
                    html_content = html_content.decode('utf-8', errors='replace')
            elif content_type == "text/plain":
                text_content = msg.get_content()
                if isinstance(text_content, bytes):
                    text_content = text_content.decode('utf-8', errors='replace')
        
        # Try HTML first, fall back to text
        if html_content:
            st.success("✅ Found HTML content in email")
            
            # Check if it contains payment data
            mapper = st.session_state.data_mapper
            if mapper.detect_payment_email_html(html_content):
                st.success("💰 Payment data detected in email HTML")
                return process_kp_payment_html(html_content, uploaded_file.name)
            else:
                st.warning("⚠️ No payment data detected in HTML content")
                
                # Show preview of HTML content
                with st.expander("🔍 HTML Content Preview"):
                    # Show first 500 characters
                    preview = html_content[:500]
                    st.text(preview)
                    if len(html_content) > 500:
                        st.write("... (truncated)")
                
                return False
        
        elif text_content:
            st.warning("📄 Only plain text content found - payment processing requires HTML tables")
            
            # Show preview of text content
            with st.expander("📄 Text Content Preview"):
                preview = text_content[:500]
                st.text(preview)
                if len(text_content) > 500:
                    st.write("... (truncated)")
            
            st.info("💡 **Tip:** Make sure your email client is set to receive HTML emails, or ask the sender to send in HTML format.")
            return False
        
        else:
            st.error("❌ No content found in email file")
            return False
        
    except Exception as e:
        st.error(f"Error processing .eml file: {e}")
        add_log(f"EML processing error: {e}")
        return False


def process_msg_file(uploaded_file) -> bool:
    """
    Process .msg Outlook email files containing payment remittance data
    Fixed to properly handle HTML content extraction and encoding
    """
    try:
        import tempfile
        import os
        
        st.info("📧 Processing Outlook .msg file...")
        
        # Save uploaded file to temporary location (required for extract-msg)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.msg') as tmp_file:
            tmp_file.write(uploaded_file.read())
            tmp_file_path = tmp_file.name
        
        try:
            # Import here to provide better error message if not installed
            try:
                import extract_msg
            except ImportError:
                st.error("❌ **Missing Dependency**")
                st.error("To process Outlook .msg files, please install: `pip install extract-msg`")
                st.info("💡 **Alternative:** Save the email as .eml format instead")
                return False
            
            # Extract the message
            msg = extract_msg.Message(tmp_file_path)
            
            # Show email metadata
            with st.expander("📧 Email Information"):
                st.write(f"**From:** {msg.sender or 'Unknown'}")
                st.write(f"**Subject:** {msg.subject or 'No Subject'}")
                st.write(f"**Date:** {msg.date or 'Unknown'}")
                if hasattr(msg, 'attachments') and msg.attachments:
                    st.write(f"**Attachments:** {len(msg.attachments)} files")
            
            # Get HTML body - FIX: Handle bytes properly
            html_content = msg.htmlBody
            text_content = msg.body
            
            # FIX: Check if HTML content is bytes and decode it
            if html_content:
                if isinstance(html_content, bytes):
                    try:
                        # Try different encodings
                        for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                            try:
                                html_content = html_content.decode(encoding)
                                st.success(f"✅ Successfully decoded HTML content using {encoding}")
                                break
                            except UnicodeDecodeError:
                                continue
                        else:
                            # If all encodings fail, use utf-8 with error handling
                            html_content = html_content.decode('utf-8', errors='replace')
                            st.warning("⚠️ Used UTF-8 with error replacement for HTML content")
                    except Exception as e:
                        st.error(f"Failed to decode HTML content: {e}")
                        html_content = None
                
                if html_content:
                    st.success("✅ Found and decoded HTML content in Outlook message")
                    
                    # Debug: Show first part of decoded HTML
                    with st.expander("🔍 Decoded HTML Preview"):
                        st.text(html_content[:500] + "..." if len(html_content) > 500 else html_content)
                    
                    # Check if it contains payment data
                    mapper = st.session_state.data_mapper
                    if mapper.detect_payment_email_html(html_content):
                        st.success("💰 Payment data detected in Outlook email HTML")
                        return process_kp_payment_html(html_content, uploaded_file.name)
                    else:
                        st.warning("⚠️ No payment data detected in decoded HTML content")
                        
                        # Debug: Show what the detector is seeing
                        content_lower = html_content.lower()
                        kp_indicators = [
                            'payment id', 'vendor id', 'blackstone consulting', 
                            'electronic funds', 'invoice id', 'gross amount', 'net amount'
                        ]
                        found_indicators = [ind for ind in kp_indicators if ind in content_lower]
                        
                        st.info(f"**Debug:** Found indicators: {found_indicators}")
                        
                        # Show table count
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(html_content, 'html.parser')
                        tables = soup.find_all('table')
                        st.info(f"**Debug:** Found {len(tables)} tables in HTML")
                        
                        return False
            
            elif text_content:
                # Handle text content similarly
                if isinstance(text_content, bytes):
                    try:
                        for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                            try:
                                text_content = text_content.decode(encoding)
                                break
                            except UnicodeDecodeError:
                                continue
                        else:
                            text_content = text_content.decode('utf-8', errors='replace')
                    except Exception as e:
                        st.error(f"Failed to decode text content: {e}")
                        text_content = None
                
                if text_content:
                    st.warning("📄 Only plain text content found - payment processing requires HTML tables")
                    
                    with st.expander("📄 Text Content Preview"):
                        preview = text_content[:500]
                        st.text(preview)
                        if len(text_content) > 500:
                            st.write("... (truncated)")
                    
                    st.info("💡 **Tip:** Ensure the original email was sent in HTML format with tables.")
                    return False
            
            else:
                st.error("❌ No content found in Outlook message")
                return False
                
        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file_path)
            except:
                pass  # Ignore cleanup errors
            
    except Exception as e:
        st.error(f"Error processing .msg file: {e}")
        add_log(f"MSG processing error: {e}")
        return False


def detect_email_file_type(uploaded_file) -> str:
    """
    Detect if uploaded file is an email file and what type
    """
    filename = uploaded_file.name.lower()
    
    if filename.endswith('.eml'):
        return 'eml'
    elif filename.endswith('.msg'):
        return 'msg'
    else:
        return 'not_email'

def process_kp_payment_html(html_content: str, filename: str = "email_content") -> bool:
    """
    Process Kaiser Permanente payment HTML email content
    """
    st.subheader("📧 Processing Kaiser Permanente Payment Email")
    
    try:
        # Step 1: Process HTML content
        with st.spinner("Parsing HTML email content..."):
            mapper = st.session_state.data_mapper
            master_data, detail_records = mapper.process_payment_email_html(html_content)
        
        st.success(f"✅ Parsed email content: {len(detail_records)} invoice records found")
        
        # Step 2: Display parsed data preview
        with st.expander("📋 Parsed Email Data Preview"):
            if detail_records:
                preview_df = pd.DataFrame(detail_records[:5])  # Show first 5
                display_df = safe_dataframe_display(preview_df, 5)
                st.dataframe(display_df)
                
                if len(detail_records) > 5:
                    st.info(f"Showing first 5 of {len(detail_records)} records")
        
        # Step 3: Continue with standard payment processing
        # Display payment summary
        st.markdown("### 📋 Payment Summary")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Payment ID", master_data['payment_id'])
        with col2:
            st.metric("Payment Date", master_data['payment_date'])
        with col3:
            st.metric("Payment Amount", f"${master_data['payment_amount']:,.2f}")
        
        # Check if payment already exists
        db = st.session_state.enhanced_db_manager
        payment_exists = db.check_payment_exists(master_data['payment_id'])
        
        if payment_exists:
            st.error("⚠️ **Payment Already Processed**")
            existing_summary = db.get_payment_summary(master_data['payment_id'])
            if existing_summary:
                st.info(f"This payment was already processed on {existing_summary['created_at']}")
            return False
        
        # Validate data
        with st.spinner("Validating payment data..."):
            validation_results = mapper.validate_payment_data(master_data, detail_records)
        
        # Display validation results
        if validation_results['is_valid']:
            st.success("✅ **Email Data Validation Passed**")
            
            # Show validation summary
            summary = validation_results.get('summary', {})
            if summary:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Master Amount", f"${summary.get('master_amount', 0):,.2f}")
                with col2:
                    st.metric("Detail Records", summary.get('detail_count', 0))
                with col3:
                    st.metric("Detail Total", f"${summary.get('detail_net_total', 0):,.2f}")
                with col4:
                    variance = summary.get('amount_variance', 0)
                    st.metric("Variance", f"${variance:,.2f}")
        else:
            st.error("❌ **Email Data Validation Failed**") 
            for error in validation_results.get('errors', []):
                st.error(f"• {error}")
            for warning in validation_results.get('warnings', []):
                st.warning(f"• {warning}")
            return False
        
        # Process button
        if st.button(f"💾 Process Email Payment ({len(detail_records)} invoices)", type="primary"):
            return execute_payment_processing(master_data, detail_records, filename, db)
        
        return False
        
    except Exception as e:
        st.error(f"Error processing payment email: {e}")
        add_log(f"Payment email processing error: {e}")
        return False

def test_html_payment_processing():
    """
    Test function for HTML payment processing
    Creates sample HTML content for testing
    """
    sample_html = """
    <html>
    <body>
        <table>
            <tr><td>Summary Table</td></tr>
            <tr><td>This is the first table</td></tr>
        </table>
        
        <table border="1">
            <tr>
                <th>Vendor Name</th>
                <th>Payment ID</th>
                <th>Payment Date</th>
                <th>Payment Amount</th>
                <th>Invoice ID</th>
                <th>Gross Amount</th>
                <th>Discount</th>
                <th>Net Amount</th>
            </tr>
            <tr>
                <td>BLACKSTONE CONSULTING INC</td>
                <td>TEST_HTML_001</td>
                <td>06/23/25</td>
                <td>3000.00</td>
                <td>INV_HTML_001</td>
                <td>1000.00</td>
                <td>0</td>
                <td>1000.00</td>
            </tr>
            <tr>
                <td>BLACKSTONE CONSULTING INC</td>
                <td>TEST_HTML_001</td>
                <td>06/23/25</td>
                <td>3000.00</td>
                <td>INV_HTML_002</td>
                <td>1200.00</td>
                <td>0</td>
                <td>1200.00</td>
            </tr>
            <tr>
                <td>BLACKSTONE CONSULTING INC</td>
                <td>TEST_HTML_001</td>
                <td>06/23/25</td>
                <td>3000.00</td>
                <td>INV_HTML_003</td>
                <td>800.00</td>
                <td>0</td>
                <td>800.00</td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    return sample_html

# Title and description
st.title("🤖 Invoice Lifecycle Management System")
st.markdown("**✨ Automatically detects file types for faster uploads**")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["🏠 Dashboard", "📤 Smart File Upload", "💰 Payment Processing", "📊 Master Data View", "🚀 Quick Process", "📝 Processing Logs"] + add_capital_project_pages()
)

# Dashboard Page
if page == "🏠 Dashboard":
    st.header("📊 Dashboard")
    
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
    
    with col3:
        st.subheader("🤖 Auto-Detection")
        st.info("""
        **Supported File Types:**
        - `TLM_BCI.xlsx` → BCI Details
        - `AUS_Invoice.xlsx` → AUS Details  
        - `Payment*.xlsx` → Payment Remittance
        - `*.msg` → Outlook Email Files
        - `*.eml` → Standard Email Files
        - `weekly_release.xlsx` → Release
        - `weekly_edi.xlsx` → EDI
        - Files with keywords auto-detected
        """)

# Smart File Upload Page
elif page == "📤 Smart File Upload":
    st.header("📤 Smart File Upload with Auto-Detection")
    
    st.markdown("""
    **🤖 Auto-Detection Features:**
    - Automatically recognizes `TLM_BCI.xlsx` and `AUS_Invoice.xlsx`
    - Detects file types from filenames and content
    - Applies appropriate processing logic
    - Prompts for dates only when needed
    """)
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload any invoice file - type will be auto-detected",
        type=['xlsx', 'xls', 'csv'],
        help="Supports: TLM_BCI.xlsx, AUS_Invoice.xlsx, weekly files, and more"
    )
    
    if uploaded_file is not None:
        st.write(f"**File:** {uploaded_file.name}")
        st.write(f"**Size:** {uploaded_file.size:,} bytes")
        
        # Process with auto-detection
        process_file_with_auto_detection(uploaded_file)

    # Payment Processing Page
    elif page == "💰 Payment Processing":
        st.header("💰 Payment Remittance Processing")
        
        # Payment dashboard tabs (following your existing tab pattern)
        tab1, tab2, tab3 = st.tabs(["📤 Upload Payment", "📊 Payment History", "🔍 Payment Search"])
        
        with tab1:
            st.subheader("💰 Payment Remittance Processing")
        
        # Create sub-tabs for different input methods
        input_tab1, input_tab2, input_tab3 = st.tabs(["📁 File Upload", "📧 Email Files", "✂️ Copy/Paste HTML"])
        
        with input_tab1:
            st.markdown("""
            **📁 Upload Payment Files:**
            - Kaiser Permanente payment Excel files (Payment*.xlsx)
            - CSV files with payment remittance details
            - Supports auto-detection and standardization
            """)
            
            uploaded_file = st.file_uploader(
                "Upload payment file",
                type=['xlsx', 'xls', 'csv'],
                help="Upload Kaiser Permanente payment remittance files"
            )
            
            if uploaded_file is not None:
                st.write(f"**File:** {uploaded_file.name}")
                st.write(f"**Size:** {uploaded_file.size:,} bytes")
                
                # Process with existing auto-detection
                if st.button("🚀 Process File", type="primary"):
                    process_file_with_auto_detection(uploaded_file)
        
        with input_tab2:
            st.markdown("""
            **📧 Upload Email Files:**
            - Outlook .msg files containing Kaiser payment notifications
            - Standard .eml email files
            - Automatically extracts HTML content and processes payment data
            """)
            
            email_file = st.file_uploader(
                "Upload email file",
                type=['msg', 'eml'],
                help="Upload Outlook .msg or standard .eml email files"
            )
            
            if email_file is not None:
                st.write(f"**File:** {email_file.name}")
                st.write(f"**Size:** {email_file.size:,} bytes")
                st.write(f"**Type:** {email_file.type}")
                
                # Process based on file type
                if st.button("📧 Process Email", type="primary"):
                    if email_file.name.endswith('.msg'):
                        process_msg_file(email_file)
                    elif email_file.name.endswith('.eml'):
                        process_eml_file(email_file)
                    else:
                        st.error("Unsupported email file type")
            
            # Instructions for saving emails
            with st.expander("📖 How to Save Emails for Upload"):
                st.markdown("""
                ### 🖥️ **For Outlook Desktop:**
                1. Open the payment notification email
                2. Go to **File** → **Save As**
                3. Choose **Outlook Message Format (*.msg)** 
                4. Save the file to your computer
                5. Upload the .msg file above
                
                ### 🌐 **For Outlook Web (Office 365):**
                1. Open the payment email
                2. Click the **three dots (⋯)** → **View** → **View message source**
                3. Copy the entire content and use the "Copy/Paste HTML" tab
                
                ### ✅ **What to Look For:**
                - Email should contain **HTML tables** with payment data
                - Look for tables with columns like "Invoice ID", "Payment Amount", "Net Amount"
                - The system will automatically detect and parse the payment information
                """)
        
        with input_tab3:
            st.markdown("""
            **✂️ Copy/Paste HTML Content:**
            - Copy HTML content directly from Kaiser Permanente payment emails
            - Paste the complete email HTML content below
            - System will automatically extract payment data from tables
            """)
            
            html_content = st.text_area(
                "Paste HTML email content here:",
                height=300,
                help="Copy the full HTML content from your email client and paste it here"
            )
            
            if html_content and len(html_content.strip()) > 100:
                # Show preview of detected content
                mapper = st.session_state.data_mapper
                
                # Show content stats
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Content Length", f"{len(html_content):,} characters")
                with col2:
                    # Count tables
                    from bs4 import BeautifulSoup
                    try:
                        soup = BeautifulSoup(html_content, 'html.parser')
                        table_count = len(soup.find_all('table'))
                        st.metric("Tables Found", table_count)
                    except:
                        st.metric("Tables Found", "Error parsing")
                
                # Check for payment data
                if mapper.detect_payment_email_html(html_content):
                    st.success("✅ Kaiser Permanente payment data detected!")
                    
                    # Show detection details
                    with st.expander("🔍 Detection Details"):
                        try:
                            soup = BeautifulSoup(html_content, 'html.parser')
                            tables = soup.find_all('table')
                            
                            st.write(f"**Total Tables:** {len(tables)}")
                            
                            # Show table summaries
                            for i, table in enumerate(tables):
                                rows = table.find_all('tr')
                                st.write(f"- Table {i+1}: {len(rows)} rows")
                            
                            # Check for key indicators
                            content_lower = html_content.lower()
                            indicators = ['payment id', 'invoice id', 'gross amount', 'net amount', 'blackstone consulting']
                            found_indicators = [ind for ind in indicators if ind in content_lower]
                            
                            st.write(f"**Payment Indicators Found:** {', '.join(found_indicators)}")
                            
                        except Exception as e:
                            st.write(f"Error analyzing content: {e}")
                    
                    if st.button("🚀 Process HTML Email", type="primary"):
                        process_kp_payment_html(html_content, "pasted_email_content.html")
                else:
                    st.warning("⚠️ No Kaiser Permanente payment data detected")
                    st.info("Please ensure you've copied the complete email content including all HTML tables.")
                    
                    # Show preview for debugging
                    with st.expander("👀 Content Preview"):
                        preview = html_content[:1000]
                        st.text(preview)
                        if len(html_content) > 1000:
                            st.write("... (truncated)")
            
            elif html_content:
                st.info("👆 Please paste more content (minimum 100 characters)")
            else:
                st.info("👆 Paste your Kaiser Permanente payment email HTML content above")
        
    with tab2:
        st.subheader("Payment Processing History")
        
        try:
            # Get recent payments from database (following your existing query patterns)
            db = st.session_state.enhanced_db_manager
            query = """
            SELECT 
                pm.payment_id,
                pm.payment_date,
                pm.payment_amount,
                pm.created_at,
                COUNT(pd.id) as detail_count,
                COALESCE(SUM(pd.net_amount), 0) as total_net
            FROM kp_payment_master pm
            LEFT JOIN kp_payment_details pd ON pm.payment_id = pd.payment_id
            GROUP BY pm.payment_id, pm.payment_date, pm.payment_amount, pm.created_at
            ORDER BY pm.created_at DESC
            LIMIT 50
            """
            
            df = db.execute_custom_query(query, [])
            
            if not df.empty:
                # Format for display (following your existing formatting patterns)
                display_df = df.copy()
                display_df['payment_amount'] = display_df['payment_amount'].apply(
                    lambda x: f"${float(x):,.2f}"
                )
                display_df['total_net'] = display_df['total_net'].apply(
                    lambda x: f"${float(x):,.2f}"
                )
                display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    column_config={
                        "payment_amount": st.column_config.TextColumn("Payment Amount"),
                        "total_net": st.column_config.TextColumn("Total Net"),
                        "detail_count": st.column_config.NumberColumn("Detail Records")
                    }
                )
                
                # Export option (following your existing export patterns)
                if st.button("📥 Export Payment History"):
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "Download CSV",
                        csv,
                        file_name=f"payment_history_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No payment history found")
                
        except Exception as e:
            st.error(f"Error loading payment history: {e}")
    
    with tab3:
        st.subheader("Search Payments")
        
        col1, col2 = st.columns(2)
        
        with col1:
            search_payment_id = st.text_input("Payment ID")
            
        with col2:
            search_date_range = st.date_input(
                "Payment Date Range",
                value=None,
                help="Select date range to filter payments"
            )
        
        if st.button("🔍 Search Payments") and search_payment_id:
            try:
                db = st.session_state.enhanced_db_manager
                summary = db.get_payment_summary(search_payment_id)
                
                if summary:
                    st.success(f"Payment {search_payment_id} found!")
                    
                    # Display payment details (following your existing metrics pattern)
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Payment Amount", f"${summary['payment_amount']:,.2f}")
                    with col2:
                        st.metric("Detail Records", summary['detail_count'])
                    with col3:
                        st.metric("Net Total", f"${summary['total_net']:,.2f}")
                    
                    # Option to download details (following your existing download pattern)
                    if st.button("📋 Download Payment Details"):
                        details = db.get_payment_details_for_export(search_payment_id)
                        if details:
                            df = pd.DataFrame(details)
                            csv = df.to_csv(index=False)
                            st.download_button(
                                "Download Payment Details",
                                csv,
                                file_name=f"payment_details_{search_payment_id}.csv",
                                mime="text/csv"
                            )
                else:
                    st.warning(f"Payment {search_payment_id} not found")
                    
            except Exception as e:
                st.error(f"Error searching payments: {e}")

# Master Data View - FINAL VERSION based on actual schema
elif page == "📊 Master Data View":
    st.header("📊 Master Invoice Data")
    
    # Get database connection
    db = st.session_state.enhanced_db_manager
    
    # Display stats
    try:
        stats = get_cached_table_stats()
        if stats:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Invoices", f"{stats.get('invoices', 0):,}")
            with col2:
                st.metric("Invoice Details", f"{stats.get('invoice_details', 0):,}")
            with col3:
                st.metric("Buildings", f"{stats.get('building_dimension', 0):,}")
    except Exception as e:
        st.error(f"Error loading stats: {e}")
    
    # Add filters section
    st.markdown("### 🔍 Filters")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        date_filter = st.selectbox(
            "Date Range",
            ["Last 30 Days", "Last 60 Days", "Last 90 Days", "This Year", "All Time", "Custom"],
            index=0  # Default to Last 30 Days
        )
        
        if date_filter == "Custom":
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
            end_date = st.date_input("End Date", value=datetime.now())
        else:
            if date_filter == "Last 30 Days":
                start_date = datetime.now() - timedelta(days=30)
                end_date = datetime.now()
            elif date_filter == "Last 60 Days":
                start_date = datetime.now() - timedelta(days=60)
                end_date = datetime.now()
            elif date_filter == "Last 90 Days":
                start_date = datetime.now() - timedelta(days=90)
                end_date = datetime.now()
            elif date_filter == "This Year":
                start_date = datetime(datetime.now().year, 1, 1)
                end_date = datetime.now()
            else:  # All Time
                start_date = None
                end_date = None
    
    with col2:
        status_filter = st.multiselect(
            "Invoice Status",
            ["All", "EDI", "Released", "Add-On"],
            default=["All"]
        )
    
    with col3:
        source_filter = st.multiselect(
            "Source System",
            ["All", "AUS", "BCI", "Other"],
            default=["All"]
        )
    
    with col4:
        search_term = st.text_input("Search Invoice #")
    
    st.markdown("---")
    st.markdown("### 📋 Invoice Data")
    
    try:
        # Query using actual column names from your schema
        query = """
        SELECT 
            i.invoice_no,
            i.invoice_date,
            i.invoice_total,
            i.edi_date,
            i.release_date,
            i.add_on_date,
            i.emid,
            i.service_area,
            i.post_name,
            i.chartfield,
            COUNT(DISTINCT id.id) as detail_count,
            STRING_AGG(DISTINCT id.source_system, ', ') as source_systems,
            MAX(id.customer_name) as customer_name,
            MAX(id.business_unit) as business_unit
        FROM invoices i
        LEFT JOIN invoice_details id ON i.invoice_no = id.invoice_no
        WHERE 1=1
        """
        
        params = []
        
        # Apply date filter
        if start_date and end_date:
            query += " AND i.invoice_date BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        
        # Apply status filter
        if "All" not in status_filter:
            status_conditions = []
            if "EDI" in status_filter:
                status_conditions.append("i.edi_date IS NOT NULL")
            if "Released" in status_filter:
                status_conditions.append("i.release_date IS NOT NULL")
            if "Add-On" in status_filter:
                status_conditions.append("i.add_on_date IS NOT NULL")
            if status_conditions:
                query += f" AND ({' OR '.join(status_conditions)})"
        
        # Apply source filter
        if "All" not in source_filter and len(source_filter) > 0:
            # Only add this condition if we're joining with invoice_details
            query += " AND ("
            source_conditions = []
            for source in source_filter:
                if source == "Other":
                    source_conditions.append("(id.source_system IS NULL OR id.source_system NOT IN ('AUS', 'BCI'))")
                else:
                    source_conditions.append(f"id.source_system = '{source}'")
            query += " OR ".join(source_conditions) + ")"
        
        # Apply search filter
        if search_term:
            query += " AND (i.invoice_no ILIKE %s OR i.emid ILIKE %s)"
            params.extend([f"%{search_term}%", f"%{search_term}%"])
        
        # Group by all non-aggregate columns
        query += """
        GROUP BY i.invoice_no, i.invoice_date, i.invoice_total, 
                 i.edi_date, i.release_date, i.add_on_date,
                 i.emid, i.service_area, i.post_name, i.chartfield
        ORDER BY i.invoice_date DESC
        LIMIT 100
        """
        
        # Execute query
        df = db.execute_custom_query(query, params)
        
        if not df.empty:
            st.success(f"Showing {len(df)} invoices (limited to 100 most recent)")
            
            # Format the dataframe for display
            display_df = df.copy()
            
            # Format currency
            if 'invoice_total' in display_df.columns:
                display_df['invoice_total'] = display_df['invoice_total'].apply(
                    lambda x: f"${float(x):,.2f}" if pd.notna(x) else "$0.00"
                )
            
            # Format dates
            date_columns = ['invoice_date', 'edi_date', 'release_date', 'add_on_date']
            for col in date_columns:
                if col in display_df.columns:
                    display_df[col] = pd.to_datetime(display_df[col]).dt.strftime('%Y-%m-%d')
                    display_df[col] = display_df[col].fillna('')
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'invoice_no': 'Invoice #',
                'invoice_date': 'Invoice Date',
                'invoice_total': 'Total',
                'edi_date': 'EDI Date',
                'release_date': 'Release Date',
                'add_on_date': 'Add-On Date',
                'emid': 'EMID',
                'service_area': 'Service Area',
                'post_name': 'Post Name',
                'chartfield': 'Chart Field',
                'customer_name': 'Customer',
                'business_unit': 'Business Unit',
                'detail_count': 'Detail Records',
                'source_systems': 'Sources'
            })
            
            # Display the dataframe
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Invoice #": st.column_config.TextColumn(width="medium"),
                    "Total": st.column_config.TextColumn(width="small"),
                    "Detail Records": st.column_config.NumberColumn(width="small"),
                }
            )
            
            # Export button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"invoice_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
        else:
            st.warning("No invoices found matching your filters. Try adjusting the date range or filters.")
            
    except Exception as e:
        st.error(f"Error loading invoice data: {e}")
        import traceback
        with st.expander("Error Details"):
            st.text(traceback.format_exc())


# Quick Process Page
elif page == "🚀 Quick Process":
    st.header("🚀 Quick Process Standard Files")
    
    # Check for standard files
    standard_files = {
        "weekly_release.xlsx": ("Release", "🔄"),
        "weekly_addons.xlsx": ("Add-On", "➕"), 
        "weekly_edi.xlsx": ("EDI", "📤"),
        "TLM_BCI.xlsx": ("BCI Details", "🏢"),
        "AUS_Invoice.xlsx": ("AUS Details", "🏢")
    }
    
    found_files = []
    for filename, (file_type, icon) in standard_files.items():
        if os.path.exists(filename):
            found_files.append((filename, file_type, icon))
    
    if found_files:
        st.subheader("📁 Found Standard Files:")
        for filename, file_type, icon in found_files:
            st.write(f"{icon} **{filename}** → {file_type}")
        
        if st.button("🚀 Process All Found Files", type="primary"):
            for filename, file_type, icon in found_files:
                with st.spinner(f"Processing {filename}..."):
                    try:
                        df = pd.read_excel(filename)
                        
                        if file_type in ["EDI", "Release", "Add-On"]:
                            # Would need date input for these
                            st.info(f"⏸️ {filename} requires date input - use Smart File Upload")
                        elif file_type == "BCI Details":
                            process_bci_details(df, filename)
                        elif file_type == "AUS Details":
                            process_aus_details(df, filename)
                        
                    except Exception as e:
                        st.error(f"Error processing {filename}: {e}")
elif page == "📊 Capital Project Dashboard":
    render_capital_project_dashboard()

elif page == "📁 Process Trimble File":
    render_process_trimble_file()

elif page == "🔍 Capital Project Search":
    render_capital_project_search()    

else:
    st.info("No standard files found in current directory")
    st.markdown("""
        **Expected files:**
        - `TLM_BCI.xlsx` (BCI Invoice Details)
        - `AUS_Invoice.xlsx` (AUS Invoice Details)
        - `weekly_release.xlsx` (Release processing)
        - `weekly_edi.xlsx` (EDI processing)
        - `weekly_addons.xlsx` (Add-On processing)
        """)

# Footer
st.markdown("---")
st.markdown("*Invoice Management System - a jbjCPA passion project*")
