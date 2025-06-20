import streamlit as st
import pandas as pd
import os
from datetime import datetime
import tempfile

# Import your existing invoice processing system
try:
    from invoice_master_upsert_system import InvoiceMasterManager
except ImportError:
    st.error("Please make sure invoice_master_upsert_system.py is in the same folder as this app")
    st.stop()

# Configure page
st.set_page_config(
    page_title="Invoice Management System",
    page_icon="📊",
    layout="wide"
)

# Title and description
st.title("📊 Invoice Management System")
st.markdown("Upload and process your weekly invoice files with ease!")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a function:", [
    "🏠 Home",
    "🔄 File Converter",
    "📤 Release Processing", 
    "➕ Add-On Processing",
    "📋 EDI Processing",
    "📈 Master Data View",
    "📝 Processing Logs"
])

# Initialize session state for logs
if 'processing_logs' not in st.session_state:
    st.session_state.processing_logs = []

def detect_file_type(filename):
    """Auto-detect file type based on filename"""
    filename_lower = filename.lower()
    
    if 'release' in filename_lower:
        return "Release"
    elif 'addon' in filename_lower or 'add-on' in filename_lower or 'add_on' in filename_lower:
        return "Add-On"
    elif 'edi' in filename_lower:
        return "EDI"
    else:
        return "Unknown"

def clean_notes_column(notes_value, unnamed_value):
    """Clean and combine notes from Notes column and unnamed column"""
    notes_clean = str(notes_value).strip() if pd.notna(notes_value) and str(notes_value).strip() != 'nan' else ""
    unnamed_clean = str(unnamed_value).strip() if pd.notna(unnamed_value) and str(unnamed_value).strip() != 'nan' else ""
    
    if notes_clean and unnamed_clean:
        return f"{notes_clean} | {unnamed_clean}"
    elif unnamed_clean:
        return unnamed_clean
    elif notes_clean:
        return notes_clean
    else:
        return None

def standardize_date_for_conversion(date_value):
    """Standardize dates during file conversion"""
    if pd.isna(date_value):
        return None
    
    date_str = str(date_value).strip()
    
    # Handle yyyymmdd format (8 digits)
    if len(date_str) == 8 and date_str.isdigit():
        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            return None
    
    # Handle ISO format with time
    try:
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if not pd.isna(parsed_date):
            return parsed_date.strftime('%Y-%m-%d')
    except:
        pass
    
    return None

def convert_source_file(uploaded_file, file_type, constant_date):
    """Convert source file to standardized format"""
    try:
        # Read the uploaded file
        if uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        else:  # .xls
            df = pd.read_excel(uploaded_file, engine='xlrd')
        
        st.info(f"📄 Initial read: {len(df)} rows from source file")
        
        # Clean empty rows - remove rows where Invoice No. is null/empty AND most other key fields are empty
        key_fields = ['EMID', 'NUID', 'SERVICE REQ\'D BY', 'Service Area', 'Post Name', 'Invoice No.']
        
        # Create a mask for rows that have data in key fields
        has_data_mask = pd.Series([False] * len(df))
        for field in key_fields:
            if field in df.columns:
                field_has_data = df[field].notna() & (df[field].astype(str).str.strip() != '') & (df[field].astype(str).str.strip() != 'nan')
                has_data_mask = has_data_mask | field_has_data
        
        # Filter to only rows with actual data
        df_cleaned = df[has_data_mask].copy()
        
        if len(df_cleaned) != len(df):
            st.success(f"🧹 Cleaned data: {len(df_cleaned)} rows with data (removed {len(df) - len(df_cleaned)} empty rows)")
        else:
            st.success(f"✅ All {len(df_cleaned)} rows contain data")
        
        # Data quality checks on cleaned data
        quality_issues = []
        
        # Check for missing Invoice No. in remaining data
        missing_invoice_nos = df_cleaned[df_cleaned['Invoice No.'].isna() | (df_cleaned['Invoice No.'].astype(str).str.strip() == '') | (df_cleaned['Invoice No.'].astype(str).str.strip() == 'nan')]
        if not missing_invoice_nos.empty:
            quality_issues.append(f"⚠️ {len(missing_invoice_nos)} rows have missing Invoice No.")
        
        # Initialize converted dataframe with required columns
        target_columns = [
            'EMID', 'NUID', 'SERVICE REQ\'D BY', 'Service Area', 'Post Name',
            'Invoice No.', 'Invoice From', 'Invoice To', 'Invoice Date',
            'Chartfield', 'Invoice Total'
        ]
        
        converted_df = pd.DataFrame()
        
        # Copy basic columns that exist in all file types
        for col in target_columns:
            if col in df_cleaned.columns:
                converted_df[col] = df_cleaned[col]
            else:
                st.warning(f"Column '{col}' not found in source file")
                converted_df[col] = None
        
        # Standardize dates
        date_columns = ['Invoice From', 'Invoice To', 'Invoice Date']
        date_errors = []
        
        for col in date_columns:
            if col in converted_df.columns:
                converted_df[col] = converted_df[col].apply(standardize_date_for_conversion)
                # Check for failed conversions
                failed_dates = converted_df[converted_df[col].isna() & df_cleaned[col].notna()]
                if not failed_dates.empty:
                    date_errors.append(f"{col}: {len(failed_dates)} failed conversions")
        
        if date_errors:
            quality_issues.extend([f"⚠️ Date conversion issues: {', '.join(date_errors)}"])
        
        # File type specific processing
        if file_type == "Release":
            # Add Release Date
            converted_df['Release Date'] = constant_date
            output_filename = "weekly_release.xlsx"
            
        elif file_type == "Add-On":
            # Handle Notes column cleaning
            notes_col = df_cleaned['Notes'] if 'Notes' in df_cleaned.columns else pd.Series([None] * len(df_cleaned))
            
            # Find unnamed column (typically the last column or one without a clear name)
            unnamed_col = None
            for col in df_cleaned.columns:
                if str(col).strip() == '' or 'Unnamed' in str(col) or col is None:
                    unnamed_col = df_cleaned[col]
                    break
            
            if unnamed_col is None:
                # If no clear unnamed column, check last few columns for vacation hold type data
                for col in df_cleaned.columns[-3:]:
                    if col not in target_columns and 'Notes' not in str(col):
                        sample_values = df_cleaned[col].dropna().astype(str).str.upper()
                        if any('VACATION' in val or 'HOLD' in val for val in sample_values):
                            unnamed_col = df_cleaned[col]
                            break
            
            if unnamed_col is not None:
                converted_df['Notes'] = [clean_notes_column(notes, unnamed) 
                                       for notes, unnamed in zip(notes_col, unnamed_col)]
            else:
                converted_df['Notes'] = notes_col
            
            # Add Original invoice # if it exists
            if 'Original invoice #' in df_cleaned.columns:
                converted_df['Original invoice #'] = df_cleaned['Original invoice #']
            elif 'Original invoice' in df_cleaned.columns:
                converted_df['Original invoice #'] = df_cleaned['Original invoice']
            
            # Add Add-On Date
            converted_df['Add-On Date'] = constant_date
            output_filename = "weekly_addons.xlsx"
            
        elif file_type == "EDI":
            # Add Notes and Not Transmitted columns
            if 'Notes' in df_cleaned.columns:
                converted_df['Notes'] = df_cleaned['Notes']
            if 'Not Transmitted' in df_cleaned.columns:
                converted_df['Not Transmitted'] = df_cleaned['Not Transmitted']
            
            # Add EDI Date
            converted_df['EDI Date'] = constant_date
            output_filename = "weekly_edi.xlsx"
        
        # Final check - ensure we don't save empty records
        final_mask = converted_df['Invoice No.'].notna() & (converted_df['Invoice No.'].astype(str).str.strip() != '') & (converted_df['Invoice No.'].astype(str).str.strip() != 'nan')
        converted_df_final = converted_df[final_mask].copy()
        
        if len(converted_df_final) != len(converted_df):
            st.warning(f"⚠️ Removed {len(converted_df) - len(converted_df_final)} additional rows with missing Invoice No.")
        
        # Save the converted file
        converted_df_final.to_excel(output_filename, index=False)
        
        return converted_df_final, output_filename, quality_issues
        
    except Exception as e:
        st.error(f"Error converting file: {str(e)}")
        return None, None, [f"Conversion failed: {str(e)}"]

# Home Page
if page == "🏠 Home":
    st.markdown("""
    ## Welcome to your Invoice Management System!
    
    This application helps you process your weekly invoice files and maintain your master database.
    
    ### 📋 Available Functions:
    - **File Converter**: Convert source files from email to standardized format
    - **Release Processing**: Process weekly release files
    - **Add-On Processing**: Handle add-on invoices with history linking
    - **EDI Processing**: Manage EDI uploads with date preservation
    - **Master Data View**: Review your complete invoice database
    - **Processing Logs**: View detailed processing history
    
    ### 🚀 Getting Started:
    1. Choose a processing function from the sidebar
    2. Upload your Excel file
    3. Review the processing results
    4. Check your updated master database
    
    ### 🚀 Quick Process (Static Files)
    If you have standard weekly_release.xlsx, weekly_addons.xlsx, and weekly_edi.xlsx files ready:
    """)
    
    # Process All button
    if st.button("🔄 Process All 3 Files (Release → Add-On → EDI)", type="primary"):
        files_to_process = [
            ("weekly_release.xlsx", "Release"),
            ("weekly_addons.xlsx", "Add-On"), 
            ("weekly_edi.xlsx", "EDI")
        ]
        
        processed_count = 0
        
        for filename, file_type in files_to_process:
            if os.path.exists(filename):
                with st.spinner(f"Processing {filename}..."):
                    try:
                        manager = InvoiceMasterManager()
                        
                        if file_type == "Release":
                            manager.process_release_file(filename)
                        elif file_type == "Add-On":
                            manager.process_addon_file(filename)
                        elif file_type == "EDI":
                            manager.process_edi_file(filename)
                        
                        st.success(f"✅ {file_type} file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                        processed_count += 1
                        
                    except Exception as e:
                        st.error(f"❌ Error processing {filename}: {str(e)}")
            else:
                st.warning(f"⚠️ {filename} not found - skipping {file_type}")
        
        if processed_count > 0:
            st.success(f"🎉 **Batch Complete!** Processed {processed_count} files in sequence.")
            if os.path.exists("invoice_master.xlsx"):
                master_df = pd.read_excel("invoice_master.xlsx")
                st.metric("📈 Updated Master File Total", len(master_df))
    
    st.markdown("**Or process individual files:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Process Release File", type="secondary"):
            if os.path.exists("weekly_release.xlsx"):
                with st.spinner("Processing weekly_release.xlsx..."):
                    try:
                        manager = InvoiceMasterManager()
                        manager.process_release_file("weekly_release.xlsx")
                        
                        st.success("✅ Release file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("weekly_release.xlsx not found. Use File Converter or Upload first.")
    
    with col2:
        if st.button("➕ Process Add-On File", type="secondary"):
            if os.path.exists("weekly_addons.xlsx"):
                with st.spinner("Processing weekly_addons.xlsx..."):
                    try:
                        manager = InvoiceMasterManager()
                        manager.process_addon_file("weekly_addons.xlsx")
                        
                        st.success("✅ Add-On file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("weekly_addons.xlsx not found. Use File Converter or Upload first.")
    
    with col3:
        if st.button("📋 Process EDI File", type="secondary"):
            if os.path.exists("weekly_edi.xlsx"):
                with st.spinner("Processing weekly_edi.xlsx..."):
                    try:
                        manager = InvoiceMasterManager()
                        manager.process_edi_file("weekly_edi.xlsx")
                        
                        st.success("✅ EDI file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("weekly_edi.xlsx not found. Use File Converter or Upload first.")
    
    st.markdown("""
    
    ### 📊 Current Master Database Status:
    """)
    
    # Show current master file status
    if os.path.exists("invoice_master.xlsx"):
        master_df = pd.read_excel("invoice_master.xlsx")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Invoices", len(master_df))
        with col2:
            released_count = len(master_df[master_df['Release Date'].notna()])
            st.metric("Released Invoices", released_count)
        with col3:
            edi_count = len(master_df[master_df['EDI Date'].notna()])
            st.metric("EDI Processed", edi_count)
    else:
        st.info("No master file found. Upload your first file to get started!")

# File Converter Page
elif page == "🔄 File Converter":
    st.header("🔄 Source File Converter")
    st.markdown("Convert your email source files into standardized formats for processing.")
    
    # Choose between single and bulk upload
    upload_mode = st.radio(
        "Upload Mode:",
        ["📁 Single File", "📂 Bulk Upload (2-3 files)"],
        horizontal=True
    )
    
    if upload_mode == "📁 Single File":
        # Original single file upload
        uploaded_file = st.file_uploader(
            "Upload your source file",
            type=['xlsx', 'xls'],
            help="Upload your source Excel file from email"
        )
        
        if uploaded_file:
            st.write(f"**File:** {uploaded_file.name}")
            st.write(f"**Size:** {uploaded_file.size:,} bytes")
            
            # Auto-detect file type
            detected_type = detect_file_type(uploaded_file.name)
            
            # File type selection
            col1, col2 = st.columns(2)
            
            with col1:
                if detected_type != "Unknown":
                    st.success(f"🎯 Auto-detected: {detected_type}")
                    file_type = st.selectbox(
                        "Confirm file type:",
                        ["Release", "Add-On", "EDI"],
                        index=["Release", "Add-On", "EDI"].index(detected_type),
                        help="Auto-detected based on filename"
                    )
                else:
                    st.warning("⚠️ Could not auto-detect file type")
                    file_type = st.selectbox(
                        "Select file type:",
                        ["Release", "Add-On", "EDI"],
                        help="Choose the type of source file you're uploading"
                    )
            
            with col2:
                constant_date = st.date_input(
                    f"{file_type} Date:",
                    help=f"Date to use as the {file_type} Date for all records in this file"
                )
            
            # Convert button
            if st.button("🔄 Convert Source File", type="primary"):
                with st.spinner(f"Converting {file_type} source file..."):
                    converted_df, output_filename, quality_issues = convert_source_file(
                        uploaded_file, file_type, constant_date.strftime('%Y-%m-%d')
                    )
                    
                    if converted_df is not None:
                        # Show quality issues if any
                        if quality_issues:
                            st.warning("Data Quality Issues Found:")
                            for issue in quality_issues:
                                st.write(issue)
                        
                        # Show conversion results
                        st.success(f"✅ File converted successfully!")
                        st.info(f"📁 Saved as: **{output_filename}**")
                        
                        # Show preview of converted data
                        st.subheader("Preview of Converted Data")
                        st.dataframe(converted_df.head(10), use_container_width=True)
                        
                        # Show conversion summary
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Records", len(converted_df))
                        with col2:
                            non_null_dates = len(converted_df[converted_df[f'{file_type} Date'].notna()])
                            st.metric(f"{file_type} Dates Added", non_null_dates)
                        with col3:
                            if file_type == "Add-On" and 'Notes' in converted_df.columns:
                                notes_count = len(converted_df[converted_df['Notes'].notna()])
                                st.metric("Records with Notes", notes_count)
                            elif file_type == "EDI" and 'Not Transmitted' in converted_df.columns:
                                not_transmitted = len(converted_df[converted_df['Not Transmitted'].notna()])
                                st.metric("Not Transmitted", not_transmitted)
                            else:
                                st.metric("Columns", len(converted_df.columns))
    
    else:  # Bulk Upload Mode
        st.subheader("📂 Bulk File Upload")
        st.markdown("Upload 2-3 files (Release, Add-On, EDI) and process them all at once!")
        
        uploaded_files = st.file_uploader(
            "Upload your weekly files",
            type=['xlsx', 'xls'],
            accept_multiple_files=True,
            help="Upload your Release, Add-On, and EDI files together"
        )
        
        if uploaded_files:
            st.markdown("### 🎯 Auto-Detection Results")
            
            file_configs = {}
            
            for i, uploaded_file in enumerate(uploaded_files):
                detected_type = detect_file_type(uploaded_file.name)
                
                with st.container():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.write(f"**📄 {uploaded_file.name}**")
                        st.write(f"Size: {uploaded_file.size:,} bytes")
                    
                    with col2:
                        if detected_type != "Unknown":
                            st.success(f"🎯 Detected: {detected_type}")
                            file_type = st.selectbox(
                                "Confirm type:",
                                ["Release", "Add-On", "EDI"],
                                index=["Release", "Add-On", "EDI"].index(detected_type),
                                key=f"type_{i}"
                            )
                        else:
                            st.warning("⚠️ Unknown type")
                            file_type = st.selectbox(
                                "Select type:",
                                ["Release", "Add-On", "EDI"],
                                key=f"type_{i}"
                            )
                    
                    with col3:
                        file_date = st.date_input(
                            f"{file_type} Date:",
                            key=f"date_{i}",
                            help=f"Date for {file_type} processing"
                        )
                    
                    file_configs[uploaded_file.name] = {
                        'file': uploaded_file,
                        'type': file_type,
                        'date': file_date.strftime('%Y-%m-%d')
                    }
                
                st.markdown("---")
            
            # Bulk convert button
            if st.button("🔄 Convert All Files", type="primary"):
                conversion_results = []
                
                for filename, config in file_configs.items():
                    with st.expander(f"Converting {filename}...", expanded=True):
                        with st.spinner(f"Converting {config['type']} file..."):
                            converted_df, output_filename, quality_issues = convert_source_file(
                                config['file'], config['type'], config['date']
                            )
                            
                            if converted_df is not None:
                                if quality_issues:
                                    st.warning("Data Quality Issues:")
                                    for issue in quality_issues:
                                        st.write(f"  • {issue}")
                                
                                st.success(f"✅ Converted to {output_filename}")
                                st.metric("Records", len(converted_df))
                                
                                conversion_results.append({
                                    'original': filename,
                                    'converted': output_filename,
                                    'type': config['type'],
                                    'records': len(converted_df)
                                })
                            else:
                                st.error(f"❌ Failed to convert {filename}")
                
                if conversion_results:
                    st.markdown("### 🎉 Conversion Complete!")
                    
                    # Show summary
                    total_records = sum(r['records'] for r in conversion_results)
                    st.info(f"📊 **Total:** {len(conversion_results)} files converted, {total_records} records processed")
    
    # Static Quick Process section - always visible
    st.markdown("---")
    st.subheader("🚀 Quick Process (Available Files)")
    st.markdown("Process your converted files or any existing weekly files:")
    
    # Process All button
    if st.button("🔄 Process All 3 Files (Release → Add-On → EDI)", type="primary", key="converter_process_all"):
        files_to_process = [
            ("weekly_release.xlsx", "Release"),
            ("weekly_addons.xlsx", "Add-On"), 
            ("weekly_edi.xlsx", "EDI")
        ]
        
        processed_count = 0
        
        for filename, file_type in files_to_process:
            if os.path.exists(filename):
                with st.spinner(f"Processing {filename}..."):
                    try:
                        manager = InvoiceMasterManager()
                        
                        if file_type == "Release":
                            manager.process_release_file(filename)
                        elif file_type == "Add-On":
                            manager.process_addon_file(filename)
                        elif file_type == "EDI":
                            manager.process_edi_file(filename)
                        
                        st.success(f"✅ {file_type} file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                        processed_count += 1
                        
                    except Exception as e:
                        st.error(f"❌ Error processing {filename}: {str(e)}")
            else:
                st.warning(f"⚠️ {filename} not found - skipping {file_type}")
        
        if processed_count > 0:
            st.success(f"🎉 **Batch Complete!** Processed {processed_count} files in sequence.")
            if os.path.exists("invoice_master.xlsx"):
                master_df = pd.read_excel("invoice_master.xlsx")
                st.metric("📈 Updated Master File Total", len(master_df))
    
    st.markdown("**Or process individual files:**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Process Release File", type="secondary", key="converter_release"):
            if os.path.exists("weekly_release.xlsx"):
                with st.spinner("Processing weekly_release.xlsx..."):
                    try:
                        manager = InvoiceMasterManager()
                        manager.process_release_file("weekly_release.xlsx")
                        
                        st.success("✅ Release file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("weekly_release.xlsx not found. Convert a file first.")
    
    with col2:
        if st.button("➕ Process Add-On File", type="secondary", key="converter_addon"):
            if os.path.exists("weekly_addons.xlsx"):
                with st.spinner("Processing weekly_addons.xlsx..."):
                    try:
                        manager = InvoiceMasterManager()
                        manager.process_addon_file("weekly_addons.xlsx")
                        
                        st.success("✅ Add-On file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("weekly_addons.xlsx not found. Convert a file first.")
    
    with col3:
        if st.button("📋 Process EDI File", type="secondary", key="converter_edi"):
            if os.path.exists("weekly_edi.xlsx"):
                with st.spinner("Processing weekly_edi.xlsx..."):
                    try:
                        manager = InvoiceMasterManager()
                        manager.process_edi_file("weekly_edi.xlsx")
                        
                        st.success("✅ EDI file processed!")
                        for msg in manager.log_messages:
                            if "processing complete:" in msg.lower():
                                st.info(f"📊 {msg.split('] ')[-1]}")
                        
                        # Store logs
                        if 'processing_logs' not in st.session_state:
                            st.session_state.processing_logs = []
                        st.session_state.processing_logs.extend(manager.log_messages)
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("weekly_edi.xlsx not found. Convert a file first.")

# Release Processing Page
elif page == "📤 Release Processing":
    st.header("📤 Release File Processing")
    st.markdown("Upload your weekly release file to update invoice statuses and release dates.")
    
    uploaded_file = st.file_uploader(
        "Choose your weekly release file",
        type=['xlsx', 'xls'],
        help="Upload your Excel file containing release information"
    )
    
    if uploaded_file:
        st.write(f"**File:** {uploaded_file.name}")
        st.write(f"**Size:** {uploaded_file.size:,} bytes")
        
        if st.button("🔄 Process Release File", type="primary"):
            with st.spinner("Processing release file..."):
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    temp_path = tmp_file.name
                
                try:
                    manager = InvoiceMasterManager()
                    manager.process_release_file(temp_path)
                    
                    st.success("Processing completed successfully!")
                    for msg in manager.log_messages:
                        if "processing complete:" in msg.lower():
                            st.info(f"✅ {msg.split('] ')[-1]}")
                    
                    if 'processing_logs' not in st.session_state:
                        st.session_state.processing_logs = []
                    st.session_state.processing_logs.extend(manager.log_messages)
                    
                    if os.path.exists("invoice_master.xlsx"):
                        master_df = pd.read_excel("invoice_master.xlsx")
                        st.metric("Total Records in Master File", len(master_df))
                
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
                
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

# Add-On Processing Page
elif page == "➕ Add-On Processing":
    st.header("➕ Add-On File Processing")
    st.markdown("Upload your add-on file to handle revised invoices and create history links.")
    
    uploaded_file = st.file_uploader(
        "Choose your add-on file",
        type=['xlsx', 'xls'],
        help="Upload your Excel file containing add-on invoice information"
    )
    
    if uploaded_file:
        st.write(f"**File:** {uploaded_file.name}")
        st.write(f"**Size:** {uploaded_file.size:,} bytes")
        
        if st.button("🔄 Process Add-On File", type="primary"):
            with st.spinner("Processing add-on file..."):
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    temp_path = tmp_file.name
                
                try:
                    manager = InvoiceMasterManager()
                    manager.process_addon_file(temp_path)
                    
                    st.success("Processing completed successfully!")
                    for msg in manager.log_messages:
                        if "processing complete:" in msg.lower():
                            st.info(f"✅ {msg.split('] ')[-1]}")
                    
                    if 'processing_logs' not in st.session_state:
                        st.session_state.processing_logs = []
                    st.session_state.processing_logs.extend(manager.log_messages)
                    
                    if os.path.exists("invoice_master.xlsx"):
                        master_df = pd.read_excel("invoice_master.xlsx")
                        st.metric("Total Records in Master File", len(master_df))
                
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
                
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

# EDI Processing Page
elif page == "📋 EDI Processing":
    st.header("📋 EDI File Processing")
    st.markdown("Upload your EDI file to manage submissions with date preservation.")
    
    uploaded_file = st.file_uploader(
        "Choose your EDI file",
        type=['xlsx', 'xls'],
        help="Upload your Excel file containing EDI submission information"
    )
    
    if uploaded_file:
        st.write(f"**File:** {uploaded_file.name}")
        st.write(f"**Size:** {uploaded_file.size:,} bytes")
        
        if st.button("🔄 Process EDI File", type="primary"):
            with st.spinner("Processing EDI file..."):
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    temp_path = tmp_file.name
                
                try:
                    manager = InvoiceMasterManager()
                    manager.process_edi_file(temp_path)
                    
                    st.success("Processing completed successfully!")
                    for msg in manager.log_messages:
                        if "processing complete:" in msg.lower():
                            st.info(f"✅ {msg.split('] ')[-1]}")
                    
                    if 'processing_logs' not in st.session_state:
                        st.session_state.processing_logs = []
                    st.session_state.processing_logs.extend(manager.log_messages)
                    
                    if os.path.exists("invoice_master.xlsx"):
                        master_df = pd.read_excel("invoice_master.xlsx")
                        st.metric("Total Records in Master File", len(master_df))
                
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
                
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

# Master Data View Page
elif page == "📈 Master Data View":
    st.header("📈 Master Invoice Database")
    
    if os.path.exists("invoice_master.xlsx"):
        master_df = pd.read_excel("invoice_master.xlsx")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Invoices", len(master_df))
        with col2:
            released = len(master_df[master_df['Release Date'].notna()])
            st.metric("Released", released)
        with col3:
            addon = len(master_df[master_df['Add-On Date'].notna()])
            st.metric("Add-Ons", addon)
        with col4:
            edi = len(master_df[master_df['EDI Date'].notna()])
            st.metric("EDI Processed", edi)
        
        # Filters
        st.subheader("Filters")
        
        # Search filter
        st.markdown("**🔍 Search:**")
        search_invoice = st.text_input(
            "Search by Invoice Number:",
            placeholder="Enter invoice number (e.g., 16535899)",
            help="Search for a specific invoice number"
        )
        
        # Dropdown filters
        st.markdown("**📋 Filter by Category:**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            service_areas = ['All'] + sorted(list(master_df['Service Area'].dropna().unique()))
            selected_area = st.selectbox("Service Area", service_areas)
        
        with col2:
            post_names = ['All'] + sorted(list(master_df['Post Name'].dropna().unique()))
            selected_post = st.selectbox("Post Name", post_names)
        
        with col3:
            approvers = ['All'] + sorted(list(master_df['SERVICE REQ\'D BY'].dropna().unique()))
            selected_approver = st.selectbox("Approver", approvers)
        
        # Apply filters
        filtered_df = master_df.copy()
        
        # Apply search filter first
        if search_invoice:
            # Convert Invoice No. to string for searching and handle various formats
            invoice_search_mask = master_df['Invoice No.'].astype(str).str.contains(
                search_invoice, case=False, na=False, regex=False
            )
            filtered_df = filtered_df[invoice_search_mask]
            
            if len(filtered_df) == 0:
                st.warning(f"No invoices found matching '{search_invoice}'")
            else:
                st.success(f"Found {len(filtered_df)} invoice(s) matching '{search_invoice}'")
        
        # Apply dropdown filters
        if selected_area != 'All':
            filtered_df = filtered_df[filtered_df['Service Area'] == selected_area]
        if selected_post != 'All':
            filtered_df = filtered_df[filtered_df['Post Name'] == selected_post]
        if selected_approver != 'All':
            filtered_df = filtered_df[filtered_df['SERVICE REQ\'D BY'] == selected_approver]
        
        # Display data
        st.subheader(f"Invoice Data ({len(filtered_df)} records)")
        
        # Make dataframe interactive
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
            file_name=f"invoice_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    else:
        st.info("No master database found. Process some files first!")

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
    
    # Show available log files
    st.subheader("Saved Log Files")
    log_files = [f for f in os.listdir('.') if f.startswith('processing_log_') and f.endswith('.txt')]
    
    if log_files:
        selected_log = st.selectbox("Choose a log file to view:", log_files)
        
        if st.button("View Log File"):
            try:
                with open(selected_log, 'r') as f:
                    log_content = f.read()
                st.text_area("Log Content", log_content, height=400)
            except Exception as e:
                st.error(f"Error reading log file: {e}")
    else:
        st.info("No saved log files found.")

# Footer
st.markdown("---")
st.markdown("*Invoice Management System - Built with Streamlit*")