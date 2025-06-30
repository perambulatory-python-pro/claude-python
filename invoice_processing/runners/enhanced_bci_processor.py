"""
Enhanced BCI Processor with Partial Upload Support
Use this to replace the BCI processing function in your Streamlit app
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging

def process_bci_details_enhanced(df: pd.DataFrame, filename: str, db_manager, data_mapper):
    """
    Enhanced BCI processing with partial upload support and detailed reporting
    """
    st.subheader("🏢 Processing BCI Invoice Details (Enhanced)")
    
    try:
        # Map the data
        with st.spinner("Mapping BCI data..."):
            mapped_data = data_mapper.map_bci_details(df)
        
        if not mapped_data:
            st.error("No valid BCI data found in file")
            return False
        
        st.success(f"✅ Mapped {len(mapped_data)} records from {filename}")
        
        # Show preview of what will be processed
        with st.expander("📋 Preview Mapped Data"):
            if len(mapped_data) > 0:
                sample = mapped_data[0]
                for key, value in sample.items():
                    st.write(f"**{key}:** {value}")
        
        # Process with enhanced validation
        if st.button("🚀 Process with Validation", type="primary"):
            with st.spinner("Processing BCI data with validation..."):
                # Use enhanced bulk insert with validation
                results = db_manager.bulk_insert_invoice_details_with_validation(mapped_data)
                
                # Display comprehensive results
                st.markdown("## 📊 Processing Results")
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Records", results['total_records'])
                with col2:
                    st.metric("✅ Inserted", results['inserted'], delta=f"{results['inserted']}")
                with col3:
                    st.metric("⚠️ Skipped", results['skipped'], delta=f"-{results['skipped']}")
                with col4:
                    success_rate = (results['inserted'] / results['total_records'] * 100) if results['total_records'] > 0 else 0
                    st.metric("Success Rate", f"{success_rate:.1f}%")
                
                # Success message
                if results['inserted'] > 0:
                    st.success(f"""
                    ✅ **Partial Processing Completed Successfully!**
                    - **File:** {filename}
                    - **Inserted:** {results['inserted']:,} records
                    - **Source:** BCI (self-performed)
                    """)
                
                # Missing invoices analysis
                if results['missing_invoices']:
                    st.warning(f"⚠️ **{results['missing_invoice_count']} invoice numbers not found in master table**")
                    
                    with st.expander(f"📋 Missing Invoice Numbers ({results['missing_invoice_count']} total)"):
                        st.write("**Missing invoice numbers that need to be added to master table:**")
                        
                        # Display missing invoices in a more readable format
                        missing_df = pd.DataFrame({
                            'Missing Invoice Numbers': results['missing_invoices']
                        })
                        st.dataframe(missing_df, use_container_width=True)
                        
                        # Download button for missing invoices
                        missing_csv = missing_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Missing Invoice Numbers",
                            data=missing_csv,
                            file_name=f"missing_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                # Detailed skipped records analysis
                if results['skipped_records']:
                    with st.expander(f"📋 Skipped Records Details ({len(results['skipped_records'])} total)"):
                        st.write("**Records that were skipped and need manual review:**")
                        
                        skipped_df = pd.DataFrame(results['skipped_records'])
                        st.dataframe(skipped_df, use_container_width=True)
                        
                        # Download button for skipped records
                        skipped_csv = skipped_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Skipped Records for Analysis",
                            data=skipped_csv,
                            file_name=f"skipped_bci_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                # Next steps guidance
                if results['missing_invoices']:
                    st.info("""
                    **🔧 Next Steps for Missing Invoices:**
                    
                    1. **Download the missing invoice numbers** using the button above
                    2. **Check your master invoice files** (weekly_edi.xlsx, weekly_release.xlsx, etc.)
                    3. **Process the master files first** if they contain these invoices
                    4. **Verify invoice numbers** - they might be in a different format
                    5. **Re-upload the BCI file** after resolving master invoice issues
                    
                    💡 **Tip:** The inserted records are already saved - only the missing ones need attention!
                    """)
                
                # Log the results
                log_message = (f"BCI processing: {results['inserted']} inserted, "
                             f"{results['skipped']} skipped, "
                             f"{results['missing_invoice_count']} missing invoices from {filename}")
                
                # Add to session logs (if available)
                if hasattr(st.session_state, 'processing_logs'):
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state.processing_logs.append(f"[{timestamp}] {log_message}")
                
                return results['success']
        
        return None  # User hasn't clicked process yet
        
    except Exception as e:
        st.error(f"BCI processing failed: {e}")
        logging.error(f"Enhanced BCI processing error: {e}")
        return False

def process_aus_details_enhanced(df: pd.DataFrame, filename: str, db_manager, data_mapper):
    """
    Enhanced AUS processing with partial upload support
    """
    st.subheader("🏢 Processing AUS Invoice Details (Enhanced)")
    
    try:
        # Map the data
        with st.spinner("Mapping AUS data..."):
            mapped_data = data_mapper.map_aus_details(df)
        
        if not mapped_data:
            st.error("No valid AUS data found in file")
            return False
        
        st.success(f"✅ Mapped {len(mapped_data)} records from {filename}")
        
        # Process with enhanced validation (same logic as BCI)
        if st.button("🚀 Process AUS with Validation", type="primary"):
            with st.spinner("Processing AUS data with validation..."):
                results = db_manager.bulk_insert_invoice_details_with_validation(mapped_data)
                
                # Display results (similar to BCI but for AUS)
                st.markdown("## 📊 AUS Processing Results")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Records", results['total_records'])
                with col2:
                    st.metric("✅ Inserted", results['inserted'])
                with col3:
                    st.metric("⚠️ Skipped", results['skipped'])
                with col4:
                    success_rate = (results['inserted'] / results['total_records'] * 100) if results['total_records'] > 0 else 0
                    st.metric("Success Rate", f"{success_rate:.1f}%")
                
                if results['inserted'] > 0:
                    st.success(f"""
                    ✅ **AUS Processing Completed!**
                    - **File:** {filename}
                    - **Inserted:** {results['inserted']:,} records
                    - **Source:** AUS (subcontractor)
                    """)
                
                # Handle missing invoices for AUS
                if results['missing_invoices']:
                    st.warning(f"⚠️ **{results['missing_invoice_count']} AUS invoice numbers not found in master table**")
                    
                    with st.expander("📋 Missing AUS Invoice Numbers"):
                        missing_df = pd.DataFrame({
                            'Missing Invoice Numbers': results['missing_invoices']
                        })
                        st.dataframe(missing_df)
                        
                        missing_csv = missing_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Missing AUS Invoice Numbers",
                            data=missing_csv,
                            file_name=f"missing_aus_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                return results['success']
        
        return None
        
    except Exception as e:
        st.error(f"AUS processing failed: {e}")
        logging.error(f"Enhanced AUS processing error: {e}")
        return False

# Example of how to integrate this into your main Streamlit app:
"""
# In your main Streamlit app, replace the BCI/AUS processing with:

if detected_type == "BCI Details":
    from enhanced_bci_processor import process_bci_details_enhanced
    return process_bci_details_enhanced(df, uploaded_file.name, 
                                       st.session_state.enhanced_db_manager, 
                                       st.session_state.data_mapper)

elif detected_type == "AUS Details":
    from enhanced_bci_processor import process_aus_details_enhanced
    return process_aus_details_enhanced(df, uploaded_file.name,
                                       st.session_state.enhanced_db_manager,
                                       st.session_state.data_mapper)
"""
