"""
Updated display function that properly shows missing invoices
Replace this in your invoice_app_auto_detect.py
"""

def display_upload_results_with_downloads(upload_results: Dict, filename: str, file_type: str):
    """
    Display upload results with focus on missing invoices and real errors
    """
    # Display upload metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Records", upload_results['total_records'])
    with col2:
        st.metric("✅ Inserted", upload_results['inserted'])
    with col3:
        st.metric("⚠️ Missing Invoices", upload_results.get('missing_invoice_count', 0))
    with col4:
        st.metric("❌ Other Errors", len(upload_results.get('error_records', [])))
    
    if upload_results['inserted'] > 0:
        st.success(f"""
        ✅ **{file_type} Upload Completed!**
        - **File:** {filename}
        - **Inserted:** {upload_results['inserted']:,} records successfully added
        """)
    
    # Show missing invoices - THIS IS THE MOST IMPORTANT PART
    if upload_results.get('missing_invoice_count', 0) > 0:
        with st.expander(f"⚠️ Missing Invoices ({upload_results['missing_invoice_count']} records)", expanded=True):
            st.warning("""
            These records were NOT inserted because their invoice numbers don't exist in the master invoices table.
            You need to process these invoices through EDI/Release first.
            """)
            
            # Show unique missing invoice numbers
            missing_invoices = upload_results.get('missing_invoices', [])
            st.write(f"**{len(missing_invoices)} unique invoice numbers not found:**")
            
            # Display in columns for readability
            cols = st.columns(3)
            for i, invoice_no in enumerate(missing_invoices[:15]):
                cols[i % 3].write(f"• {invoice_no}")
            
            if len(missing_invoices) > 15:
                st.write(f"... and {len(missing_invoices) - 15} more")
            
            # Download missing invoice numbers
            if missing_invoices:
                missing_inv_df = pd.DataFrame({
                    'Missing_Invoice_Number': missing_invoices
                })
                missing_inv_csv = missing_inv_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Missing Invoice Numbers",
                    data=missing_inv_csv,
                    file_name=f"missing_invoice_numbers_{file_type.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key=f"download_missing_inv_{file_type}"
                )
            
            # Download full records with missing invoices
            missing_records = upload_results.get('missing_invoice_records', [])
            if missing_records:
                missing_records_df = pd.DataFrame(missing_records)
                missing_records_csv = missing_records_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Full Records with Missing Invoices",
                    data=missing_records_csv,
                    file_name=f"records_missing_invoices_{file_type.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key=f"download_missing_records_{file_type}"
                )
            
            st.info("""
            💡 **Next Steps:**
            1. Download the missing invoice numbers
            2. Check if these invoices exist in your EDI/Release files
            3. Process the master invoice files first
            4. Then re-upload this detail file
            """)
    
    # Show other errors if any
    error_records = upload_results.get('error_records', [])
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
                file_name=f"error_records_{file_type.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"download_errors_{file_type}"
            )


# Also update the process functions to use the new method
# In both process_bci_details and process_aus_details, change this line:
# upload_results = db.bulk_insert_invoice_details_no_validation(mapped_data)
# To:
# upload_results = db.bulk_insert_invoice_details_fast_validated(mapped_data)
