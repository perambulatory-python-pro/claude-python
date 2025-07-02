"""
Complete test of your Dropbox Business setup
Now that you have both token and team member ID configured
"""

import streamlit as st
import dropbox
import os
from dotenv import load_dotenv
import pandas as pd
import tempfile
from datetime import datetime

load_dotenv()

def complete_dropbox_test():
    st.title("🎉 Complete Dropbox Business Test")
    st.markdown("Let's verify your full setup is working perfectly!")
    
    # Load configuration
    token = os.getenv('DROPBOX_ACCESS_TOKEN')
    member_id = os.getenv('DROPBOX_TEAM_MEMBER_ID')
    
    # Configuration check
    st.header("📋 Configuration Check")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if token:
            masked_token = token[:8] + "..." + token[-4:] if len(token) > 12 else "***"
            st.success(f"✅ Token: {masked_token}")
        else:
            st.error("❌ No access token found")
            
    with col2:
        if member_id:
            masked_id = member_id[:15] + "..." + member_id[-4:] if len(member_id) > 19 else member_id
            st.success(f"✅ Member ID: {masked_id}")
        else:
            st.error("❌ No team member ID found")
    
    if not token or not member_id:
        st.stop()
    
    # Test connections
    st.header("🔗 Connection Tests")
    
    try:
        # Test 1: Team connection
        with st.spinner("Testing team connection..."):
            team_client = dropbox.DropboxTeam(token)
            team_info = team_client.team_get_info()
            
        st.success(f"✅ **Team Connection**: {team_info.name}")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Licensed Users", team_info.num_licensed_users)
        with col2:
            st.metric("Provisioned Users", team_info.num_provisioned_users)
        with col3:
            st.metric("Team ID", "Connected", delta="✓")
        
        # Test 2: User connection
        with st.spinner("Testing user connection..."):
            user_client = team_client.as_user(member_id)
            user_account = user_client.users_get_current_account()
            
        st.success(f"✅ **User Connection**: {user_account.name.display_name} ({user_account.email})")
        
        # Test 3: File operations
        st.header("📁 File Operations Test")
        
        with st.spinner("Testing file access..."):
            files_result = user_client.files_list_folder("")
            
        st.success(f"✅ **File Access**: Found {len(files_result.entries)} items in root folder")
        
        # Show file structure
        if files_result.entries:
            st.subheader("📂 Your Dropbox Structure")
            
            files_data = []
            folders_data = []
            
            for entry in files_result.entries:
                if hasattr(entry, 'size'):  # File
                    files_data.append({
                        'Name': entry.name,
                        'Size (MB)': round(entry.size / (1024*1024), 2),
                        'Modified': entry.server_modified.strftime('%Y-%m-%d %H:%M'),
                        'Path': entry.path_display
                    })
                else:  # Folder
                    folders_data.append({
                        'Name': entry.name,
                        'Type': 'Folder',
                        'Path': entry.path_display
                    })
            
            # Show folders first
            if folders_data:
                st.write("**📁 Folders:**")
                for folder in folders_data[:10]:  # Show first 10 folders
                    st.write(f"📁 `{folder['Name']}`")
                
                if len(folders_data) > 10:
                    st.write(f"... and {len(folders_data) - 10} more folders")
            
            # Show files
            if files_data:
                st.write("**📄 Files:**")
                files_df = pd.DataFrame(files_data)
                st.dataframe(files_df.head(10), use_container_width=True)
                
                if len(files_data) > 10:
                    st.write(f"... and {len(files_data) - 10} more files")
        
        # Store working clients in session state
        st.session_state.team_client = team_client
        st.session_state.user_client = user_client
        st.session_state.setup_complete = True
        
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()
    
    # Interactive file operations
    st.header("🚀 Interactive File Operations")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Browse", "⬇️ Download", "⬆️ Upload", "🔗 Share"])
    
    with tab1:
        st.subheader("📋 Browse Files and Folders")
        
        folder_path = st.text_input(
            "Folder path to browse:",
            value="",
            placeholder="/invoices or /reports",
            help="Leave empty for root folder"
        )
        
        # File type filter
        file_types = st.multiselect(
            "Filter by file type:",
            ['.xlsx', '.csv', '.pdf', '.txt', '.docx', '.png', '.jpg'],
            default=['.xlsx', '.csv']
        )
        
        if st.button("🔍 Browse Folder", type="primary"):
            try:
                browse_result = user_client.files_list_folder(folder_path)
                
                # Filter and organize results
                folders = []
                files = []
                
                for entry in browse_result.entries:
                    if hasattr(entry, 'size'):  # File
                        if not file_types or any(entry.name.lower().endswith(ext.lower()) for ext in file_types):
                            files.append({
                                'Name': entry.name,
                                'Size (MB)': round(entry.size / (1024*1024), 2),
                                'Modified': entry.server_modified.strftime('%Y-%m-%d %H:%M'),
                                'Full Path': entry.path_display
                            })
                    else:  # Folder
                        folders.append({
                            'Name': entry.name,
                            'Full Path': entry.path_display
                        })
                
                # Display results
                if folders:
                    st.write(f"**📁 Folders ({len(folders)}):**")
                    for folder in folders:
                        if st.button(f"📁 {folder['Name']}", key=f"folder_{folder['Name']}"):
                            st.info(f"Folder path: `{folder['Full Path']}`")
                
                if files:
                    st.write(f"**📄 Files ({len(files)}):**")
                    files_df = pd.DataFrame(files)
                    st.dataframe(files_df, use_container_width=True)
                    
                    # Quick download buttons for first few files
                    st.write("**Quick download:**")
                    for i, file in enumerate(files[:5]):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(f"📄 {file['Name']}")
                        with col2:
                            if st.button("⬇️", key=f"quick_download_{i}"):
                                st.session_state.quick_download_path = file['Full Path']
                
                if not folders and not files:
                    st.info("📭 Folder is empty or no files match your filter")
                    
            except Exception as e:
                st.error(f"Error browsing folder: {e}")
    
    with tab2:
        st.subheader("⬇️ Download Files")
        
        # Check for quick download
        if st.session_state.get('quick_download_path'):
            download_path = st.session_state.quick_download_path
            st.info(f"Selected file: `{download_path}`")
            del st.session_state.quick_download_path
        else:
            download_path = st.text_input(
                "Enter file path to download:",
                placeholder="/invoices/weekly_release.xlsx",
                help="Full path to the file you want to download"
            )
        
        if st.button("📥 Download File", type="primary") and download_path:
            try:
                with st.spinner(f"Downloading {os.path.basename(download_path)}..."):
                    # Download to temporary file
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        metadata, response = user_client.files_download(download_path)
                        tmp_file.write(response.content)
                        tmp_path = tmp_file.name
                
                st.success("✅ File downloaded successfully!")
                
                # Show file info
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"**File:** {metadata.name}")
                    st.info(f"**Size:** {round(metadata.size / (1024*1024), 2)} MB")
                with col2:
                    st.info(f"**Modified:** {metadata.server_modified}")
                    st.info(f"**Type:** {os.path.splitext(metadata.name)[1]}")
                
                # Offer download to computer
                with open(tmp_path, 'rb') as f:
                    st.download_button(
                        "💾 Save to Computer",
                        f.read(),
                        file_name=metadata.name,
                        type="primary"
                    )
                
                # Preview if it's a data file
                if download_path.lower().endswith(('.csv', '.xlsx')):
                    try:
                        st.subheader("👀 File Preview")
                        if download_path.lower().endswith('.csv'):
                            preview_df = pd.read_csv(tmp_path)
                        else:
                            preview_df = pd.read_excel(tmp_path)
                        
                        st.write(f"**Rows:** {len(preview_df)} | **Columns:** {len(preview_df.columns)}")
                        st.dataframe(preview_df.head(), use_container_width=True)
                        
                    except Exception as e:
                        st.warning(f"Could not preview file: {e}")
                
                # Clean up
                os.unlink(tmp_path)
                
            except Exception as e:
                st.error(f"Download failed: {e}")
    
    with tab3:
        st.subheader("⬆️ Upload Files")
        
        uploaded_file = st.file_uploader(
            "Choose file to upload:",
            type=['xlsx', 'csv', 'pdf', 'txt', 'docx', 'png', 'jpg'],
            help="Select a file from your computer to upload to Dropbox"
        )
        
        if uploaded_file:
            # Destination path
            default_path = f"/uploads/{uploaded_file.name}"
            upload_path = st.text_input(
                "Dropbox destination path:",
                value=default_path,
                help="Where to save the file in your Dropbox"
            )
            
            # Upload options
            col1, col2 = st.columns(2)
            with col1:
                overwrite = st.checkbox("Overwrite if exists", value=True)
            with col2:
                create_folder = st.checkbox("Create folder if needed", value=True)
            
            if st.button("📤 Upload File", type="primary"):
                try:
                    # Save uploaded file temporarily
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        tmp_file.write(uploaded_file.getvalue())
                        tmp_path = tmp_file.name
                    
                    with st.spinner(f"Uploading {uploaded_file.name}..."):
                        # Upload mode
                        mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
                        
                        # Upload file
                        with open(tmp_path, 'rb') as f:
                            metadata = user_client.files_upload(
                                f.read(),
                                upload_path,
                                mode=mode,
                                autorename=not overwrite
                            )
                    
                    st.success("✅ File uploaded successfully!")
                    
                    # Show upload info
                    st.info(f"**Uploaded to:** `{metadata.path_display}`")
                    st.info(f"**Size:** {round(metadata.size / (1024*1024), 2)} MB")
                    
                    # Clean up
                    os.unlink(tmp_path)
                    
                except Exception as e:
                    st.error(f"Upload failed: {e}")
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
    
    with tab4:
        st.subheader("🔗 Create Shared Links")
        
        share_path = st.text_input(
            "File path to share:",
            placeholder="/reports/weekly_summary.xlsx",
            help="Path to the file you want to create a shared link for"
        )
        
        if st.button("🔗 Create Shared Link", type="primary") and share_path:
            try:
                with st.spinner("Creating shared link..."):
                    # Create shared link
                    link_metadata = user_client.sharing_create_shared_link_with_settings(share_path)
                
                st.success("✅ Shared link created!")
                
                # Display the link
                st.code(link_metadata.url, language=None)
                
                # Copy button (JavaScript will handle the actual copying)
                st.markdown(f"""
                <button onclick="navigator.clipboard.writeText('{link_metadata.url}')">
                    📋 Copy Link
                </button>
                """, unsafe_allow_html=True)
                
                st.info("Share this link with team members or stakeholders to give them access to the file.")
                
            except Exception as e:
                st.error(f"Failed to create shared link: {e}")
    
    # Summary and next steps
    st.header("🎯 Setup Complete!")
    
    st.success("""
    🎉 **Congratulations!** Your Dropbox Business integration is fully configured and working!
    
    **What you can do now:**
    - ✅ Browse all your team files and folders
    - ✅ Download invoice files automatically
    - ✅ Upload processed reports and data
    - ✅ Create shared links for stakeholders
    - ✅ Integrate with your existing invoice processing workflows
    """)
    
    st.subheader("🚀 Next Steps for Your Security Services Operations")
    
    st.markdown("""
    **Integration with your existing systems:**
    1. **Invoice Processing**: Automatically download weekly invoice files
    2. **Report Upload**: Upload processed reports to shared folders
    3. **Team Collaboration**: Share results with Finance Ops team members
    4. **Automated Workflows**: Set up scheduled processing for your 140,000+ weekly hours
    
    **Ready to integrate with your invoice processing app?**
    """)
    
    if st.button("📊 Open Invoice Processing Integration", type="primary"):
        st.info("You can now add Dropbox integration to your existing invoice_app.py!")
        
        st.code("""
# Add this to your invoice processing workflow:
from dropbox_team_manager import DropboxTeamManager

# Initialize Dropbox
dbx = DropboxTeamManager()

# Download weekly files
dbx.download_file("/invoices/weekly_release.xlsx", "weekly_release.xlsx")
dbx.download_file("/invoices/weekly_addons.xlsx", "weekly_addons.xlsx")

# Process with your existing system
manager = InvoiceMasterManager()
manager.process_release_file("weekly_release.xlsx")
manager.process_addon_file("weekly_addons.xlsx")

# Upload results
dbx.upload_file("invoice_master.xlsx", "/processed/invoice_master_latest.xlsx")
        """, language="python")


def main():
    st.set_page_config(
        page_title="Complete Dropbox Test",
        page_icon="🎉",
        layout="wide"
    )
    
    complete_dropbox_test()


if __name__ == "__main__":
    main()
