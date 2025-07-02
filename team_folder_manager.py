import streamlit as st
import dropbox
from dropbox import files, common
from dropbox.exceptions import ApiError, AuthError
import os
from datetime import datetime

class DropboxTeamManager:
    def __init__(self):
        """Initialize Dropbox Team Manager with proper team folder support"""
        self.dbx = None
        self.team_dbx = None
        self.current_user_id = None
        
    def connect(self):
        """Connect to Dropbox with team member context"""
        try:
            # Get credentials
            if hasattr(st, 'secrets'):
                token = st.secrets["dropbox"]["access_token"]
                member_id = st.secrets["dropbox"]["team_member_id"]
            else:
                token = os.getenv('DROPBOX_ACCESS_TOKEN')
                member_id = os.getenv('DROPBOX_TEAM_MEMBER_ID')
            
            if not token or not member_id:
                st.error("Missing Dropbox credentials")
                return False
            
            # Create team client
            self.team_dbx = dropbox.DropboxTeam(token)
            
            # Create user client with member context
            self.dbx = self.team_dbx.as_user(member_id)
            self.current_user_id = member_id
            
            # Test connection
            account = self.dbx.users_get_current_account()
            st.success(f"✅ Connected as: {account.name.display_name}")
            
            return True
            
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")
            return False
    
    def find_team_folders(self):
        """Find all team folders"""
        try:
            # List team folders using the team client
            team_folders_response = self.team_dbx.team_team_folder_list()
            
            folders = []
            for folder in team_folders_response.team_folders:
                folders.append({
                    'team_folder_id': folder.team_folder_id,
                    'name': folder.name,
                    'status': folder.status._tag,
                    'access_type': getattr(folder, 'access_type', {}).get('_tag', 'unknown') if hasattr(folder, 'access_type') else 'team_folder'
                })
            
            return folders
            
        except Exception as e:
            st.error(f"Error finding team folders: {str(e)}")
            return []
    
    def browse_team_folder(self, team_folder_id, path=""):
        """Browse contents of a team folder using proper namespace"""
        try:
            st.write(f"🔍 **Debug Info:** Attempting to browse team folder ID: `{team_folder_id}`")
            
            # Get team folder info first
            team_folder_info_response = self.team_dbx.team_team_folder_get_info([team_folder_id])
            if not team_folder_info_response:
                raise Exception("Team folder not found")
            
            team_folder_item = team_folder_info_response[0]
            
            # Handle the union type response
            if team_folder_item.is_team_folder_metadata():
                team_folder = team_folder_item.get_team_folder_metadata()
                st.success(f"✅ Found team folder: {team_folder.name}")
                st.write(f"Team Folder ID: {team_folder.team_folder_id}")
                st.write(f"Status: {team_folder.status}")
                
            elif team_folder_item.is_id_not_found():
                st.error(f"❌ Team folder ID not found: {team_folder_id}")
                return []
            else:
                st.error("❌ Unexpected response type")
                return []
            
            # IMPORTANT: For team folders, we need to use the namespace_id in the API call
            # Team folders are accessed differently than regular folders
            
            try:
                # Get the current user's member ID
                if hasattr(self, 'current_user_id') and self.current_user_id:
                    member_id = self.current_user_id
                else:
                    # If not set, get it from the current account
                    account = self.dbx.users_get_current_account()
                    member_id = account.team_member_id
                
                st.write(f"Using member ID: {member_id}")
                
                # Create a user client with the proper context
                user_dbx = self.team_dbx.as_user(member_id)
                
                # For team folders, we need to use the namespace ID
                # Try to list with the namespace path
                try:
                    # The path for a team folder should be namespace-relative
                    # For root of team folder, use empty string
                    list_path = path if path else ""
                    
                    st.write(f"📂 Attempting to list team folder contents at path: `{list_path}`")
                    
                    # Use with_path_root to set the namespace context
                    path_root = dropbox.common.PathRoot.namespace_id(team_folder_id)
                    user_dbx_with_ns = user_dbx.with_path_root(path_root)
                    
                    # Now list the folder with the namespace context
                    result = user_dbx_with_ns.files_list_folder(
                        path=list_path,
                        include_mounted_folders=True,
                        include_non_downloadable_files=True
                    )
                    
                    entries = result.entries
                    while result.has_more:
                        result = user_dbx_with_ns.files_list_folder_continue(result.cursor)
                        entries.extend(result.entries)
                    
                    st.success(f"✅ Found {len(entries)} items in team folder")
                    return entries
                    
                except dropbox.exceptions.PathError as e:
                    st.error(f"Path error: {str(e)}")
                    # Try alternate approach - list team folder mounts
                    st.info("Trying alternate approach: checking mounted folders...")
                    
                    # List the user's root to find where team folder is mounted
                    root_result = user_dbx.files_list_folder("", include_mounted_folders=True)
                    
                    # Look for the team folder by name
                    team_folder_mount = None
                    for entry in root_result.entries:
                        if isinstance(entry, dropbox.files.FolderMetadata):
                            # Check if this is our team folder
                            if entry.name == team_folder.name or (hasattr(entry, 'shared_folder_id') and entry.shared_folder_id == team_folder_id):
                                team_folder_mount = entry
                                st.success(f"✅ Found team folder mounted at: {entry.path_display}")
                                break
                    
                    if team_folder_mount:
                        # List the contents of the mounted team folder
                        mount_path = team_folder_mount.path_display or team_folder_mount.path_lower
                        full_path = f"{mount_path}/{path}" if path else mount_path
                        
                        result = user_dbx.files_list_folder(full_path)
                        entries = result.entries
                        while result.has_more:
                            result = user_dbx.files_list_folder_continue(result.cursor)
                            entries.extend(result.entries)
                        
                        return entries
                    else:
                        st.error("Could not find team folder mount point")
                        return []
                        
            except Exception as e:
                st.error(f"Error accessing team folder: {str(e)}")
                st.exception(e)
                return []
                
        except Exception as e:
            st.error(f"❌ Critical error: {str(e)}")
            st.exception(e)
            return []
    
    def diagnose_team_folder_access(self, team_folder_id):
        """Diagnose team folder access issues"""
        try:
            st.write("🔍 **Diagnostic Information:**")

            # Check team folder info
            team_folder_info = self.team_dbx.team_team_folder_get_info([team_folder_id])
            if team_folder_info:
                folder_item = team_folder_info[0]
                if hasattr(folder_item, 'team_folder') and folder_item.team_folder:
                    folder = folder_item.team_folder
                    st.write(f"- Team Folder Name: {folder.name}")
                    st.write(f"- Status: {folder.status._tag}")
                    st.write(f"- Access Type: {folder.access_type._tag if folder.access_type else 'unknown'}")
                else:
                    st.write("- Team folder metadata not found in response item")

                # Check if user has access
                try:
                    # Try to get the team folder's mounted path
                    st.write("- Attempting to find mounted path...")

                    # List all folders in root to see how team folder appears
                    root_folders = self.dbx.files_list_folder("", include_mounted_folders=True)
                    st.write("- Root folder contents:")
                    for entry in root_folders.entries:
                        if isinstance(entry, dropbox.files.FolderMetadata):
                            st.write(f"  📁 {entry.name} (path: {entry.path_display})")

                except Exception as diag_e:
                    st.write(f"- Root folder listing failed: {str(diag_e)}")

        except Exception as e:
            st.write(f"Diagnostic failed: {str(e)}")
    
    def create_folder_structure(self, base_path="/Finance_Ops_Invoice_Processing"):
        """Create organized folder structure for invoice processing"""
        folders_to_create = [
            f"{base_path}/01_Source_Files/Weekly_Release",
            f"{base_path}/01_Source_Files/Weekly_AddOns", 
            f"{base_path}/01_Source_Files/Weekly_EDI",
            f"{base_path}/01_Source_Files/Archive",
            f"{base_path}/02_Processed_Files/Standardized_Release",
            f"{base_path}/02_Processed_Files/Standardized_AddOns",
            f"{base_path}/02_Processed_Files/Standardized_EDI",
            f"{base_path}/03_Master_Database/Current",
            f"{base_path}/03_Master_Database/Backups",
            f"{base_path}/04_Reports/Weekly_Reports",
            f"{base_path}/04_Reports/Monthly_Reports",
            f"{base_path}/04_Reports/Quality_Reports",
            f"{base_path}/05_Reference_Data/Mapping_Tables",
            f"{base_path}/05_Reference_Data/Lookups",
            f"{base_path}/06_Logs/Processing_Logs",
            f"{base_path}/06_Logs/Error_Logs"
        ]
        
        created_folders = []
        failed_folders = []
        
        for folder_path in folders_to_create:
            try:
                self.dbx.files_create_folder_v2(folder_path)
                created_folders.append(folder_path)
                st.success(f"✅ Created: {folder_path}")
            except Exception as e:
                if "conflict" in str(e).lower():
                    st.info(f"📁 Already exists: {folder_path}")
                else:
                    failed_folders.append((folder_path, str(e)))
                    st.error(f"❌ Failed to create {folder_path}: {str(e)}")
        
        return created_folders, failed_folders

def main():
    st.title("🗂️ Team Folder Manager & Organization")
    st.markdown("Access your Finance Ops Team Folder and organize your invoice processing structure")
    
    # Navigation tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔍 Find Team Folders",
        "📂 Browse Team Folder", 
        "🏗️ Create Structure",
        "📖 Integration Guide"
    ])
    
    # Initialize manager
    if 'manager' not in st.session_state:
        st.session_state.manager = DropboxTeamManager()
        st.session_state.connected = False
    
    # Connect if not already connected
    if not st.session_state.connected:
        with st.spinner("Connecting to Dropbox Business..."):
            st.session_state.connected = st.session_state.manager.connect()
    
    if not st.session_state.connected:
        st.stop()
    
    with tab1:
        st.header("🔍 Find Your Team Folders")
        
        if st.button("Search for Team Folders", type="primary"):
            with st.spinner("Searching for team folders..."):
                team_folders = st.session_state.manager.find_team_folders()
                
                if team_folders:
                    st.success(f"Found {len(team_folders)} shared folders!")
                    
                    for folder in team_folders:
                        with st.container():
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                st.write(f"📁 **{folder['name']}**")
                                st.caption(f"Access: {folder['access_type']}, Status: {folder['status']}")
                            
                            with col2:
                                # Store folder info and switch to browse tab manually
                                browse_key = f"browse_{folder['team_folder_id']}"
                                if st.button("Browse", key=browse_key):
                                    # Store the folder selection
                                    st.session_state.selected_team_folder = folder
                                    st.session_state.browse_tab_selected = True
                                    # Force a rerun to update the page
                                    st.rerun()
                            
                            st.divider()
                    
                    # Store folders in session state
                    st.session_state.team_folders = team_folders
                else:
                    st.warning("No team folders found or access denied")
    
    with tab2:
        st.header("📂 Browse Team Folder Contents")
        
        # Check if Browse was clicked from tab 1
        if st.session_state.get('browse_tab_selected', False):
            st.info("🎯 **Folder selected from Find Team Folders tab**")
            # Clear the flag
            st.session_state.browse_tab_selected = False
        
        # Check if a team folder was selected from tab 1
        if 'selected_team_folder' in st.session_state:
            selected_folder = st.session_state.selected_team_folder
            st.success(f"🎯 **Currently selected:** {selected_folder['name']}")
        elif 'team_folders' in st.session_state and st.session_state.team_folders:
            # Manual selection
            folder_names = [f"{folder['name']}" for folder in st.session_state.team_folders]
            selected_folder_name = st.selectbox("Select team folder:", folder_names)
            
            # Find the selected folder
            selected_folder = None
            for folder in st.session_state.team_folders:
                if folder['name'] == selected_folder_name:
                    selected_folder = folder
                    break
        else:
            st.info("Please find team folders first using the 'Find Team Folders' tab")
            selected_folder = None
        
        if selected_folder:
            with st.container():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"**📁 Browsing:** {selected_folder['name']}")
                    st.write(f"**🆔 Team Folder ID:** `{selected_folder['team_folder_id']}`")
                with col2:
                    # Clear selection button
                    if st.button("🔄 Clear Selection"):
                        if 'selected_team_folder' in st.session_state:
                            del st.session_state.selected_team_folder
                        if 'browse_tab_selected' in st.session_state:
                            del st.session_state.browse_tab_selected
                        st.rerun()
            
            # Path input
            folder_path = st.text_input(
                "Folder path to browse:",
                value="",
                help="Leave empty for root, or enter path like '/subfolder'",
                key="team_folder_path_input"
            )
            
            if st.button("Browse Folder Contents", type="primary"):
                with st.spinner(f"Loading contents from {selected_folder['name']}..."):
                    items = st.session_state.manager.browse_team_folder(
                        selected_folder['team_folder_id'], 
                        folder_path
                    )
                    
                    if items:
                        st.success(f"🎉 Successfully found {len(items)} items!")
                        
                        # Display items in a nice table format
                        st.markdown("### 📋 Folder Contents")
                        
                        for item in items:
                            with st.container():
                                col1, col2, col3 = st.columns([3, 1, 1])
                                
                                with col1:
                                    # Check if it's a folder or file using isinstance
                                    if isinstance(item, dropbox.files.FolderMetadata):
                                        icon = "📁"
                                        item_name = item.name
                                        item_path = item.path_display or item.path_lower
                                    elif isinstance(item, dropbox.files.FileMetadata):
                                        icon = "📄"
                                        item_name = item.name
                                        item_path = item.path_display or item.path_lower
                                    else:
                                        # Handle other metadata types (DeletedMetadata, etc.)
                                        icon = "❓"
                                        item_name = getattr(item, 'name', 'Unknown')
                                        item_path = getattr(item, 'path_display', '')
                                    
                                    st.write(f"{icon} **{item_name}**")
                                    if item_path:
                                        st.caption(f"Path: {item_path}")
                                
                                with col2:
                                    if isinstance(item, dropbox.files.FileMetadata):
                                        # Files have size attribute
                                        size_kb = item.size / 1024
                                        if size_kb > 1024:
                                            st.write(f"{size_kb/1024:.1f} MB")
                                        else:
                                            st.write(f"{size_kb:.1f} KB")
                                    elif isinstance(item, dropbox.files.FolderMetadata):
                                        st.write("📁 Folder")
                                    else:
                                        st.write("-")
                                
                                with col3:
                                    # For files, show modification date
                                    if isinstance(item, dropbox.files.FileMetadata) and hasattr(item, 'client_modified'):
                                        st.write(item.client_modified.strftime("%Y-%m-%d"))
                                    else:
                                        st.write("-")
                                
                                st.divider()
                    else:
                        st.warning("📭 No items found or access denied")
    
    with tab3:
        st.header("🏗️ Create Invoice Processing Structure")
        st.markdown("Create an organized folder structure for your invoice processing workflow")
        
        # Base path selection
        if 'team_folders' in st.session_state and st.session_state.team_folders:
            folder_options = ["Personal Folder"] + [folder['name'] for folder in st.session_state.team_folders]
            target_folder = st.selectbox("Where to create structure:", folder_options)
            
            if target_folder == "Personal Folder":
                base_path = "/Finance_Ops_Invoice_Processing"
            else:
                # For team folders, we need to use the proper path
                selected_team_folder = None
                for folder in st.session_state.team_folders:
                    if folder['name'] == target_folder:
                        selected_team_folder = folder
                        break
                
                if selected_team_folder:
                    base_path = f"/{target_folder}/Finance_Ops_Invoice_Processing"
                else:
                    base_path = "/Finance_Ops_Invoice_Processing"
        else:
            base_path = "/Finance_Ops_Invoice_Processing"
        
        st.info(f"📍 **Structure will be created at:** `{base_path}`")
        
        # Show proposed structure
        with st.expander("📋 View Proposed Folder Structure"):
            structure = [
                "📁 01_Source_Files/",
                "   📁 Weekly_Release/",
                "   📁 Weekly_AddOns/",
                "   📁 Weekly_EDI/",
                "   📁 Archive/",
                "📁 02_Processed_Files/",
                "   📁 Standardized_Release/",
                "   📁 Standardized_AddOns/",
                "   📁 Standardized_EDI/",
                "📁 03_Master_Database/",
                "   📁 Current/",
                "   📁 Backups/",
                "📁 04_Reports/",
                "   📁 Weekly_Reports/",
                "   📁 Monthly_Reports/",
                "   📁 Quality_Reports/",
                "📁 05_Reference_Data/",
                "   📁 Mapping_Tables/",
                "   📁 Lookups/",
                "📁 06_Logs/",
                "   📁 Processing_Logs/",
                "   📁 Error_Logs/"
            ]
            
            for folder in structure:
                st.text(folder)
        
        if st.button("🚀 Create Folder Structure", type="primary"):
            with st.spinner("Creating folder structure..."):
                created, failed = st.session_state.manager.create_folder_structure(base_path)
                
                if created:
                    st.success(f"✅ Successfully created {len(created)} folders!")
                
                if failed:
                    st.warning(f"⚠️ {len(failed)} folders had issues")
                    for folder_path, error in failed:
                        st.write(f"- {folder_path}: {error}")
    
    with tab4:
        st.header("📖 Integration Guide")
        st.markdown("""
        ### Using Your Team Folder in Invoice Processing
        
        Once you've identified your team folder path, you can integrate it into your invoice processing apps:
        
        #### 1. Update Your Configuration
        Add the team folder path to your `.streamlit/secrets.toml`:
        ```toml
        [dropbox]
        access_token = "your_token"
        team_member_id = "your_member_id"
        team_folder_path = "/Finance Ops Team Folder"
        ```
        
        #### 2. Integration with Invoice App
        Your invoice processing app can now:
        - 📥 Download source files from team folder
        - 📤 Upload processed files to organized structure
        - 🔄 Sync weekly processing workflow
        - 👥 Share results with team members
        
        #### 3. Workflow Benefits
        - **Centralized Storage**: All invoice files in one team location
        - **Organized Structure**: Clear folder hierarchy for different file types
        - **Team Collaboration**: Shared access for Finance Ops team
        - **Automated Processing**: Direct integration with your Python scripts
        - **Backup & Archive**: Systematic file organization and retention
        
        #### 4. Security Features
        - Team admin controls access
        - Audit logs track all file operations
        - Version history for all processed files
        - Secure API access with team member permissions
        """)

if __name__ == "__main__":
    main()