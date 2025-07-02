"""
Team Folder Manager and Automated Folder Organization
Access shared team folders and create organized structure for invoice processing
"""

import streamlit as st
import dropbox
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import tempfile

load_dotenv()

class TeamFolderManager:
    """
    Enhanced manager for team folders and organized file structure
    """
    
    def __init__(self):
        self.token = os.getenv('DROPBOX_ACCESS_TOKEN')
        self.member_id = os.getenv('DROPBOX_TEAM_MEMBER_ID')
        
        if not self.token or not self.member_id:
            st.error("❌ Missing configuration")
            self.connected = False
            return
        
        try:
            self.team_client = dropbox.DropboxTeam(self.token)
            self.user_client = self.team_client.as_user(self.member_id)
            self.connected = True
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")
            self.connected = False
    
    def find_team_folders(self):
        """Find and list team folders including Finance Ops Team Folder"""
        if not self.connected:
            return []
        
        try:
            # Get shared folders
            shared_folders = self.user_client.sharing_list_folders()
            
            team_folders = []
            for folder in shared_folders.entries:
                folder_info = {
                    'name': folder.name,
                    'path': folder.path_lower,
                    'shared_folder_id': folder.shared_folder_id,
                    'access_type': str(folder.access_type) if hasattr(folder, 'access_type') else 'unknown'
                }
                team_folders.append(folder_info)
            
            return team_folders
            
        except Exception as e:
            st.error(f"Error finding team folders: {e}")
            return []
    
    def browse_team_folder(self, folder_path):
        """Browse contents of a team folder"""
        try:
            result = self.user_client.files_list_folder(folder_path)
            
            folders = []
            files = []
            
            for entry in result.entries:
                if hasattr(entry, 'size'):  # File
                    files.append({
                        'name': entry.name,
                        'path': entry.path_display,
                        'size': entry.size,
                        'modified': entry.server_modified,
                        'size_mb': round(entry.size / (1024*1024), 2)
                    })
                else:  # Folder
                    folders.append({
                        'name': entry.name,
                        'path': entry.path_display
                    })
            
            return folders, files
            
        except Exception as e:
            st.error(f"Error browsing folder {folder_path}: {e}")
            return [], []
    
    def create_invoice_folder_structure(self, base_path="/Finance Ops Team Folder"):
        """Create organized folder structure for invoice processing"""
        
        # Define the ideal folder structure for your invoice operations
        folder_structure = {
            "01_Source_Files": {
                "description": "Raw files from email and systems",
                "subfolders": [
                    "Weekly_Release_Files",
                    "Weekly_AddOn_Files", 
                    "Weekly_EDI_Files",
                    "SCR_Files",
                    "Archive"
                ]
            },
            "02_Processed_Files": {
                "description": "Cleaned and standardized files",
                "subfolders": [
                    "Standardized_Release",
                    "Standardized_AddOn",
                    "Standardized_EDI",
                    "Archive"
                ]
            },
            "03_Master_Database": {
                "description": "Master invoice database and backups",
                "subfolders": [
                    "Current",
                    "Daily_Backups",
                    "Weekly_Backups",
                    "Monthly_Backups"
                ]
            },
            "04_Reports_Analytics": {
                "description": "Generated reports and analysis",
                "subfolders": [
                    "Weekly_Reports",
                    "Monthly_Reports",
                    "Quarterly_Reports",
                    "Ad_Hoc_Analysis",
                    "Quality_Reports"
                ]
            },
            "05_Reference_Data": {
                "description": "Lookup tables and reference files",
                "subfolders": [
                    "EMID_Mappings",
                    "Building_References",
                    "Master_Lookups",
                    "Dimension_Tables"
                ]
            },
            "06_Reconciliation": {
                "description": "Reconciliation files and variance reports",
                "subfolders": [
                    "SCR_vs_Actual",
                    "Invoice_Validations",
                    "Discrepancy_Reports"
                ]
            },
            "07_Shared_Stakeholder": {
                "description": "Files shared with stakeholders",
                "subfolders": [
                    "Customer_Validation_Files",
                    "Executive_Reports",
                    "Audit_Files"
                ]
            },
            "08_Templates_Config": {
                "description": "Templates and configuration files",
                "subfolders": [
                    "Report_Templates",
                    "Config_Files",
                    "Documentation"
                ]
            }
        }
        
        return folder_structure
    
    def create_folders(self, base_path, folder_structure):
        """Actually create the folder structure in Dropbox"""
        created_folders = []
        errors = []
        
        try:
            for main_folder, config in folder_structure.items():
                # Create main folder
                main_path = f"{base_path}/{main_folder}"
                
                try:
                    self.user_client.files_create_folder_v2(main_path)
                    created_folders.append(main_path)
                except dropbox.exceptions.ApiError as e:
                    if "path/conflict/folder" in str(e):
                        # Folder already exists
                        pass
                    else:
                        errors.append(f"Main folder {main_folder}: {e}")
                
                # Create subfolders
                for subfolder in config.get("subfolders", []):
                    subfolder_path = f"{main_path}/{subfolder}"
                    
                    try:
                        self.user_client.files_create_folder_v2(subfolder_path)
                        created_folders.append(subfolder_path)
                    except dropbox.exceptions.ApiError as e:
                        if "path/conflict/folder" in str(e):
                            # Folder already exists
                            pass
                        else:
                            errors.append(f"Subfolder {subfolder}: {e}")
        
        except Exception as e:
            errors.append(f"General error: {e}")
        
        return created_folders, errors


def main_team_folder_app():
    st.title("🏢 Team Folder Manager & Organization")
    st.markdown("Access your Finance Ops Team Folder and organize your invoice processing structure")
    
    # Initialize manager
    if 'team_folder_manager' not in st.session_state:
        st.session_state.team_folder_manager = TeamFolderManager()
    
    manager = st.session_state.team_folder_manager
    
    if not manager.connected:
        st.error("❌ Not connected to Dropbox")
        return
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🔍 Find Team Folders", "📁 Browse Team Folder", "🏗️ Create Structure", "📊 Integration Guide"])
    
    with tab1:
        st.subheader("🔍 Find Your Team Folders")
        
        if st.button("🔍 Search for Team Folders", type="primary"):
            with st.spinner("Searching for shared team folders..."):
                team_folders = manager.find_team_folders()
            
            if team_folders:
                st.success(f"✅ Found {len(team_folders)} shared folders!")
                
                # Display team folders
                for folder in team_folders:
                    with st.container():
                        col1, col2, col3 = st.columns([3, 2, 1])
                        
                        with col1:
                            st.write(f"**📁 {folder['name']}**")
                            st.caption(f"Path: {folder['path']}")
                        
                        with col2:
                            st.write(f"Access: {folder['access_type']}")
                        
                        with col3:
                            if st.button("📂 Browse", key=f"browse_{folder['name']}"):
                                st.session_state.selected_team_folder = folder['path']
                                st.success(f"Selected: {folder['name']}")
                        
                        # Highlight Finance Ops Team Folder
                        if "finance ops" in folder['name'].lower():
                            st.info("👆 This is your Finance Ops Team Folder!")
                        
                        st.divider()
            else:
                st.warning("⚠️ No shared team folders found")
                st.info("Make sure you have access to shared folders or check with your team admin")
    
    with tab2:
        st.subheader("📁 Browse Team Folder Contents")
        
        # Show selected folder or allow manual input
        if st.session_state.get('selected_team_folder'):
            current_folder = st.session_state.selected_team_folder
            st.success(f"📁 Selected folder: **{current_folder}**")
        else:
            current_folder = "/Finance Ops Team Folder"  # Default assumption
        
        # Folder path input
        browse_path = st.text_input(
            "Folder path to browse:",
            value=current_folder,
            placeholder="/Finance Ops Team Folder",
            help="Enter the full path to your team folder"
        )
        
        if st.button("📂 Browse Folder Contents", type="primary"):
            with st.spinner(f"Browsing {browse_path}..."):
                folders, files = manager.browse_team_folder(browse_path)
            
            # Display results
            col1, col2 = st.columns(2)
            
            with col1:
                if folders:
                    st.write(f"**📁 Folders ({len(folders)}):**")
                    for folder in folders:
                        st.write(f"📁 `{folder['name']}`")
                        if st.button(f"Enter {folder['name']}", key=f"enter_{folder['name']}"):
                            st.session_state.browse_path = folder['path']
                            st.rerun()
            
            with col2:
                if files:
                    st.write(f"**📄 Files ({len(files)}):**")
                    files_df = pd.DataFrame(files)
                    st.dataframe(
                        files_df[['name', 'size_mb', 'modified']],
                        use_container_width=True
                    )
            
            if not folders and not files:
                st.info("📭 Folder appears to be empty")
    
    with tab3:
        st.subheader("🏗️ Create Organized Folder Structure")
        
        st.markdown("""
        **Create a professional folder structure for your invoice processing operations:**
        
        This will organize your 140,000+ weekly hours of data processing into logical, 
        efficient folders that align with your invoice workflow.
        """)
        
        # Base path selection
        base_path = st.text_input(
            "Base path for folder structure:",
            value="/Finance Ops Team Folder",
            help="Where to create the organized folder structure"
        )
        
        # Show proposed structure
        st.subheader("📋 Proposed Folder Structure")
        
        structure = manager.create_invoice_folder_structure(base_path)
        
        for main_folder, config in structure.items():
            with st.expander(f"📁 {main_folder} - {config['description']}"):
                st.write(f"**Purpose:** {config['description']}")
                st.write("**Subfolders:**")
                for subfolder in config.get('subfolders', []):
                    st.write(f"  📂 {subfolder}")
        
        # Create structure button
        if st.button("🚀 Create Folder Structure", type="primary"):
            with st.spinner("Creating organized folder structure..."):
                created, errors = manager.create_folders(base_path, structure)
            
            if created:
                st.success(f"✅ Created {len(created)} folders successfully!")
                
                # Show created folders
                with st.expander("📁 Created Folders"):
                    for folder in created:
                        st.write(f"✅ {folder}")
            
            if errors:
                st.warning(f"⚠️ {len(errors)} issues encountered:")
                for error in errors:
                    st.write(f"⚠️ {error}")
            
            st.balloons()
    
    with tab4:
        st.subheader("📊 Integration with Invoice Processing")
        
        st.markdown("""
        ## 🔗 How to Integrate with Your Invoice App
        
        Now that your team folder is accessible, here's how to integrate it with your 
        existing invoice processing workflows:
        """)
        
        # Integration examples
        st.subheader("💻 Code Integration Examples")
        
        st.write("**1. Download Source Files from Team Folder:**")
        st.code("""
# Enhanced weekly processing with team folder
def automated_weekly_processing():
    # Initialize team folder manager
    manager = TeamFolderManager()
    
    # Download source files from team folder
    source_path = "/Finance Ops Team Folder/01_Source_Files"
    
    # Download weekly files
    manager.user_client.files_download_to_file(
        "weekly_release.xlsx",
        f"{source_path}/Weekly_Release_Files/latest_release.xlsx"
    )
    
    manager.user_client.files_download_to_file(
        "weekly_addons.xlsx", 
        f"{source_path}/Weekly_AddOn_Files/latest_addons.xlsx"
    )
    
    # Process with your existing system
    invoice_manager = InvoiceMasterManager()
    invoice_manager.process_release_file("weekly_release.xlsx")
    invoice_manager.process_addon_file("weekly_addons.xlsx")
    
    # Upload results to team folder
    results_path = "/Finance Ops Team Folder/03_Master_Database/Current"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    manager.user_client.files_upload_to(
        f"{results_path}/invoice_master_{timestamp}.xlsx",
        "invoice_master.xlsx"
    )
        """, language="python")
        
        st.write("**2. Automated Backup Strategy:**")
        st.code("""
def create_automated_backups():
    manager = TeamFolderManager()
    today = datetime.now()
    
    # Daily backup
    daily_path = f"/Finance Ops Team Folder/03_Master_Database/Daily_Backups"
    daily_file = f"invoice_master_{today.strftime('%Y%m%d')}.xlsx"
    
    # Weekly backup (Fridays)
    if today.weekday() == 4:  # Friday
        weekly_path = f"/Finance Ops Team Folder/03_Master_Database/Weekly_Backups"
        weekly_file = f"invoice_master_week_{today.strftime('%Y_W%U')}.xlsx"
        
        # Copy to weekly backup
        manager.user_client.files_copy_v2(
            f"{daily_path}/{daily_file}",
            f"{weekly_path}/{weekly_file}"
        )
        """, language="python")
        
        st.write("**3. Stakeholder Report Distribution:**")
        st.code("""
def distribute_stakeholder_reports():
    manager = TeamFolderManager()
    
    # Generate report
    report_data = generate_weekly_summary()
    
    # Save to stakeholder folder
    stakeholder_path = "/Finance Ops Team Folder/07_Shared_Stakeholder"
    
    # Upload report
    report_file = f"weekly_summary_{datetime.now().strftime('%Y%m%d')}.xlsx"
    manager.user_client.files_upload(
        report_data,
        f"{stakeholder_path}/Executive_Reports/{report_file}"
    )
    
    # Create shared link
    shared_link = manager.user_client.sharing_create_shared_link_with_settings(
        f"{stakeholder_path}/Executive_Reports/{report_file}"
    )
    
    return shared_link.url
        """, language="python")
        
        st.subheader("🔄 Workflow Integration")
        
        workflow_steps = {
            "1. Morning Setup": "Download overnight files from 01_Source_Files",
            "2. File Processing": "Convert to standardized format → 02_Processed_Files", 
            "3. Database Update": "Update master database → 03_Master_Database/Current",
            "4. Backup Creation": "Automatic daily backup → 03_Master_Database/Daily_Backups",
            "5. Report Generation": "Create analytics → 04_Reports_Analytics/Weekly_Reports",
            "6. Stakeholder Share": "Upload to 07_Shared_Stakeholder with shared links",
            "7. Quality Checks": "Reconciliation reports → 06_Reconciliation"
        }
        
        for step, description in workflow_steps.items():
            st.write(f"**{step}:** {description}")
        
        st.subheader("📱 Quick Actions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📥 Set Up Source File Sync"):
                st.info("This would set up automatic syncing of source files from email to your organized folders")
        
        with col2:
            if st.button("📊 Configure Report Automation"):
                st.info("This would set up automatic report generation and distribution to stakeholders")
        
        # Benefits summary
        st.subheader("🎯 Benefits of This Organization")
        
        benefits = [
            "**Audit Trail**: Complete history of all file processing",
            "**Team Collaboration**: Shared access for all Finance Ops team members", 
            "**Automated Workflows**: Reduce manual file management by 80%",
            "**Stakeholder Access**: Easy sharing with customers and executives",
            "**Disaster Recovery**: Organized backups with easy restore",
            "**Scalability**: Structure grows with your 140,000+ weekly hours",
            "**Compliance**: Clear documentation and version control"
        ]
        
        for benefit in benefits:
            st.write(f"✅ {benefit}")


if __name__ == "__main__":
    st.set_page_config(
        page_title="Team Folder Manager",
        page_icon="🏢",
        layout="wide"
    )
    
    main_team_folder_app()
