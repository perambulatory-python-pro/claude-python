# Dropbox Team Folder Integration - Summary & Next Steps

## Summary of Completed Work

### Problem Solved
Successfully integrated Dropbox Business API to access Finance Ops Team Folders, resolving authentication and API compatibility issues.

### Key Technical Achievements

#### 1. Fixed TeamFolderMetadata Union Type Issue
- **Problem**: Dropbox SDK returns union types for team folder responses
- **Solution**: Used `is_team_folder_metadata()` and `get_team_folder_metadata()` methods to properly handle the response
- **Result**: Can now correctly retrieve team folder information

#### 2. Resolved Folder Content Display Error
- **Problem**: 'FolderMetadata' object is not subscriptable
- **Solution**: Used `isinstance()` checks instead of dictionary access for Dropbox SDK objects
- **Result**: Properly displays files and folders with metadata

#### 3. Implemented Refresh Token Authentication
- **Problem**: Access tokens expire after 4 hours
- **Solution**: Implemented OAuth2 flow with refresh tokens
- **Configuration**:
  ```toml
  [dropbox]
  app_key = "your_app_key"
  app_secret = "your_app_secret"
  refresh_token = "your_refresh_token"
  team_member_id = "your_team_member_id"
  ```
- **Result**: Permanent authentication without token expiration

#### 4. Team Folder Access with Namespace Context
- **Problem**: Team folders require special namespace handling
- **Solution**: Used `with_path_root()` to set namespace context for team folders
- **Result**: Can browse and access team folder contents correctly

### Current Capabilities
- ✅ List and browse team folders
- ✅ Navigate folder hierarchy
- ✅ Display file/folder metadata (size, modified date)
- ✅ Persistent authentication with refresh tokens
- ✅ Proper error handling and user feedback

## Next Steps: Invoice App Integration

### Phase 1: Add Dropbox Client to Invoice App

#### 1.1 Create Shared Dropbox Module
Create `dropbox_client.py` to share between apps:

```python
# dropbox_client.py
import dropbox
import streamlit as st
from typing import List, Dict, Optional, Tuple
import pandas as pd
from io import BytesIO

class DropboxClient:
    """Shared Dropbox client for invoice processing apps"""
    
    def __init__(self):
        self.team_dbx = None
        self.user_dbx = None
        self.team_folder_id = None
        self.team_folder_path = None
    
    def connect(self) -> bool:
        """Initialize Dropbox connection with refresh token"""
        # Implementation from team_folder_manager.py
        
    def set_team_folder(self, folder_id: str, folder_path: str):
        """Set the active team folder for operations"""
        self.team_folder_id = folder_id
        self.team_folder_path = folder_path
    
    def list_files(self, path: str = "", file_extension: str = None) -> List[Dict]:
        """List files in team folder, optionally filtered by extension"""
        # Return list of file metadata dictionaries
        
    def download_file(self, file_path: str) -> BytesIO:
        """Download file from Dropbox to memory"""
        # Return BytesIO object for pandas to read
        
    def upload_file(self, file_data: BytesIO, destination_path: str) -> bool:
        """Upload file from memory to Dropbox"""
        # Upload and return success status
        
    def read_excel_file(self, file_path: str) -> pd.DataFrame:
        """Read Excel file directly from Dropbox into DataFrame"""
        file_data = self.download_file(file_path)
        return pd.read_excel(file_data)
    
    def save_excel_file(self, df: pd.DataFrame, destination_path: str) -> bool:
        """Save DataFrame as Excel file to Dropbox"""
        buffer = BytesIO()
        df.to_excel(buffer, index=False)
        buffer.seek(0)
        return self.upload_file(buffer, destination_path)
```

#### 1.2 Update Invoice App Configuration
Add Dropbox settings to `invoice_app_auto_detect.py`:

```python
# Add to session state initialization
if 'dropbox_client' not in st.session_state:
    st.session_state.dropbox_client = None
if 'use_dropbox' not in st.session_state:
    st.session_state.use_dropbox = False
if 'dropbox_source_folder' not in st.session_state:
    st.session_state.dropbox_source_folder = "/01_Source_Files"
if 'dropbox_output_folder' not in st.session_state:
    st.session_state.dropbox_output_folder = "/02_Processed_Files"
```

### Phase 2: Add File Browser to Invoice App

#### 2.1 Create File Browser Component
Add a new tab or sidebar section for Dropbox file browsing:

```python
def dropbox_file_browser():
    """Browse and select files from Dropbox"""
    st.subheader("📁 Dropbox File Browser")
    
    if not st.session_state.dropbox_client:
        if st.button("Connect to Dropbox"):
            client = DropboxClient()
            if client.connect():
                st.session_state.dropbox_client = client
                st.success("Connected to Dropbox!")
    
    if st.session_state.dropbox_client:
        # Folder navigation
        current_path = st.text_input("Folder path:", value="/01_Source_Files")
        
        # List files
        files = st.session_state.dropbox_client.list_files(
            current_path, 
            file_extension=".xlsx"
        )
        
        # Display files with selection
        selected_files = []
        for file in files:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                if st.checkbox(file['name'], key=f"file_{file['path']}"):
                    selected_files.append(file)
            with col2:
                st.write(f"{file['size_mb']:.1f} MB")
            with col3:
                st.write(file['modified'].strftime("%Y-%m-%d"))
        
        return selected_files
```

#### 2.2 Integrate with File Upload Section
Modify the file upload section to support both local and Dropbox files:

```python
# In the file upload section
upload_method = st.radio(
    "File source:",
    ["Upload from computer", "Select from Dropbox"]
)

if upload_method == "Upload from computer":
    # Existing file upload code
    uploaded_files = st.file_uploader(...)
    
elif upload_method == "Select from Dropbox":
    # Dropbox file selection
    selected_files = dropbox_file_browser()
    
    if selected_files and st.button("Process Selected Files"):
        for file_info in selected_files:
            # Download file
            file_data = st.session_state.dropbox_client.download_file(
                file_info['path']
            )
            # Process file (existing logic)
            process_file(file_data, file_info['name'])
```

### Phase 3: Implement Read/Write Operations

#### 3.1 Reading Source Files from Dropbox
```python
def load_source_files_from_dropbox():
    """Load weekly source files from Dropbox folders"""
    client = st.session_state.dropbox_client
    
    # Define source folders
    folders = {
        'release': '/01_Source_Files/Weekly_Release',
        'addon': '/01_Source_Files/Weekly_AddOns',
        'edi': '/01_Source_Files/Weekly_EDI'
    }
    
    source_files = {}
    for file_type, folder_path in folders.items():
        files = client.list_files(folder_path, '.xlsx')
        if files:
            # Get most recent file
            latest_file = max(files, key=lambda x: x['modified'])
            source_files[file_type] = latest_file
    
    return source_files
```

#### 3.2 Writing Processed Files to Dropbox
```python
def save_to_dropbox(df: pd.DataFrame, file_type: str, original_filename: str):
    """Save processed file to Dropbox"""
    client = st.session_state.dropbox_client
    
    # Generate output path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"processed_{file_type}_{timestamp}.xlsx"
    output_path = f"/02_Processed_Files/Standardized_{file_type.title()}/{output_filename}"
    
    # Save file
    success = client.save_excel_file(df, output_path)
    
    if success:
        st.success(f"✅ Saved to Dropbox: {output_path}")
        
        # Archive original
        archive_path = f"/01_Source_Files/Archive/{original_filename}"
        # Move original file to archive
    
    return success
```

### Phase 4: Update Main Processing Workflow

#### 4.1 Add Dropbox Integration Toggle
```python
# In sidebar or settings
st.sidebar.header("💾 Storage Settings")
use_dropbox = st.sidebar.checkbox(
    "Use Dropbox for file storage",
    value=st.session_state.use_dropbox
)
st.session_state.use_dropbox = use_dropbox

if use_dropbox and not st.session_state.dropbox_client:
    if st.sidebar.button("Connect to Dropbox"):
        # Initialize connection
```

#### 4.2 Update Process All Button
```python
if st.button("🔄 Process All 3 Files", type="primary"):
    if st.session_state.use_dropbox:
        # Load from Dropbox
        source_files = load_source_files_from_dropbox()
        
        for file_type, file_info in source_files.items():
            # Download and process
            file_data = st.session_state.dropbox_client.download_file(
                file_info['path']
            )
            processed_df = process_file_type(file_data, file_type)
            
            # Save back to Dropbox
            save_to_dropbox(processed_df, file_type, file_info['name'])
    else:
        # Existing local file processing
```

### Phase 5: Advanced Features

#### 5.1 Folder Structure Validation
```python
def validate_folder_structure():
    """Ensure required folders exist in Dropbox"""
    required_folders = [
        "/01_Source_Files/Weekly_Release",
        "/01_Source_Files/Weekly_AddOns",
        "/01_Source_Files/Weekly_EDI",
        "/02_Processed_Files/Standardized_Release",
        # ... etc
    ]
    
    for folder in required_folders:
        # Check if exists, create if not
```

#### 5.2 File History and Versioning
```python
def get_file_history(file_path: str, limit: int = 10):
    """Get processing history for a file type"""
    # List files with timestamps
    # Show processing history
```

#### 5.3 Batch Operations
```python
def batch_download_for_processing(file_patterns: List[str]):
    """Download multiple files matching patterns"""
    # Download files matching patterns
    # Return file data for processing
```

## Implementation Priority

1. **Week 1**: Create `dropbox_client.py` module and test basic operations
2. **Week 2**: Add file browser to invoice app and implement read operations  
3. **Week 3**: Implement write operations and folder structure
4. **Week 4**: Add advanced features and error handling

## Benefits of Integration

- 📁 **Centralized Storage**: All files in team-accessible location
- 🔄 **Automated Workflow**: Direct processing from Dropbox folders
- 👥 **Team Collaboration**: Multiple users can access same files
- 📊 **Version Control**: Automatic archiving of processed files
- 🔍 **Audit Trail**: Track all file operations
- 💾 **No Local Storage**: Process files directly in memory

## Technical Considerations

- **Memory Management**: Use streaming for large files
- **Error Handling**: Implement retry logic for network issues
- **Rate Limiting**: Respect Dropbox API limits
- **Caching**: Cache folder listings for performance
- **Security**: Never store credentials in code

## Testing Checklist

- [ ] Test file upload/download with various file sizes
- [ ] Verify error handling for network interruptions
- [ ] Test concurrent access by multiple users
- [ ] Validate file permissions and access controls
- [ ] Test with different file formats and encodings
- [ ] Verify archive and cleanup operations

## OAuth Setup Reference

### Quick Setup Script
```python
# dropbox_oauth_setup.py
import dropbox
from dropbox import DropboxOAuth2FlowNoRedirect
import webbrowser

APP_KEY = "your_app_key"
APP_SECRET = "your_app_secret"

def get_refresh_token():
    auth_flow = DropboxOAuth2FlowNoRedirect(
        APP_KEY,
        APP_SECRET,
        token_access_type='offline'
    )
    
    authorize_url = auth_flow.start()
    print(f"1. Go to: {authorize_url}")
    print("2. Click 'Allow' (you might have to log in first)")
    print("3. Copy the authorization code")
    
    auth_code = input("4. Enter the authorization code here: ").strip()
    
    oauth_result = auth_flow.finish(auth_code)
    print(f"\nRefresh token: {oauth_result.refresh_token}")
```

### Required Dropbox App Permissions
- `team_info.read` - Read team information
- `team_data.member` - Access team member data
- `files.content.read` - Read file content
- `files.content.write` - Write file content
- `files.metadata.read` - Read file metadata
- `files.metadata.write` - Write file metadata
- `sharing.read` - Read sharing information

## Troubleshooting Guide

### Common Issues and Solutions

1. **"expired_access_token" Error**
   - Switch to refresh token authentication
   - Ensure token_access_type='offline' in OAuth flow

2. **"This API function operates on a single Dropbox account" Error**
   - Use DropboxTeam instead of Dropbox class
   - Specify team member with as_user()

3. **"path/not_found" Error**
   - Check namespace context with with_path_root()
   - Verify team folder is properly mounted

4. **Union Type Errors**
   - Use is_*() methods to check type
   - Use get_*() methods to retrieve values

5. **File Access Errors**
   - Verify user has access to team folder
   - Check file permissions in Dropbox admin console

## Code Snippets for Common Tasks

### Download Excel File to DataFrame
```python
def download_excel_from_dropbox(file_path: str) -> pd.DataFrame:
    """Download Excel file from Dropbox and return as DataFrame"""
    _, response = dbx.files_download(file_path)
    file_data = BytesIO(response.content)
    return pd.read_excel(file_data)
```

### Upload DataFrame to Dropbox
```python
def upload_dataframe_to_dropbox(df: pd.DataFrame, dropbox_path: str):
    """Upload DataFrame as Excel file to Dropbox"""
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    
    dbx.files_upload(
        buffer.read(),
        dropbox_path,
        mode=dropbox.files.WriteMode.overwrite
    )
```

### List Files with Filtering
```python
def list_excel_files(folder_path: str) -> List[Dict]:
    """List all Excel files in a Dropbox folder"""
    result = dbx.files_list_folder(folder_path)
    
    excel_files = []
    for entry in result.entries:
        if isinstance(entry, dropbox.files.FileMetadata) and \
           entry.name.lower().endswith(('.xlsx', '.xls')):
            excel_files.append({
                'name': entry.name,
                'path': entry.path_display,
                'size': entry.size,
                'modified': entry.client_modified
            })
    
    return excel_files
```

### Move File to Archive
```python
def archive_file(source_path: str, archive_folder: str = "/Archive"):
    """Move a file to archive folder with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.basename(source_path)
    archive_path = f"{archive_folder}/{timestamp}_{filename}"
    
    dbx.files_move_v2(source_path, archive_path)
    return archive_path
```

## Next Session Goals

1. **Create `dropbox_client.py`** - Implement the shared Dropbox client module
2. **Test Basic Operations** - Verify file list, download, and upload work correctly
3. **Update Invoice App** - Add Dropbox file browser to the UI
4. **Implement File Processing** - Read source files from Dropbox instead of local uploads
5. **Add Save to Dropbox** - Save processed files back to team folders

---

*This document serves as a comprehensive guide for integrating Dropbox Business API with the invoice processing automation system. It captures the solutions to authentication issues, provides implementation roadmaps, and includes code snippets for common operations.*