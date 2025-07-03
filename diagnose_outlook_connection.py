import win32com.client

def diagnose_outlook_connection():
    """
    Diagnose what we're actually connecting to
    """
    try:
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        
        print("OUTLOOK CONNECTION DIAGNOSTICS")
        print("=" * 50)
        
        # Check what stores (data files) are available
        print("Available Data Stores:")
        for i, store in enumerate(outlook.Stores):
            print(f"  {i+1}. {store.DisplayName}")
            print(f"      File: {store.FilePath if hasattr(store, 'FilePath') else 'N/A'}")
            print(f"      Type: {store.ExchangeStoreType if hasattr(store, 'ExchangeStoreType') else 'Unknown'}")
            
            # Try to get the inbox for this store
            try:
                root_folder = store.GetRootFolder()
                print(f"      Root Folder: {root_folder.Name}")
                
                # Look for inbox-like folders
                for folder in root_folder.Folders:
                    if folder.Name.lower() in ['inbox', 'sent items']:
                        print(f"        📁 {folder.Name}: {folder.Items.Count} emails")
            except Exception as e:
                print(f"        Error accessing store: {e}")
            print()
        
        # Check current default folders
        print("Default Folders:")
        folder_types = {
            5: "Sent Items",
            6: "Inbox", 
            3: "Deleted Items",
            4: "Outbox",
            9: "Calendar",
            10: "Contacts",
            16: "Drafts"
        }
        
        for folder_id, folder_name in folder_types.items():
            try:
                folder = outlook.GetDefaultFolder(folder_id)
                print(f"  {folder_name}: {folder.Items.Count} items")
            except Exception as e:
                print(f"  {folder_name}: Error - {e}")
        
    except Exception as e:
        print(f"Connection error: {e}")

# Run this diagnostic
if __name__ == "__main__":
    diagnose_outlook_connection()