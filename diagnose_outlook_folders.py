import win32com.client

def diagnose_outlook_folders():
    """
    Show all available folders and subfolders to help find the right path
    """
    try:
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        inbox = outlook.GetDefaultFolder(6)
        
        print("OUTLOOK FOLDER STRUCTURE")
        print("=" * 50)
        print(f"Inbox: {inbox.Items.Count} emails")
        print("\nDirect subfolders of Inbox:")
        
        for folder in inbox.Folders:
            print(f"  📁 {folder.Name}: {folder.Items.Count} emails")
            
            # Check if this folder has subfolders
            try:
                if folder.Folders.Count > 0:
                    print(f"    Subfolders:")
                    for subfolder in folder.Folders:
                        print(f"      📁 {subfolder.Name}: {subfolder.Items.Count} emails")
            except:
                pass
        
        print("\n" + "="*50)
        
    except Exception as e:
        print(f"Error exploring folders: {str(e)}")

# Add this to your script and run it
if __name__ == "__main__":
    diagnose_outlook_folders()