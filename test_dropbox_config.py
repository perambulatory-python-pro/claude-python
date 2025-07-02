"""
Complete Dropbox Connection Test
Tests both .env and Streamlit secrets configurations
"""

import streamlit as st
import os
from dotenv import load_dotenv
import dropbox
from dropbox.exceptions import ApiError, AuthError

def test_dropbox_configuration():
    """
    Comprehensive test of Dropbox configuration
    """
    st.title("🧪 Dropbox Configuration Test")
    st.markdown("Let's verify your Dropbox setup is working correctly!")
    
    # Load environment variables
    load_dotenv()
    
    # Test results storage
    test_results = {
        "env_file": {"status": "❌", "details": ""},
        "streamlit_secrets": {"status": "❌", "details": ""},
        "dropbox_connection": {"status": "❌", "details": ""},
        "api_test": {"status": "❌", "details": ""}
    }
    
    st.header("📋 Configuration Check")
    
    # Test 1: .env file
    st.subheader("1. Testing .env File")
    env_token = os.getenv('DROPBOX_ACCESS_TOKEN')
    
    if env_token:
        # Mask token for security
        masked_env = env_token[:8] + "..." + env_token[-4:] if len(env_token) > 12 else "***"
        test_results["env_file"]["status"] = "✅"
        test_results["env_file"]["details"] = f"Token found: {masked_env}"
        st.success(f"✅ .env file token found: {masked_env}")
    else:
        test_results["env_file"]["details"] = "No DROPBOX_ACCESS_TOKEN found in environment"
        st.error("❌ .env file: No token found")
        st.info("Make sure your .env file contains: DROPBOX_ACCESS_TOKEN=your_token_here")
    
    # Test 2: Streamlit secrets
    st.subheader("2. Testing Streamlit Secrets")
    try:
        secrets_token = st.secrets["dropbox"]["access_token"]
        if secrets_token:
            # Mask token for security
            masked_secrets = secrets_token[:8] + "..." + secrets_token[-4:] if len(secrets_token) > 12 else "***"
            test_results["streamlit_secrets"]["status"] = "✅"
            test_results["streamlit_secrets"]["details"] = f"Token found: {masked_secrets}"
            st.success(f"✅ Streamlit secrets token found: {masked_secrets}")
        else:
            test_results["streamlit_secrets"]["details"] = "Empty token in secrets.toml"
            st.error("❌ Streamlit secrets: Empty token")
    except KeyError as e:
        test_results["streamlit_secrets"]["details"] = f"Missing key in secrets.toml: {e}"
        st.error("❌ Streamlit secrets: Configuration error")
        st.info("Check that your secrets.toml has: [dropbox] \\n access_token = \"your_token\"")
    except Exception as e:
        test_results["streamlit_secrets"]["details"] = f"Error reading secrets: {e}"
        st.error(f"❌ Streamlit secrets: {e}")
    
    # Test 3: Token comparison
    st.subheader("3. Token Comparison")
    if env_token and test_results["streamlit_secrets"]["status"] == "✅":
        secrets_token = st.secrets["dropbox"]["access_token"]
        if env_token == secrets_token:
            st.success("✅ Both tokens match perfectly!")
        else:
            st.warning("⚠️ Tokens don't match - this might cause issues")
            st.info("Make sure you copied the same token to both files")
    
    # Test 4: Dropbox connection
    st.subheader("4. Testing Dropbox Connection")
    
    # Determine which token to use
    token_to_test = env_token or (st.secrets.get("dropbox", {}).get("access_token"))
    
    if token_to_test:
        try:
            # Test connection
            dbx = dropbox.Dropbox(token_to_test)
            account_info = dbx.users_get_current_account()
            
            test_results["dropbox_connection"]["status"] = "✅"
            test_results["dropbox_connection"]["details"] = f"Connected as: {account_info.name.display_name}"
            
            st.success(f"✅ Successfully connected to Dropbox!")
            
            # Show account details
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"**Account Name:** {account_info.name.display_name}")
                st.info(f"**Email:** {account_info.email}")
            with col2:
                st.info(f"**Account Type:** {account_info.account_type}")
                st.info(f"**Country:** {account_info.country}")
            
            # Test 5: Basic API operation
            st.subheader("5. Testing API Operations")
            try:
                # List files in root directory
                result = dbx.files_list_folder("")
                file_count = len(result.entries)
                
                test_results["api_test"]["status"] = "✅"
                test_results["api_test"]["details"] = f"Found {file_count} items in root folder"
                
                st.success(f"✅ API test successful! Found {file_count} items in your Dropbox root folder.")
                
                # Show some file examples (if any)
                if result.entries:
                    st.write("**Sample files/folders:**")
                    for i, entry in enumerate(result.entries[:5]):  # Show first 5 items
                        if hasattr(entry, 'name'):
                            entry_type = "📁" if entry.__class__.__name__ == "FolderMetadata" else "📄"
                            st.write(f"{entry_type} {entry.name}")
                    
                    if len(result.entries) > 5:
                        st.write(f"... and {len(result.entries) - 5} more items")
                else:
                    st.info("Your Dropbox appears to be empty (root folder)")
                
            except ApiError as e:
                test_results["api_test"]["details"] = f"API Error: {e}"
                st.error(f"❌ API test failed: {e}")
            except Exception as e:
                test_results["api_test"]["details"] = f"Unexpected error: {e}"
                st.error(f"❌ API test failed: {e}")
                
        except AuthError as e:
            test_results["dropbox_connection"]["details"] = f"Authentication failed: {e}"
            st.error(f"❌ Authentication failed: {e}")
            st.info("Your token might be invalid or expired. Try generating a new one.")
            
        except Exception as e:
            test_results["dropbox_connection"]["details"] = f"Connection error: {e}"
            st.error(f"❌ Connection failed: {e}")
    else:
        st.error("❌ No valid token found to test connection")
    
    # Summary
    st.header("📊 Test Summary")
    
    all_passed = all(result["status"] == "✅" for result in test_results.values())
    
    if all_passed:
        st.success("🎉 All tests passed! Your Dropbox integration is ready to use!")
        st.balloons()
    else:
        st.warning("⚠️ Some tests failed. See details below:")
    
    # Results table
    import pandas as pd
    
    results_df = pd.DataFrame([
        {"Test": "Environment File (.env)", "Status": test_results["env_file"]["status"], "Details": test_results["env_file"]["details"]},
        {"Test": "Streamlit Secrets", "Status": test_results["streamlit_secrets"]["status"], "Details": test_results["streamlit_secrets"]["details"]},
        {"Test": "Dropbox Connection", "Status": test_results["dropbox_connection"]["status"], "Details": test_results["dropbox_connection"]["details"]},
        {"Test": "API Operations", "Status": test_results["api_test"]["status"], "Details": test_results["api_test"]["details"]}
    ])
    
    st.dataframe(results_df, use_container_width=True)
    
    # Next steps
    st.header("🚀 Next Steps")
    
    if all_passed:
        st.markdown("""
        **Your Dropbox integration is working perfectly! You can now:**
        
        1. **Start using the Dropbox features** in your invoice processing app
        2. **Upload/download files** programmatically
        3. **Sync folders** between local and Dropbox
        4. **Automate your weekly workflows**
        
        Try running your main Streamlit app with Dropbox integration!
        """)
        
        # Show sample code
        st.subheader("💻 Sample Usage")
        st.code("""
# Example: List your invoice files
from dropbox_manager import DropboxManager

dbx = DropboxManager()
files = dbx.list_files("/invoices", ['.xlsx', '.csv'])
print(f"Found {len(files)} invoice files")

# Example: Download a file
local_path = dbx.download_file("/invoices/weekly_release.xlsx")
if local_path:
    print(f"Downloaded to: {local_path}")
        """, language="python")
        
    else:
        st.markdown("""
        **To fix the issues:**
        
        1. **Check your token** - Make sure it's valid and hasn't expired
        2. **Verify file locations** - Ensure .env and secrets.toml are in the right places
        3. **Check file contents** - Make sure there are no extra spaces or quotes
        4. **Restart your app** - Environment changes sometimes need a restart
        
        **Need a new token?**
        1. Go to [Dropbox App Console](https://www.dropbox.com/developers/apps)
        2. Select your app
        3. Generate a new access token
        4. Update both your .env and secrets.toml files
        """)
    
    return all_passed, test_results


def show_configuration_examples():
    """
    Show examples of correct configuration files
    """
    st.header("📝 Configuration Examples")
    
    st.subheader("Example .env file:")
    st.code("""
# .env file (in your project root)
DROPBOX_ACCESS_TOKEN=sl.B1234567890abcdefghijklmnopqrstuvwxyz123456789
    """, language="bash")
    
    st.subheader("Example .streamlit/secrets.toml file:")
    st.code("""
# .streamlit/secrets.toml
[dropbox]
access_token = "sl.B1234567890abcdefghijklmnopqrstuvwxyz123456789"

[paths]
invoice_folder = "/invoices"
reports_folder = "/reports"
scr_folder = "/scr"

[settings]
max_file_size_mb = 150
    """, language="toml")


def main():
    st.set_page_config(
        page_title="Dropbox Configuration Test",
        page_icon="🧪",
        layout="wide"
    )
    
    # Navigation
    tab1, tab2 = st.tabs(["🧪 Run Tests", "📝 Configuration Help"])
    
    with tab1:
        success, results = test_dropbox_configuration()
        
    with tab2:
        show_configuration_examples()
        
        st.subheader("📁 File Structure Check")
        
        if st.button("🔍 Check My Project Structure"):
            st.markdown("**Your project should look like this:**")
            st.code("""
your_project/
├── .env                          # Your environment variables
├── .gitignore                    # Git ignore file (protects secrets)
├── .streamlit/
│   └── secrets.toml             # Streamlit configuration
├── scripts/
│   ├── invoice_app.py           # Your existing scripts
│   └── other_files.py
├── requirements.txt             # Python dependencies
└── main_app.py                  # Your main Streamlit app
            """)
            
            st.markdown("**Check that these files exist:**")
            
            import os
            
            files_to_check = [
                (".env", "Environment variables"),
                (".gitignore", "Git ignore file"),
                (".streamlit/secrets.toml", "Streamlit secrets"),
                ("requirements.txt", "Python dependencies")
            ]
            
            for file_path, description in files_to_check:
                if os.path.exists(file_path):
                    st.success(f"✅ {file_path} - {description}")
                else:
                    st.error(f"❌ {file_path} - {description} (Missing)")


if __name__ == "__main__":
    main()
