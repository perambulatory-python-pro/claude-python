"""
Debug and fix team member ID format issues
"""

import streamlit as st
import dropbox
import os
from dotenv import load_dotenv
import re

load_dotenv()

def debug_team_member_id():
    st.title("🔧 Debug Team Member ID Format")
    
    token = os.getenv('DROPBOX_ACCESS_TOKEN')
    member_id = os.getenv('DROPBOX_TEAM_MEMBER_ID')
    
    if not token:
        st.error("❌ No access token found")
        return
    
    # Show current configuration
    st.subheader("📋 Current Configuration")
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**Token:** {token[:8]}...{token[-4:]}")
    with col2:
        if member_id:
            st.info(f"**Member ID:** `{member_id}`")
            st.caption(f"Length: {len(member_id)} characters")
        else:
            st.warning("**Member ID:** Not configured")
    
    # Validate member ID format
    st.subheader("🔍 Member ID Format Analysis")
    
    if member_id:
        # Check format
        st.write(f"**Raw ID:** `{member_id}`")
        st.write(f"**Length:** {len(member_id)} characters")
        st.write(f"**Starts with:** `{member_id[:10]}...`")
        st.write(f"**Ends with:** `...{member_id[-10:]}`")
        
        # Expected format validation
        if member_id.startswith('dbmid:'):
            st.success("✅ Correct prefix: `dbmid:`")
        else:
            st.error("❌ Missing `dbmid:` prefix")
        
        # Check for common issues
        issues = []
        
        if '"' in member_id:
            issues.append("Contains quotes")
        if "'" in member_id:
            issues.append("Contains single quotes")
        if ' ' in member_id:
            issues.append("Contains spaces")
        if '\n' in member_id or '\r' in member_id:
            issues.append("Contains newlines")
        if not re.match(r'^dbmid:[A-Za-z0-9_-]+$', member_id):
            issues.append("Invalid characters detected")
        
        if issues:
            st.warning(f"⚠️ **Potential issues found:** {', '.join(issues)}")
        else:
            st.success("✅ Format appears correct")
    
    # Get fresh team member list
    st.subheader("👥 Fresh Team Member List")
    
    try:
        team_client = dropbox.DropboxTeam(token)
        team_info = team_client.team_get_info()
        
        st.success(f"✅ Connected to: {team_info.name}")
        
        # Get team members
        members_result = team_client.team_members_list()
        
        st.write("**Available team members:**")
        
        for i, member in enumerate(members_result.members):
            profile = member.profile
            
            with st.container():
                # Show member info
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.write(f"**{profile.name.display_name}**")
                    st.caption(profile.email)
                
                with col2:
                    # Clean member ID display
                    clean_id = profile.team_member_id.strip()
                    st.code(clean_id, language=None)
                    
                    # Check if this matches your configured ID
                    if member_id and clean_id == member_id.strip():
                        st.success("👆 This matches your configured ID!")
                    elif member_id and clean_id.lower() == member_id.strip().lower():
                        st.warning("👆 Case mismatch with your configured ID")
                
                with col3:
                    # Test button for each member
                    if st.button(f"🧪 Test", key=f"test_{i}"):
                        test_member_id(team_client, clean_id, profile.name.display_name)
                
                st.divider()
    
    except Exception as e:
        st.error(f"❌ Failed to get team members: {e}")
    
    # Manual ID input and test
    st.subheader("🔧 Manual ID Test")
    
    manual_id = st.text_input(
        "Paste a team member ID to test:",
        value=member_id if member_id else "",
        placeholder="dbmid:AABcD3fGhIjKlMnOpQrStUvWxYz...",
        help="Copy/paste the exact ID from above"
    )
    
    if st.button("🚀 Test Manual ID") and manual_id:
        try:
            team_client = dropbox.DropboxTeam(token)
            test_member_id(team_client, manual_id.strip(), "Manual Test")
        except Exception as e:
            st.error(f"Failed to create team client: {e}")
    
    # Configuration fix helper
    st.subheader("🛠️ Fix Configuration")
    
    if st.button("📝 Generate Configuration Fix"):
        st.markdown("**If you found the correct team member ID above, update your files:**")
        
        # Show correct format for .env
        st.write("**Update your .env file:**")
        st.code(f"""
DROPBOX_ACCESS_TOKEN={token}
DROPBOX_TEAM_MEMBER_ID=paste_correct_id_here
        """, language="bash")
        
        # Show correct format for secrets.toml
        st.write("**Update your .streamlit/secrets.toml file:**")
        st.code(f"""
[dropbox]
access_token = "{token}"
team_member_id = "paste_correct_id_here"
        """, language="toml")
        
        st.info("💡 **Important:** Make sure to remove any quotes, spaces, or newlines when copying the ID!")


def test_member_id(team_client, member_id, display_name):
    """Test a specific team member ID"""
    try:
        with st.spinner(f"Testing {display_name}..."):
            # Test user client creation
            user_client = team_client.as_user(member_id)
            
            # Test getting account info
            user_account = user_client.users_get_current_account()
            
            st.success(f"✅ **{display_name}** - Connection successful!")
            st.info(f"Operating as: {user_account.name.display_name} ({user_account.email})")
            
            # Test file access
            try:
                files = user_client.files_list_folder("")
                st.success(f"📁 File access successful! Found {len(files.entries)} items.")
                
                # Show this is the working ID
                st.markdown(f"**✨ Working Team Member ID:** `{member_id}`")
                
                # Suggest updating configuration
                if st.button(f"💾 Use This ID", key=f"use_{member_id}"):
                    st.success("Copy this ID to your .env and secrets.toml files!")
                    st.code(f"DROPBOX_TEAM_MEMBER_ID={member_id}", language="bash")
                
            except Exception as e:
                st.warning(f"⚠️ User client works but file access failed: {e}")
                
    except Exception as e:
        st.error(f"❌ **{display_name}** - Failed: {e}")


def main():
    st.set_page_config(
        page_title="Debug Team Member ID",
        page_icon="🔧",
        layout="wide"
    )
    
    debug_team_member_id()


if __name__ == "__main__":
    main()
