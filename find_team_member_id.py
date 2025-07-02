"""
Simple script to find your Dropbox Business team member ID
"""

import streamlit as st
import dropbox
import os
from dotenv import load_dotenv

load_dotenv()

def find_team_member_id():
    st.title("🔍 Find Your Dropbox Team Member ID")
    
    token = os.getenv('DROPBOX_ACCESS_TOKEN')
    
    if not token:
        st.error("❌ No DROPBOX_ACCESS_TOKEN found in .env file")
        return
    
    try:
        # Connect to team
        team_client = dropbox.DropboxTeam(token)
        team_info = team_client.team_get_info()
        
        st.success(f"✅ Connected to Dropbox Business: **{team_info.name}**")
        st.info(f"📊 Licensed users: {team_info.num_licensed_users}")
        
        # Get team members
        st.subheader("👥 Team Members")
        members_result = team_client.team_members_list()
        
        if not members_result.members:
            st.warning("No team members found")
            return
        
        st.write("**Click 'Copy ID' for your account:**")
        
        for i, member in enumerate(members_result.members):
            profile = member.profile
            
            # Create a container for each member
            with st.container():
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.write(f"**{profile.name.display_name}**")
                    st.write(f"📧 {profile.email}")
                
                with col2:
                    st.code(profile.team_member_id, language=None)
                
                with col3:
                    if st.button("📋 Copy ID", key=f"copy_{i}"):
                        st.session_state[f"copied_id_{i}"] = profile.team_member_id
                        st.success("Copied!")
                
                # Show if this might be you
                if "brendon" in profile.email.lower() or "jewell" in profile.email.lower():
                    st.info("👆 This looks like it might be your account!")
                
                st.divider()
        
        # Instructions
        st.subheader("📝 What to Do Next")
        
        st.markdown("""
        1. **Find your account** in the list above (probably the one with your email)
        2. **Copy the Team Member ID** (the long string)
        3. **Add it to your configuration files**:
        """)
        
        # Show how to update files
        st.code("""
# Update your .env file:
DROPBOX_ACCESS_TOKEN=your_current_token
DROPBOX_TEAM_MEMBER_ID=paste_your_team_member_id_here
        """, language="bash")
        
        st.code("""
# Update your .streamlit/secrets.toml file:
[dropbox]
access_token = "your_current_token"
team_member_id = "paste_your_team_member_id_here"
        """, language="toml")
        
        # Test button
        st.subheader("🧪 Test a Team Member")
        
        test_member_id = st.text_input("Paste a Team Member ID to test:", placeholder="dbmid:...")
        
        if st.button("🚀 Test This Member ID") and test_member_id:
            try:
                # Test user client
                user_client = team_client.as_user(test_member_id)
                user_account = user_client.users_get_current_account()
                
                st.success(f"✅ Test successful!")
                st.info(f"**Operating as:** {user_account.name.display_name} ({user_account.email})")
                
                # Test file access
                try:
                    files = user_client.files_list_folder("")
                    st.success(f"📁 File access works! Found {len(files.entries)} items in root folder.")
                    
                    if files.entries:
                        st.write("**Sample files/folders:**")
                        for entry in files.entries[:5]:
                            icon = "📁" if hasattr(entry, 'name') and not hasattr(entry, 'size') else "📄"
                            st.write(f"{icon} {entry.name}")
                
                except Exception as e:
                    st.warning(f"⚠️ File access test failed: {e}")
                
            except Exception as e:
                st.error(f"❌ Test failed: {e}")
        
    except Exception as e:
        st.error(f"❌ Failed to connect to Dropbox Business: {e}")
        
        # Troubleshooting help
        st.subheader("🔧 Troubleshooting")
        st.markdown("""
        **If you see authentication errors:**
        1. Make sure your token is for a Dropbox Business account
        2. Check that your app has team member management permissions
        3. Verify your token hasn't expired
        
        **If you see permission errors:**
        1. Contact your Dropbox Business admin
        2. Make sure your app has the right scopes enabled
        3. You might need admin approval for team access
        """)

if __name__ == "__main__":
    find_team_member_id()
