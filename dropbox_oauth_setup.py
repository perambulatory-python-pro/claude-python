# dropbox_oauth_setup.py
import dropbox
from dropbox import DropboxOAuth2FlowNoRedirect
import webbrowser

# Replace these with your app's values
APP_KEY = "nfoc06sj9cho1rw"
APP_SECRET = "ep4v52wi24ursrn"

def get_refresh_token():
    """
    Run this script once to get your refresh token for team accounts
    """
    print("=== Dropbox Team OAuth Setup ===\n")
    print("This script will help you get a refresh token for your Dropbox Business app.\n")
    
    # Create OAuth flow object with offline access
    auth_flow = DropboxOAuth2FlowNoRedirect(
        APP_KEY,
        APP_SECRET,
        token_access_type='offline'  # This requests a refresh token
    )
    
    # Get authorization URL
    authorize_url = auth_flow.start()
    
    print("1. Go to this URL in your browser:")
    print(f"\n{authorize_url}\n")
    print("2. Click 'Allow' to authorize the app")
    print("3. Copy the authorization code\n")
    
    # Open browser automatically
    webbrowser.open(authorize_url)
    
    # Get the authorization code from user
    auth_code = input("4. Paste the authorization code here: ").strip()
    
    try:
        # Exchange auth code for tokens
        oauth_result = auth_flow.finish(auth_code)
        
        print("\n✅ Success! Here are your tokens:\n")
        print(f"Access Token: {oauth_result.access_token}")
        print(f"Refresh Token: {oauth_result.refresh_token}")
        
        # For team accounts, we need to use DropboxTeam
        print("\n🧪 Testing team connection...")
        
        # Create team client
        team_dbx = dropbox.DropboxTeam(
            oauth2_refresh_token=oauth_result.refresh_token,
            app_key=APP_KEY,
            app_secret=APP_SECRET
        )
        
        # Get team information
        team_info = team_dbx.team_get_info()
        print(f"✅ Connected to team: {team_info.name}")
        print(f"Team ID: {team_info.team_id}")
        
        # List team members to find yourself
        print("\n👥 Finding your team member ID...")
        members_result = team_dbx.team_members_list(limit=100)
        
        your_member_id = None
        print("\nTeam members:")
        for i, member in enumerate(members_result.members):
            if hasattr(member, 'profile'):
                print(f"{i+1}. {member.profile.email} - {member.profile.name.display_name}")
                print(f"   Member ID: {member.profile.team_member_id}")
                print(f"   Status: {member.profile.status._tag}")
                print()
        
        # Ask user to identify themselves
        print("\n❓ Which team member are you?")
        member_choice = input("Enter your email or the number from the list above: ").strip()
        
        # Find the selected member
        selected_member = None
        if member_choice.isdigit():
            # Selected by number
            idx = int(member_choice) - 1
            if 0 <= idx < len(members_result.members):
                selected_member = members_result.members[idx]
        else:
            # Selected by email
            for member in members_result.members:
                if hasattr(member, 'profile') and member.profile.email.lower() == member_choice.lower():
                    selected_member = member
                    break
        
        if selected_member and hasattr(selected_member, 'profile'):
            your_member_id = selected_member.profile.team_member_id
            print(f"\n✅ Found you! Member ID: {your_member_id}")
            
            # Test user access
            user_dbx = team_dbx.as_user(your_member_id)
            account = user_dbx.users_get_current_account()
            print(f"✅ Verified access as: {account.name.display_name}")
            
            print("\n📝 Save these in your .streamlit/secrets.toml:")
            print(f"""
[dropbox]
app_key = "{APP_KEY}"
app_secret = "{APP_SECRET}"
refresh_token = "{oauth_result.refresh_token}"
team_member_id = "{your_member_id}"
            """)
        else:
            print("\n❌ Could not find that team member")
            print("\n📝 Save the refresh token and add your team_member_id manually:")
            print(f"""
[dropbox]
app_key = "{APP_KEY}"
app_secret = "{APP_SECRET}"
refresh_token = "{oauth_result.refresh_token}"
team_member_id = "YOUR_TEAM_MEMBER_ID_HERE"
            """)
        
    except dropbox.exceptions.BadInputError as e:
        print(f"\n❌ Team API Error: {str(e)}")
        print("\nThis error usually means you're using a team app but trying to access it as an individual.")
        print("Make sure your app has 'Team member file access' permission.")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("Make sure you copied the entire authorization code")

if __name__ == "__main__":
    get_refresh_token()