"""
Simple validation test that prompts for database URL
No .env file required
"""

import os
from database_manager_compatible import CompatibleEnhancedDatabaseManager

def test_with_manual_url():
    """Test by manually providing database URL"""
    print("🔧 MANUAL DATABASE CONNECTION TEST")
    print("=" * 40)
    
    # Get database URL from user input
    print("Since .env file is missing, please provide your database URL:")
    print("Format: postgresql://username:password@host/database")
    print()
    
    database_url = input("Enter your DATABASE_URL: ").strip()
    
    if not database_url:
        print("❌ No database URL provided")
        return False
    
    try:
        # Test connection with manual URL
        print(f"\n🔄 Testing connection...")
        
        # Set environment variable temporarily
        os.environ['DATABASE_URL'] = database_url
        
        # Test connection
        db = CompatibleEnhancedDatabaseManager()
        
        if db.test_connection():
            print("✅ Database connection successful!")
            
            # Get basic stats
            stats = db.get_table_stats()
            print(f"📊 Database stats: {stats}")
            
            print(f"\n🎉 Enhanced validation system is ready!")
            print(f"💡 Create a .env file with:")
            print(f"DATABASE_URL={database_url}")
            
            return True
        else:
            print("❌ Database connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_env_file_status():
    """Check the status of .env file"""
    print("📁 CHECKING .ENV FILE STATUS")
    print("=" * 30)
    
    env_path = ".env"
    
    if os.path.exists(env_path):
        print(f"✅ .env file exists")
        
        try:
            with open(env_path, 'r') as f:
                content = f.read()
            
            if 'DATABASE_URL' in content:
                print(f"✅ DATABASE_URL found in .env file")
                
                # Show the URL (masked for security)
                lines = content.split('\n')
                for line in lines:
                    if line.startswith('DATABASE_URL'):
                        # Mask the password for security
                        if '@' in line and ':' in line:
                            parts = line.split('@')
                            if len(parts) > 1:
                                masked = parts[0].split(':')[:-1] + ['***'] + ['@'] + parts[1:]
                                print(f"   📋 URL: {':'.join(masked)}")
                        else:
                            print(f"   📋 URL: {line}")
                        break
            else:
                print(f"❌ DATABASE_URL not found in .env file")
                print(f"   💡 Add this line to your .env file:")
                print(f"   DATABASE_URL=postgresql://username:password@host/database")
        
        except Exception as e:
            print(f"❌ Error reading .env file: {e}")
    
    else:
        print(f"❌ .env file does not exist")
        print(f"   💡 Create .env file in current directory")
        print(f"   📁 Current directory: {os.getcwd()}")

if __name__ == "__main__":
    check_env_file_status()
    print()
    
    # Ask user what they want to do
    choice = input("Choose option:\n1. Test with manual URL entry\n2. Exit and fix .env file\nChoice (1 or 2): ").strip()
    
    if choice == "1":
        test_with_manual_url()
    else:
        print("\n🔧 To fix .env file:")
        print("1. Create a file named '.env' in your current directory")
        print("2. Add this line: DATABASE_URL=your_actual_database_url")
        print("3. Run the test again")
