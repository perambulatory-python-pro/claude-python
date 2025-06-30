"""
Debug script to test .msg file processing and understand the HTML structure
Run this script to diagnose issues with Kaiser payment .msg files
"""

import extract_msg
import os
from pathlib import Path

def debug_msg_file(msg_file_path):
    """
    Debug a .msg file to understand its structure and content
    """
    print(f"Debugging .msg file: {msg_file_path}")
    print("=" * 60)
    
    try:
        # Load the message
        msg = extract_msg.Message(msg_file_path)
        
        # Print basic metadata
        print(f"Subject: {msg.subject}")
        print(f"Sender: {msg.sender}")
        print(f"Date: {msg.date}")
        print()
        
        # Check HTML body
        print("HTML Body Analysis:")
        print("-" * 20)
        
        html_content = msg.htmlBody
        print(f"HTML Body Type: {type(html_content)}")
        print(f"HTML Body Length: {len(html_content) if html_content else 0}")
        
        if html_content:
            # Handle bytes vs string
            if isinstance(html_content, bytes):
                print("HTML content is bytes - attempting to decode...")
                
                # Try different encodings
                for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                    try:
                        decoded_html = html_content.decode(encoding)
                        print(f"✅ Successfully decoded with {encoding}")
                        html_content = decoded_html
                        break
                    except UnicodeDecodeError as e:
                        print(f"❌ Failed with {encoding}: {e}")
                        continue
                else:
                    print("⚠️ All encodings failed, using utf-8 with errors='replace'")
                    html_content = html_content.decode('utf-8', errors='replace')
            
            # Show preview
            print(f"\nHTML Content Preview (first 500 chars):")
            print("-" * 40)
            print(html_content[:500])
            print("...")
            
            # Test for payment detection patterns
            print(f"\nPayment Detection Tests:")
            print("-" * 25)
            
            # Check for Kaiser payment indicators
            indicators = [
                "BLACKSTONE CONSULTING INC",
                "Kaiser Permanente",
                "Payment ID",
                "Invoice ID", 
                "Net Amount",
                "Payment Date",
                "Payment Amount"
            ]
            
            html_lower = html_content.lower()
            found_indicators = []
            
            for indicator in indicators:
                if indicator.lower() in html_lower:
                    found_indicators.append(indicator)
                    print(f"✅ Found: {indicator}")
                else:
                    print(f"❌ Missing: {indicator}")
            
            print(f"\nTotal indicators found: {len(found_indicators)}/{len(indicators)}")
            
            # Check for tables
            print(f"\nTable Analysis:")
            print("-" * 15)
            
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                tables = soup.find_all('table')
                
                print(f"Total tables found: {len(tables)}")
                
                for i, table in enumerate(tables):
                    rows = table.find_all('tr')
                    print(f"  Table {i+1}: {len(rows)} rows")
                    
                    # Show first few cells of first row
                    if rows:
                        first_row = rows[0]
                        cells = first_row.find_all(['td', 'th'])
                        if cells:
                            cell_texts = [cell.get_text(strip=True) for cell in cells[:3]]
                            print(f"    First cells: {cell_texts}")
                
            except ImportError:
                print("BeautifulSoup not available for table analysis")
            except Exception as e:
                print(f"Error analyzing tables: {e}")
        
        else:
            print("No HTML content found")
            
            # Check text body as fallback
            text_content = msg.body
            if text_content:
                print(f"\nText Body Available:")
                print(f"Type: {type(text_content)}")
                print(f"Length: {len(text_content)}")
                print(f"Preview: {text_content[:200]}...")
            else:
                print("No text content either")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"Error processing .msg file: {e}")
        import traceback
        traceback.print_exc()


def test_msg_files_in_folder(folder_path):
    """
    Test all .msg files in a folder
    """
    folder = Path(folder_path)
    msg_files = list(folder.glob('*.msg'))
    
    print(f"Found {len(msg_files)} .msg files in {folder_path}")
    
    for msg_file in msg_files[:3]:  # Test first 3 files
        print(f"\n{'='*80}")
        debug_msg_file(msg_file)
        
        # Ask if user wants to continue
        response = input("\nPress Enter to continue to next file, or 'q' to quit: ")
        if response.lower() == 'q':
            break


if __name__ == "__main__":
    # Update this path to your .msg files location
    MSG_FOLDER = r"C:\Users\Brendon Jewell\Finance Ops Dropbox\Finance Ops Team Folder\DOMO Source Files\[DOMO] Accounts Receivable (AR)\Payment Notifications"
    
    print("Kaiser Payment .msg File Debugger")
    print("=" * 50)
    
    # Test specific file or folder
    choice = input("Test (f)older or specific (file)? [f/file]: ").strip().lower()
    
    if choice == 'file':
        file_path = input("Enter path to .msg file: ").strip()
        if os.path.exists(file_path):
            debug_msg_file(file_path)
        else:
            print(f"File not found: {file_path}")
    else:
        if os.path.exists(MSG_FOLDER):
            test_msg_files_in_folder(MSG_FOLDER)
        else:
            print(f"Folder not found: {MSG_FOLDER}")
            folder_path = input("Enter folder path: ").strip()
            if os.path.exists(folder_path):
                test_msg_files_in_folder(folder_path)
