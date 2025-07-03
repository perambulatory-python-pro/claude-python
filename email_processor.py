"""
Email Processor for Invoice Workflows
Automates email reading, filtering, and attachment processing
"""

import win32com.client
import os
import pandas as pd
from datetime import datetime, timedelta
import re
from pathlib import Path

class OutlookEmailProcessor:
    def __init__(self):
        """Initialize connection to Outlook"""
        print("Connecting to Outlook...")
        try:
            self.outlook = win32com.client.Dispatch("Outlook.Application")
            self.namespace = self.outlook.GetNamespace("MAPI")
            print("✓ Connected to Outlook successfully")
        except Exception as e:
            print(f"❌ Failed to connect to Outlook: {e}")
            raise
    
    def get_folder_by_name(self, folder_name, parent_folder=None):
        """
        Find a folder by name in Outlook
        
        Args:
            folder_name: Name of the folder to find
            parent_folder: Parent folder to search in (None = search all)
        
        Returns:
            Outlook folder object or None
        """
        try:
            if parent_folder is None:
                # Search in default store (main mailbox)
                parent_folder = self.namespace.GetDefaultFolder(6).Parent  # Inbox parent
            
            # Search through folders
            for folder in parent_folder.Folders:
                if folder.Name.lower() == folder_name.lower():
                    return folder
                    
                # Search subfolders recursively
                subfolder = self.get_folder_by_name(folder_name, folder)
                if subfolder:
                    return subfolder
            
            return None
        
        except Exception as e:
            print(f"Error searching for folder '{folder_name}': {e}")
            return None
    
    def get_emails_from_folder(self, folder_name=None, days_back=7, sender_filter=None):
        """
        Get emails from a specific folder with optional filtering
        
        Args:
            folder_name: Name of folder to search (None = Inbox)
            days_back: How many days back to search
            sender_filter: Filter by sender email/domain
        
        Returns:
            List of email information dictionaries
        """
        try:
            # Get the folder
            if folder_name is None:
                folder = self.namespace.GetDefaultFolder(6)  # Inbox
                print(f"Searching in Inbox...")
            else:
                folder = self.get_folder_by_name(folder_name)
                if folder is None:
                    print(f"❌ Folder '{folder_name}' not found")
                    return []
                print(f"Searching in folder: {folder.Name}")
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            print(f"Searching for emails from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # Get emails
            emails = []
            items = folder.Items
            
            # Sort by received time (newest first)
            items.Sort("[ReceivedTime]", True)
            
            for item in items:
                try:
                    # Skip if not a mail item
                    if item.Class != 43:  # 43 = Mail item
                        continue
                    
                    # Check date range
                    received_time = item.ReceivedTime
                    if received_time < start_date:
                        break  # Since sorted by date, we can stop here
                    
                    # Apply sender filter if specified
                    if sender_filter and sender_filter.lower() not in item.SenderEmailAddress.lower():
                        continue
                    
                    # Extract email information
                    email_info = {
                        'subject': item.Subject,
                        'sender': item.SenderEmailAddress,
                        'sender_name': item.SenderName,
                        'received_time': received_time,
                        'size': item.Size,
                        'has_attachments': item.Attachments.Count > 0,
                        'attachment_count': item.Attachments.Count,
                        'body_preview': item.Body[:200] + "..." if len(item.Body) > 200 else item.Body,
                        'outlook_item': item  # Keep reference for further processing
                    }
                    
                    emails.append(email_info)
                    
                except Exception as e:
                    print(f"Error processing email: {e}")
                    continue
            
            print(f"✓ Found {len(emails)} emails matching criteria")
            return emails
            
        except Exception as e:
            print(f"❌ Error getting emails: {e}")
            return []
    
    def find_invoice_emails(self, days_back=7):
        """
        Find emails that likely contain invoice information
        
        Args:
            days_back: How many days back to search
        
        Returns:
            List of potential invoice emails
        """
        print("Searching for invoice-related emails...")
        
        # Define keywords that suggest invoice emails
        invoice_keywords = [
            'invoice', 'billing', 'statement', 'charges', 'payment',
            'weekly release', 'edi', 'validation', 'hours worked'
        ]
        
        # Get all recent emails
        all_emails = self.get_emails_from_folder(days_back=days_back)
        
        # Filter for invoice-related emails
        invoice_emails = []
        
        for email in all_emails:
            subject_lower = email['subject'].lower()
            body_lower = email['body_preview'].lower()
            
            # Check if any invoice keywords are in subject or body
            if any(keyword in subject_lower or keyword in body_lower for keyword in invoice_keywords):
                email['match_reason'] = []
                
                # Record which keywords matched
                for keyword in invoice_keywords:
                    if keyword in subject_lower:
                        email['match_reason'].append(f"Subject: '{keyword}'")
                    if keyword in body_lower:
                        email['match_reason'].append(f"Body: '{keyword}'")
                
                invoice_emails.append(email)
        
        print(f"✓ Found {len(invoice_emails)} potential invoice emails")
        return invoice_emails
    
    def extract_attachments(self, email_item, save_folder="attachments"):
        """
        Extract attachments from an email
        
        Args:
            email_item: Outlook email item
            save_folder: Folder to save attachments
        
        Returns:
            List of saved attachment file paths
        """
        try:
            # Create save folder if it doesn't exist
            Path(save_folder).mkdir(exist_ok=True)
            
            saved_files = []
            
            for attachment in email_item['outlook_item'].Attachments:
                # Clean filename
                filename = attachment.FileName
                safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
                
                # Add timestamp to prevent overwrites
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name, ext = os.path.splitext(safe_filename)
                unique_filename = f"{name}_{timestamp}{ext}"
                
                # Save attachment
                file_path = os.path.join(save_folder, unique_filename)
                attachment.SaveAsFile(file_path)
                
                saved_files.append({
                    'original_name': filename,
                    'saved_path': file_path,
                    'size': attachment.Size,
                    'type': attachment.Type
                })
                
                print(f"✓ Saved attachment: {unique_filename}")
            
            return saved_files
            
        except Exception as e:
            print(f"❌ Error extracting attachments: {e}")
            return []
    
    def create_email_summary_report(self, emails, output_file="email_summary.xlsx"):
        """
        Create an Excel report of email summary
        
        Args:
            emails: List of email dictionaries
            output_file: Output Excel file path
        """
        try:
            # Prepare data for DataFrame
            report_data = []
            
            for email in emails:
                report_data.append({
                    'Subject': email['subject'],
                    'Sender': email['sender_name'],
                    'Sender_Email': email['sender'],
                    'Received_Time': email['received_time'],
                    'Has_Attachments': email['has_attachments'],
                    'Attachment_Count': email['attachment_count'],
                    'Size_KB': round(email['size'] / 1024, 2),
                    'Body_Preview': email['body_preview'],
                    'Match_Reason': ', '.join(email.get('match_reason', []))
                })
            
            # Create DataFrame and save to Excel
            df = pd.DataFrame(report_data)
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Email_Summary', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Email_Summary']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            print(f"✓ Email summary report saved to: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error creating summary report: {e}")
            return False

def demo_email_processing():
    """Demonstrate the email processing capabilities"""
    
    print("OUTLOOK EMAIL PROCESSING DEMO")
    print("=" * 50)
    
    try:
        # Initialize processor
        processor = OutlookEmailProcessor()
        
        # Find invoice-related emails from last 7 days
        print("\n1. Searching for invoice-related emails...")
        invoice_emails = processor.find_invoice_emails(days_back=7)
        
        if invoice_emails:
            print(f"\nFound {len(invoice_emails)} invoice-related emails:")
            for i, email in enumerate(invoice_emails[:5], 1):  # Show first 5
                print(f"{i}. {email['subject']} - {email['sender_name']}")
                print(f"   Received: {email['received_time']}")
                print(f"   Attachments: {email['attachment_count']}")
                print(f"   Match reasons: {', '.join(email['match_reason'])}")
                print()
            
            # Create summary report
            print("2. Creating email summary report...")
            processor.create_email_summary_report(invoice_emails)
            
            # Ask if user wants to extract attachments
            print("3. Attachment extraction available:")
            print("   Run extract_attachments() on any email to save attachments")
            
        else:
            print("No invoice-related emails found in the last 7 days")
            
            # Show recent emails instead
            print("\nShowing recent emails instead:")
            recent_emails = processor.get_emails_from_folder(days_back=3)
            for email in recent_emails[:5]:
                print(f"- {email['subject']} from {email['sender_name']}")
        
        print("\n" + "=" * 50)
        print("✅ Email processing demo completed!")
        print("Check the 'email_summary.xlsx' file for detailed results.")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")

if __name__ == "__main__":
    demo_email_processing()
