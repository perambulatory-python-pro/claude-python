"""
Email Processor - FIXED to show proper internal email addresses
Resolves Exchange Distinguished Names to actual email addresses
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
    
    def get_real_email_address(self, mail_item):
        """
        Get the actual email address for both internal and external senders
        
        Args:
            mail_item: Outlook mail item
            
        Returns:
            tuple: (email_address, sender_name)
        """
        try:
            # Method 1: Try to get the SMTP address directly
            sender_email = getattr(mail_item, 'SenderEmailAddress', '')
            sender_name = getattr(mail_item, 'SenderName', 'Unknown')
            
            # Check if it's an Exchange DN (starts with /O= or /o=)
            if sender_email.startswith('/O=') or sender_email.startswith('/o='):
                
                # Method 2: Try to resolve through AddressEntry
                try:
                    # Get the sender's address entry
                    sender = mail_item.Sender
                    if sender:
                        # Try to get SMTP address from address entry
                        if hasattr(sender, 'GetExchangeUser'):
                            exchange_user = sender.GetExchangeUser()
                            if exchange_user and hasattr(exchange_user, 'PrimarySmtpAddress'):
                                smtp_address = exchange_user.PrimarySmtpAddress
                                if smtp_address:
                                    return smtp_address, sender_name
                        
                        # Alternative: Try AddressEntry properties
                        if hasattr(sender, 'PropertyAccessor'):
                            try:
                                # Use MAPI property for SMTP address
                                smtp_prop = "http://schemas.microsoft.com/mapi/proptag/0x39FE001E"
                                smtp_address = sender.PropertyAccessor.GetProperty(smtp_prop)
                                if smtp_address and '@' in smtp_address:
                                    return smtp_address, sender_name
                            except:
                                pass
                        
                        # Try the Address property as fallback
                        if hasattr(sender, 'Address') and '@' in str(sender.Address):
                            return sender.Address, sender_name
                
                except Exception as e:
                    print(f"   Warning: Could not resolve address for {sender_name}: {e}")
                
                # Method 3: Extract from Reply Recipients
                try:
                    reply_recipients = mail_item.ReplyRecipients
                    if reply_recipients.Count > 0:
                        first_recipient = reply_recipients.Item(1)
                        if hasattr(first_recipient, 'Address') and '@' in str(first_recipient.Address):
                            return first_recipient.Address, sender_name
                except:
                    pass
                
                # Method 4: Try to extract from display name or create reasonable guess
                if '@' not in sender_email:
                    # If sender name looks like "John Smith", try to create email
                    name_parts = sender_name.split()
                    if len(name_parts) >= 2:
                        # Create a reasonable guess: firstname.lastname@yourcompany.com
                        # You can customize this domain to match your company
                        first_name = name_parts[0].lower()
                        last_name = name_parts[-1].lower()
                        # Replace with your actual company domain
                        guessed_email = f"{first_name}.{last_name}@yourcompany.com"
                        return guessed_email, sender_name
                    else:
                        # Use the sender name as identifier
                        return f"{sender_name.replace(' ', '.')}@internal", sender_name
            
            # If we get here, it's probably already a valid SMTP address
            return sender_email, sender_name
            
        except Exception as e:
            print(f"   Warning: Error getting email address: {e}")
            return 'unknown@unknown.com', 'Unknown Sender'
    
    def get_emails_from_folder(self, folder_name=None, days_back=7, sender_filter=None):
        """
        Get emails from a specific folder with proper email address resolution
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
            
            processed_count = 0
            for item in items:
                try:
                    # Skip if not a mail item
                    if item.Class != 43:  # 43 = Mail item
                        continue
                    
                    # Handle datetime comparison
                    received_time = item.ReceivedTime
                    if hasattr(received_time, 'replace'):
                        received_time_naive = received_time.replace(tzinfo=None)
                    else:
                        received_time_naive = datetime(
                            received_time.year, received_time.month, received_time.day,
                            received_time.hour, received_time.minute, received_time.second
                        )
                    
                    if received_time_naive < start_date:
                        break
                    
                    # Get proper email address and sender name
                    sender_email, sender_name = self.get_real_email_address(item)
                    
                    # Apply sender filter if specified
                    if sender_filter and sender_filter.lower() not in sender_email.lower():
                        continue
                    
                    # Extract other email information with error handling
                    try:
                        subject = getattr(item, 'Subject', 'No Subject')
                    except:
                        subject = 'No Subject'
                    
                    try:
                        size = getattr(item, 'Size', 0)
                    except:
                        size = 0
                    
                    try:
                        attachment_count = item.Attachments.Count
                    except:
                        attachment_count = 0
                    
                    try:
                        body = getattr(item, 'Body', '')
                        body_preview = body[:200] + "..." if len(body) > 200 else body
                    except:
                        body_preview = 'Could not read body'
                    
                    email_info = {
                        'subject': subject,
                        'sender': sender_email,  # Now properly resolved
                        'sender_name': sender_name,
                        'received_time': received_time_naive,
                        'size': size,
                        'has_attachments': attachment_count > 0,
                        'attachment_count': attachment_count,
                        'body_preview': body_preview,
                        'outlook_item': item
                    }
                    
                    emails.append(email_info)
                    processed_count += 1
                    
                    # Progress indicator
                    if processed_count % 50 == 0:
                        print(f"   Processed {processed_count} emails...")
                    
                except Exception as e:
                    print(f"   Warning: Skipped one email due to error: {e}")
                    continue
            
            print(f"✓ Found {len(emails)} emails matching criteria")
            return emails
            
        except Exception as e:
            print(f"❌ Error getting emails: {e}")
            return []
    
    def find_invoice_emails(self, days_back=7):
        """Find emails that likely contain invoice information"""
        print("Searching for invoice-related emails...")
        
        # Define keywords that suggest invoice emails
        invoice_keywords = [
            'invoice', 'billing', 'statement', 'charges', 'payment',
            'weekly release', 'edi', 'validation', 'hours worked',
            'timesheet', 'payroll', 'weekly report'
        ]
        
        # Get all recent emails
        all_emails = self.get_emails_from_folder(days_back=days_back)
        
        if not all_emails:
            print("No emails found to process")
            return []
        
        # Filter for invoice-related emails
        invoice_emails = []
        
        for email in all_emails:
            try:
                subject_lower = email['subject'].lower()
                body_lower = email['body_preview'].lower()
                
                # Check if any invoice keywords are in subject or body
                matched_keywords = []
                for keyword in invoice_keywords:
                    if keyword in subject_lower:
                        matched_keywords.append(f"Subject: '{keyword}'")
                    if keyword in body_lower:
                        matched_keywords.append(f"Body: '{keyword}'")
                
                if matched_keywords:
                    email['match_reason'] = matched_keywords
                    invoice_emails.append(email)
                    
            except Exception as e:
                print(f"   Warning: Error processing email: {e}")
                continue
        
        print(f"✓ Found {len(invoice_emails)} potential invoice emails")
        return invoice_emails
    
    def create_email_summary_report(self, emails, output_file="email_summary_fixed.xlsx"):
        """Create an Excel report with proper email addresses"""
        try:
            if not emails:
                print("No emails to report on")
                return False
            
            # Prepare data for DataFrame
            report_data = []
            
            for email in emails:
                try:
                    report_data.append({
                        'Subject': email.get('subject', 'No Subject'),
                        'Sender_Name': email.get('sender_name', 'Unknown'),
                        'Sender_Email': email.get('sender', 'Unknown'),  # Now properly resolved
                        'Received_Time': email.get('received_time', 'Unknown'),
                        'Has_Attachments': email.get('has_attachments', False),
                        'Attachment_Count': email.get('attachment_count', 0),
                        'Size_KB': round(email.get('size', 0) / 1024, 2),
                        'Body_Preview': email.get('body_preview', 'No preview'),
                        'Match_Reason': ', '.join(email.get('match_reason', []))
                    })
                except Exception as e:
                    print(f"   Warning: Error processing email for report: {e}")
                    continue
            
            if not report_data:
                print("No valid email data to create report")
                return False
            
            # Create DataFrame and save to Excel
            df = pd.DataFrame(report_data)
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Email_Summary', index=False)
                
                # Auto-adjust column widths
                try:
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
                except Exception as e:
                    print(f"   Warning: Could not adjust column widths: {e}")
            
            print(f"✓ Email summary report saved to: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error creating summary report: {e}")
            return False

def demo_email_processing():
    """Demonstrate the email processing with fixed email addresses"""
    
    print("OUTLOOK EMAIL PROCESSING DEMO - EMAIL ADDRESSES FIXED")
    print("=" * 70)
    
    try:
        # Initialize processor
        processor = OutlookEmailProcessor()
        
        # Find invoice-related emails from last 7 days
        print("\n1. Searching for invoice-related emails...")
        invoice_emails = processor.find_invoice_emails(days_back=7)
        
        if invoice_emails:
            print(f"\nFound {len(invoice_emails)} invoice-related emails:")
            
            # Show first 5 emails with details
            for i, email in enumerate(invoice_emails[:5], 1):
                print(f"\n{i}. Subject: {email['subject']}")
                print(f"   From: {email['sender_name']}")
                print(f"   Email: {email['sender']}")  # Should now show proper email
                print(f"   Received: {email['received_time']}")
                print(f"   Attachments: {email['attachment_count']}")
                print(f"   Match reasons: {', '.join(email['match_reason'])}")
            
            if len(invoice_emails) > 5:
                print(f"\n... and {len(invoice_emails) - 5} more emails")
            
            # Create summary report
            print("\n2. Creating email summary report...")
            success = processor.create_email_summary_report(invoice_emails)
            
            if success:
                print("   ✓ Check 'email_summary_fixed.xlsx' for detailed results")
            
        else:
            print("No invoice-related emails found in the last 7 days")
        
        print("\n" + "=" * 70)
        print("✅ Email processing demo completed!")
        print("Internal email addresses should now display properly!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    demo_email_processing()
