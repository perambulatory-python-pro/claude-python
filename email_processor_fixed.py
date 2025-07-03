"""
Email Processor for Invoice Workflows - FIXED VERSION
Automates email reading, filtering, and attachment processing
"""

import win32com.client
import os
import pandas as pd
from datetime import datetime, timedelta
import re
from pathlib import Path
import pytz  # For timezone handling

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
    
    def get_emails_from_folder(self, folder_name=None, days_back=7, sender_filter=None):
        """
        Get emails from a specific folder with optional filtering
        FIXED: Handles timezone-aware datetime comparisons
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
            
            # Calculate date range - FIXED timezone handling
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
                    
                    # FIXED: Handle timezone-aware datetime comparison
                    received_time = item.ReceivedTime
                    
                    # Convert Outlook datetime to Python datetime and make it timezone-naive
                    if hasattr(received_time, 'replace'):
                        # If it's already a datetime object, remove timezone info
                        received_time_naive = received_time.replace(tzinfo=None)
                    else:
                        # Convert COM datetime to Python datetime
                        received_time_naive = datetime(
                            received_time.year, received_time.month, received_time.day,
                            received_time.hour, received_time.minute, received_time.second
                        )
                    
                    # Now we can safely compare
                    if received_time_naive < start_date:
                        # Since emails are sorted by date, we can stop here
                        break
                    
                    # Apply sender filter if specified
                    if sender_filter:
                        try:
                            sender_email = getattr(item, 'SenderEmailAddress', '')
                            if sender_filter.lower() not in sender_email.lower():
                                continue
                        except:
                            continue
                    
                    # Extract email information - with error handling for each field
                    try:
                        subject = getattr(item, 'Subject', 'No Subject')
                    except:
                        subject = 'No Subject'
                    
                    try:
                        sender_email = getattr(item, 'SenderEmailAddress', 'Unknown')
                    except:
                        sender_email = 'Unknown'
                    
                    try:
                        sender_name = getattr(item, 'SenderName', 'Unknown')
                    except:
                        sender_name = 'Unknown'
                    
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
                        'sender': sender_email,
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
                    
                    # Add progress indicator for large inboxes
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
        """
        Find emails that likely contain invoice information
        """
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
    
    def create_email_summary_report(self, emails, output_file="email_summary.xlsx"):
        """
        Create an Excel report of email summary
        """
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
                        'Sender': email.get('sender_name', 'Unknown'),
                        'Sender_Email': email.get('sender', 'Unknown'),
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
    """Demonstrate the email processing capabilities - FIXED VERSION"""
    
    print("OUTLOOK EMAIL PROCESSING DEMO - FIXED VERSION")
    print("=" * 60)
    
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
                print(f"   From: {email['sender_name']} ({email['sender']})")
                print(f"   Received: {email['received_time']}")
                print(f"   Attachments: {email['attachment_count']}")
                print(f"   Size: {email['size']/1024:.1f} KB")
                print(f"   Match reasons: {', '.join(email['match_reason'])}")
            
            if len(invoice_emails) > 5:
                print(f"\n... and {len(invoice_emails) - 5} more emails")
            
            # Create summary report
            print("\n2. Creating email summary report...")
            success = processor.create_email_summary_report(invoice_emails)
            
            if success:
                print("   ✓ Check 'email_summary.xlsx' for detailed results")
            
        else:
            print("No invoice-related emails found in the last 7 days")
            
            # Show recent emails instead
            print("\nShowing some recent emails instead:")
            recent_emails = processor.get_emails_from_folder(days_back=3)
            
            if recent_emails:
                for i, email in enumerate(recent_emails[:5], 1):
                    print(f"{i}. {email['subject']} from {email['sender_name']}")
            else:
                print("No recent emails found")
        
        print("\n" + "=" * 60)
        print("✅ Email processing demo completed!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    demo_email_processing()
