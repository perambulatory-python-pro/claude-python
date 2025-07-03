"""
Microsoft Graph API Invoice Email Processor
Searches for Blackstone Consulting invoices and inserts data into PostgreSQL database
Uses .env file for credentials and implements resubmission logic

Requirements:
- Microsoft Graph API access configured
- .env file with credentials
- Required packages: msal, requests, pandas, psycopg2-binary, python-dotenv
"""

import requests
import json
from datetime import datetime, timezone
import re
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass
import os
from dotenv import load_dotenv

# For PostgreSQL connection
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

@dataclass
class InvoiceEmail:
    """Data structure for invoice email information"""
    message_id: str
    subject: str
    sender_email: str
    sender_name: str
    received_datetime: datetime
    to_recipients: List[str]
    cc_recipients: List[str]
    has_attachments: bool
    attachment_count: int
    pdf_attachments: List[str]
    invoice_numbers: List[str]
    body_preview: str

class GraphInvoiceProcessor:
    def __init__(self):
        """
        Initialize Graph API processor with credentials from .env file
        """
        # Load environment variables
        load_dotenv()
        
        # Get Graph API token from .env
        self.access_token = os.getenv('GRAPH_ACCESS_TOKEN')
        if not self.access_token:
            raise ValueError("GRAPH_ACCESS_TOKEN not found in .env file")
        
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        self.base_url = 'https://graph.microsoft.com/v1.0'
        
        # PostgreSQL configuration from .env
        self.pg_config = {
            'host': os.getenv('PGHOST'),
            'port': os.getenv('PGPORT', '5432'),
            'database': os.getenv('PGDATABASE'),
            'user': os.getenv('PGUSER'),
            'password': os.getenv('PGPASSWORD')
        }
        
        # Validate required PostgreSQL credentials
        required_pg_vars = ['PGHOST', 'PGDATABASE', 'PGUSER', 'PGPASSWORD']
        missing_vars = [var for var in required_pg_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required .env variables: {', '.join(missing_vars)}")
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Test database connection
        self._test_db_connection()
    
    def _test_db_connection(self):
        """Test database connection on initialization"""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    self.logger.info("✅ Database connection successful")
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def search_invoice_emails(self, 
                        mailbox: str = 'brendon@blackstone-consulting.com',
                        sender_domains: List[str] = ['blackstone-consulting.com'],
                        sender_names: List[str] = ['inessa', 'azalea.khan'],
                        subject_pattern: str = 'Non-Recurring BCI Invoices',
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        days_back: int = 30,
                        specific_folder: Optional[str] = None,
                        search_all_folders: bool = True) -> List[InvoiceEmail]:
        
        """
        Search for invoice emails using Microsoft Graph API

        Args:
            mailbox: Email address of the mailbox to search
            sender_domains: List of sender domain filters
            sender_names: List of sender name filters  
            subject_pattern: Subject line pattern to match
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional) 
            days_back: How many days back to search (used if start_date not provided)
            specific_folder: Specific folder name to search (e.g., "Inbox/1-Reference")
            search_all_folders: If True, search all folders including subfolders
            
        Returns:
            List of InvoiceEmail objects
        """

        if specific_folder:
            self.logger.info(f"Searching for invoice emails in {mailbox} - Specific folder: {specific_folder}")
        elif start_date or end_date:
            self.logger.info(f"Searching for invoice emails in {mailbox} - Date range specified")
        else:
            self.logger.info(f"Searching for invoice emails in {mailbox} from last {days_back} days...")
        
        # Build search query using OData syntax
        search_filters = []
        
        # Date filter - use provided dates or calculate from days_back
        from datetime import timedelta

        if start_date or end_date:
            # Use provided date range
            if start_date:
                start_date_iso = f"{start_date}T00:00:00Z"
                search_filters.append(f"receivedDateTime ge {start_date_iso}")
                self.logger.info(f"Using start date: {start_date}")
            
            if end_date:
                end_date_iso = f"{end_date}T23:59:59Z"
                search_filters.append(f"receivedDateTime le {end_date_iso}")
                self.logger.info(f"Using end date: {end_date}")
                
            if start_date and end_date:
                self.logger.info(f"Searching date range: {start_date} to {end_date}")
        else:
            # Only use days_back if no specific dates provided
            start_date_calc = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_filters.append(f"receivedDateTime ge {start_date_calc}")
            self.logger.info(f"Using days_back: {days_back} days")

        # Subject filter
        search_filters.append(f"contains(subject,'{subject_pattern}')")
        
        # Has attachments filter
        search_filters.append("hasAttachments eq true")
        
        # Combine filters with AND
        filter_query = " and ".join(search_filters)
        
        # Collect all invoice emails
        all_invoice_emails = []
        
        try:
            if specific_folder:
                # Search only the specified folder
                self.logger.info(f"Searching specific folder: {specific_folder}")
                folder_id = self._find_folder_by_name(mailbox, specific_folder)
                if folder_id:
                    search_url = f"{self.base_url}/users/{mailbox}/mailFolders/{folder_id}/messages"
                    all_invoice_emails = self._search_folder(search_url, filter_query, sender_domains, sender_names)
                else:
                    self.logger.error(f"Folder '{specific_folder}' not found")
                    return []
            elif search_all_folders:
                # Search all mail folders
                folders_to_search = self._get_all_mail_folders(mailbox)
                
                for folder_id, folder_name in folders_to_search:
                    self.logger.info(f"Searching folder: {folder_name}")
                    search_url = f"{self.base_url}/users/{mailbox}/mailFolders/{folder_id}/messages"
                    
                    folder_emails = self._search_folder(search_url, filter_query, sender_domains, sender_names)
                    all_invoice_emails.extend(folder_emails)
            else:
                # Search only main messages folder
                search_url = f"{self.base_url}/users/{mailbox}/messages"
                all_invoice_emails = self._search_folder(search_url, filter_query, sender_domains, sender_names)
            
            self.logger.info(f"Found {len(all_invoice_emails)} invoice emails with PDF attachments")
            return all_invoice_emails
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error searching emails: {e}")
            return []
    
    def _get_all_mail_folders(self, mailbox: str) -> List[Tuple[str, str]]:
        """
        Get all mail folders in the mailbox
        
        Returns:
            List of (folder_id, folder_name) tuples
        """
        folders = []
        
        try:
            # Get root mail folders
            url = f"{self.base_url}/users/{mailbox}/mailFolders"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            root_folders = response.json().get('value', [])
            
            for folder in root_folders:
                folder_id = folder.get('id')
                folder_name = folder.get('displayName', 'Unknown')
                folders.append((folder_id, folder_name))
                
                # Get child folders recursively
                child_folders = self._get_child_folders(mailbox, folder_id, folder_name)
                folders.extend(child_folders)
            
            self.logger.info(f"Found {len(folders)} folders to search")
            return folders
            
        except Exception as e:
            self.logger.error(f"Error getting mail folders: {e}")
            # Fallback to common folder names
            return [
                ('inbox', 'Inbox'),
                ('sentitems', 'Sent Items'),
                ('drafts', 'Drafts')
            ]
    
    def _get_child_folders(self, mailbox: str, parent_folder_id: str, parent_name: str) -> List[Tuple[str, str]]:
        """
        Recursively get child folders
        """
        child_folders = []
        
        try:
            url = f"{self.base_url}/users/{mailbox}/mailFolders/{parent_folder_id}/childFolders"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            children = response.json().get('value', [])
            
            for child in children:
                child_id = child.get('id')
                child_name = f"{parent_name}/{child.get('displayName', 'Unknown')}"
                child_folders.append((child_id, child_name))
                
                # Recursively get grandchildren
                grandchildren = self._get_child_folders(mailbox, child_id, child_name)
                child_folders.extend(grandchildren)
            
        except Exception as e:
            self.logger.error(f"Error getting child folders for {parent_name}: {e}")
        
        return child_folders
    
    def _find_folder_by_name(self, mailbox: str, folder_name: str) -> Optional[str]:
        """
        Find a folder by its display name (supports nested paths like "Inbox/1-Reference")
        
        Args:
            mailbox: Email address of the mailbox
            folder_name: Folder name or path (e.g., "Inbox/1-Reference")
            
        Returns:
            Folder ID if found, None otherwise
        """
        try:
            self.logger.info(f"Looking for folder: '{folder_name}'")
            
            # Get all folders
            all_folders = self._get_all_mail_folders(mailbox)
            self.logger.info(f"Found {len(all_folders)} total folders to check")
            
            # Look for exact match
            for folder_id, display_name in all_folders:
                self.logger.info(f"Checking folder: '{display_name}'")
                if display_name == folder_name:
                    self.logger.info(f"✅ Found exact match for '{folder_name}' with ID: {folder_id}")
                    return folder_id
            
            # If no exact match, try case-insensitive
            self.logger.info(f"No exact match found, trying case-insensitive...")
            for folder_id, display_name in all_folders:
                if display_name.lower() == folder_name.lower():
                    self.logger.info(f"✅ Found case-insensitive match for '{folder_name}' with ID: {folder_id}")
                    return folder_id
            
            # List all available folders for debugging
            self.logger.error(f"❌ Folder '{folder_name}' not found. Available folders:")
            for _, display_name in all_folders:
                self.logger.error(f"     '{display_name}'")
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding folder '{folder_name}': {e}")
            return None
    
    def _search_folder(self, search_url: str, filter_query: str, 
                      sender_domains: List[str], sender_names: List[str]) -> List[InvoiceEmail]:
        """
        Search a specific folder for invoice emails
        """
        params = {
            '$filter': filter_query,
            '$select': 'id,subject,sender,receivedDateTime,toRecipients,ccRecipients,hasAttachments,bodyPreview',
            '$expand': 'attachments($select=name,contentType,size)',
            '$orderby': 'receivedDateTime desc',
            '$top': 100  # Adjust as needed
        }
        
        try:
            response = requests.get(search_url, headers=self.headers, params=params)
            response.raise_for_status()
            
            messages = response.json().get('value', [])
            
            # Filter and process results
            invoice_emails = []
            for message in messages:
                # Additional filtering for sender
                sender_email = message.get('sender', {}).get('emailAddress', {}).get('address', '').lower()
                sender_name = message.get('sender', {}).get('emailAddress', {}).get('name', '').lower()
                
                # Check if sender matches our criteria
                sender_match = False
                for domain in sender_domains:
                    if domain.lower() in sender_email:
                        sender_match = True
                        break
                
                # Also check sender names
                if not sender_match:
                    for name in sender_names:
                        if name.lower() in sender_name or name.lower() in sender_email:
                            sender_match = True
                            break
                
                if sender_match:
                    invoice_email = self._process_message(message)
                    if invoice_email and invoice_email.pdf_attachments:
                        invoice_emails.append(invoice_email)
            
            return invoice_emails
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error searching folder: {e}")
            return []
    
    def _process_message(self, message: Dict) -> Optional[InvoiceEmail]:
        """
        Process individual message and extract invoice information
        
        Args:
            message: Message data from Graph API
            
        Returns:
            InvoiceEmail object or None if not valid
        """
        try:
            # Extract basic message info
            message_id = message.get('id', '')
            subject = message.get('subject', '')
            received_dt = datetime.fromisoformat(
                message.get('receivedDateTime', '').replace('Z', '+00:00')
            )
            
            # Sender info
            sender_info = message.get('sender', {}).get('emailAddress', {})
            sender_email = sender_info.get('address', '')
            sender_name = sender_info.get('name', '')
            
            # Recipients
            to_recipients = [
                r.get('emailAddress', {}).get('address', '') 
                for r in message.get('toRecipients', [])
            ]
            cc_recipients = [
                r.get('emailAddress', {}).get('address', '') 
                for r in message.get('ccRecipients', [])
            ]
            
            # Body preview
            body_preview = message.get('bodyPreview', '')
            
            # Process attachments
            attachments = message.get('attachments', [])
            pdf_attachments = []
            invoice_numbers = []
            
            for attachment in attachments:
                attachment_name = attachment.get('name', '')
                content_type = attachment.get('contentType', '')
                
                # Check if it's a PDF
                if (content_type == 'application/pdf' or 
                    attachment_name.lower().endswith('.pdf')):
                    
                    pdf_attachments.append(attachment_name)
                    
                    # Extract invoice number (string before underscore)
                    invoice_match = re.match(r'^([^_]+)', attachment_name)
                    if invoice_match:
                        invoice_num = invoice_match.group(1)
                        # Clean up common prefixes/suffixes
                        invoice_num = re.sub(r'\.(pdf|PDF)$', '', invoice_num)
                        invoice_numbers.append(invoice_num)
            
            # Only return if we have PDF attachments
            if pdf_attachments:
                return InvoiceEmail(
                    message_id=message_id,
                    subject=subject,
                    sender_email=sender_email,
                    sender_name=sender_name,
                    received_datetime=received_dt,
                    to_recipients=to_recipients,
                    cc_recipients=cc_recipients,
                    has_attachments=len(attachments) > 0,
                    attachment_count=len(attachments),
                    pdf_attachments=pdf_attachments,
                    invoice_numbers=invoice_numbers,
                    body_preview=body_preview
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing message {message.get('id', 'unknown')}: {e}")
            return None
    
    def check_existing_invoice(self, invoice_number: str) -> Optional[Dict]:
        """
        Check if invoice exists and return existing record details
        
        Args:
            invoice_number: Invoice number to check
            
        Returns:
            Dictionary with existing record details or None if not found
        """
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT id, emailed_date, prior_emailed_date, attachment_count, 
                               emailed_to, email_subject, emailed_by
                        FROM capital_project_invoices_emailed 
                        WHERE invoice_no = %s
                    """
                    
                    cursor.execute(query, (invoice_number,))
                    row = cursor.fetchone()
                    
                    if row:
                        return dict(row)
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error checking existing invoice {invoice_number}: {e}")
            return None
    
    def insert_or_update_invoice(self, invoice_email: InvoiceEmail, 
                               invoice_number: str, attachment_name: str) -> str:
        """
        Insert new invoice or update existing with resubmission logic
        
        Args:
            invoice_email: InvoiceEmail object
            invoice_number: Specific invoice number to process
            attachment_name: PDF attachment filename
            
        Returns:
            Status string ('inserted', 'updated', 'skipped', or 'error')
        """
        try:
            # Check if invoice already exists
            existing = self.check_existing_invoice(invoice_number)
            
            # Prepare data (convert to date only, no time)
            emailed_date = invoice_email.received_datetime.date()
            to_recipients_str = ', '.join(invoice_email.to_recipients)
            
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor() as cursor:
                    
                    if existing:
                        # Check if this is a newer submission
                        existing_date = existing['emailed_date']
                        if isinstance(existing_date, datetime):
                            existing_date = existing_date.date()
                        
                        if emailed_date > existing_date:
                            # This is a resubmission - update record
                            update_query = """
                                UPDATE capital_project_invoices_emailed 
                                SET emailed_date = %s,
                                    prior_emailed_date = %s,
                                    attachment_count = %s,
                                    emailed_to = %s,
                                    email_subject = %s,
                                    emailed_by = %s
                                WHERE invoice_no = %s
                            """
                            
                            cursor.execute(update_query, (
                                emailed_date,
                                existing_date,  # Move current date to prior_emailed_date
                                invoice_email.attachment_count,
                                to_recipients_str,
                                invoice_email.subject,
                                invoice_email.sender_name,
                                invoice_number
                            ))
                            
                            conn.commit()
                            self.logger.info(f"Updated resubmitted invoice: {invoice_number} "
                                           f"(prev: {existing_date}, new: {emailed_date})")
                            return 'updated'
                        
                        elif emailed_date == existing_date:
                            # Same date - skip
                            self.logger.info(f"Skipping duplicate invoice: {invoice_number} "
                                           f"(same date: {emailed_date})")
                            return 'skipped'
                        
                        else:
                            # Older email - skip
                            self.logger.info(f"Skipping older invoice: {invoice_number} "
                                           f"(existing: {existing_date}, this: {emailed_date})")
                            return 'skipped'
                    
                    else:
                        # New invoice - insert
                        insert_query = """
                            INSERT INTO capital_project_invoices_emailed (
                                emailed_date,
                                attachment_count,
                                invoice_no,
                                emailed_to,
                                email_subject,
                                emailed_by
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_query, (
                            emailed_date,
                            invoice_email.attachment_count,
                            invoice_number,
                            to_recipients_str,
                            invoice_email.subject,
                            invoice_email.sender_name
                        ))
                        
                        conn.commit()
                        self.logger.info(f"Inserted new invoice: {invoice_number}")
                        return 'inserted'
                        
        except Exception as e:
            self.logger.error(f"Error processing invoice {invoice_number}: {e}")
            return 'error'
    
    def process_invoice_emails(self, invoice_emails: List[InvoiceEmail]) -> Dict[str, int]:
        """
        Process all invoice emails with resubmission logic
        
        Args:
            invoice_emails: List of InvoiceEmail objects to process
            
        Returns:
            Dictionary with counts of each operation type
        """
        results = {
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        if not invoice_emails:
            return results
        
        self.logger.info(f"Processing {len(invoice_emails)} invoice emails...")
        
        for email in invoice_emails:
            # Process each invoice number in this email
            for i, invoice_num in enumerate(email.invoice_numbers):
                attachment_name = email.pdf_attachments[i] if i < len(email.pdf_attachments) else ''
                
                status = self.insert_or_update_invoice(email, invoice_num, attachment_name)
                results[status] = results.get(status, 0) + 1
        
        return results
    
    def get_database_summary(self) -> Dict:
        """
        Get summary statistics from the database
        
        Returns:
            Dictionary with database summary stats
        """
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Total records
                    cursor.execute("SELECT COUNT(*) as total FROM capital_project_invoices_emailed")
                    total = cursor.fetchone()['total']
                    
                    # Records with resubmissions
                    cursor.execute("""
                        SELECT COUNT(*) as resubmissions 
                        FROM capital_project_invoices_emailed 
                        WHERE prior_emailed_date IS NOT NULL
                    """)
                    resubmissions = cursor.fetchone()['resubmissions']
                    
                    # Recent activity (last 30 days)
                    cursor.execute("""
                        SELECT COUNT(*) as recent 
                        FROM capital_project_invoices_emailed 
                        WHERE emailed_date >= CURRENT_DATE - INTERVAL '30 days'
                    """)
                    recent = cursor.fetchone()['recent']
                    
                    # Date range
                    cursor.execute("""
                        SELECT MIN(emailed_date) as earliest, MAX(emailed_date) as latest
                        FROM capital_project_invoices_emailed
                    """)
                    date_range = cursor.fetchone()
                    
                    return {
                        'total_invoices': total,
                        'resubmissions': resubmissions,
                        'recent_30_days': recent,
                        'earliest_date': date_range['earliest'],
                        'latest_date': date_range['latest']
                    }
                    
        except Exception as e:
            self.logger.error(f"Error getting database summary: {e}")
            return {}
    
    def generate_processing_report(self, invoice_emails: List[InvoiceEmail], 
                                 results: Dict[str, int]) -> pd.DataFrame:
        """
        Generate a summary report of processing results
        
        Args:
            invoice_emails: List of processed emails
            results: Dictionary with processing results counts
            
        Returns:
            pandas DataFrame with summary data
        """
        # Create detailed data for each invoice
        detail_data = []
        
        for email in invoice_emails:
            for i, invoice_num in enumerate(email.invoice_numbers):
                attachment_name = email.pdf_attachments[i] if i < len(email.pdf_attachments) else ''
                
                # Check current status in database
                existing = self.check_existing_invoice(invoice_num)
                
                detail_data.append({
                    'Invoice_Number': invoice_num,
                    'Attachment_Name': attachment_name,
                    'Email_Subject': email.subject,
                    'Sender': email.sender_email,
                    'Received_Date': email.received_datetime.strftime('%Y-%m-%d %H:%M'),
                    'To_Recipients': ', '.join(email.to_recipients),
                    'Attachment_Count': email.attachment_count,
                    'Current_DB_Date': existing['emailed_date'] if existing else 'New',
                    'Prior_DB_Date': existing['prior_emailed_date'] if existing and existing['prior_emailed_date'] else 'None',
                    'Message_ID': email.message_id
                })
        
        detail_df = pd.DataFrame(detail_data)
        
        # Get database summary
        db_summary = self.get_database_summary()
        
        # Create summary statistics
        summary_data = [
            ['Total Emails Found', len(invoice_emails)],
            ['Total Invoice Numbers', len(detail_data)],
            ['New Invoices Inserted', results.get('inserted', 0)],
            ['Resubmissions Updated', results.get('updated', 0)],
            ['Duplicates Skipped', results.get('skipped', 0)],
            ['Processing Errors', results.get('errors', 0)],
            ['Processing Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ['--- Database Summary ---', ''],
            ['Total Invoices in DB', db_summary.get('total_invoices', 'N/A')],
            ['Total Resubmissions in DB', db_summary.get('resubmissions', 'N/A')],
            ['Recent Activity (30 days)', db_summary.get('recent_30_days', 'N/A')],
            ['Earliest Invoice Date', db_summary.get('earliest_date', 'N/A')],
            ['Latest Invoice Date', db_summary.get('latest_date', 'N/A')]
        ]
        
        # Save to Excel for easy review
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'invoice_processing_report_{timestamp}.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            detail_df.to_excel(writer, sheet_name='Invoice_Details', index=False)
            
            summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Processing_Summary', index=False)
            
            # Add resubmission analysis
            resubmissions = detail_df[detail_df['Prior_DB_Date'] != 'None']
            if not resubmissions.empty:
                resubmissions.to_excel(writer, sheet_name='Resubmissions', index=False)
        
        self.logger.info(f"Processing report saved to: {output_file}")
        return detail_df

def main():
    """
    Main execution function
    """
    try:
        # Initialize processor (loads from .env automatically)
        processor = GraphInvoiceProcessor()
        
        # Search for invoice emails
        print("🔍 Searching for Blackstone Consulting invoice emails...")
        invoice_emails = processor.search_invoice_emails(
            mailbox='brendon@blackstone-consulting.com',
            sender_domains=['blackstone-consulting.com'],
            sender_names=['inessa', 'azalea.khan'], 
            subject_pattern='Non-Recurring BCI Invoices',
            start_date='2024-10-01',  # Start date (YYYY-MM-DD)
            end_date='2025-05-31',    # End date (YYYY-MM-DD)
            specific_folder='Inbox/1-Reference',  # Specific folder to search
            search_all_folders=False  # Set to False when using specific folder
        )
        
        if not invoice_emails:
            print("❌ No invoice emails found matching criteria")
            return
        
        print(f"✅ Found {len(invoice_emails)} invoice emails")
        
        # Show summary of found emails
        total_invoices = sum(len(email.invoice_numbers) for email in invoice_emails)
        print(f"📄 Total invoice numbers extracted: {total_invoices}")
        
        # Process emails with resubmission logic
        print("💾 Processing invoices with resubmission logic...")
        results = processor.process_invoice_emails(invoice_emails)
        
        print(f"✅ Processing complete!")
        print(f"   📝 New invoices inserted: {results['inserted']}")
        print(f"   🔄 Resubmissions updated: {results['updated']}")
        print(f"   ⏭️  Duplicates skipped: {results['skipped']}")
        print(f"   ❌ Errors: {results['errors']}")
        
        # Generate report
        print("📊 Generating processing report...")
        report_df = processor.generate_processing_report(invoice_emails, results)
        
        print(f"📋 Report generated with {len(report_df)} records")
        
        if results['updated'] > 0:
            print(f"\n🔄 {results['updated']} resubmissions detected and processed!")
            print("   Previous emailed dates moved to 'prior_emailed_date' column")
        
        print("\nNext steps:")
        print("1. Review the Excel report for accuracy")
        print("2. Check database for inserted/updated records")
        print("3. Verify resubmission logic worked correctly")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Application error: {e}", exc_info=True)

if __name__ == "__main__":
    main()
