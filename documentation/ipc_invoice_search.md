# Conversation with Claude

## Human:

I have connected to my outlook via graph api! Now, I want to search my emails for messages from inessa or azalea.khan @blackstone-consulting.com, subject containing 'Non-Recurring BCI Invoices' and containing attachment(s). The PDF attachments are invoices and the invoice number is the string before the underscore delimiter. The emails are sent to IPCSecurity@kp.org, and I am copied on them.

I need to insert these records into my table in my database, capital_project_invoices_emailed. We did work on this in a prior chat, so if I need to pull more info from one of those let me know and I will. We wrote script that was able to pull the emails that are in my inbox currently and write that data, but could not access the remaining messages in a subfolder because we were not connected via graph api initially and could only access local outlook files


## Claude:

Excellent! Since you're now connected via Microsoft Graph API, we can search across all your email folders and access those emails that were previously unreachable. Let me help you build a comprehensive solution to find those Blackstone Consulting invoices and insert them into your database.


Based on your requirements, I'll create a script that:


1. Searches for emails from specific senders with the subject pattern
2. Finds emails with PDF attachments
3. Extracts invoice numbers from attachment names
4. Inserts records into your `capital_project_invoices_emailed` table


<artifacts_info>
The assistant can create and reference artifacts during conversations. Artifacts should be used for substantial, high-quality code, analysis, and writing that the user is asking the assistant to create.


You must use artifacts for


- Writing custom code to solve a specific user problem (such as building new applications, components, or tools), creating data visualizations, developing new algorithms, generating technical documents/guides that are meant to be used as reference materials.
- Content intended for eventual use outside the conversation (such as reports, emails, presentations, one-pagers, blog posts, advertisement).
- Creative writing of any length (such as stories, poems, essays, narratives, fiction, scripts, or any imaginative content).
- Structured content that users will reference, save, or follow (such as meal plans, workout routines, schedules, study guides, or any organized information meant to be used as a reference).
- Modifying/iterating on content that's already in an existing artifact.
- Content that will be edited, expanded, or reused.
- A standalone text-heavy markdown or plain text document (longer than 20 lines or 1500 characters).


Design principles for visual artifacts


When creating visual artifacts (HTML, React components, or any UI elements):


- For complex applications (Three.js, games, simulations): Prioritize functionality, performance, and user experience over visual flair. Focus on:
  - Smooth frame rates and responsive controls
  - Clear, intuitive user interfaces
  - Efficient resource usage and optimized rendering
  - Stable, bug-free interactions
  - Simple, functional design that doesn't interfere with the core experience
- For landing pages, marketing sites, and presentational content: Consider the emotional impact and "wow factor" of the design. Ask yourself: "Would this make someone stop scrolling and say 'whoa'?" Modern users expect visually engaging, interactive experiences that feel alive and dynamic.
- Default to contemporary design trends and modern aesthetic choices unless specifically asked for something traditional. Consider what's cutting-edge in current web design (dark modes, glassmorphism, micro-animations, 3D elements, bold typography, vibrant gradients).
- Static designs should be the exception, not the rule. Include thoughtful animations, hover effects, and interactive elements that make the interface feel responsive and alive. Even subtle movements can dramatically improve user engagement.
- When faced with design decisions, lean toward the bold and unexpected rather than the safe and conventional. This includes:
  - Color choices (vibrant vs muted)
  - Layout decisions (dynamic vs traditional)
  - Typography (expressive vs conservative)
  - Visual effects (immersive vs minimal)
- Push the boundaries of what's possible with the available technologies. Use advanced CSS features, complex animations, and creative JavaScript interactions. The goal is to create experiences that feel premium and cutting-edge.
- Ensure accessibility with proper contrast and semantic markup
- Create functional, working demonstrations rather than placeholders


Usage notes


- Create artifacts for text over EITHER 20 lines OR 1500 characters that meet the criteria above. Shorter text should remain in the conversation, except for creative writing which should always be in artifacts.
- For structured reference content (meal plans, workout schedules, study guides, etc.), prefer markdown artifacts as they're easily saved and referenced by users
- Strictly limit to one artifact per response - use the update mechanism for corrections
- Focus on creating complete, functional solutions
- For code artifacts: Use concise variable names (e.g., `i`, `j` for indices, `e` for event, `el` for element) to maximize content within context limits while maintaining readability


CRITICAL BROWSER STORAGE RESTRICTION


NEVER use localStorage, sessionStorage, or ANY browser storage APIs in artifacts. These APIs are NOT supported and will cause artifacts to fail in the Claude.ai environment.


Instead, you MUST:


- Use React state (useState, useReducer) for React components
- Use JavaScript variables or objects for HTML artifacts
- Store all data in memory during the session


Exception: If a user explicitly requests localStorage/sessionStorage usage, explain that these APIs are not supported in Claude.ai artifacts and will cause the artifact to fail. Offer to implement the functionality using in-memory storage instead, or suggest they copy the code to use in their own environment where browser storage is available.
</artifacts_info>


<artifacts>
<artifact identifier="graph-invoice-processor" type="application/vnd.ant.code" language="python" title="Microsoft Graph API Invoice Email Processor">
"""
Microsoft Graph API Invoice Email Processor
Searches for Blackstone Consulting invoices and inserts data into database
Requirements:


- Microsoft Graph API access configured
- Database connection (adjust connection parameters)
- Required packages: msal, requests, pandas, pyodbc/sqlalchemy
"""


import requests
import json
from datetime import datetime, timezone
import re
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass


For database connection - adjust based on your setup


import pyodbc  # or use sqlalchemy if preferred


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
def init(self, access_token: str, database_config: Dict[str, str]):
"""
Initialize Graph API processor


```
Args:
        access_token: Microsoft Graph API access token
        database_config: Database connection parameters
    """
    self.access_token = access_token
    self.headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    self.base_url = 'https://graph.microsoft.com/v1.0'
    
    # Database configuration
    self.db_config = database_config
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    self.logger = logging.getLogger(__name__)

def search_invoice_emails(self, 
                        sender_domains: List[str] = ['blackstone-consulting.com'],
                        sender_names: List[str] = ['inessa', 'azalea.khan'],
                        subject_pattern: str = 'Non-Recurring BCI Invoices',
                        days_back: int = 90) -> List[InvoiceEmail]:
    """
    Search for invoice emails using Microsoft Graph API
    
    Args:
        sender_domains: List of sender domain filters
        sender_names: List of sender name filters  
        subject_pattern: Subject line pattern to match
        days_back: How many days back to search
        
    Returns:
        List of InvoiceEmail objects
    """
    self.logger.info(f"Searching for invoice emails from last {days_back} days...")
    
    # Build search query
    # Microsoft Graph uses OData query syntax
    search_filters = []
    
    # Date filter (last N days)
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
    search_filters.append(f"receivedDateTime ge {start_date}")
    
    # Subject filter
    search_filters.append(f"contains(subject,'{subject_pattern}')")
    
    # Has attachments filter
    search_filters.append("hasAttachments eq true")
    
    # Combine filters with AND
    filter_query = " and ".join(search_filters)
    
    # Search in all folders (inbox, subfolders, etc.)
    search_url = f"{self.base_url}/me/messages"
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
        self.logger.info(f"Found {len(messages)} potential emails")
        
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
        
        self.logger.info(f"Found {len(invoice_emails)} invoice emails with PDF attachments")
        return invoice_emails
        
    except requests.exceptions.RequestException as e:
        self.logger.error(f"Error searching emails: {e}")
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

def check_existing_invoices(self, invoice_numbers: List[str]) -> List[str]:
    """
    Check which invoice numbers already exist in database
    
    Args:
        invoice_numbers: List of invoice numbers to check
        
    Returns:
        List of invoice numbers that already exist
    """
    if not invoice_numbers:
        return []
    
    try:
        # Create connection string - adjust based on your database type
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']}"
        )
        
        with pyodbc.connect(conn_str) as conn:
            # Create parameterized query
            placeholders = ','.join(['?' for _ in invoice_numbers])
            query = f"""
                SELECT DISTINCT invoice_number 
                FROM capital_project_invoices_emailed 
                WHERE invoice_number IN ({placeholders})
            """
            
            cursor = conn.cursor()
            cursor.execute(query, invoice_numbers)
            existing = [row[0] for row in cursor.fetchall()]
            
            self.logger.info(f"Found {len(existing)} existing invoices out of {len(invoice_numbers)} checked")
            return existing
            
    except Exception as e:
        self.logger.error(f"Error checking existing invoices: {e}")
        return []

def insert_invoice_records(self, invoice_emails: List[InvoiceEmail]) -> Tuple[int, int]:
    """
    Insert invoice email records into database
    
    Args:
        invoice_emails: List of InvoiceEmail objects to insert
        
    Returns:
        Tuple of (successful_inserts, skipped_duplicates)
    """
    if not invoice_emails:
        return 0, 0
    
    successful_inserts = 0
    skipped_duplicates = 0
    
    try:
        # Create connection
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']}"
        )
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            for email in invoice_emails:
                # Process each invoice number in this email
                for i, invoice_num in enumerate(email.invoice_numbers):
                    # Check if this invoice already exists
                    check_query = """
                        SELECT COUNT(*) FROM capital_project_invoices_emailed 
                        WHERE invoice_number = ?
                    """
                    cursor.execute(check_query, (invoice_num,))
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        skipped_duplicates += 1
                        self.logger.info(f"Skipping duplicate invoice: {invoice_num}")
                        continue
                    
                    # Prepare insert data
                    attachment_name = email.pdf_attachments[i] if i < len(email.pdf_attachments) else ''
                    
                    # Insert record - adjust column names based on your table schema
                    insert_query = """
                        INSERT INTO capital_project_invoices_emailed (
                            invoice_number,
                            email_subject,
                            sender_email,
                            sender_name,
                            received_date,
                            attachment_name,
                            message_id,
                            to_recipients,
                            cc_recipients,
                            body_preview,
                            date_processed
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    # Convert recipients lists to comma-separated strings
                    to_recipients_str = ', '.join(email.to_recipients)
                    cc_recipients_str = ', '.join(email.cc_recipients)
                    
                    cursor.execute(insert_query, (
                        invoice_num,
                        email.subject,
                        email.sender_email,
                        email.sender_name,
                        email.received_datetime,
                        attachment_name,
                        email.message_id,
                        to_recipients_str,
                        cc_recipients_str,
                        email.body_preview[:500],  # Limit preview length
                        datetime.now()
                    ))
                    
                    successful_inserts += 1
                    self.logger.info(f"Inserted invoice: {invoice_num}")
            
            # Commit all changes
            conn.commit()
            
    except Exception as e:
        self.logger.error(f"Error inserting records: {e}")
        
    return successful_inserts, skipped_duplicates

def generate_processing_report(self, invoice_emails: List[InvoiceEmail], 
                             inserts: int, skipped: int) -> pd.DataFrame:
    """
    Generate a summary report of processing results
    
    Args:
        invoice_emails: List of processed emails
        inserts: Number of successful inserts
        skipped: Number of skipped duplicates
        
    Returns:
        pandas DataFrame with summary data
    """
    # Create summary data
    summary_data = []
    
    for email in invoice_emails:
        for i, invoice_num in enumerate(email.invoice_numbers):
            attachment_name = email.pdf_attachments[i] if i < len(email.pdf_attachments) else ''
            
            summary_data.append({
                'Invoice_Number': invoice_num,
                'Attachment_Name': attachment_name,
                'Email_Subject': email.subject,
                'Sender': email.sender_email,
                'Received_Date': email.received_datetime.strftime('%Y-%m-%d %H:%M'),
                'Message_ID': email.message_id
            })
    
    summary_df = pd.DataFrame(summary_data)
    
    # Add processing statistics
    stats_data = [
        ['Total Emails Found', len(invoice_emails)],
        ['Total Invoice Numbers', len(summary_data)],
        ['Successful Inserts', inserts],
        ['Skipped Duplicates', skipped],
        ['Processing Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
    ]
    
    # Save to Excel for easy review
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'invoice_processing_report_{timestamp}.xlsx'
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Invoice_Details', index=False)
        
        stats_df = pd.DataFrame(stats_data, columns=['Metric', 'Value'])
        stats_df.to_excel(writer, sheet_name='Processing_Summary', index=False)
    
    self.logger.info(f"Processing report saved to: {output_file}")
    return summary_df
```


def main():
"""
Main execution function
"""
# Configuration - UPDATE THESE VALUES
ACCESS_TOKEN = "your_graph_api_access_token_here"


```
DATABASE_CONFIG = {
    'server': 'your_server_name',
    'database': 'your_database_name', 
    'username': 'your_username',
    'password': 'your_password'
}

# Initialize processor
processor = GraphInvoiceProcessor(ACCESS_TOKEN, DATABASE_CONFIG)

# Search for invoice emails
print("🔍 Searching for Blackstone Consulting invoice emails...")
invoice_emails = processor.search_invoice_emails(
    sender_domains=['blackstone-consulting.com'],
    sender_names=['inessa', 'azalea.khan'],
    subject_pattern='Non-Recurring BCI Invoices',
    days_back=90  # Adjust search window as needed
)

if not invoice_emails:
    print("❌ No invoice emails found matching criteria")
    return

print(f"✅ Found {len(invoice_emails)} invoice emails")

# Show summary of found emails
total_invoices = sum(len(email.invoice_numbers) for email in invoice_emails)
print(f"📄 Total invoice numbers extracted: {total_invoices}")

# Insert into database
print("💾 Inserting records into database...")
inserts, skipped = processor.insert_invoice_records(invoice_emails)

print(f"✅ Processing complete!")
print(f"   📝 Successful inserts: {inserts}")
print(f"   ⏭️  Skipped duplicates: {skipped}")

# Generate report
print("📊 Generating processing report...")
report_df = processor.generate_processing_report(invoice_emails, inserts, skipped)

print(f"📋 Report generated with {len(report_df)} records")
print("\nNext steps:")
print("1. Review the Excel report for accuracy")
print("2. Check database for inserted records")
print("3. Update search parameters if needed")
```


if name == "main":
main()
</artifact>
</code>
</artifacts>


This comprehensive script provides enterprise-grade email processing using Microsoft Graph API. Here's what it does and the key Python concepts involved:


Key Features


1. Microsoft Graph API Integration
  - Uses REST API calls instead of COM automation
  - Searches across all email folders (not just local cache)
  - Handles authentication with bearer tokens
2. Advanced Email Filtering
  - Searches by sender domain and name patterns
  - Filters by subject line containing specific text
  - Only processes emails with PDF attachments
  - Date range filtering for performance
3. Invoice Number Extraction
  - Uses regex to extract invoice numbers from PDF filenames
  - Handles the pattern "before underscore delimiter"
  - Cleans up common file extensions
4. Database Integration
  - Checks for existing records to avoid duplicates
  - Parameterized queries for security
  - Transaction handling for data integrity


Python Concepts Demonstrated


1. Dataclasses for Data Structure


```python
@dataclass
class InvoiceEmail:
    message_id: str
    sender_email: str
    # ... other fields
```


Why: Creates clean, type-hinted data containers without boilerplate code.


2. Regular Expressions (Regex)


```python
invoice_match = re.match(r'^([^_]+)', attachment_name)
```


Explanation: `^([^_]+)` means "from start of string, capture everything that's not an underscore"


3. List Comprehensions


```python
to_recipients = [
    r.get('emailAddress', {}).get('address', '') 
    for r in message.get('toRecipients', [])
]
```


Why: Concise way to transform data from nested dictionaries.


4. Exception Handling with Context Managers


```python
with pyodbc.connect(conn_str) as conn:
    # Database operations
```


Benefits: Automatically closes connections even if errors occur.


5. Type Hints and Return Types


```python
def search_invoice_emails(self) -> List[InvoiceEmail]:
```


Purpose: Makes code self-documenting and enables better IDE support.


Setup Required


Before running, you'll need to:


1. Get your Graph API access token (from your existing setup)
2. Update database configuration with your SQL Server details
3. Adjust table schema if your column names differ
4. Install required packages:
bash`pip install requests pandas pyodbc openpyxl msal`


Database Table Assumptions


I've assumed your `capital_project_invoices_emailed` table has these columns:


- `invoice_number`
- `email_subject`
- `sender_email`
- `sender_name`
- `received_date`
- `attachment_name`
- `message_id`
- `to_recipients`
- `cc_recipients`
- `body_preview`
- `date_processed`


Please let me know if your table schema is different!


Next Steps


1. Test the search first by commenting out the database insert section
2. Review the generated Excel report to verify data accuracy
3. Check a few records manually to ensure invoice number extraction is working correctly


Would you like me to:


1. Modify the database schema assumptions?
2. Add attachment download functionality?
3. Create a version that processes emails from specific folders?
4. Add more sophisticated invoice number pattern matching?


The script is designed to be production-ready with comprehensive logging, error handling, and reporting capabilities!


## Human:

This is my schema, please use this.

We previously built in logic that would recognize an invoice as already in the table, but would then check the date of the email. Sometimes we must resubmit invoices, so when that happens the information should then overwrite to the more recent submission, but should move the emailed_date to the prior_emailed_date so we can see that it was previously submitted.


## Claude:

Perfect! Now I can see your exact table schema. Let me update the script to match your columns and implement the resubmission logic where newer emails overwrite older ones while preserving the previous submission date.


<artifacts>
<artifact identifier="graph-invoice-processor" type="application/vnd.ant.code" language="python" title="Microsoft Graph API Invoice Email Processor - Updated for Schema">
"""
Microsoft Graph API Invoice Email Processor
Searches for Blackstone Consulting invoices and inserts data into database
Updated to match capital_project_invoices_emailed schema with resubmission logic
Requirements:


- Microsoft Graph API access configured
- Database connection (adjust connection parameters)
- Required packages: msal, requests, pandas, pyodbc/sqlalchemy
"""


import requests
import json
from datetime import datetime, timezone
import re
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass


For database connection - adjust based on your setup


import pyodbc  # or use sqlalchemy if preferred


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
def init(self, access_token: str, database_config: Dict[str, str]):
"""
Initialize Graph API processor


```
Args:
        access_token: Microsoft Graph API access token
        database_config: Database connection parameters
    """
    self.access_token = access_token
    self.headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    self.base_url = 'https://graph.microsoft.com/v1.0'
    
    # Database configuration
    self.db_config = database_config
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    self.logger = logging.getLogger(__name__)

def search_invoice_emails(self, 
                        sender_domains: List[str] = ['blackstone-consulting.com'],
                        sender_names: List[str] = ['inessa', 'azalea.khan'],
                        subject_pattern: str = 'Non-Recurring BCI Invoices',
                        days_back: int = 90) -> List[InvoiceEmail]:
    """
    Search for invoice emails using Microsoft Graph API
    
    Args:
        sender_domains: List of sender domain filters
        sender_names: List of sender name filters  
        subject_pattern: Subject line pattern to match
        days_back: How many days back to search
        
    Returns:
        List of InvoiceEmail objects
    """
    self.logger.info(f"Searching for invoice emails from last {days_back} days...")
    
    # Build search query
    # Microsoft Graph uses OData query syntax
    search_filters = []
    
    # Date filter (last N days)
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
    search_filters.append(f"receivedDateTime ge {start_date}")
    
    # Subject filter
    search_filters.append(f"contains(subject,'{subject_pattern}')")
    
    # Has attachments filter
    search_filters.append("hasAttachments eq true")
    
    # Combine filters with AND
    filter_query = " and ".join(search_filters)
    
    # Search in all folders (inbox, subfolders, etc.)
    search_url = f"{self.base_url}/me/messages"
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
        self.logger.info(f"Found {len(messages)} potential emails")
        
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
        
        self.logger.info(f"Found {len(invoice_emails)} invoice emails with PDF attachments")
        return invoice_emails
        
    except requests.exceptions.RequestException as e:
        self.logger.error(f"Error searching emails: {e}")
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
        # Create connection string - adjust based on your database type
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']}"
        )
        
        with pyodbc.connect(conn_str) as conn:
            query = """
                SELECT id, emailed_date, prior_emailed_date, attachment_count, 
                       emailed_to, email_subject, emailed_by
                FROM capital_project_invoices_emailed 
                WHERE invoice_no = ?
            """
            
            cursor = conn.cursor()
            cursor.execute(query, (invoice_number,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row[0],
                    'emailed_date': row[1],
                    'prior_emailed_date': row[2],
                    'attachment_count': row[3],
                    'emailed_to': row[4],
                    'email_subject': row[5],
                    'emailed_by': row[6]
                }
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
        
        # Create connection
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']}"
        )
        
        # Prepare data (convert to date only, no time)
        emailed_date = invoice_email.received_datetime.date()
        to_recipients_str = ', '.join(invoice_email.to_recipients)
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            if existing:
                # Check if this is a newer submission
                existing_date = existing['emailed_date']
                if isinstance(existing_date, datetime):
                    existing_date = existing_date.date()
                
                if emailed_date > existing_date:
                    # This is a resubmission - update record
                    update_query = """
                        UPDATE capital_project_invoices_emailed 
                        SET emailed_date = ?,
                            prior_emailed_date = ?,
                            attachment_count = ?,
                            emailed_to = ?,
                            email_subject = ?,
                            emailed_by = ?
                        WHERE invoice_no = ?
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
                    ) VALUES (?, ?, ?, ?, ?, ?)
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
    
    # Create summary statistics
    summary_data = [
        ['Total Emails Found', len(invoice_emails)],
        ['Total Invoice Numbers', len(detail_data)],
        ['New Invoices Inserted', results.get('inserted', 0)],
        ['Resubmissions Updated', results.get('updated', 0)],
        ['Duplicates Skipped', results.get('skipped', 0)],
        ['Processing Errors', results.get('errors', 0)],
        ['Processing Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
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
```


def main():
"""
Main execution function
"""
# Configuration - UPDATE THESE VALUES
ACCESS_TOKEN = "your_graph_api_access_token_here"


```
DATABASE_CONFIG = {
    'server': 'your_server_name',
    'database': 'your_database_name', 
    'username': 'your_username',
    'password': 'your_password'
}

# Initialize processor
processor = GraphInvoiceProcessor(ACCESS_TOKEN, DATABASE_CONFIG)

# Search for invoice emails
print("🔍 Searching for Blackstone Consulting invoice emails...")
invoice_emails = processor.search_invoice_emails(
    sender_domains=['blackstone-consulting.com'],
    sender_names=['inessa', 'azalea.khan'],
    subject_pattern='Non-Recurring BCI Invoices',
    days_back=90  # Adjust search window as needed
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
```


if name == "main":
main()
</artifact>
</artifacts>


Perfect! I've updated the script to match your exact table schema and implemented the resubmission logic. Here are the key changes and Python concepts involved:


Key Updates for Your Schema


1. Column Mapping to Your Schema


```python
# Your actual columns:
# id (auto-increment)
# emailed_date (timestamp without timezone)  
# attachment_count (integer)
# created_at (auto timestamp)
# prior_emailed_date (timestamp without timezone)
# invoice_no (character varying 50)
# emailed_to (character varying 255)
# email_subject (character varying 255)
# emailed_by (character varying 100)
```


2. Resubmission Logic Implementation


```python
def insert_or_update_invoice(self, invoice_email, invoice_number, attachment_name):
    existing = self.check_existing_invoice(invoice_number)
    
    if existing:
        if new_date > existing_date:
            # Resubmission - move old date to prior_emailed_date
            UPDATE SET emailed_date = new_date,
                      prior_emailed_date = existing_date
```


Python Concepts Demonstrated


1. Date Comparison and Handling


```python
# Convert datetime to date for comparison
emailed_date = invoice_email.received_datetime.date()
existing_date = existing['emailed_date']
if isinstance(existing_date, datetime):
    existing_date = existing_date.date()

if emailed_date > existing_date:
    # This is a resubmission
```


Why: Your database stores dates without time zones, so we need to strip time for accurate comparison.


2. Conditional Database Operations


```python
if existing:
    if emailed_date > existing_date:
        # UPDATE - resubmission
    elif emailed_date == existing_date:
        # SKIP - duplicate
    else:
        # SKIP - older email
else:
    # INSERT - new invoice
```


Business Logic: Only update if the new email is newer than what's in the database.


3. Dictionary Return Values for Status Tracking


```python
results = {
    'inserted': 0,
    'updated': 0, 
    'skipped': 0,
    'errors': 0
}
```


Purpose: Clean way to track and return multiple operation counts.


4. Database Transaction Management


```python
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()  # Explicit commit for data integrity
```


Safety: Ensures all changes are committed or rolled back together.


The Resubmission Logic Flow


1. Check if invoice exists in database
2. Compare dates:
  - New email newer → Update record, move old date to `prior_emailed_date`
  - Same date → Skip (duplicate)
  - New email older → Skip (out of order)
3. Update other fields (subject, sender, recipients) for resubmissions
4. Track all operations for reporting


Key Benefits


1. Data Integrity


- Never loses historical information
- Maintains audit trail of resubmissions
- Prevents duplicate entries


2. Business Intelligence


- Can track how often invoices are resubmitted
- See patterns in the `prior_emailed_date` column
- Identify problematic invoices that require multiple submissions


3. Comprehensive Reporting


- Excel report shows current and prior dates
- Separate sheet for resubmissions analysis
- Full audit trail of processing decisions


Sample Usage Output


```
🔍 Searching for Blackstone Consulting invoice emails...
✅ Found 15 invoice emails
📄 Total invoice numbers extracted: 23

💾 Processing invoices with resubmission logic...
✅ Processing complete!
   📝 New invoices inserted: 18
   🔄 Resubmissions updated: 3
   ⏭️  Duplicates skipped: 2
   ❌ Errors: 0

🔄 3 resubmissions detected and processed!
   Previous emailed dates moved to 'prior_emailed_date' column
```


Setup Requirements


1. Update your access token and database config
2. Install required packages:
bash`pip install requests pandas pyodbc openpyxl msal`
3. Test with a small date range first (`days_back=7`)


The script now perfectly matches your schema and implements the sophisticated resubmission logic you described. It will help you maintain a clean, accurate record of invoice submissions while preserving the audit trail of resubmissions!


## Human:

I have a .env file with all the necessary credentials, please reference those. I use a postgresql table so please use that language and reference the credential names for PG not DB, and will need to use psycopg2 not pyodbc


## Claude:

Perfect! Let me update the script to use PostgreSQL with psycopg2 and load credentials from your .env file. This is a much cleaner and more secure approach.


<artifacts>
<artifact identifier="graph-invoice-processor" type="application/vnd.ant.code" language="python" title="Microsoft Graph API Invoice Email Processor - PostgreSQL with .env">
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


For PostgreSQL connection


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
def init(self):
"""
Initialize Graph API processor with credentials from .env file
"""
# Load environment variables
load_dotenv()


```
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
        'host': os.getenv('PG_HOST'),
        'port': os.getenv('PG_PORT', '5432'),
        'database': os.getenv('PG_DATABASE'),
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD')
    }
    
    # Validate required PostgreSQL credentials
    required_pg_vars = ['PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
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
                        sender_domains: List[str] = ['blackstone-consulting.com'],
                        sender_names: List[str] = ['inessa', 'azalea.khan'],
                        subject_pattern: str = 'Non-Recurring BCI Invoices',
                        days_back: int = 90) -> List[InvoiceEmail]:
    """
    Search for invoice emails using Microsoft Graph API
    
    Args:
        sender_domains: List of sender domain filters
        sender_names: List of sender name filters  
        subject_pattern: Subject line pattern to match
        days_back: How many days back to search
        
    Returns:
        List of InvoiceEmail objects
    """
    self.logger.info(f"Searching for invoice emails from last {days_back} days...")
    
    # Build search query using OData syntax
    search_filters = []
    
    # Date filter (last N days)
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
    search_filters.append(f"receivedDateTime ge {start_date}")
    
    # Subject filter
    search_filters.append(f"contains(subject,'{subject_pattern}')")
    
    # Has attachments filter
    search_filters.append("hasAttachments eq true")
    
    # Combine filters with AND
    filter_query = " and ".join(search_filters)
    
    # Search in all folders (inbox, subfolders, etc.)
    search_url = f"{self.base_url}/me/messages"
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
        self.logger.info(f"Found {len(messages)} potential emails")
        
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
        
        self.logger.info(f"Found {len(invoice_emails)} invoice emails with PDF attachments")
        return invoice_emails
        
    except requests.exceptions.RequestException as e:
        self.logger.error(f"Error searching emails: {e}")
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
```


def main():
"""
Main execution function
"""
try:
# Initialize processor (loads from .env automatically)
processor = GraphInvoiceProcessor()


```
# Search for invoice emails
    print("🔍 Searching for Blackstone Consulting invoice emails...")
    invoice_emails = processor.search_invoice_emails(
        sender_domains=['blackstone-consulting.com'],
        sender_names=['inessa', 'azalea.khan'],
        subject_pattern='Non-Recurring BCI Invoices',
        days_back=90  # Adjust search window as needed
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
```


if name == "main":
main()
</artifact>
</artifacts>


Perfect! I've updated the script to use PostgreSQL with psycopg2 and load all credentials from your .env file. Here are the key improvements and Python concepts:


Key Updates


1. Environment Variable Management with python-dotenv


```python
from dotenv import load_dotenv
import os

load_dotenv()  # Loads .env file automatically
access_token = os.getenv('GRAPH_ACCESS_TOKEN')
```


Why: Keeps sensitive credentials out of code and enables easy environment switching.


2. PostgreSQL Integration with psycopg2


```python
import psycopg2
from psycopg2.extras import RealDictCursor

# PostgreSQL parameterized queries (safer than string formatting)
cursor.execute("SELECT * FROM table WHERE id = %s", (invoice_id,))
```


Security: Uses parameterized queries to prevent SQL injection attacks.


3. Context Manager for Database Connections


```python
with psycopg2.connect(**self.pg_config) as conn:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Database operations
```


Benefits:


- Automatically closes connections and cursors
- Handles exceptions gracefully
- `RealDictCursor` returns results as dictionaries for easier access


4. Error Handling and Validation


```python
# Validate required environment variables
required_pg_vars = ['PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
missing_vars = [var for var in required_pg_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required .env variables: {', '.join(missing_vars)}")
```


Purpose: Fails fast if configuration is incomplete, preventing runtime errors.


Required .env File Structure


Create a `.env` file in your project directory:


```env
# Microsoft Graph API
GRAPH_ACCESS_TOKEN=your_graph_api_token_here

# PostgreSQL Database
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DATABASE=your_database_name
PG_USER=your_username
PG_PASSWORD=your_password
```


Required Package Installation


```bash
pip install requests pandas psycopg2-binary python-dotenv openpyxl msal
```


Note: `psycopg2-binary` includes the compiled binaries, so you don't need PostgreSQL development headers installed.


Key Python Concepts Demonstrated


1. Dictionary Unpacking for Function Arguments


```python
self.pg_config = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT', '5432'),  # Default port
    'database': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD')
}

# Unpack dictionary as keyword arguments
psycopg2.connect(**self.pg_config)
```


Equivalent to: `psycopg2.connect(host=host, port=port, database=db, ...)`


2. PostgreSQL-Specific Features


```python
# Use %s for all parameter types (PostgreSQL style)
cursor.execute("INSERT INTO table (col1, col2) VALUES (%s, %s)", (val1, val2))

# RealDictCursor returns dictionaries instead of tuples
cursor = conn.cursor(cursor_factory=RealDictCursor)
row = cursor.fetchone()
print(row['column_name'])  # Dictionary access
```


3. Enhanced Database Statistics


```python
# PostgreSQL interval syntax for date calculations
cursor.execute("""
    SELECT COUNT(*) as recent 
    FROM capital_project_invoices_emailed 
    WHERE emailed_date >= CURRENT_DATE - INTERVAL '30 days'
""")
```


4. Automatic Connection Testing


```python
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
```


Purpose: Validates database connectivity immediately when the processor is created.


Key Advantages of This Approach


1. Security


- Credentials stored in .env file (add to .gitignore)
- Parameterized queries prevent SQL injection
- No hardcoded sensitive information


2. Maintainability


- Easy to switch between dev/staging/prod environments
- Clean separation of configuration and code
- Proper error handling and logging


3. PostgreSQL Optimizations


- Uses PostgreSQL-specific syntax where beneficial
- Proper handling of date types and intervals
- Dictionary cursor for easier data access


4. Enterprise Ready


- Comprehensive logging
- Database connection testing
- Graceful error handling
- Detailed reporting with database statistics


Sample .env File for Your Setup


```env
# Microsoft Graph API (get this from your Azure app registration)
GRAPH_ACCESS_TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6...

# PostgreSQL Database
PG_HOST=your-postgres-server.amazonaws.com
PG_PORT=5432
PG_DATABASE=your_database_name
PG_USER=your_username
PG_PASSWORD=your_secure_password
```


Now the script is much more secure, maintainable, and properly configured for your PostgreSQL environment! Just update your .env file with the actual credentials and you're ready to go.


## Human:

There seems to have been some formatting issues with the artifacts. In fact, none were created. Can you please reattempt to create the necessary artifacts and walk me through implementing the scripts and running them?


## Claude:

You're absolutely right! Let me recreate the artifact properly and then walk you through the complete implementation process.


## Human:

Slow down and go step by step, please. I'm on Step 4. I have 
GRAPH_CLIENT_ID=
CLIENT_SECRET=
GRAPH_TENANT_ID=

Not AZURE...

Do I just need to update those names in the Configuration section of the token helper?


## Claude:

Yes, exactly! You just need to update the variable names in the token helper script to match what you have in your .env file.


Let me update the token helper script for you:


## Human:

I also already have a .env file in my root directory, can I just use that or do I need another .env in this specific directory (invoice-processor)?


## Claude:

You can absolutely use your existing .env file in the root directory! Python's `python-dotenv` package will look for `.env` files in several places by default.


Here are your options:


Option 1: Use Your Existing Root .env File (Recommended)


Just add the new variables to your existing root `.env` file:


```env
# Your existing variables...
GRAPH_CLIENT_ID=your_existing_value
CLIENT_SECRET=your_existing_value  
GRAPH_TENANT_ID=your_existing_value

# Add these new ones:
GRAPH_ACCESS_TOKEN=will_be_generated_by_token_helper
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DATABASE=your_database_name
PG_USER=your_username
PG_PASSWORD=your_password
```


Option 2: Specify the Path to Your Root .env File


If you want to be explicit about which `.env` file to use, you can modify the scripts to point to your root directory:


```python
# Instead of just load_dotenv()
from dotenv import load_dotenv
import os

# Load from parent directory (adjust path as needed)
load_dotenv(dotenv_path='../.env')  # If root is one level up
# OR
load_dotenv(dotenv_path='/full/path/to/your/root/.env')
```


My Recommendation


Use Option 1 - just add the new variables to your existing root `.env` file. The `load_dotenv()` function will automatically find it by searching:


1. Current directory
2. Parent directories (going up the tree)
3. Your home directory


Since you're likely working in a subdirectory of where your root `.env` file is located, it should find it automatically.


Next Steps:


1. Add the PostgreSQL variables to your existing root `.env` file
2. Run the token helper script from your invoice-processor directory
3. Add the generated `GRAPH_ACCESS_TOKEN` to your root `.env` file


This keeps all your credentials in one place and avoids duplication!


## Human:

Received this when running the token helper script
❌ Failed to obtain token
Error: invalid_scope
Description: AADSTS1002012: The provided value for scope https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Mail.ReadBasic https://graph.microsoft.com/MailboxSettings.Read is not valid. Client credential flows must have a scope value with /.default suffixed to the resource identifier (application ID URI). Trace ID: 8ba06c3d-7cba-467e-b810-3b287d1a3700 Correlation ID: 1fdfff0d-82b6-4313-ad89-768e74bb35d0 Timestamp: 2025-07-03 17:33:16Z


## Claude:

Ah! The error is telling us that for client credential flows (which is what we're using), the scope needs to be different. Let me fix the token helper script:


## Human:

Success! I have updated my .env file with the GRAPH_ACCESS_TOKEN


## Claude:

Excellent! 🎉 Now let's move to Step 5 and test your setup before running the main invoice processor.


Step 5: Test Your Setup


Let's create two quick test files to make sure everything is working correctly.


5.1 Test Database Connection


Create a file called `test_db.py` in your invoice-processor directory:


```python
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

pg_config = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT', '5432'),
    'database': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD')
}

try:
    with psycopg2.connect(**pg_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM capital_project_invoices_emailed")
            count = cursor.fetchone()[0]
            print(f"✅ Database connection successful! Current records: {count}")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
```


Run it:


```bash
python test_db.py
```


5.2 Test Graph API Connection


Create a file called `test_graph.py`:


```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

try:
    # Test basic API access
    response = requests.get('https://graph.microsoft.com/v1.0/me', headers=headers)
    if response.status_code == 200:
        user_info = response.json()
        print(f"✅ Graph API connection successful!")
        print(f"User: {user_info.get('displayName')} ({user_info.get('mail')})")
        
        # Test email access
        print("\n🔍 Testing email access...")
        email_response = requests.get('https://graph.microsoft.com/v1.0/me/messages?$top=1', headers=headers)
        if email_response.status_code == 200:
            print("✅ Email access successful!")
        else:
            print(f"❌ Email access failed: {email_response.status_code}")
            print(email_response.text)
    else:
        print(f"❌ Graph API connection failed: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"❌ Graph API test failed: {e}")
```


Run it:


```bash
python test_graph.py
```


Let me know the results of both tests, and then we'll move to Step 6 (running the actual invoice processor)!


Expected Results:


- Database test should show your current record count
- Graph API test should show your name/email and confirm email access


What do you see when you run these tests?


## Human:

Success on the db! failure on the graph api:

❌ Graph API connection failed: 400
{"error":{"code":"BadRequest","message":"/me request is only valid with delegated authentication flow.","innerError":{"date":"2025-07-03T17:36:35","request-id":"4d569826-23bb-46e8-925d-89a8f1284d3e","client-request-id":"4d569826-23bb-46e8-925d-89a8f1284d3e"}}}


## Claude:

Ah! That makes perfect sense. We're using client credential flow (app-only authentication), so we can't access `/me` because there's no "me" - the app is authenticating as itself, not as a specific user.


Let me fix the Graph API test to work with your authentication type:


```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

try:
    # For client credential flow, we need to specify a user explicitly
    # Let's test with your email address
    your_email = "your_email@kp.org"  # Replace with your actual email
    
    print(f"🔍 Testing email access for {your_email}...")
    
    # Test accessing specific user's messages
    url = f'https://graph.microsoft.com/v1.0/users/{your_email}/messages'
    params = {
        '$top': 1,
        '$select': 'id,subject,receivedDateTime'
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        messages = response.json().get('value', [])
        print("✅ Graph API email access successful!")
        if messages:
            print(f"   Found messages. Latest: {messages[0].get('subject', 'No subject')}")
        else:
            print("   No messages found (but access is working)")
    else:
        print(f"❌ Graph API email access failed: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ Graph API test failed: {e}")
```


Replace `your_email@kp.org` with your actual KP email address and run this test.


If this still fails, it might mean your Azure app doesn't have the right permissions configured. The app needs Application permissions (not Delegated) for:


- `Mail.Read`
- `Mail.ReadBasic`


But let's test this first and see what happens!


What to replace:


```python
your_email = "your_actual_email@kp.org"  # Put your real email here
```


Run the updated test and let me know what you get!


## Human:

🔍 Testing email access for brendon@blackstone-consulting.com...
❌ Graph API email access failed: 403
{"error":{"code":"ErrorAccessDenied","message":"Access is denied. Check credentials and try again."}}


## Claude:

Perfect! This tells us exactly what's happening. The token is working, but your Azure app registration doesn't have the necessary Application permissions to read emails.


Since you're using client credential flow (app-only authentication), you need Application permissions, not Delegated permissions.


Here's what needs to be configured in your Azure App Registration:


Required Application Permissions:


- `Mail.Read` (Application)
- `Mail.ReadBasic` (Application)
- `User.Read.All` (Application) - to access other users' mailboxes


Steps to Fix This:


1. Go to Azure Portal → App Registrations → Your App
2. Navigate to "API permissions"
3. Add permissions → Microsoft Graph → Application permissions
4. Search for and add:
  - `Mail.Read`
  - `Mail.ReadBasic`
  - `User.Read.All`
5. Remove any Delegated permissions for these (we only need Application)
6. Click "Grant admin consent" (this is crucial!)


Alternative Quick Test


If you can't modify the Azure app right now, let's test with a different approach that might work with your current permissions:


```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Test what permissions we actually have
try:
    # Try to get the service principal info
    response = requests.get('https://graph.microsoft.com/v1.0/servicePrincipals', headers=headers)
    print(f"Service principals test: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ Basic Graph API access is working")
        print("❌ But we need additional permissions for email access")
    else:
        print("❌ Basic Graph API access failed")
        print(response.text)
        
except Exception as e:
    print(f"Error: {e}")
```


What would you like to do?


1. Option A: Configure the Azure app permissions (requires admin access)
2. Option B: Proceed with the current setup and see if we can get it working another way
3. Option C: Use a different authentication method


Which option works best for your situation?


## Human:

I modified the app permissions. Can you confirm this looks correct?


## Claude:

Perfect! Your permissions look exactly right for what we need. Let me break down what you have:


✅ Correct Permissions for Our Use Case:


Application Permissions (the ones we need):


- ✅ `Mail.Read` - "Read mail in all mailboxes" - PERFECT!
- ✅ `Mail.ReadBasic` - "Read basic mail in all mailboxes" - PERFECT!
- ✅ `User.Read.All` - "Read all users' full profiles" - PERFECT!


Admin Consent Status:


- ✅ All the Application permissions show "Yes" for admin consent - PERFECT!


The Delegated permissions (the ones with "No" for admin consent) are fine - we're not using those for our client credential flow.


Let's Test This Now!


Your configuration should work perfectly. Let's test it:


Run the Graph API test again:


```bash
python test_graph.py
```


Use this updated test file:


```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

try:
    # Test with your actual email
    your_email = "brendon@blackstone-consulting.com" # or your KP email if different
    
    print(f"🔍 Testing email access for {your_email}...")
    
    url = f'https://graph.microsoft.com/v1.0/users/{your_email}/messages'
    params = {
        '$top': 1,
        '$select': 'id,subject,receivedDateTime,sender'
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        messages = response.json().get('value', [])
        print("✅ Graph API email access successful!")
        if messages:
            msg = messages[0]
            print(f"   Latest email: '{msg.get('subject', 'No subject')}'")
            print(f"   From: {msg.get('sender', {}).get('emailAddress', {}).get('address', 'Unknown')}")
        else:
            print("   No messages found (but access is working)")
    else:
        print(f"❌ Graph API email access failed: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ Graph API test failed: {e}")
```


With those permissions, this should now work! Let me know what you get.


## Human:

🔍 Testing email access for brendon@blackstone-consulting.com...
❌ Graph API email access failed: 403
{"error":{"code":"ErrorAccessDenied","message":"Access is denied. Check credentials and try again."}}


## Claude:

Hmm, still getting 403 even with the correct permissions. This could be a few things:


Let's troubleshoot step by step:


1. Check if the token includes the new permissions


Your token was generated before you added the permissions. Let's get a fresh token:


```bash
python get_token.py
```


Update your `.env` file with the new `GRAPH_ACCESS_TOKEN` and try the test again.


2. Check which email address to use


Since you're trying to access emails from `brendon@blackstone-consulting.com`, but the invoice emails are sent to `IPCSecurity@kp.org`, we might need to test with the KP email address instead.


Try updating the test:


```python
your_email = "IPCSecurity@kp.org"  # or your actual KP email address
```


3. Test with a more basic Graph API call first


Let's make sure the permissions are working at all:


```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

try:
    # Test basic user lookup
    print("🔍 Testing basic user access...")
    response = requests.get('https://graph.microsoft.com/v1.0/users?$top=1', headers=headers)
    
    if response.status_code == 200:
        print("✅ User.Read.All permission is working!")
        users = response.json().get('value', [])
        if users:
            print(f"   Found user: {users[0].get('displayName', 'Unknown')}")
    else:
        print(f"❌ Basic user access failed: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ Test failed: {e}")
```


Try these steps in order:


1. Generate a fresh token
2. Test basic user access
3. Try with the KP email address instead


What happens when you try step 1 (fresh token)?


## Human:

Just needed an updated token after we modified permissions. Success!
🔍 Testing email access for brendon@blackstone-consulting.com...
✅ Graph API email access successful!
   Latest email: 'Create fireworks with these discounts.'
   From: sales@merchology.com


## Claude:

Perfect! 🎉 Everything is working beautifully now. The fresh token picked up the new permissions.


Step 6: Run the Main Invoice Processor


Now let's run the actual invoice processor script. Since this is your first run, let's start with a small date range to be safe.


6.1 Modify the Script for Initial Testing


Before running, let's make one small change to the main script for testing. In `invoice_processor.py`, find this line in the `main()` function:


```python
days_back=90  # Adjust search window as needed
```


And change it to:


```python
days_back=30  # Start with 30 days for first test
```


6.2 Run the Invoice Processor


```bash
python invoice_processor.py
```


6.3 What to Expect


You should see output like:


```
✅ Database connection successful
🔍 Searching for Blackstone Consulting invoice emails...
INFO - Searching for invoice emails from last 30 days...
INFO - Found X potential emails
INFO - Found Y invoice emails with PDF attachments
✅ Found Y invoice emails
📄 Total invoice numbers extracted: Z
💾 Processing invoices with resubmission logic...
```


The script will search for emails:


- From: `inessa` or `azalea.khan` at `blackstone-consulting.com`
- Subject containing: `Non-Recurring BCI Invoices`
- With PDF attachments
- In the last 30 days


6.4 After It Runs


The script will:


1. Extract invoice numbers from PDF filenames (everything before the underscore)
2. Check your database for existing invoices
3. Insert new ones or update resubmissions
4. Generate an Excel report: `invoice_processing_report_YYYYMMDD_HHMMSS.xlsx`


Run it and let me know what you see!


We can adjust the search parameters if needed (different date range, different email patterns, etc.).


## Human:

🔍 Searching for Blackstone Consulting invoice emails...
2025-07-03 13:57:15,494 - INFO - Searching for invoice emails from last 30 days...
2025-07-03 13:57:15,586 - ERROR - Error searching emails: 400 Client Error: Bad Request for url: https://graph.microsoft.com/v1.0/me/messages?%24filter=receivedDateTime+ge+2025-06-03T13%3A57%3A15Z+and+contains%28subject%2C%27Non-Recurring+BCI+Invoices%27%29+and+hasAttachments+eq+true&%24select=id%2Csubject%2Csender%2CreceivedDateTime%2CtoRecipients%2CccRecipients%2ChasAttachments%2CbodyPreview&%24expand=attachments%28%24select%3Dname%2CcontentType%2Csize%29&%24orderby=receivedDateTime+desc&%24top=100
❌ No invoice emails found matching criteria


## Claude:

Ah! The issue is that the script is still trying to use `/me/messages` but we're using client credential flow (app-only authentication), so there's no "me". We need to specify a specific user's mailbox.


Let me update the main invoice processor script to work with your authentication method:


## Human:

I am the mailbox, which is brendon@blackstone-consulting.com, not the IPC. I am copied when my colleagues send the emails to IPCSecurity@kp.org. I updated the script accordingly, but now get this:
2025-07-03 14:01:28,597 - INFO - ✅ Database connection successful
🔍 Searching for Blackstone Consulting invoice emails...
2025-07-03 14:01:28,610 - INFO - Searching for invoice emails in bredon@blackstone-consulting.com from last 30 days...
2025-07-03 14:01:29,511 - ERROR - Error searching emails: 404 Client Error: Not Found for url: https://graph.microsoft.com/v1.0/users/bredon@blackstone-consulting.com/messages?%24filter=receivedDateTime+ge+2025-06-03T14%3A01%3A28Z+and+contains%28subject%2C%27Non-Recurring+BCI+Invoices%27%29+and+hasAttachments+eq+true&%24select=id%2Csubject%2Csender%2CreceivedDateTime%2CtoRecipients%2CccRecipients%2ChasAttachments%2CbodyPreview&%24expand=attachments%28%24select%3Dname%2CcontentType%2Csize%29&%24orderby=receivedDateTime+desc&%24top=100
❌ No invoice emails found matching criteria


## Claude:

I see two issues:


1. Typo in the email: The error shows `bredon@blackstone-consulting.com` (missing 'n') but should be `brendon@blackstone-consulting.com`
2. Cross-tenant access issue: The 404 suggests that your Azure app (which is probably registered in the KP tenant) doesn't have permission to access mailboxes in the Blackstone Consulting tenant.


Quick Fix Options:


Option 1: Fix the typo first


Make sure you're using `brendon@blackstone-consulting.com` (with the 'n').


Option 2: Use a different approach for cross-tenant access


Since you mentioned you're copied on the emails, let's try searching the emails differently. We could:


A) Search in a KP mailbox you have access to
If you have a KP email address that receives these emails, use that instead.


B) Use delegated authentication instead of app-only
This would authenticate as you personally rather than as the app.


Option 3: Test what mailboxes you can access


Let's first see what mailboxes your current permissions can access:


```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv('GRAPH_ACCESS_TOKEN')
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Test what users/mailboxes we can see
try:
    response = requests.get('https://graph.microsoft.com/v1.0/users?$top=5', headers=headers)
    if response.status_code == 200:
        users = response.json().get('value', [])
        print("✅ Available users/mailboxes:")
        for user in users:
            print(f"   - {user.get('mail', 'No email')} ({user.get('displayName', 'No name')})")
    else:
        print(f"❌ Cannot list users: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"Error: {e}")
```


What email addresses do you have access to?


- Do you have a KP email address?
- Are the invoice emails forwarded anywhere else?
- Should we try a different authentication approach?


Let me know what you find out!


## Human:

It was just the typo because...IT WORKED!

2025-07-03 14:03:09,056 - INFO - ✅ Database connection successful
🔍 Searching for Blackstone Consulting invoice emails...
2025-07-03 14:03:09,069 - INFO - Searching for invoice emails in brendon@blackstone-consulting.com from last 30 days...
2025-07-03 14:03:10,697 - INFO - Found 10 potential emails
2025-07-03 14:03:10,697 - INFO - Found 6 invoice emails with PDF attachments
✅ Found 6 invoice emails
📄 Total invoice numbers extracted: 11
💾 Processing invoices with resubmission logic...
2025-07-03 14:03:10,698 - INFO - Processing 6 invoice emails...
2025-07-03 14:03:10,989 - INFO - Skipping duplicate invoice: 40011284 (same date: 2025-06-23)
2025-07-03 14:03:11,218 - INFO - Skipping duplicate invoice: 40010698 (same date: 2025-06-23)
2025-07-03 14:03:11,432 - INFO - Skipping duplicate invoice: 40010702 (same date: 2025-06-15)
2025-07-03 14:03:11,628 - INFO - Skipping duplicate invoice: 40011235 (same date: 2025-06-15)
2025-07-03 14:03:11,845 - INFO - Skipping older invoice: 40011284 (existing: 2025-06-23, this: 2025-06-15)
2025-07-03 14:03:12,047 - INFO - Skipping duplicate invoice: 40011229 (same date: 2025-06-15)
2025-07-03 14:03:12,249 - INFO - Skipping older invoice: 40010698 (existing: 2025-06-23, this: 2025-06-15)
2025-07-03 14:03:12,467 - INFO - Skipping duplicate invoice: 40010754 (same date: 2025-06-15)
2025-07-03 14:03:12,685 - INFO - Skipping duplicate invoice: 40011697 (same date: 2025-06-15)
2025-07-03 14:03:12,899 - INFO - Skipping duplicate invoice: 40011712 (same date: 2025-06-15)
2025-07-03 14:03:13,139 - INFO - Skipping duplicate invoice: 40011693 (same date: 2025-06-15)
✅ Processing complete!
   📝 New invoices inserted: 0
   🔄 Resubmissions updated: 0
   ⏭️  Duplicates skipped: 11
   ❌ Errors: 0
📊 Generating processing report...
2025-07-03 14:03:14,942 - INFO - Processing report saved to: invoice_processing_report_20250703_140314.xlsx
📋 Report generated with 11 records
Next steps:
1. Review the Excel report for accuracy
2. Check database for inserted/updated records
3. Verify resubmission logic worked correctly


## Claude:

🎉 FANTASTIC! IT'S WORKING PERFECTLY!


This is exactly what we want to see! Let me break down what just happened:


What the Script Successfully Did:


✅ Email Discovery


- Found 10 potential emails matching your criteria
- Filtered down to 6 emails with PDF attachments
- Extracted 11 invoice numbers from the PDF filenames


✅ Smart Duplicate/Resubmission Logic Working


The script correctly identified that all these invoices already exist in your database and made intelligent decisions:


- Duplicate invoices (same date): Skipped 9 invoices that were already processed on the same date
- Older invoices: Skipped 2 invoices that were older than what's already in the database
- Resubmission logic: Ready to detect and update newer submissions (none found this time)


✅ Perfect Database Integration


- Connected successfully to PostgreSQL
- Checked existing records properly
- Applied your business logic correctly


✅ Comprehensive Reporting


- Generated Excel report: `invoice_processing_report_20250703_140314.xlsx`


Next Steps:


1. Review the Excel Report


Open `invoice_processing_report_20250703_140314.xlsx` to see:


- Invoice_Details: All found invoices with their status
- Processing_Summary: Statistics and database summary
- Database summary: Current state of your table


2. Test the Resubmission Logic


To see the resubmission logic in action, you could:


- Increase the date range: `days_back=60` or `days_back=90`
- This might find older emails that show the resubmission pattern


3. Production Ready!


The script is working perfectly. You can now:


- Run it regularly to catch new invoice emails
- Expand the date range for historical processing
- Schedule it to run automatically


Key Python Concepts Demonstrated Successfully:


1. Microsoft Graph API integration with client credential flow
2. PostgreSQL database operations with proper parameterized queries
3. Business logic implementation (duplicate detection, resubmission handling)
4. Data extraction using regex for invoice number parsing
5. Comprehensive error handling and logging
6. Professional reporting with Excel export


This is a production-ready enterprise solution! 🏆


Would you like to test with a larger date range or make any adjustments to the search criteria?


## Human:

Will this script search the entire mailbox, including folders and subfolders?


## Claude:

No, by default it only searches the main message folder. The current script uses:


```python
search_url = f"{self.base_url}/users/{mailbox}/messages"
```


This searches the primary messages collection but does not include subfolders by default.


To Search All Folders Including Subfolders:


We have a couple of options to make it search everywhere:


Option 1: Search Across All Mail Folders (Recommended)


Let me update the script to search all mail folders:


## Human:

Walk me through, step by step, little by little, what I should update if I want to add the ability to enter in the date range for my search, and if I want to specify a subfolder. This is the folder where most of these emails belong once I move them from my Inbox: 2025-07-03 14:09:54,685 - INFO - Searching folder: Inbox/1-Reference


## Claude:

Great idea! Let's add those features step by step. I'll walk you through each change carefully.


Step 1: Add Date Range Parameters


First, let's modify the function to accept start and end dates instead of just "days back":


1.1 Update the function signature


Find this line in the `search_invoice_emails` function:


```python
def search_invoice_emails(self, 
                        mailbox: str = 'IPCSecurity@kp.org',
                        sender_domains: List[str] = ['blackstone-consulting.com'],
                        sender_names: List[str] = ['inessa', 'azalea.khan'],
                        subject_pattern: str = 'Non-Recurring BCI Invoices',
                        days_back: int = 90,
                        search_all_folders: bool = True) -> List[InvoiceEmail]:
```


Replace it with:


```python
def search_invoice_emails(self, 
                        mailbox: str = 'IPCSecurity@kp.org',
                        sender_domains: List[str] = ['blackstone-consulting.com'],
                        sender_names: List[str] = ['inessa', 'azalea.khan'],
                        subject_pattern: str = 'Non-Recurring BCI Invoices',
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        days_back: int = 30,
                        specific_folder: Optional[str] = None,
                        search_all_folders: bool = True) -> List[InvoiceEmail]:
```


1.2 Update the docstring


Find the docstring and replace it with:


```python
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
```


1.3 Update the date filter logic


Find this section:


```python
# Date filter (last N days)
from datetime import timedelta
start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
search_filters.append(f"receivedDateTime ge {start_date}")
```


Replace it with:


```python
# Date filter - use provided dates or calculate from days_back
from datetime import timedelta
if start_date:
    # Convert YYYY-MM-DD to ISO format
    start_date_iso = f"{start_date}T00:00:00Z"
    search_filters.append(f"receivedDateTime ge {start_date_iso}")
else:
    # Use days_back as before
    start_date_calc = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
    search_filters.append(f"receivedDateTime ge {start_date_calc}")

if end_date:
    # Convert YYYY-MM-DD to ISO format (end of day)
    end_date_iso = f"{end_date}T23:59:59Z"
    search_filters.append(f"receivedDateTime le {end_date_iso}")
```


Step 2: Add Specific Folder Search


2.1 Update the folder search logic


Find this section:


```python
if search_all_folders:
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
```


Replace it with:


```python
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
```


Step 3: Add the Folder Finding Function


Add this new function right after the `_get_child_folders` function:


```python
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
        # Get all folders
        all_folders = self._get_all_mail_folders(mailbox)
        
        # Look for exact match
        for folder_id, display_name in all_folders:
            if display_name == folder_name:
                self.logger.info(f"Found folder '{folder_name}' with ID: {folder_id}")
                return folder_id
        
        # If no exact match, try case-insensitive
        for folder_id, display_name in all_folders:
            if display_name.lower() == folder_name.lower():
                self.logger.info(f"Found folder '{folder_name}' (case-insensitive) with ID: {folder_id}")
                return folder_id
        
        # List available folders for debugging
        self.logger.error(f"Folder '{folder_name}' not found. Available folders:")
        for _, display_name in all_folders[:10]:  # Show first 10
            self.logger.error(f"  - {display_name}")
        
        return None
        
    except Exception as e:
        self.logger.error(f"Error finding folder '{folder_name}': {e}")
        return None
```


Step 4: Update the Main Function


Find the main function call and replace it with:


```python
# Search for invoice emails
print("🔍 Searching for Blackstone Consulting invoice emails...")
invoice_emails = processor.search_invoice_emails(
    mailbox='brendon@blackstone-consulting.com',
    sender_domains=['blackstone-consulting.com'],
    sender_names=['inessa', 'azalea.khan'], 
    subject_pattern='Non-Recurring BCI Invoices',
    start_date='2025-06-01',  # Start date (YYYY-MM-DD)
    end_date='2025-07-03',    # End date (YYYY-MM-DD)
    specific_folder='Inbox/1-Reference',  # Specific folder to search
    search_all_folders=False  # Set to False when using specific folder
)
```


Step 5: Add the Missing Import


At the top of the file, find the imports and make sure you have:


```python
from typing import List, Dict, Optional, Tuple
```


Step 6: Test Your Changes


Now you can run the script with these new options:


Example 1: Search specific folder with date range


```python
invoice_emails = processor.search_invoice_emails(
    mailbox='brendon@blackstone-consulting.com',
    start_date='2025-06-01',
    end_date='2025-07-03', 
    specific_folder='Inbox/1-Reference'
)
```


Example 2: Search all folders with date range


```python
invoice_emails = processor.search_invoice_emails(
    mailbox='brendon@blackstone-consulting.com',
    start_date='2025-06-15',
    end_date='2025-06-30',
    search_all_folders=True
)
```


Example 3: Use days_back (original behavior)


```python
invoice_emails = processor.search_invoice_emails(
    mailbox='brendon@blackstone-consulting.com',
    days_back=7,
    search_all_folders=True
)
```


Would you like me to walk you through making any of these specific changes, or do you want to try implementing them and let me know if you run into any issues?


## Human:

I implemented the changes. However, It defaulted to searching for the last 30 days. How do I avoid that with the search_invoie_emails being set to that?


## Claude:

Good catch! The issue is that even when you provide `start_date` and `end_date`, the function is still using the `days_back=30` default because of how the date logic is structured.


Let's fix this by updating the date filter logic to prioritize the provided dates over the `days_back` parameter.


Fix the Date Logic


Find this section in your `search_invoice_emails` function:


```python
# Date filter - use provided dates or calculate from days_back
from datetime import timedelta
if start_date:
    # Convert YYYY-MM-DD to ISO format
    start_date_iso = f"{start_date}T00:00:00Z"
    search_filters.append(f"receivedDateTime ge {start_date_iso}")
else:
    # Use days_back as before
    start_date_calc = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
    search_filters.append(f"receivedDateTime ge {start_date_calc}")

if end_date:
    # Convert YYYY-MM-DD to ISO format (end of day)
    end_date_iso = f"{end_date}T23:59:59Z"
    search_filters.append(f"receivedDateTime le {end_date_iso}")
```


Replace it with:


```python
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
```


What This Fix Does:


1. Checks if ANY date is provided (`if start_date or end_date`)
2. Uses your provided dates and ignores `days_back` entirely
3. Falls back to `days_back` only if neither date is provided
4. Adds logging so you can see which date range is actually being used


Now when you run:


```python
invoice_emails = processor.search_invoice_emails(
    mailbox='brendon@blackstone-consulting.com',
    start_date='2025-06-01',
    end_date='2025-07-03', 
    specific_folder='Inbox/1-Reference'
)
```


You should see in the logs:


```
INFO - Using start date: 2025-06-01
INFO - Using end date: 2025-07-03
INFO - Searching date range: 2025-06-01 to 2025-07-03
```


Instead of it defaulting to 30 days back!


Try this fix and let me know if it now respects your date range properly.


## Human:

Updated and reran, but I don't believe it searched the specific_folder I asked it to, which was 'Inbox/1-Reference'. I know I have emails in there that meet the criteria.

2025-07-03 14:24:06,123 - INFO - ✅ Database connection successful
🔍 Searching for Blackstone Consulting invoice emails...
2025-07-03 14:24:06,136 - INFO - Searching for invoice emails in brendon@blackstone-consulting.com from last 30 days...
2025-07-03 14:24:06,136 - INFO - Using start date: 2024-10-01
2025-07-03 14:24:06,136 - INFO - Using end date: 2025-05-31
2025-07-03 14:24:06,136 - INFO - Searching date range: 2024-10-01 to 2025-05-31
❌ No invoice emails found matching criteria


## Claude:

I see two issues:


1. The function is still logging "from last 30 days" instead of showing it's searching the specific folder
2. It's not finding emails that you know are there


Let's debug this step by step:


Step 1: Update the Logging


First, let's fix the logging so we can see what's actually happening. Find this line in the `search_invoice_emails` function:


```python
self.logger.info(f"Searching for invoice emails in {mailbox} from last {days_back} days...")
```


Replace it with:


```python
if specific_folder:
    self.logger.info(f"Searching for invoice emails in {mailbox} - Specific folder: {specific_folder}")
elif start_date or end_date:
    self.logger.info(f"Searching for invoice emails in {mailbox} - Date range specified")
else:
    self.logger.info(f"Searching for invoice emails in {mailbox} from last {days_back} days...")
```


Step 2: Add Debug Logging to Folder Search


Let's add more detailed logging to see what's happening with the folder search. Find the `_find_folder_by_name` function and replace the entire function with:


```python
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
```


Step 3: Test with Enhanced Logging


Run the script again and you should see detailed output like:


```
INFO - Searching for invoice emails in brendon@blackstone-consulting.com - Specific folder: Inbox/1-Reference
INFO - Looking for folder: 'Inbox/1-Reference'
INFO - Found 15 total folders to check
INFO - Checking folder: 'Inbox'
INFO - Checking folder: 'Sent Items'
INFO - Checking folder: 'Inbox/1-Reference'
INFO - ✅ Found exact match for 'Inbox/1-Reference' with ID: ABC123
```


Step 4: Possible Issues to Check


If it still doesn't find the folder, here are the most likely issues:


Issue A: Folder Name Format


The folder might be displayed differently than expected. Common variations:


- `1-Reference` (without "Inbox/")
- `Inbox\1-Reference` (backslash instead of forward slash)
- `Inbox > 1-Reference` (different separator)


Issue B: Try Different Folder Names


Based on your earlier log output, try these variations in your main function:


```python
# Try just the subfolder name
specific_folder='1-Reference'

# Or try without the slash
specific_folder='Inbox\\1-Reference'
```


Step 5: Quick Test to List All Your Folders


Let's create a quick test to see all your folder names. Add this to a test file:


```python
from your_main_script import GraphInvoiceProcessor

processor = GraphInvoiceProcessor()
folders = processor._get_all_mail_folders('brendon@blackstone-consulting.com')

print("All your folders:")
for folder_id, folder_name in folders:
    print(f"  '{folder_name}' (ID: {folder_id})")
```


Run the enhanced version and let me know:


1. What folder names it shows in the log
2. Whether it finds the `Inbox/1-Reference` folder
3. If it doesn't find it, what the actual folder names are


This will help us pinpoint exactly what's happening!


