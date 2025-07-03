import win32com.client
import re
from datetime import datetime

def get_actual_email_address(mail):
    """Get the real SMTP email address"""
    try:
        sender_email = mail.SenderEmailAddress
        if sender_email and sender_email.startswith('/O='):
            try:
                sender = mail.Sender
                if sender:
                    exchange_user = sender.GetExchangeUser()
                    if exchange_user:
                        smtp_address = exchange_user.PrimarySmtpAddress
                        return smtp_address.lower() if smtp_address else ""
            except:
                pass
        else:
            return sender_email.lower() if sender_email else ""
    except:
        pass
    return ""

def extract_invoice_no(pdf_filename):
    """Extract invoice number from PDF filename"""
    match = re.match(r'^(.+?)_\d{8}\.pdf$', pdf_filename, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return None

def process_invoice_emails():
    """
    Process matching emails and extract data for database insertion
    """
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    
    # Search criteria
    target_senders = ['azalea.khan@blackstone-consulting.com', 'inessa@blackstone-consulting.com']
    target_recipient = 'IPCSecurity@kp.org'
    subject_keyword = 'Non-Recurring BCI Invoices'
    
    # This will hold our data for the database
    invoice_records = []
    
    print("Processing invoice emails for database insertion...")
    print("-" * 60)
    
    for mail in inbox.Items:
        try:
            sender_email = get_actual_email_address(mail)
            subject = mail.Subject if mail.Subject else ""
            to_addresses = mail.To.lower() if mail.To else ""
            
            # Check for PDF attachments
            pdf_files = []
            for attachment in mail.Attachments:
                if attachment.FileName.lower().endswith('.pdf'):
                    pdf_files.append(attachment.FileName)
            
            # Check if email matches our criteria
            if (sender_email in target_senders and 
                target_recipient.lower() in to_addresses and
                subject_keyword in subject and 
                len(pdf_files) > 0):
                
                # Process each PDF attachment
                for pdf_filename in pdf_files:
                    invoice_no = extract_invoice_no(pdf_filename)
                    
                    if invoice_no:  # Only process if we can extract invoice number
                        # Create record for database
                        record = {
                            'invoice_no': invoice_no,
                            'emailed_to': mail.To,
                            'emailed_date': mail.ReceivedTime,  # When you received it
                            'emailed_by': sender_email,
                            'email_subject': subject,
                            'attachment_count': len(pdf_files),
                            'pdf_filename': pdf_filename
                        }
                        
                        invoice_records.append(record)
                        
                        print(f"✅ Processed: Invoice {invoice_no}")
                        print(f"   Emailed To: {mail.To}")
                        print(f"   Email Date: {mail.ReceivedTime}")
                        print(f"   Sent By: {sender_email}")
                        print(f"   PDF File: {pdf_filename}")
                        print(f"   Subject: {subject}")
                        print()
                
        except Exception as e:
            continue
    
    print(f"Total invoice records processed: {len(invoice_records)}")
    return invoice_records

# Run the processing
if __name__ == "__main__":
    records = process_invoice_emails()
    
    # Show summary of what we found
    print("\n" + "="*60)
    print("SUMMARY OF EXTRACTED DATA")
    print("="*60)
    
    for i, record in enumerate(records, 1):
        print(f"{i}. Invoice: {record['invoice_no']}")
        print(f"   Date: {record['emailed_date']}")
        print(f"   From: {record['emailed_by']}")
        print(f"   File: {record['pdf_filename']}")