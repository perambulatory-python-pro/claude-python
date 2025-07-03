import win32com.client

def get_actual_email_address(mail):
    """
    Get the real SMTP email address from an Outlook mail item
    """
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

def find_matching_emails():
    """
    Find emails that match our specific criteria for capital project invoices
    """
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    
    # Our search criteria
    target_senders = ['azalea.khan@blackstone-consulting.com', 'inessa@blackstone-consulting.com']
    target_recipient = 'IPCSecurity@kp.org'
    subject_keyword = 'Non-Recurring BCI Invoices'
    
    matching_emails = []
    
    print("Searching for matching capital project invoice emails...")
    print(f"Looking for emails from: {target_senders}")
    print(f"Sent to: {target_recipient}")
    print(f"Subject containing: '{subject_keyword}'")
    print(f"With PDF attachments")
    print("-" * 60)
    
    for mail in inbox.Items:
        try:
            # Get the resolved email address
            sender_email = get_actual_email_address(mail)
            subject = mail.Subject if mail.Subject else ""
            has_attachments = mail.Attachments.Count > 0
            to_addresses = mail.To.lower() if mail.To else ""
            
            # Check for PDF attachments specifically
            has_pdf_attachments = False
            pdf_files = []
            for attachment in mail.Attachments:
                if attachment.FileName.lower().endswith('.pdf'):
                    has_pdf_attachments = True
                    pdf_files.append(attachment.FileName)
            
            # Check if email matches ALL our criteria
            if (sender_email in target_senders and 
                target_recipient.lower() in to_addresses and
                subject_keyword in subject and 
                has_pdf_attachments):
                
                matching_emails.append(mail)
                print(f"\n✅ MATCH FOUND:")
                print(f"  From: {mail.SenderName} ({sender_email})")
                print(f"  To: {mail.To}")
                print(f"  Subject: {subject}")
                print(f"  Date: {mail.ReceivedTime}")
                print(f"  PDF Attachments ({len(pdf_files)}):")
                for pdf in pdf_files:
                    print(f"    - {pdf}")
            
            # Also show near misses to help troubleshoot
            elif (sender_email in target_senders and 
                  target_recipient.lower() in to_addresses and
                  has_pdf_attachments):
                print(f"\n⚠️  NEAR MISS (wrong subject):")
                print(f"  From: {mail.SenderName} ({sender_email})")
                print(f"  Subject: {subject}")
                print(f"  Expected: '{subject_keyword}'")
                
        except Exception as e:
            continue
    
    print("-" * 60)
    print(f"\nTotal matching emails found: {len(matching_emails)}")
    
    if len(matching_emails) == 0:
        print("\n🔍 No exact matches found. This could mean:")
        print("   - The subject line is slightly different")
        print("   - The sender email addresses are different")
        print("   - The emails might be in a subfolder")
    
    return matching_emails

# Run the updated search
if __name__ == "__main__":
    emails = find_matching_emails()