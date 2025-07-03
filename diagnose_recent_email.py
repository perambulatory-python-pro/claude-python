import win32com.client

def get_actual_email_address(mail):
    """
    Get the real SMTP email address from an Outlook mail item
    Handles both regular emails and Exchange DN format
    """
    try:
        sender_email = mail.SenderEmailAddress
        
        # Check if it's Exchange DN format (starts with /O=)
        if sender_email and sender_email.startswith('/O='):
            # Try to resolve to real email address
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
            # Regular email format
            return sender_email.lower() if sender_email else ""
    except:
        pass
    
    return ""

def diagnose_recent_emails():
    """
    Look at recent emails to see the actual sender addresses and subjects
    Now with proper email address resolution
    """
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    
    print("Looking at recent emails with attachments...")
    print("Now resolving actual email addresses...")
    print("-" * 60)
    
    emails_checked = 0
    for mail in inbox.Items:
        try:
            if mail.Attachments.Count > 0:
                emails_checked += 1
                
                # Get the real email address
                actual_email = get_actual_email_address(mail)
                
                print(f"\nEmail {emails_checked}:")
                print(f"  From Name: {mail.SenderName}")
                print(f"  From Email (raw): {mail.SenderEmailAddress}")
                print(f"  From Email (resolved): {actual_email}")
                print(f"  To: {mail.To}")
                print(f"  Subject: {mail.Subject}")
                print(f"  Date: {mail.ReceivedTime}")
                print(f"  Attachments: {mail.Attachments.Count}")
                
                # Show PDF attachments specifically
                pdf_attachments = []
                for attachment in mail.Attachments:
                    if attachment.FileName.lower().endswith('.pdf'):
                        pdf_attachments.append(attachment.FileName)
                
                if pdf_attachments:
                    print(f"  PDF Attachments:")
                    for pdf in pdf_attachments:
                        print(f"    - {pdf}")
                
                if emails_checked >= 10:
                    break
                    
        except Exception as e:
            continue
    
    print(f"\nChecked {emails_checked} recent emails with attachments")

# Run the enhanced diagnostic
if __name__ == "__main__":
    diagnose_recent_emails()