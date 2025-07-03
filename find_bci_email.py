# Test finding BCI emails in Outlook

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

def find_blackstone_emails():
    """
    Look specifically for emails from blackstone-consulting.com domain
    """
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    
    print("Looking for emails from blackstone-consulting.com...")
    print("-" * 60)
    
    blackstone_emails = []
    
    for mail in inbox.Items:
        try:
            actual_email = get_actual_email_address(mail)
            
            # Check if email is from blackstone-consulting.com
            if "blackstone-consulting.com" in actual_email:
                blackstone_emails.append(mail)
                
                print(f"\nBlackstone Email Found:")
                print(f"  From: {mail.SenderName} ({actual_email})")
                print(f"  To: {mail.To}")
                print(f"  Subject: {mail.Subject}")
                print(f"  Date: {mail.ReceivedTime}")
                print(f"  Has Attachments: {mail.Attachments.Count > 0}")
                
                # Show PDF attachments
                pdf_attachments = []
                for attachment in mail.Attachments:
                    if attachment.FileName.lower().endswith('.pdf'):
                        pdf_attachments.append(attachment.FileName)
                        print(f"  PDF: {attachment.FileName}")
                
                # Check if this might be our target email
                if ("IPCSecurity@kp.org" in mail.To.lower() and 
                    mail.Attachments.Count > 0):
                    print(f"  *** POTENTIAL MATCH - Sent to IPC with attachments! ***")
                
        except Exception as e:
            continue
    
    print(f"\nTotal Blackstone emails found: {len(blackstone_emails)}")

# Run the search
if __name__ == "__main__":
    find_blackstone_emails()

    