import psycopg2
from psycopg2.extras import RealDictCursor
import win32com.client
import os
from ipc_email_invoice_extract import process_invoice_emails, get_actual_email_address, extract_invoice_no
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """
    Create PostgreSQL connection using environment variables
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('PGHOST'),
            database=os.getenv('PGDATABASE'),
            user=os.getenv('PGUSER'),
            password=os.getenv('PGPASSWORD'),
            port=os.getenv('PGPORT', 5432)
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        return None

def insert_invoice_records_with_resubmission(records):
    """
    Insert invoice records with smart date handling
    - Always keeps most recent date in emailed_date
    - Always keeps older date in prior_emailed_date
    - Handles processing order independence
    """
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        print("Connected to PostgreSQL successfully!")
        print(f"Processing {len(records)} records with smart date handling...")
        print("-" * 60)
        
        # Check if exact record exists (same invoice + same date)
        check_exact_sql = """
        SELECT COUNT(*) 
        FROM capital_project_invoices_emailed 
        WHERE invoice_no = %s AND emailed_date = %s
        """
        
        # Get existing record with all details
        check_invoice_sql = """
        SELECT invoice_no, emailed_date, emailed_to, emailed_by, email_subject, attachment_count, prior_emailed_date
        FROM capital_project_invoices_emailed 
        WHERE invoice_no = %s
        """
        
        # Update existing record
        update_sql = """
        UPDATE capital_project_invoices_emailed 
        SET emailed_to = %s,
            emailed_date = %s,
            emailed_by = %s,
            email_subject = %s,
            attachment_count = %s,
            prior_emailed_date = %s
        WHERE invoice_no = %s
        """
        
        # Insert new record
        insert_sql = """
        INSERT INTO capital_project_invoices_emailed 
        (invoice_no, emailed_to, emailed_date, emailed_by, email_subject, attachment_count) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        for record in records:
            try:
                # Check 1: Exact duplicate (same invoice + same date)?
                cursor.execute(check_exact_sql, (
                    record['invoice_no'],  # This comes from your record dict
                    record['emailed_date']
                ))
                exact_match = cursor.fetchone()[0] > 0
                
                if exact_match:
                    skipped_count += 1
                    print(f"⏭️  Skipped: Invoice {record['invoice_no']} (exact duplicate)")
                    continue
                
                # Check 2: Invoice exists with any date?
                cursor.execute(check_invoice_sql, (record['invoice_no'],))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Invoice exists - need to handle dates smartly
                    (existing_invoice, existing_date, existing_to, existing_by, 
                     existing_subject, existing_count, existing_prior) = existing_record
                    
                    new_date = record['emailed_date']
                    
                    # Compare dates to determine which is more recent
                    if new_date > existing_date:
                        # New record is more recent - update with new as current, existing as prior
                        cursor.execute(update_sql, (
                            record['emailed_to'],      # New current data
                            record['emailed_date'],
                            record['emailed_by'],
                            record['email_subject'],
                            record['attachment_count'],
                            existing_date,              # Old date becomes prior
                            record['invoice_no']    # WHERE clause
                        ))
                        
                        updated_count += 1
                        print(f"🔄 Updated: Invoice {record['invoice_no']} (NEW submission is more recent)")
                        print(f"   Current date: {new_date}")
                        print(f"   Prior date: {existing_date}")
                        
                    elif new_date < existing_date:
                        # Existing record is more recent - update prior_emailed_date only if it's older
                        if existing_prior is None or new_date < existing_prior:
                            cursor.execute("""
                                UPDATE capital_project_invoices_emailed 
                                SET prior_emailed_date = %s
                                WHERE invoice_no = %s
                            """, (new_date, record['invoice_no']))
                            
                            updated_count += 1
                            print(f"🔄 Updated: Invoice {record['invoice_no']} (OLD submission found)")
                            print(f"   Current date: {existing_date} (unchanged)")
                            print(f"   Prior date: {new_date} (updated)")
                        else:
                            skipped_count += 1
                            print(f"⏭️  Skipped: Invoice {record['invoice_no']} (older than existing prior date)")
                    
                    # If dates are equal, we already caught this in the exact duplicate check
                    
                else:
                    # New invoice - regular insert
                    cursor.execute(insert_sql, (
                        record['invoice_no'],
                        record['emailed_to'],
                        record['emailed_date'],
                        record['emailed_by'],
                        record['email_subject'],
                        record['attachment_count']
                    ))
                    
                    inserted_count += 1
                    print(f"✅ Inserted: Invoice {record['invoice_no']} (NEW)")
                    
            except Exception as e:
                print(f"❌ Error processing {record['invoice_no']}: {str(e)}")
                continue
        
        conn.commit()
        
        print("-" * 60)
        print(f"✅ New invoices: {inserted_count}")
        print(f"🔄 Records updated: {updated_count}")
        print(f"⏭️  Duplicates skipped: {skipped_count}")
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        conn.rollback()
        
    finally:
        cursor.close()
        conn.close()

def search_all_emails_for_invoices():
    """
    Brute force search through ALL emails in the entire mailbox
    """
    try:
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        
        print("SEARCHING ALL EMAILS IN MAILBOX")
        print("=" * 50)
        
        # Get all the main folders to search
        folders_to_search = []
        
        # Add Inbox
        inbox = outlook.GetDefaultFolder(6)
        folders_to_search.append(("Inbox", inbox))
        
        # Add Sent Items
        sent_items = outlook.GetDefaultFolder(5)
        folders_to_search.append(("Sent Items", sent_items))
        
        # Add all subfolders of inbox (including your organized folders)
        try:
            for subfolder in inbox.Folders:
                folders_to_search.append((f"Inbox/{subfolder.Name}", subfolder))
        except:
            pass
        
        # Search criteria
        target_senders = ['azalea.khan@blackstone-consulting.com', 'inessa@blackstone-consulting.com']
        target_recipient = 'IPCSecurity@kp.org'
        subject_keyword = 'Non-Recurring BCI Invoices'
        
        all_invoice_records = []
        total_emails_checked = 0
        
        # Search each folder
        for folder_name, folder in folders_to_search:
            try:
                folder_count = folder.Items.Count
                print(f"\nSearching {folder_name}: {folder_count} emails")
                
                if folder_count == 0:
                    print("  Skipping empty folder")
                    continue
                
                emails_in_folder = 0
                matches_in_folder = 0
                
                for mail in folder.Items:
                    try:
                        total_emails_checked += 1
                        emails_in_folder += 1
                        
                        # Show progress every 500 emails
                        if total_emails_checked % 500 == 0:
                            print(f"  Progress: {total_emails_checked} total emails checked...")
                        
                        # Get email details
                        sender_email = get_actual_email_address(mail)
                        subject = mail.Subject if mail.Subject else ""
                        to_addresses = mail.To.lower() if mail.To else ""
                        
                        # Quick check for Blackstone emails first
                        if "blackstone-consulting.com" in sender_email:
                            print(f"  🔍 Found Blackstone email: {sender_email} - {subject[:50]}...")
                            
                            # Check for PDF attachments
                            pdf_files = []
                            for attachment in mail.Attachments:
                                if attachment.FileName.lower().endswith('.pdf'):
                                    pdf_files.append(attachment.FileName)
                            
                            # Check all criteria
                            if (sender_email in target_senders and 
                                target_recipient.lower() in to_addresses and
                                subject_keyword in subject and 
                                len(pdf_files) > 0):
                                
                                matches_in_folder += 1
                                print(f"    ✅ MATCH! {mail.ReceivedTime.date()}")
                                
                                # Process PDFs
                                for pdf_filename in pdf_files:
                                    invoice_number = extract_invoice_no(pdf_filename)
                                    
                                    if invoice_number:
                                        record = {
                                            'invoice_no': invoice_number,
                                            'emailed_to': mail.To,
                                            'emailed_date': mail.ReceivedTime,
                                            'emailed_by': sender_email,
                                            'email_subject': subject,
                                            'attachment_count': len(pdf_files),
                                            'pdf_filename': pdf_filename,
                                            'found_in_folder': folder_name
                                        }
                                        
                                        all_invoice_records.append(record)
                        
                    except Exception as e:
                        continue
                
                print(f"  Completed {folder_name}: {emails_in_folder} emails checked, {matches_in_folder} matches")
                
            except Exception as e:
                print(f"  Error searching {folder_name}: {str(e)}")
                continue
        
        print(f"\n" + "="*50)
        print(f"SEARCH COMPLETE")
        print(f"Total emails checked: {total_emails_checked}")
        print(f"Invoice emails found: {len(all_invoice_records)}")
        
        # Show which folders had matches
        if all_invoice_records:
            print(f"\nMatches found in these folders:")
            folder_counts = {}
            for record in all_invoice_records:
                folder = record['found_in_folder']
                folder_counts[folder] = folder_counts.get(folder, 0) + 1
            
            for folder, count in folder_counts.items():
                print(f"  {folder}: {count} invoices")
        
        return all_invoice_records
        
    except Exception as e:
        print(f"Error in search: {str(e)}")
        return []

def process_and_insert_invoices():
    """
    Complete workflow: Search ALL emails and insert into database
    """
    print("CAPITAL PROJECT INVOICE PROCESSING - COMPREHENSIVE SEARCH")
    print("=" * 70)
    
    # Search ALL emails in mailbox
    print("Searching entire mailbox for invoice emails...")
    all_records = search_all_emails_for_invoices()
    
    if len(all_records) > 0:
        print(f"\nProcessing {len(all_records)} total records for database...")
        insert_invoice_records_with_resubmission(all_records)
    else:
        print("No invoice records found in entire mailbox")
    
    print("\n✅ Complete processing finished!")

# Run the complete process
if __name__ == "__main__":
    process_and_insert_invoices()