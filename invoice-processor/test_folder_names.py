from invoice_processor import GraphInvoiceProcessor

processor = GraphInvoiceProcessor()
folders = processor._get_all_mail_folders('brendon@blackstone-consulting.com')

print("All your folders:")
for folder_id, folder_name in folders:
    print(f"  '{folder_name}' (ID: {folder_id})")