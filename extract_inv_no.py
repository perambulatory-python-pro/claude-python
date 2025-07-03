import re

def extract_invoice_number(pdf_filename):
    """
    Extract invoice number from PDF filename
    Expected format: invoiceNumber_YYYYMMDD.pdf
    """
    # Use regex to find the pattern: everything before the underscore
    # Pattern explanation: ^(.+?)_ means "capture everything from start until first underscore"
    match = re.match(r'^(.+?)_\d{8}\.pdf$', pdf_filename, re.IGNORECASE)
    
    if match:
        return match.group(1)  # Return the captured group (invoice number)
    else:
        return None  # Filename doesn't match expected pattern

# Test our extraction function
test_files = [
    "40011284_20250501.pdf",
    "40010698_20250417.pdf", 
    "40010702_20250417.pdf"
]

print("Testing invoice number extraction:")
for filename in test_files:
    invoice_num = extract_invoice_number(filename)
    print(f"  {filename} → Invoice: {invoice_num}")