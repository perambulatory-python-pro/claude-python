# Quick check for processing logs
import pandas as pd

# Read the original file to compare
df = pd.read_csv('invoice_details_aus.csv')
print(f"Original file records: {len(df)}")

# Filter out -ORG
non_org = df[~df['Invoice Number'].str.contains('-ORG', na=False)]
print(f"Non-ORG records: {len(non_org)}")

# Check for duplicates
print(f"Duplicate invoice rows: {non_org.duplicated().sum()}")