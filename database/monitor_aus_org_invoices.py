def monitor_org_invoices(file_path: str) -> pd.DataFrame:
    """
    Monitor -ORG invoices over time
    
    Returns DataFrame with weekly summary
    """
    df = pd.read_csv(file_path)
    
    # Filter for -ORG invoices
    org_df = df[df['Invoice Number'].str.contains('-ORG', na=False)].copy()
    
    if len(org_df) == 0:
        print("No -ORG invoices found!")
        return pd.DataFrame()
    
    # Convert Week Ending to datetime
    org_df['Week Ending'] = pd.to_datetime(org_df['Week Ending'])
    
    # Create weekly summary
    weekly_summary = org_df.groupby(org_df['Week Ending'].dt.to_period('W')).agg({
        'Invoice Number': 'nunique',
        'Bill Amount': 'sum',
        'Hours': 'sum',
        'Employee Number': 'nunique'
    }).round(2)
    
    weekly_summary.columns = ['Unique_Invoices', 'Total_Amount', 'Total_Hours', 'Unique_Employees']
    
    print("\nWeekly -ORG Invoice Summary:")
    print(weekly_summary)
    
    # Save to CSV
    weekly_summary.to_csv('org_invoice_weekly_summary.csv')
    
    return weekly_summary

# Run monitoring
monitor_org_invoices('invoice_details_aus.csv')