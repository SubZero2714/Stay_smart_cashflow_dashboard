import pandas as pd
from google.oauth2.credentials import Credentials
import gspread
from pathlib import Path
import random
from datetime import datetime, timedelta

# Constants
SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
SHEET_NAMES = [
    '01_Apr_2023_to_30_Apr_2023',
    '01_May_2023_to_31_May_2023',
    '01_Jun_2023_to_30_Jun_2023',
    # ... rest of your sheets
]

def create_controlled_dataset():
    """Create controlled test data with specific scenarios"""
    data = {
        'Date': [],
        'Transaction': [],
        'Paid In (£)': [],
        'Withdrawn (£)': [],
        'Balance (£)': [],
        'Notes': [],
        'Subcategory': []
    }
    
    # 1. Deposit Scenarios (20 rows)
    deposits_50 = [
        ('2024-01-01', 'DEPOSIT RECEIVED FROM JOHN', 50.00, None),
        ('2024-01-05', 'ROOM DEPOSIT 101', 50.00, None),
        ('2024-01-10', 'SECURITY DEPOSIT FLAT 3', 50.00, None),
        ('2024-01-15', 'DEPOSIT PAYMENT R15', 50.00, None),
        ('2024-01-20', 'DEPOSIT ROOM 7', 50.00, None)
    ]
    
    deposits_100 = [
        ('2024-01-02', 'DAMAGE DEPOSIT 205', 100.00, None),
        ('2024-01-07', 'SECURITY DEPOSIT APT 12', 100.00, None),
        ('2024-01-12', 'DEPOSIT RECEIVED FROM SMITH', 100.00, None),
        ('2024-01-17', 'ROOM DEPOSIT 303', 100.00, None),
        ('2024-01-22', 'DEPOSIT FLAT 8', 100.00, None)
    ]
    
    deposit_returns_50 = [
        ('2024-02-01', 'DEPOSIT REFUND TO JOHN', None, 50.00),
        ('2024-02-05', 'DEPOSIT RETURN R101', None, 50.00),
        ('2024-02-10', 'SECURITY DEPOSIT RETURN F3', None, 50.00),
        ('2024-02-15', 'DEPOSIT REFUND R15', None, 50.00),
        ('2024-02-20', 'DEPOSIT RETURN ROOM 7', None, 50.00)
    ]
    
    deposit_returns_100 = [
        ('2024-02-02', 'DAMAGE DEPOSIT RETURN 205', None, 100.00),
        ('2024-02-07', 'DEPOSIT REFUND APT 12', None, 100.00),
        ('2024-02-12', 'DEPOSIT RETURN TO SMITH', None, 100.00),
        ('2024-02-17', 'DEPOSIT REFUND 303', None, 100.00),
        ('2024-02-22', 'DEPOSIT RETURN F8', None, 100.00)
    ]
    
    # 2. Regular Transactions (40 rows)
    groceries = [
        ('2024-01-03', 'TESCO SUPERSTORE', None, 45.67),
        ('2024-01-08', 'ASDA SUPERMARKET', None, 32.50),
        ('2024-01-13', 'SAINSBURYS LOCAL', None, 28.99),
        ('2024-01-18', 'LIDL GB', None, 25.75),
        ('2024-01-23', 'ALDI STORES', None, 30.25),
        ('2024-01-28', 'MORRISONS STORE', None, 42.80),
        ('2024-02-03', 'TESCO EXPRESS', None, 15.99),
        ('2024-02-08', 'ASDA GEORGE', None, 22.50)
    ]
    
    utilities = [
        ('2024-01-04', 'BRITISH GAS', None, 85.00),
        ('2024-01-09', 'WATER PLUS', None, 45.00),
        ('2024-01-14', 'EDF ENERGY', None, 65.50),
        ('2024-01-19', 'VIRGIN MEDIA', None, 52.99),
        ('2024-01-24', 'SCOTTISH POWER', None, 78.25),
        ('2024-01-29', 'ANGLIAN WATER', None, 38.50),
        ('2024-02-04', 'BT GROUP PLC', None, 45.99),
        ('2024-02-09', 'SSE ENERGY', None, 72.75)
    ]
    
    shopping = [
        ('2024-01-06', 'AMAZON MARKETPLACE', None, 23.99),
        ('2024-01-11', 'EBAY PAYMENT', None, 15.50),
        ('2024-01-16', 'AMAZON PRIME', None, 7.99),
        ('2024-01-21', 'AMAZON.CO.UK', None, 34.50),
        ('2024-01-26', 'EBAY ITEM', None, 12.99),
        ('2024-01-31', 'AMAZON DIGITAL', None, 9.99),
        ('2024-02-06', 'AMAZON RETAIL', None, 28.50),
        ('2024-02-11', 'EBAY PURCHASE', None, 19.99)
    ]
    
    entertainment = [
        ('2024-01-05', 'NETFLIX.COM', None, 13.99),
        ('2024-01-10', 'SPOTIFY', None, 9.99),
        ('2024-01-15', 'DISNEY PLUS', None, 7.99),
        ('2024-01-20', 'PRIME VIDEO', None, 5.99),
        ('2024-01-25', 'APPLE TV', None, 6.99),
        ('2024-01-30', 'NOW TV', None, 11.99),
        ('2024-02-05', 'YOUTUBE PREMIUM', None, 11.99),
        ('2024-02-10', 'NETFLIX.COM', None, 13.99)
    ]
    
    transport = [
        ('2024-01-03', 'SHELL FUELS', None, 65.50),
        ('2024-01-08', 'TFL TRAVEL', None, 35.00),
        ('2024-01-13', 'TRAINLINE', None, 45.80),
        ('2024-01-18', 'BP FUEL', None, 70.25),
        ('2024-01-23', 'BUS TICKET', None, 25.00),
        ('2024-01-28', 'ESSO PETROL', None, 68.75),
        ('2024-02-03', 'RAIL TICKET', None, 32.50),
        ('2024-02-08', 'UBER TRIP', None, 15.99)
    ]
    
    # 3. Edge Cases (20 rows)
    multiple_categories = [
        ('2024-01-04', 'AMAZON PRIME TESCO', None, 25.99),
        ('2024-01-09', 'SHELL GARAGE SHOP', None, 15.50),
        ('2024-01-14', 'ASDA FUEL STATION', None, 55.75),
        ('2024-01-19', 'TESCO MOBILE', None, 12.99),
        ('2024-01-24', 'BP SHOP', None, 8.99)
    ]
    
    uncategorized = [
        ('2024-01-06', 'PAYMENT REFERENCE XYZ', None, 45.00),
        ('2024-01-11', 'TRANSFER 123456', None, 30.00),
        ('2024-01-16', 'LOCAL SHOP PAYMENT', None, 22.50),
        ('2024-01-21', 'MISC PAYMENT', None, 18.75),
        ('2024-01-26', 'UNKNOWN VENDOR', None, 15.99)
    ]
    
    special_chars = [
        ('2024-01-07', 'PAYMENT @ STORE', None, 12.99),
        ('2024-01-12', 'SHOP & SAVE', None, 25.50),
        ('2024-01-17', 'STORE #123', None, 18.99),
        ('2024-01-22', 'CAFÉ PAYMENT', None, 8.50),
        ('2024-01-27', 'STORE-MART', None, 22.99)
    ]
    
    zero_amount = [
        ('2024-01-08', 'PENDING TRANSACTION', 0.00, None),
        ('2024-01-13', 'ZERO VALUE TRANSFER', None, 0.00),
        ('2024-01-18', 'ADJUSTMENT', 0.00, 0.00),
        ('2024-01-23', 'BALANCE CHECK', None, 0.00),
        ('2024-01-28', 'PENDING DEPOSIT', 0.00, None)
    ]
    
    # 4. Specific Test Cases (20 rows)
    matching_deposits = [
        ('2024-03-01', 'DEPOSIT FROM TENANT A', 50.00, None),
        ('2024-03-05', 'DEPOSIT RETURN TENANT A', None, 50.00),
        ('2024-03-10', 'DEPOSIT FROM TENANT B', 100.00, None),
        ('2024-03-15', 'DEPOSIT RETURN TENANT B', None, 100.00),
        ('2024-03-20', 'DEPOSIT FROM TENANT C', 50.00, None),
        ('2024-03-25', 'DEPOSIT RETURN TENANT C', None, 50.00),
        ('2024-03-02', 'DEPOSIT FROM TENANT D', 100.00, None),
        ('2024-03-07', 'DEPOSIT RETURN TENANT D', None, 100.00),
        ('2024-03-12', 'DEPOSIT FROM TENANT E', 50.00, None),
        ('2024-03-17', 'DEPOSIT RETURN TENANT E', None, 50.00)
    ]
    
    with_notes = [
        ('2024-01-09', 'MAINTENANCE PAYMENT', None, 150.00, 'Emergency repair'),
        ('2024-01-14', 'INSURANCE PREMIUM', None, 200.00, 'Annual renewal'),
        ('2024-01-19', 'CONTRACTOR PAYMENT', None, 350.00, 'Kitchen renovation'),
        ('2024-01-24', 'SERVICE CHARGE', None, 175.00, 'Quarterly payment'),
        ('2024-01-29', 'LEGAL FEES', None, 250.00, 'Contract review')
    ]
    
    with_subcategories = [
        ('2024-01-10', 'COUNCIL TAX PAYMENT', None, 145.00, '', 'Council Tax'),
        ('2024-01-15', 'MORTGAGE PAYMENT', None, 750.00, '', 'Mortgage'),
        ('2024-01-20', 'LETTING AGENT FEE', None, 120.00, '', 'Agency Fees'),
        ('2024-01-25', 'BUILDINGS INSURANCE', None, 45.00, '', 'Insurance'),
        ('2024-01-30', 'GROUND RENT', None, 100.00, '', 'Ground Rent')
    ]
    
    # Combine all transactions
    all_transactions = (
        deposits_50 + deposits_100 + deposit_returns_50 + deposit_returns_100 +
        groceries + utilities + shopping + entertainment + transport +
        multiple_categories + uncategorized + special_chars + zero_amount +
        matching_deposits + 
        [(date, desc, paid_in, withdrawn, note, '') for date, desc, paid_in, withdrawn, note in with_notes] +
        with_subcategories
    )
    
    # Sort by date and calculate running balance
    all_transactions.sort(key=lambda x: x[0])
    balance = 2000.00  # Starting balance
    
    for transaction in all_transactions:
        date = transaction[0]
        desc = transaction[1]
        paid_in = transaction[2] if len(transaction) > 2 else None
        withdrawn = transaction[3] if len(transaction) > 3 else None
        note = transaction[4] if len(transaction) > 4 else ''
        subcategory = transaction[5] if len(transaction) > 5 else ''
        
        data['Date'].append(date)
        data['Transaction'].append(desc)
        data['Paid In (£)'].append(paid_in if paid_in is not None else '')
        data['Withdrawn (£)'].append(withdrawn if withdrawn is not None else '')
        balance = balance + (paid_in or 0) - (withdrawn or 0)
        data['Balance (£)'].append(f"{balance:.2f}")
        data['Notes'].append(note)
        data['Subcategory'].append(subcategory)
    
    df = pd.DataFrame(data)
    return df

def get_random_sample():
    """Get random sample from existing sheets"""
    try:
        # Initialize Google Sheets connection
        credentials_file = str(Path.cwd() / 'creds' / 'credentials.json')
        gc = gspread.service_account(filename=credentials_file)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        all_data = []
        print("\nCollecting data from sheets...")
        
        # Collect data from each sheet
        for sheet_name in SHEET_NAMES:
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                data = worksheet.get_all_values()
                headers = data[0]
                rows = data[1:]  # Skip header row
                df = pd.DataFrame(rows, columns=headers)
                all_data.append(df)
                print(f"✓ Collected data from {sheet_name}")
            except Exception as e:
                print(f"Error collecting from {sheet_name}: {str(e)}")
        
        # Combine all data
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # Select random 100 rows
        random_sample = combined_data.sample(n=100, random_state=42)
        return random_sample
        
    except Exception as e:
        print(f"Error creating random sample: {str(e)}")
        return None

def main():
    # Create output directory
    output_dir = Path('test_data')
    output_dir.mkdir(exist_ok=True)
    
    # Create controlled dataset
    print("\nCreating controlled test dataset...")
    controlled_df = create_controlled_dataset()
    controlled_df.to_csv(output_dir / 'test_data_controlled.csv', index=False)
    print("✓ Controlled dataset created")
    
    # Create random sample
    print("\nCreating random sample dataset...")
    random_df = get_random_sample()
    if random_df is not None:
        random_df.to_csv(output_dir / 'test_data_random.csv', index=False)
        print("✓ Random sample dataset created")
    
    print("\nDatasets created in test_data directory")
    print("Next steps:")
    print("1. Upload test_data_controlled.csv as 'Test_Data_Controlled'")
    print("2. Upload test_data_random.csv as 'Test_Data_Random'")
    print("3. Update test_small_dataset.py with these sheet names")

if __name__ == "__main__":
    main()


