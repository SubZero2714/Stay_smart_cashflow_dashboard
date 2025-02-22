import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from src.google_sheets_connection import GoogleSheetsConnection

class TestDataGenerator:
    def __init__(self, gs_connection):
        self.gs_connection = gs_connection
        self.spreadsheet_id = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
        self.test_data = None
        
    def generate_test_transactions(self):
        """Generate test transactions with various deposit scenarios"""
        
        # Test scenarios for deposits
        deposit_scenarios = [
            # Standard deposit cases
            {
                'desc': 'DEPOSIT FROM JOHN £50',
                'amount_in': 50.0,
                'amount_out': 0.0,
                'expected_category': 'Deposit'
            },
            {
                'desc': 'SECURITY DEPOSIT £100',
                'amount_in': 100.0,
                'amount_out': 0.0,
                'expected_category': 'Deposit'
            },
            
            # Standard return cases
            {
                'desc': 'DEPOSIT RETURN TO JOHN',
                'amount_in': 0.0,
                'amount_out': 50.0,
                'expected_category': 'Deposit Return'
            },
            {
                'desc': 'REFUND SECURITY DEPOSIT',
                'amount_in': 0.0,
                'amount_out': 100.0,
                'expected_category': 'Deposit Return'
            },
            
            # Edge cases - Invalid amounts
            {
                'desc': 'DEPOSIT PAYMENT',
                'amount_in': 75.0,  # Non-standard amount
                'amount_out': 0.0,
                'expected_category': 'Review Required'
            },
            {
                'desc': 'DEPOSIT REFUND',
                'amount_in': 0.0,
                'amount_out': 25.0,  # Non-standard amount
                'expected_category': 'Review Required'
            },
            
            # Edge cases - Description mismatches
            {
                'desc': 'DEPOSIT FROM JANE',
                'amount_in': 150.0,  # Wrong amount
                'amount_out': 0.0,
                'expected_category': 'Review Required'
            },
            {
                'desc': 'PAYMENT DEPOSIT RELATED',
                'amount_in': 50.0,
                'amount_out': 50.0,  # Both in and out
                'expected_category': 'Review Required'
            },
            
            # Regular transactions (non-deposits)
            {
                'desc': 'COSTA LOCUS CLEANING',
                'amount_in': 0.0,
                'amount_out': 75.0,
                'expected_category': ''  # Should be set by keyword mapping
            },
            {
                'desc': 'CARMEN ORTIZ GIRON',
                'amount_in': 120.0,
                'amount_out': 0.0,
                'expected_category': ''  # Should be set by keyword mapping
            }
        ]
        
        # Generate dates starting from today
        start_date = datetime.now()
        transactions = []
        
        for i, scenario in enumerate(deposit_scenarios):
            date = start_date + timedelta(days=i)
            
            transactions.append({
                'Date': date.strftime('%d/%m/%Y'),
                'Transaction': scenario['desc'],
                'Paid In (£)': scenario['amount_in'],
                'Withdrawn (£)': scenario['amount_out'],
                'Balance (£)': 0.0,  # Will calculate later
                'Notes': '',
                'Subcategory': '',
                'Expected_Category': scenario['expected_category']  # For validation
            })
        
        # Add some random regular transactions
        for i in range(20):
            date = start_date + timedelta(days=len(deposit_scenarios) + i)
            amount = round(random.uniform(10, 1000), 2)
            
            transactions.append({
                'Date': date.strftime('%d/%m/%Y'),
                'Transaction': f'TEST TRANSACTION {i+1}',
                'Paid In (£)': amount if random.choice([True, False]) else 0.0,
                'Withdrawn (£)': 0.0 if amount > 0 else amount,
                'Balance (£)': 0.0,
                'Notes': '',
                'Subcategory': '',
                'Expected_Category': ''
            })
        
        # Create DataFrame
        df = pd.DataFrame(transactions)
        
        # Calculate running balance
        balance = 1000.0  # Starting balance
        for idx in df.index:
            balance = balance + df.at[idx, 'Paid In (£)'] - df.at[idx, 'Withdrawn (£)']
            df.at[idx, 'Balance (£)'] = round(balance, 2)
        
        self.test_data = df
        return df

    def generate_controlled_data(self, output_file):
        """Generate controlled test data with known scenarios"""
        print("\n📊 Generating controlled test data...")
        
        # Your existing controlled data generation code
        # ... (keep the controlled test data as is)
        
    def sample_real_transactions(self, output_file, sample_size=50):
        """
        Sample random transactions from actual Google Sheets data
        
        Args:
            output_file: Path to save sampled data
            sample_size: Number of transactions to sample (default 50)
        """
        try:
            print("\n🎲 Sampling real transaction data...")
            
            # Get all sheets to sample from
            sheets_file = 'Text_files/Column_uniformity_sheets_to_update.txt'
            with open(sheets_file, 'r') as f:
                sheet_names = [line.strip() for line in f if line.strip()]
            
            all_transactions = []
            
            # Collect transactions from each sheet
            for sheet_name in sheet_names:
                print(f"Reading from sheet: {sheet_name}")
                worksheet = self.gs_connection.get_worksheet(self.spreadsheet_id, sheet_name)
                if worksheet:
                    data = worksheet.get_all_records()
                    df = pd.DataFrame(data)
                    df['Source_Sheet'] = sheet_name
                    all_transactions.append(df)
            
            # Combine all transactions
            combined_df = pd.concat(all_transactions, ignore_index=True)
            
            # Randomly sample transactions
            sampled_data = combined_df.sample(n=min(sample_size, len(combined_df)), 
                                            random_state=42)  # for reproducibility
            
            # Sort by date
            sampled_data['Date'] = pd.to_datetime(sampled_data['Date'])
            sampled_data = sampled_data.sort_values('Date')
            
            # Reset balance for test data
            starting_balance = 10000.0
            sampled_data['Balance (£)'] = starting_balance
            for idx in sampled_data.index:
                paid_in = sampled_data.at[idx, 'Paid In (£)'] or 0
                withdrawn = sampled_data.at[idx, 'Withdrawn (£)'] or 0
                if idx == sampled_data.index[0]:
                    sampled_data.at[idx, 'Balance (£)'] = starting_balance + paid_in - withdrawn
                else:
                    prev_balance = sampled_data.at[sampled_data.index[idx-1], 'Balance (£)']
                    sampled_data.at[idx, 'Balance (£)'] = prev_balance + paid_in - withdrawn
            
            # Clear categorization
            sampled_data['Notes'] = ''
            sampled_data['Subcategory'] = ''
            
            # Save to CSV
            sampled_data.to_csv(output_file, index=False)
            print(f"✓ Sampled {len(sampled_data)} transactions saved to {output_file}")
            
            # Print sample statistics
            print("\n📊 Sample Statistics:")
            print(f"Total Transactions: {len(sampled_data)}")
            print(f"Date Range: {sampled_data['Date'].min().strftime('%d/%m/%Y')} to "
                  f"{sampled_data['Date'].max().strftime('%d/%m/%Y')}")
            print(f"Sheets Represented: {sampled_data['Source_Sheet'].nunique()}")
            print(f"Total Deposits (£50/£100): "
                  f"{len(sampled_data[sampled_data['Paid In (£)'].isin([50.0, 100.0])])}")
            
        except Exception as e:
            print(f"Error sampling transactions: {str(e)}")
            raise

def generate_test_data(gs_connection):
    """Generate both controlled and sampled test data"""
    generator = TestDataGenerator(gs_connection)
    
    # Generate controlled test data
    generator.generate_controlled_data('tests/test_data/test_data_controlled.csv')
    
    # Sample real transactions
    generator.sample_real_transactions('tests/test_data/test_data_sampled.csv')

if __name__ == "__main__":
    # Initialize Google Sheets connection
    credentials_file = 'creds/credentials.json'
    gs_connection = GoogleSheetsConnection(credentials_file)
    
    # Generate test data
    generate_test_data(gs_connection)