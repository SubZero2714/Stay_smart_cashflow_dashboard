import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor
from src.deposit_categorizer import DepositCategorizer

def test_deposit_categorizer():
    # Constants
    CREDENTIALS_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "creds",
        "credentials.json"
    )
    SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
    SHEETS_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "Text_files",
        "Column_uniformity_sheets_to_update.txt"
    )
    
    print("=== Starting Deposit Categorizer Test ===\n")
    
    try:
        # 1. Initialize connections and process bank statements
        print("1. Setting up connections and processing bank statements...")
        gs_connection = GoogleSheetsConnection(CREDENTIALS_FILE)
        processor = BankStatementProcessor(gs_connection)
        processor.process_all_statements(SPREADSHEET_ID, SHEETS_FILE)
        print("✓ Bank statements processed successfully!")
        
        # 2. Initialize and run deposit categorization
        print("\n2. Running deposit categorization...")
        categorizer = DepositCategorizer(processor)
        summary = categorizer.categorize_deposits()
        
        # 3. Print deposit analysis summary
        print("\n3. Deposit Analysis Summary:")
        print(f"Total Deposits Collected: {summary['total_deposits_collected']}")
        print(f"Total Deposits Returned: {summary['total_deposits_returned']}")
        print(f"Total Deposit Amount: £{summary['total_deposit_amount']:,.2f}")
        print(f"Total Return Amount: £{summary['total_return_amount']:,.2f}")
        print(f"Matched Deposits: {summary['matched_deposits']}")
        print(f"Unmatched Deposits: {summary['unmatched_deposits']}")
        print(f"Transactions Needing Review: {summary['transactions_needing_review']}")
        
        # 4. Export detailed analysis
        print("\n4. Exporting detailed analysis...")
        categorizer.export_deposit_analysis('test_deposit_analysis.xlsx')
        
        # 5. Perform validation checks
        print("\n5. Running validation checks...")
        
        # Check if total deposits and returns match
        deposit_diff = abs(summary['total_deposit_amount'] - summary['total_return_amount'])
        if deposit_diff > 0:
            print(f"⚠️ Warning: Difference in deposit and return amounts: £{deposit_diff:,.2f}")
        else:
            print("✓ Deposit and return amounts match!")
        
        # Check for unmatched deposits
        if summary['unmatched_deposits'] > 0:
            print(f"⚠️ Warning: {summary['unmatched_deposits']} deposits have no matching returns")
        else:
            print("✓ All deposits have matching returns!")
        
        # Check for suspicious transactions
        if summary['transactions_needing_review'] > 0:
            print(f"⚠️ Warning: {summary['transactions_needing_review']} transactions need review")
        else:
            print("✓ No suspicious transactions found!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        return False

def run_specific_tests():
    """Run specific test cases"""
    print("\n=== Running Specific Test Cases ===")
    
    # Add the constants inside the function
    CREDENTIALS_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "creds",
        "credentials.json"
    )
    SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
    SHEETS_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "Text_files",
        "Column_uniformity_sheets_to_update.txt"
    )
    
    try:
        # Initialize components
        gs_connection = GoogleSheetsConnection(CREDENTIALS_FILE)
        processor = BankStatementProcessor(gs_connection)
        processor.process_all_statements(SPREADSHEET_ID, SHEETS_FILE)
        categorizer = DepositCategorizer(processor)
        categorizer.categorize_deposits()
        
        # Test case 1: Check deposit amounts
        print("\nTest Case 1: Checking deposit amounts...")
        invalid_deposits = categorizer.data[
            (categorizer.data['Paid In (£)'].notnull()) &
            (categorizer.data['Paid In (£)'] > 0) &
            (categorizer.data['Transaction'].str.lower().str.contains('deposit', na=False)) &
            (~categorizer.data['Paid In (£)'].isin([50.0, 100.0]))
        ]
        if len(invalid_deposits) > 0:
            print(f"⚠️ Found {len(invalid_deposits)} deposits with non-standard amounts")
            print("Sample of invalid deposits:")
            print(invalid_deposits[['Date', 'Transaction', 'Paid In (£)']].head())
        else:
            print("✓ All deposits have standard amounts (£50 or £100)")
        
        # Test case 2: Check return timing
        print("\nTest Case 2: Checking deposit return timing...")
        if categorizer.matched_deposits is not None and not categorizer.matched_deposits.empty:
            late_returns = categorizer.matched_deposits[
                categorizer.matched_deposits['Days_to_Return'] > 60
            ]
            if len(late_returns) > 0:
                print(f"⚠️ Found {len(late_returns)} deposits returned after 60 days")
                print("Sample of late returns:")
                print(late_returns[['Deposit_Date', 'Return_Date', 'Days_to_Return']].head())
            else:
                print("✓ All deposits returned within 60 days")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error in specific tests: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_deposit_categorizer()
    
    if success:
        print("\n✓ Main deposit categorization test completed successfully!")
        # Run additional specific tests
        specific_success = run_specific_tests()
        if specific_success:
            print("\n✓ All specific tests completed successfully!")
        else:
            print("\n❌ Some specific tests failed!")
    else:
        print("\n❌ Main deposit categorization test failed!")