import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor

def test_bank_statement_processor():
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
    
    print("Starting Bank Statement Processor test...")
    
    try:
        # Initialize connections
        print("\n1. Setting up connections...")
        gs_connection = GoogleSheetsConnection(CREDENTIALS_FILE)
        processor = BankStatementProcessor(gs_connection)
        print("✓ Connections initialized successfully!")
        
        # Process all statements
        print("\n2. Processing bank statements...")
        processed_data = processor.process_all_statements(SPREADSHEET_ID, SHEETS_FILE)
        print("✓ All statements processed successfully!")
        
        # Generate and print summary
        print("\n3. Generating summary statistics...")
        summary = processor.get_summary_statistics()
        print("\nSummary Statistics:")
        print(f"Total Transactions: {summary['total_transactions']}")
        print(f"Date Range: {summary['date_range']['start']} to {summary['date_range']['end']}")
        print(f"Total Income: £{summary['total_income']:,.2f}")
        print(f"Total Expenses: £{summary['total_expenses']:,.2f}")
        print(f"Net Position: £{summary['net_position']:,.2f}")
        
        # Export to Excel
        print("\n4. Exporting to Excel...")
        processor.export_to_excel('test_processed_statements.xlsx')
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_bank_statement_processor()
    
    if success:
        print("\n✓ All tests completed successfully!")
    else:
        print("\n❌ Tests failed!")