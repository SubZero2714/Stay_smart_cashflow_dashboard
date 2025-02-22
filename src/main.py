from datetime import datetime
from pathlib import Path
import sys
import pandas as pd

from google_sheets_connection import GoogleSheetsConnection
from bank_statement_processor import BankStatementProcessor
from categorisation import Categorisation
from deposit_categorizer import DepositCategorizer
from utils.error_handler import handle_error

def main():
    """
    Main processing pipeline for bank statement categorization
    """
    try:
        # Initialize timestamp for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup paths
        project_root = Path(__file__).parent.parent
        credentials_file = project_root / 'creds' / 'credentials.json'
        sheets_list_file = project_root / 'Text_files' / 'Column_uniformity_sheets_to_update.txt'
        output_dir = project_root / 'output' / f'run_{timestamp}'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print("\n🚀 Starting bank statement processing...")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. Initialize Google Sheets connection
        print("\n1️⃣ Initializing Google Sheets connection...")
        gs_connection = GoogleSheetsConnection(str(credentials_file))
        
        # 2. Clear existing categorization
        print("\n2️⃣ Clearing existing categorization...")
        gs_connection.clear_categorization_columns(SPREADSHEET_ID, str(sheets_list_file))
        
        # 3. Process bank statements
        print("\n3️⃣ Processing bank statements...")
        processor = BankStatementProcessor(gs_connection)
        processor.process_all_statements(SPREADSHEET_ID, str(sheets_list_file))
        
        # 4. Apply categorization
        print("\n4️⃣ Applying transaction categorization...")
        categorizer = Categorisation(processor)
        categorized_data = categorizer.apply_categorization(SPREADSHEET_ID, "Keyword Mapping")
        
        # 5. Handle deposits
        print("\n5️⃣ Processing deposits...")
        deposit_handler = DepositCategorizer(processor)
        deposit_handler.categorize_deposits()
        
        # 6. Export results
        print("\n6️⃣ Exporting results...")
        
        # Main processed data
        processor.export_to_excel(
            output_dir / 'processed_statements.xlsx'
        )
        
        # Removed rows analysis
        processor.export_removed_rows(
            output_dir / 'removed_rows_analysis.xlsx'
        )
        
        # Deposit analysis
        deposit_handler.export_deposit_analysis(
            output_dir / 'deposit_analysis.xlsx'
        )
        
        # Processing summary
        _export_processing_summary(
            output_dir / 'processing_summary.xlsx',
            processor,
            deposit_handler,
            timestamp
        )
        
        print(f"\n✅ Processing complete! Results saved in: {output_dir}")
        
    except Exception as e:
        handle_error(e, "main", "main.py")
        sys.exit(1)

def _export_processing_summary(output_file, processor, deposit_handler, timestamp):
    """Export processing summary with statistics"""
    try:
        summary_data = {
            'Processing Time': [timestamp],
            'Total Transactions': [len(processor.processed_data)],
            'Removed Rows': [len(processor.removed_rows) if processor.removed_rows is not None else 0],
            'Matched Deposits': [len(deposit_handler.matched_deposits)],
            'Unmatched Deposits': [len(deposit_handler.unmatched_deposits)],
            'Unmatched Returns': [len(deposit_handler.unmatched_returns)],
            'Issues Found': [len(deposit_handler.deposit_issues)]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(output_file, index=False)
        print(f"✓ Processing summary exported to {output_file}")
        
    except Exception as e:
        handle_error(e, "_export_processing_summary", "main.py")
        raise

if __name__ == "__main__":
    main()
