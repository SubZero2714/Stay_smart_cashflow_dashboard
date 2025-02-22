import click
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor
from src.categorisation import Categorisation
from src.deposit_categorizer import DepositCategorizer
from tests.run_tests import TestRunner

@click.group()
def cli():
    """Bank Statement Processing CLI"""
    pass

@cli.command()
@click.option('--test-mode', is_flag=True, help='Run in test mode')
def process_all(test_mode):
    """Process all bank statements with categorization"""
    try:
        print("\n🚀 Starting bank statement processing...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize components
        credentials_file = project_root / 'creds' / 'credentials.json'
        spreadsheet_id = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
        sheets_list_file = project_root / 'Text_files' / 'Column_uniformity_sheets_to_update.txt'
        
        # Create output directory
        output_dir = project_root / 'output' / f'run_{timestamp}'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize connection
        gs_connection = GoogleSheetsConnection(str(credentials_file))
        
        if not test_mode:
            # Clear existing categorization
            print("\n1️⃣ Clearing existing categorization...")
            gs_connection.clear_categorization_columns(spreadsheet_id, str(sheets_list_file))
        
        # Process statements
        print("\n2️⃣ Processing bank statements...")
        processor = BankStatementProcessor(gs_connection)
        processor.process_all_statements(spreadsheet_id, str(sheets_list_file))
        
        # Apply categorization
        print("\n3️⃣ Applying transaction categorization...")
        categorizer = Categorisation(processor)
        categorized_data = categorizer.apply_categorization(spreadsheet_id, "Keyword Mapping")
        
        # Process deposits
        print("\n4️⃣ Processing deposits...")
        deposit_handler = DepositCategorizer(processor)
        deposit_handler.categorize_deposits()
        
        # Export results
        print("\n5️⃣ Exporting results...")
        _export_results(processor, deposit_handler, output_dir)
        
        print(f"\n✅ Processing complete! Results saved in: {output_dir}")
        
    except Exception as e:
        print(f"\n❌ Processing failed: {str(e)}")
        sys.exit(1)

@cli.command()
def run_tests():
    """Run complete test suite"""
    try:
        test_runner = TestRunner()
        test_runner.run_all_tests()
    except Exception as e:
        print(f"\n❌ Tests failed: {str(e)}")
        sys.exit(1)

@cli.command()
def generate_test_data():
    """Generate test data sets"""
    try:
        from tests.test_data_generator import generate_test_data
        
        # Initialize Google Sheets connection
        credentials_file = project_root / 'creds' / 'credentials.json'
        gs_connection = GoogleSheetsConnection(str(credentials_file))
        
        # Generate test data
        generate_test_data(gs_connection)
        
    except Exception as e:
        print(f"\n❌ Test data generation failed: {str(e)}")
        sys.exit(1)

def _export_results(processor, deposit_handler, output_dir):
    """Export all processing results"""
    try:
        # Main processed data
        processor.export_to_excel(
            output_dir / 'processed_statements.xlsx'
        )
        
        # Removed rows analysis
        if processor.removed_rows is not None:
            processor.export_removed_rows(
                output_dir / 'removed_rows_analysis.xlsx'
            )
        
        # Deposit analysis
        deposit_handler.export_deposit_analysis(
            output_dir / 'deposit_analysis.xlsx'
        )
        
        # Generate summary report
        _generate_summary_report(processor, deposit_handler, output_dir)
        
    except Exception as e:
        print(f"Error exporting results: {str(e)}")
        raise

def _generate_summary_report(processor, deposit_handler, output_dir):
    """Generate processing summary report"""
    try:
        with open(output_dir / 'processing_summary.txt', 'w') as f:
            f.write("Bank Statement Processing Summary\n")
            f.write("=" * 30 + "\n\n")
            
            f.write(f"Processing Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Transactions: {len(processor.processed_data)}\n")
            f.write(f"Removed Rows: {len(processor.removed_rows) if processor.removed_rows is not None else 0}\n")
            f.write(f"Matched Deposits: {len(deposit_handler.matched_deposits)}\n")
            f.write(f"Unmatched Deposits: {len(deposit_handler.unmatched_deposits)}\n")
            f.write(f"Unmatched Returns: {len(deposit_handler.unmatched_returns)}\n")
            f.write(f"Issues Found: {len(deposit_handler.deposit_issues)}\n")
            
    except Exception as e:
        print(f"Error generating summary report: {str(e)}")
        raise

if __name__ == "__main__":
    cli() 