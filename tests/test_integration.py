import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor
from src.categorisation import Categorisation
from src.deposit_categorizer import DepositCategorizer

class TestIntegration:
    @classmethod
    def setup_class(cls):
        """Setup test environment"""
        cls.credentials_file = project_root / 'creds' / 'credentials.json'
        cls.test_spreadsheet_id = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
        cls.test_sheets_file = project_root / 'Text_files' / 'Column_uniformity_sheets_to_update.txt'
        
        # Create test output directory
        cls.test_output_dir = project_root / 'test_output'
        cls.test_output_dir.mkdir(exist_ok=True)
        
    def test_full_processing_pipeline(self):
        """Test complete processing pipeline"""
        try:
            print("\n🧪 Starting integration test...")
            
            # 1. Test Google Sheets Connection
            print("\n1️⃣ Testing Google Sheets connection...")
            gs_connection = GoogleSheetsConnection(str(self.credentials_file))
            assert gs_connection is not None, "Failed to create Google Sheets connection"
            
            # 2. Test Clear Categorization
            print("\n2️⃣ Testing categorization clearing...")
            gs_connection.clear_categorization_columns(
                self.test_spreadsheet_id, 
                str(self.test_sheets_file)
            )
            
            # 3. Test Bank Statement Processing
            print("\n3️⃣ Testing bank statement processing...")
            processor = BankStatementProcessor(gs_connection)
            processor.process_all_statements(
                self.test_spreadsheet_id, 
                str(self.test_sheets_file)
            )
            assert not processor.processed_data.empty, "No data was processed"
            
            # 4. Test Categorization
            print("\n4️⃣ Testing transaction categorization...")
            categorizer = Categorisation(processor)
            categorized_data = categorizer.apply_categorization(
                self.test_spreadsheet_id, 
                "Keyword Mapping"
            )
            assert not categorized_data.empty, "No data was categorized"
            
            # 5. Test Deposit Processing
            print("\n5️⃣ Testing deposit processing...")
            deposit_handler = DepositCategorizer(processor)
            deposit_handler.categorize_deposits()
            
            # 6. Test Data Export
            print("\n6️⃣ Testing data export...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Export and verify main data
            main_output = self.test_output_dir / f'test_processed_{timestamp}.xlsx'
            processor.export_to_excel(main_output)
            assert main_output.exists(), "Main output file not created"
            
            # Export and verify deposit analysis
            deposit_output = self.test_output_dir / f'test_deposits_{timestamp}.xlsx'
            deposit_handler.export_deposit_analysis(deposit_output)
            assert deposit_output.exists(), "Deposit analysis file not created"
            
            # 7. Validate Results
            print("\n7️⃣ Validating results...")
            self._validate_results(main_output, deposit_output)
            
            print("\n✅ Integration test completed successfully!")
            return True
            
        except Exception as e:
            print(f"\n❌ Integration test failed: {str(e)}")
            raise
            
    def _validate_results(self, main_output, deposit_output):
        """Validate output files and data integrity"""
        try:
            # Check main output
            main_data = pd.read_excel(main_output)
            assert 'Date' in main_data.columns, "Date column missing"
            assert 'Transaction' in main_data.columns, "Transaction column missing"
            assert 'Notes' in main_data.columns, "Notes column missing"
            assert 'Subcategory' in main_data.columns, "Subcategory column missing"
            
            # Check deposit analysis
            deposit_data = pd.read_excel(deposit_output, sheet_name='Matched_Deposits')
            assert 'Deposit_Date' in deposit_data.columns, "Deposit_Date column missing"
            assert 'Return_Date' in deposit_data.columns, "Return_Date column missing"
            assert 'Amount' in deposit_data.columns, "Amount column missing"
            
            # Validate deposit amounts
            deposit_amounts = deposit_data['Amount'].unique()
            assert all(amt in [50.0, 100.0] for amt in deposit_amounts), \
                "Invalid deposit amounts found"
            
            print("✓ Data validation passed")
            
        except Exception as e:
            print(f"❌ Validation failed: {str(e)}")
            raise

def run_integration_test():
    """Run integration test"""
    test = TestIntegration()
    test.setup_class()
    success = test.test_full_processing_pipeline()
    return success

if __name__ == "__main__":
    success = run_integration_test()
    if not success:
        sys.exit(1) 