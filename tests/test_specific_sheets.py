import pytest
from pathlib import Path
from datetime import datetime

# Fix the imports
from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor
from src.categorisation import Categorisation
from src.deposit_categorizer import DepositCategorizer  # Make sure this matches exactly

# Add this if needed
SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"  # Your spreadsheet ID

class TestSpecificSheets:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment"""
        try:
            # Setup paths
            self.project_root = Path(__file__).parent.parent
            self.credentials_file = self.project_root / 'creds' / 'credentials.json'
            self.test_sheets_file = self.project_root / 'Text_files' / 'testing_files.txt'
            
            # Create test sheets file if it doesn't exist
            if not self.test_sheets_file.exists():
                with open(self.test_sheets_file, 'w') as f:
                    f.write("01_Sep_2024_to_30_Sep_2024\n")  # First test sheet
                    f.write("02_Oct_2024_to_01_Nov_2024\n")  # Second test sheet
            
            # Create test output directory
            self.output_dir = self.project_root / 'test_output'
            self.output_dir.mkdir(exist_ok=True)
            
            # Initialize components
            self.gs_connection = GoogleSheetsConnection(str(self.credentials_file))
            self.processor = BankStatementProcessor(self.gs_connection)
            
            # Process specific sheets
            with open(self.test_sheets_file, 'r') as f:
                self.test_sheets = [line.strip() for line in f if line.strip()]
            
            print(f"\nTesting with sheets: {self.test_sheets}")
            
            yield
            
        except Exception as e:
            pytest.fail(f"Test setup failed: {str(e)}")
    
    def test_specific_sheets_processing(self):
        """Test processing of specific sheets"""
        try:
            # Process sheets
            processed_data = self.processor.process_all_statements(
                SPREADSHEET_ID,
                str(self.test_sheets_file)
            )
            
            # Basic validations
            assert processed_data is not None, "No data processed"
            assert not processed_data.empty, "Processed data is empty"
            
            # Check source sheet names
            unique_sources = processed_data['Source_Sheet'].unique()
            assert len(unique_sources) <= len(self.test_sheets), "Too many source sheets"
            assert all(sheet in self.test_sheets for sheet in unique_sources), "Unknown source sheet"
            
            # Export results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_dir / f'test_processed_statements_{timestamp}.xlsx'
            self.processor.export_to_excel(output_file)
            
            print(f"\nResults exported to: {output_file}")
            
            # Print some statistics
            print("\nProcessing Statistics:")
            print(f"Total rows processed: {len(processed_data)}")
            print(f"Unique source sheets: {list(unique_sources)}")
            print(f"Rows per sheet:")
            for sheet in unique_sources:
                count = len(processed_data[processed_data['Source_Sheet'] == sheet])
                print(f"  {sheet}: {count} rows")
            
        except Exception as e:
            pytest.fail(f"Test failed: {str(e)}")