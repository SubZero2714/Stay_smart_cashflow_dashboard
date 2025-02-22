import os
import sys
from pathlib import Path
import pandas as pd
import pytest
from datetime import datetime, timedelta

# Add the src directory to Python path
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'
sys.path.append(str(src_path))

# Now import our modules from src
from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor
from src.categorisation import Categorisation
from src.deposit_categorizer import DepositCategorizer

# Constants
SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
KEYWORD_MAPPING_SHEET = "Keyword Mapping"

def verify_sheets_exist(gs_connection, spreadsheet_id, sheets_list):
    """Verify all sheets in the list exist in the Google Sheet"""
    try:
        missing_sheets = []
        for sheet_name in sheets_list:
            try:
                worksheet = gs_connection.get_worksheet(spreadsheet_id, sheet_name)
                if worksheet is None:
                    missing_sheets.append(sheet_name)
            except Exception:
                missing_sheets.append(sheet_name)
        
        if missing_sheets:
            print("\nWarning: Following sheets not found:")
            for sheet in missing_sheets:
                print(f"- {sheet}")
            return False
        return True
    except Exception as e:
        print(f"Error verifying sheets: {str(e)}")
        return False

class TestProcessor:
    @pytest.fixture(autouse=True)
    def setup_test_data(self):
        """Setup test data and connections"""
        try:
            # Test Configuration
            credentials_file = str(project_root / 'creds' / 'credentials.json')
            self.spreadsheet_id = SPREADSHEET_ID
            self.sheets_list_file = str(project_root / 'Text_files' / 'Column_uniformity_sheets_to_update.txt')
            
            # Load and verify sheets list
            with open(self.sheets_list_file, 'r') as f:
                self.sheets_list = [line.strip() for line in f if line.strip()]
            print(f"\nFound {len(self.sheets_list)} sheets to process")
            
            # Initialize connection
            self.gs_connection = GoogleSheetsConnection(credentials_file)
            
            # Verify sheets exist
            sheets_exist = verify_sheets_exist(self.gs_connection, self.spreadsheet_id, self.sheets_list)
            assert sheets_exist, "Some sheets are missing from the Google Sheet"
            
            # Initialize processor and process data
            self.processor = BankStatementProcessor(self.gs_connection)
            self.processor.process_all_statements(self.spreadsheet_id, self.sheets_list_file)
            
            # Create test output directory
            self.output_dir = project_root / 'test_output'
            self.output_dir.mkdir(exist_ok=True)
            
            yield
            
        except Exception as e:
            pytest.fail(f"Test setup failed: {str(e)}")

    def test_bank_statement_processing(self):
        """Test bank statement processing"""
        try:
            # Process statements
            processed_data = self.processor.process_all_statements(
                self.spreadsheet_id,
                self.sheets_list_file
            )
            
            # Validate processed data
            assert processed_data is not None, "Processed data is None"
            assert not processed_data.empty, "Processed data is empty"
            assert all(col in processed_data.columns for col in [
                'Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)', 
                'Balance (£)', 'Notes', 'Subcategory'
            ]), "Missing required columns"
            
            # Test data types
            assert pd.api.types.is_datetime64_dtype(processed_data['Date']), "Date column is not datetime"
            assert processed_data['Paid In (£)'].dtype == float, "Paid In column is not float"
            assert processed_data['Withdrawn (£)'].dtype == float, "Withdrawn column is not float"
            
        except Exception as e:
            pytest.fail(f"Bank statement processing test failed: {str(e)}")

    def test_categorisation(self):
        """Test general categorization"""
        try:
            # Apply categorization
            categorizer = Categorisation(self.processor)
            categorized_data = categorizer.apply_categorization(
                self.spreadsheet_id, 
                KEYWORD_MAPPING_SHEET
            )
            
            # Validate categorization
            assert categorized_data is not None, "Categorized data is None"
            assert not categorized_data.empty, "Categorized data is empty"
            
            # Check if any transactions were categorized
            categorized_count = len(categorized_data[
                categorized_data['Subcategory'].notna() & 
                (categorized_data['Subcategory'] != '')
            ])
            assert categorized_count > 0, "No transactions were categorized"
            
            # Export results
            categorizer.export_categorization_analysis(
                str(self.output_dir / 'test_categorization.xlsx')
            )
            
        except Exception as e:
            pytest.fail(f"Categorization test failed: {str(e)}")

    def test_deposit_categorization(self):
        """Test deposit categorization"""
        try:
            # Process deposits
            deposit_handler = DepositCategorizer(self.processor)
            deposit_summary = deposit_handler.categorize_deposits()
            
            # Validate deposit processing
            assert deposit_summary is not None, "Deposit summary is None"
            assert isinstance(deposit_summary, dict), "Deposit summary is not a dictionary"
            assert 'total_deposits_collected' in deposit_summary, "Missing deposit count"
            
            # Validate deposit amounts
            deposits = deposit_handler.data[
                deposit_handler.data['Deposit_Status'] == 'Deposit Collected'
            ]
            assert all(amount in [50.0, 100.0] for amount in deposits['Paid In (£)']), \
                "Invalid deposit amounts found"
            
            # Test deposit matching
            deposit_handler._match_deposits_and_returns()
            assert hasattr(deposit_handler, 'matched_deposits'), "No matched deposits attribute"
            
            # Export results
            deposit_handler.export_deposit_analysis(
                str(self.output_dir / 'test_deposits.xlsx')
            )
            
        except Exception as e:
            pytest.fail(f"Deposit categorization test failed: {str(e)}")

    def test_logic_validation(self):
        """Test logic validation"""
        try:
            # Process deposits first
            deposit_handler = DepositCategorizer(self.processor)
            deposit_handler.categorize_deposits()
            
            # Validate logic
            logic_issues = deposit_handler.validate_subcategory_logic()
            
            # Check logic issues
            assert logic_issues is not None, "Logic issues is None"
            if not logic_issues.empty:
                # Export issues
                deposit_handler.export_logic_issues(
                    str(self.output_dir / 'test_logic_issues.xlsx')
                )
            
        except Exception as e:
            pytest.fail(f"Logic validation test failed: {str(e)}")

    def test_export_functionality(self):
        """Test export functionality"""
        try:
            # Test main export
            self.processor.export_to_excel(
                str(self.output_dir / 'test_processed_statements.xlsx')
            )
            assert (self.output_dir / 'test_processed_statements.xlsx').exists(), \
                "Main export file not created"
            
            # Test removed rows export
            if self.processor.removed_rows is not None:
                self.processor.export_removed_rows(
                    str(self.output_dir / 'test_removed_rows.xlsx')
                )
                assert (self.output_dir / 'test_removed_rows.xlsx').exists(), \
                    "Removed rows export file not created"
            
        except Exception as e:
            pytest.fail(f"Export functionality test failed: {str(e)}")

def verify_outputs():
    """Verify the output files and their contents"""
    try:
        output_dir = 'test_output'
        expected_files = [
            'test_processed_statements.xlsx',
            'test_removed_rows.xlsx',
            'test_keyword_mapping.xlsx',
            'test_deposit_analysis.xlsx',
            'test_logic_issues.xlsx'
        ]
        
        print("\nVerifying outputs...")
        for file in expected_files:
            file_path = os.path.join(output_dir, file)
            if os.path.exists(file_path):
                print(f"✓ {file} created successfully")
            else:
                print(f"✗ {file} not found")
                
    except Exception as e:
        print(f"Output verification failed: {str(e)}")

if __name__ == "__main__":
    # Run pytest directly
    pytest.main([__file__, '-v'])
    # Then verify outputs
    verify_outputs()
