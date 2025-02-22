import sys
from pathlib import Path
import pandas as pd
import pytest

# Add src directory to Python path - Fixed path resolution
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import modules - Updated imports
from src.google_sheets_connection import GoogleSheetsConnection
from src.bank_statement_processor import BankStatementProcessor
from src.categorisation import Categorisation
from src.deposit_categorizer import DepositCategorizer

# Constants
SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
TEST_SHEET = "Test_Data_Controlled"  # Your new sheet name

class TestSmallDataset:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test with small dataset"""
        try:
            credentials_file = str(project_root / 'creds' / 'credentials.json')
            self.gs_connection = GoogleSheetsConnection(credentials_file)
            self.processor = BankStatementProcessor(self.gs_connection)
            
            # Get worksheet data first
            print("\nFetching test data...")
            worksheet = self.gs_connection.client.open_by_key(SPREADSHEET_ID).worksheet(TEST_SHEET)
            worksheet_data = worksheet.get_all_values()
            
            # Process worksheet data
            print("Processing test data...")
            processed_data = self.processor.process_sheet(worksheet_data)
            
            # Store the processed data in the processor
            self.processor.processed_data = processed_data
            
            print(f"Processed {len(processed_data)} rows")
            
            # Show sample of data
            print("\nSample of processed data:")
            print(processed_data.head())
            
            yield
            
        except Exception as e:
            pytest.fail(f"Setup failed: {str(e)}")

    def test_basic_categorization(self):
        """Test basic transaction categorization"""
        try:
            categorizer = Categorisation(self.processor)
            result = categorizer.apply_categorization(SPREADSHEET_ID, "Keyword Mapping")
            
            print("\nCategorization Results:")
            print(f"Total transactions: {len(result)}")
            print(f"Categorized: {len(result[result['Subcategory'].notna()])}")
            print(f"Uncategorized: {len(result[result['Subcategory'].isna()])}")
            
            # Show sample categorizations
            print("\nSample categorizations:")
            print(result[['Transaction', 'Subcategory']].head(10))
            
        except Exception as e:
            pytest.fail(f"Basic categorization test failed: {str(e)}")

    def test_deposit_identification(self):
        """Test deposit identification"""
        try:
            deposit_handler = DepositCategorizer(self.processor)
            result = deposit_handler.categorize_deposits()
            
            print("\nDeposit Results:")
            deposits = deposit_handler.data[
                deposit_handler.data['Is_Deposit_Transaction'] == True
            ]
            print(f"Total deposits found: {len(deposits)}")
            
            # Show all deposits
            print("\nAll identified deposits:")
            print(deposits[['Date', 'Transaction', 'Paid In (£)', 'Deposit_Status']])
            
        except Exception as e:
            pytest.fail(f"Deposit identification test failed: {str(e)}")

    def test_export_and_review(self):
        """Export processed data for review"""
        try:
            # 1. Export processed data
            output_dir = project_root / 'test_output'
            output_dir.mkdir(exist_ok=True)
            
            # Export main processed data
            processed_file = output_dir / 'test_processed_data.xlsx'
            self.processor.processed_data.to_excel(
                processed_file, 
                sheet_name='Processed Data',
                index=False
            )
            
            # 2. Export categorization results
            categorizer = Categorisation(self.processor)
            categorized_data = categorizer.apply_categorization(SPREADSHEET_ID, "Keyword Mapping")
            
            categorized_file = output_dir / 'test_categorized_data.xlsx'
            categorized_data.to_excel(
                categorized_file,
                sheet_name='Categorized Data',
                index=False
            )
            
            # 3. Export deposit analysis
            deposit_handler = DepositCategorizer(self.processor)
            deposit_handler.categorize_deposits()
            
            deposits = deposit_handler.data[
                deposit_handler.data['Is_Deposit_Transaction'] == True
            ]
            
            deposits_file = output_dir / 'test_deposits.xlsx'
            deposits.to_excel(
                deposits_file,
                sheet_name='Deposits',
                index=False
            )
            
            # Print summary
            print("\nExport Summary:")
            print(f"1. Processed Data: {processed_file}")
            print(f"   - Total rows: {len(self.processor.processed_data)}")
            
            print(f"\n2. Categorized Data: {categorized_file}")
            print(f"   - Total rows: {len(categorized_data)}")
            print(f"   - Categorized: {len(categorized_data[categorized_data['Subcategory'].notna()])}")
            print(f"   - Uncategorized: {len(categorized_data[categorized_data['Subcategory'].isna()])}")
            
            print(f"\n3. Deposits: {deposits_file}")
            print(f"   - Total deposits: {len(deposits)}")
            
        except Exception as e:
            pytest.fail(f"Export and review test failed: {str(e)}")

if __name__ == "__main__":
    pytest.main([__file__, '-v'])