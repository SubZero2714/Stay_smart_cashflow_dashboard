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
from tests.test_data_generator import TestDataGenerator

class TestPipeline:
    @classmethod
    def setup_class(cls):
        """Setup test environment and data"""
        cls.credentials_file = project_root / 'creds' / 'credentials.json'
        cls.test_output_dir = project_root / 'test_output'
        cls.test_output_dir.mkdir(exist_ok=True)
        
        # Test data paths
        cls.controlled_data = project_root / 'tests' / 'test_data' / 'test_data_controlled.csv'
        cls.random_data = project_root / 'tests' / 'test_data' / 'test_data_random.csv'
        
        print("\n🔧 Setting up test environment...")
        
    def test_controlled_data(self):
        """Test pipeline with controlled test data"""
        try:
            print("\n🎯 Testing with controlled data...")
            self._run_pipeline_test(
                self.controlled_data,
                "controlled",
                expected_deposits=10,  # Adjust based on your controlled dataset
                expected_returns=10
            )
        except Exception as e:
            print(f"❌ Controlled data test failed: {str(e)}")
            raise

    def test_random_data(self):
        """Test pipeline with random test data"""
        try:
            print("\n🎲 Testing with random data...")
            self._run_pipeline_test(
                self.random_data,
                "random"
            )
        except Exception as e:
            print(f"❌ Random data test failed: {str(e)}")
            raise

    def _run_pipeline_test(self, test_data_file, test_type, expected_deposits=None, expected_returns=None):
        """Run complete pipeline test with specified test data"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 1. Load test data
            print(f"\n1️⃣ Loading {test_type} test data...")
            test_data = pd.read_csv(test_data_file)
            assert not test_data.empty, f"Failed to load {test_type} test data"
            
            # 2. Process statements
            print("\n2️⃣ Processing statements...")
            processor = BankStatementProcessor(None)  # No GS connection needed for test
            processor.processed_data = test_data.copy()
            
            # 3. Apply categorization
            print("\n3️⃣ Applying categorization...")
            categorizer = Categorisation(processor)
            categorized_data = categorizer.apply_categorization(None, None)  # No GS needed for test
            
            # 4. Process deposits
            print("\n4️⃣ Processing deposits...")
            deposit_handler = DepositCategorizer(processor)
            deposit_handler.categorize_deposits()
            
            # 5. Export results
            print("\n5️⃣ Exporting test results...")
            output_base = self.test_output_dir / f'test_{test_type}_{timestamp}'
            
            # Export main data
            main_output = f"{output_base}_processed.xlsx"
            processor.export_to_excel(main_output)
            
            # Export deposit analysis
            deposit_output = f"{output_base}_deposits.xlsx"
            deposit_handler.export_deposit_analysis(deposit_output)
            
            # 6. Validate results
            print("\n6️⃣ Validating results...")
            self._validate_test_results(
                main_output,
                deposit_output,
                test_type,
                expected_deposits,
                expected_returns
            )
            
            print(f"\n✅ {test_type.capitalize()} data test completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Pipeline test failed for {test_type} data: {str(e)}")
            raise

    def _validate_test_results(self, main_output, deposit_output, test_type, 
                             expected_deposits=None, expected_returns=None):
        """Validate test outputs and data integrity"""
        try:
            # Load output files
            main_data = pd.read_excel(main_output)
            deposit_data = pd.read_excel(deposit_output, sheet_name='Matched_Deposits')
            
            # Basic validation
            self._validate_data_structure(main_data, deposit_data)
            
            # Validate deposit amounts
            self._validate_deposit_amounts(deposit_data)
            
            # Validate expected counts for controlled data
            if test_type == "controlled" and expected_deposits and expected_returns:
                self._validate_controlled_data_counts(
                    deposit_data,
                    expected_deposits,
                    expected_returns
                )
            
            # Validate categorization
            self._validate_categorization(main_data)
            
            print("✓ All validations passed")
            
        except Exception as e:
            print(f"❌ Validation failed: {str(e)}")
            raise

    def _validate_data_structure(self, main_data, deposit_data):
        """Validate data structure and required columns"""
        # Check main data columns
        required_main_columns = [
            'Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)',
            'Balance (£)', 'Notes', 'Subcategory'
        ]
        for col in required_main_columns:
            assert col in main_data.columns, f"Missing column in main data: {col}"
        
        # Check deposit data columns
        required_deposit_columns = [
            'Deposit_Date', 'Return_Date', 'Amount',
            'Deposit_Transaction', 'Return_Transaction'
        ]
        for col in required_deposit_columns:
            assert col in deposit_data.columns, f"Missing column in deposit data: {col}"

    def _validate_deposit_amounts(self, deposit_data):
        """Validate deposit amounts are correct"""
        valid_amounts = [50.0, 100.0]
        invalid_amounts = deposit_data[~deposit_data['Amount'].isin(valid_amounts)]
        assert len(invalid_amounts) == 0, f"Found invalid deposit amounts: {invalid_amounts['Amount'].unique()}"

    def _validate_controlled_data_counts(self, deposit_data, expected_deposits, expected_returns):
        """Validate expected counts in controlled data"""
        actual_matches = len(deposit_data)
        assert actual_matches == expected_deposits, \
            f"Expected {expected_deposits} matched deposits, found {actual_matches}"

    def _validate_categorization(self, main_data):
        """Validate categorization logic"""
        # Check deposit categorization
        deposits = main_data[main_data['Subcategory'] == 'Deposit']
        deposit_returns = main_data[main_data['Subcategory'] == 'Deposit Return']
        
        # Validate deposit amounts
        assert all(deposits['Paid In (£)'].isin([50.0, 100.0])), "Invalid deposit amounts found"
        assert all(deposit_returns['Withdrawn (£)'].isin([50.0, 100.0])), "Invalid return amounts found"

def run_pipeline_tests():
    """Run all pipeline tests"""
    test = TestPipeline()
    test.setup_class()
    
    print("\n🚀 Starting pipeline tests...")
    
    try:
        test.test_controlled_data()
        test.test_random_data()
        print("\n✅ All pipeline tests completed successfully!")
        return True
    except Exception as e:
        print(f"\n❌ Pipeline tests failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = run_pipeline_tests()
    if not success:
        sys.exit(1) 