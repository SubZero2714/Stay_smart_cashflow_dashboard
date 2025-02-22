import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.google_sheets_connection import GoogleSheetsConnection
from tests.test_pipeline import TestPipeline
from tests.test_integration import TestIntegration
from tests.test_data_generator import generate_test_data

class TestRunner:
    def __init__(self):
        self.test_results = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'controlled_data_results': {},
            'sampled_data_results': {},
            'integration_results': {}
        }
        
        # Setup output directory
        self.output_dir = project_root / 'test_results' / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_all_tests(self):
        """Run complete test suite"""
        try:
            print("\n🚀 Starting Complete Test Suite")
            print("=" * 50)
            
            # 1. Generate Test Data
            print("\n1️⃣ Generating Test Data...")
            gs_connection = GoogleSheetsConnection(str(project_root / 'creds' / 'credentials.json'))
            generate_test_data(gs_connection)
            
            # 2. Run Pipeline Tests
            print("\n2️⃣ Running Pipeline Tests...")
            pipeline_test = TestPipeline()
            
            # Test with controlled data
            print("\nTesting with controlled data...")
            self.test_results['controlled_data_results'] = self._run_test_safe(
                pipeline_test.test_controlled_data
            )
            
            # Test with sampled data
            print("\nTesting with sampled data...")
            self.test_results['sampled_data_results'] = self._run_test_safe(
                pipeline_test.test_random_data  # This will use our sampled data
            )
            
            # 3. Run Integration Tests
            print("\n3️⃣ Running Integration Tests...")
            integration_test = TestIntegration()
            self.test_results['integration_results'] = self._run_test_safe(
                integration_test.test_full_processing_pipeline
            )
            
            # 4. Generate Test Report
            self._generate_test_report()
            
            # Final summary
            self._print_final_summary()
            
        except Exception as e:
            print(f"\n❌ Test suite failed: {str(e)}")
            raise

    def _run_test_safe(self, test_func):
        """Run a test function safely and return results"""
        try:
            start_time = datetime.now()
            test_func()
            end_time = datetime.now()
            
            return {
                'status': 'PASSED',
                'duration': str(end_time - start_time),
                'error': None
            }
        except Exception as e:
            return {
                'status': 'FAILED',
                'duration': None,
                'error': str(e)
            }

    def _generate_test_report(self):
        """Generate detailed test report"""
        try:
            # Save JSON results
            with open(self.output_dir / 'test_results.json', 'w') as f:
                json.dump(self.test_results, f, indent=4)
            
            # Generate HTML report
            html_report = self._create_html_report()
            with open(self.output_dir / 'test_report.html', 'w') as f:
                f.write(html_report)
            
            print(f"\n📊 Test reports generated in: {self.output_dir}")
            
        except Exception as e:
            print(f"Error generating test report: {str(e)}")

    def _create_html_report(self):
        """Create HTML test report"""
        return f"""
        <html>
            <head>
                <title>Test Suite Results</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .passed {{ color: green; }}
                    .failed {{ color: red; }}
                    .section {{ margin: 20px 0; padding: 10px; border: 1px solid #ccc; }}
                </style>
            </head>
            <body>
                <h1>Test Suite Results</h1>
                <p>Timestamp: {self.test_results['timestamp']}</p>
                
                <div class="section">
                    <h2>Controlled Data Tests</h2>
                    <p>Status: <span class="{self.test_results['controlled_data_results']['status'].lower()}">
                        {self.test_results['controlled_data_results']['status']}</span></p>
                    <p>Duration: {self.test_results['controlled_data_results']['duration']}</p>
                    {self._format_error(self.test_results['controlled_data_results']['error'])}
                </div>
                
                <div class="section">
                    <h2>Sampled Data Tests</h2>
                    <p>Status: <span class="{self.test_results['sampled_data_results']['status'].lower()}">
                        {self.test_results['sampled_data_results']['status']}</span></p>
                    <p>Duration: {self.test_results['sampled_data_results']['duration']}</p>
                    {self._format_error(self.test_results['sampled_data_results']['error'])}
                </div>
                
                <div class="section">
                    <h2>Integration Tests</h2>
                    <p>Status: <span class="{self.test_results['integration_results']['status'].lower()}">
                        {self.test_results['integration_results']['status']}</span></p>
                    <p>Duration: {self.test_results['integration_results']['duration']}</p>
                    {self._format_error(self.test_results['integration_results']['error'])}
                </div>
            </body>
        </html>
        """

    def _format_error(self, error):
        """Format error message for HTML report"""
        return f'<p class="failed">Error: {error}</p>' if error else ''

    def _print_final_summary(self):
        """Print final test summary to console"""
        print("\n📋 Test Suite Summary")
        print("=" * 50)
        print(f"Timestamp: {self.test_results['timestamp']}")
        print("\nControlled Data Tests:", 
              self.test_results['controlled_data_results']['status'])
        print("Sampled Data Tests:", 
              self.test_results['sampled_data_results']['status'])
        print("Integration Tests:", 
              self.test_results['integration_results']['status'])
        print("\nDetailed reports available in:", self.output_dir)

if __name__ == "__main__":
    runner = TestRunner()
    runner.run_all_tests()