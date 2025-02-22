import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.google_sheets_connection import GoogleSheetsConnection
import pandas as pd

def test_google_sheets_connection():
    # Your constants
    CREDENTIALS_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "creds",
        "credentials.json"
    )
    SPREADSHEET_ID = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
    KEYWORD_MAPPING_SHEET = "Keyword Mapping"
    
    print("Starting Google Sheets connection test...")
    
    try:
        # Initialize connection
        print("\n1. Testing connection initialization...")
        gs_connection = GoogleSheetsConnection(CREDENTIALS_FILE)
        print("✓ Connection initialized successfully!")
        
        # Test accessing keyword mapping sheet
        print("\n2. Testing access to Keyword Mapping sheet...")
        mapping_df = gs_connection.load_keyword_mapping(SPREADSHEET_ID, KEYWORD_MAPPING_SHEET)
        print("✓ Keyword mapping loaded successfully!")
        print("\nFirst few rows of keyword mapping:")
        print(mapping_df.head())
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_google_sheets_connection()
    
    if success:
        print("\n✓ All tests completed successfully!")
    else:
        print("\n❌ Tests failed!")