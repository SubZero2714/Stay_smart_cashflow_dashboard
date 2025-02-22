import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import gspread
import time
from datetime import datetime
import random

class GoogleSheetsConnection:
    def __init__(self, credentials_file):
        """
        Initialize Google Sheets connection
        
        Parameters:
        credentials_file (str): Path to Google Sheets API credentials file
        """
        self.credentials_file = credentials_file
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self.client = self._setup_connection()
        self.last_request_time = datetime.now()
        self.min_delay = 1.5  # Minimum delay between requests in seconds
        self.max_retries = 3  # Maximum number of retry attempts

    def _setup_connection(self):
        """Setup connection to Google Sheets"""
        try:
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=self.scope
            )
            client = gspread.authorize(creds)
            return client
        except Exception as e:
            raise Exception(f"Failed to connect to Google Sheets: {str(e)}")

    def _wait_for_rate_limit(self):
        """
        Ensure we don't exceed rate limits by adding delay between requests
        Adds random jitter to avoid synchronized requests
        """
        now = datetime.now()
        elapsed = (now - self.last_request_time).total_seconds()
        if elapsed < self.min_delay:
            delay = self.min_delay - elapsed + random.uniform(0.1, 0.5)
            time.sleep(delay)
        self.last_request_time = datetime.now()

    def _handle_api_error(self, attempt, e):
        """Handle API errors with exponential backoff"""
        if 'RESOURCE_EXHAUSTED' in str(e) and attempt < self.max_retries - 1:
            wait_time = (2 ** attempt) * 5  # Exponential backoff
            print(f"Rate limit hit, waiting {wait_time} seconds...")
            time.sleep(wait_time)
            return True
        return False

    def get_worksheet(self, spreadsheet_id, sheet_name):
        """
        Get specific worksheet from spreadsheet with retry logic
        
        Parameters:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Name of the worksheet to get
        """
        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()
                spreadsheet = self.client.open_by_key(spreadsheet_id)
                worksheet = spreadsheet.worksheet(sheet_name)
                return worksheet
            except gspread.exceptions.APIError as e:
                if self._handle_api_error(attempt, e):
                    continue
                raise Exception(f"Failed to get worksheet {sheet_name}: {str(e)}")
            except Exception as e:
                raise Exception(f"Failed to get worksheet {sheet_name}: {str(e)}")

    def get_all_data(self, worksheet):
        """
        Get all data from worksheet with retry logic
        
        Parameters:
        worksheet (gspread.Worksheet): Worksheet object to get data from
        """
        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()
                data = worksheet.get_all_values()
                
                # Validate data
                if not data:
                    raise ValueError(f"No data found in worksheet")
                if not all(isinstance(row, list) for row in data):
                    raise ValueError(f"Invalid data format in worksheet")
                    
                return data
            except gspread.exceptions.APIError as e:
                if self._handle_api_error(attempt, e):
                    continue
                raise Exception(f"Failed to get data from worksheet: {str(e)}")
            except Exception as e:
                raise Exception(f"Failed to get data from worksheet: {str(e)}")

    def get_sheet_data(self, spreadsheet_id, sheet_name):
        """
        Get data from a specific sheet and return as DataFrame
        
        Parameters:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Name of the worksheet to get data from
        
        Returns:
        pd.DataFrame: DataFrame containing sheet data
        """
        try:
            worksheet = self.get_worksheet(spreadsheet_id, sheet_name)
            data = self.get_all_data(worksheet)
            
            if not data:
                return pd.DataFrame()
                
            # Convert to DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            return df
            
        except Exception as e:
            raise Exception(f"Failed to get sheet data: {str(e)}")

    def load_keyword_mapping(self, spreadsheet_id, keyword_sheet_name):
        """
        Load keyword mapping data from a specific worksheet
        
        Parameters:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        keyword_sheet_name (str): Name of the worksheet containing keyword mappings
        """
        try:
            worksheet = self.get_worksheet(spreadsheet_id, keyword_sheet_name)
            data = self.get_all_data(worksheet)
            
            # Convert to DataFrame
            mapping_df = pd.DataFrame(data)
            
            # Clean up mapping data
            if not mapping_df.empty:
                mapping_df.columns = mapping_df.iloc[0]
                mapping_df = mapping_df[1:]
                mapping_df = mapping_df.dropna(subset=['Keyword'])
                mapping_df['Keyword'] = mapping_df['Keyword'].str.lower()
            
            return mapping_df
        except Exception as e:
            raise Exception(f"Failed to load keyword mapping: {str(e)}")

    def clear_categorization_columns(self, spreadsheet_id, sheets_list_file):
        """
        Clear Notes and Subcategory columns with safety verification.
        """
        try:
            with open(sheets_list_file, 'r') as f:
                sheet_names = [line.strip() for line in f if line.strip()]
            
            print("\nStarting to clear Notes and Subcategory columns...")
            for sheet_name in sheet_names:
                try:
                    worksheet = self.get_worksheet(spreadsheet_id, sheet_name)
                    if worksheet:
                        # Get original row 2 data
                        original_row = worksheet.row_values(2)
                        headers = worksheet.row_values(1)
                        
                        # Find columns to clear
                        notes_idx = headers.index('Notes')
                        subcat_idx = headers.index('Subcategory')
                        notes_col = chr(65 + notes_idx)
                        subcat_col = chr(65 + subcat_idx)
                        
                        # Clear only row 2 first
                        worksheet.batch_clear([
                            f"{notes_col}2:{notes_col}2",
                            f"{subcat_col}2:{subcat_col}2"
                        ])
                        
                        # Verify the clear operation
                        test_row = worksheet.row_values(2)
                        
                        # Check if only target columns were cleared
                        is_safe = True
                        for idx, (orig, test) in enumerate(zip(original_row, test_row)):
                            if idx not in [notes_idx, subcat_idx]:
                                if orig != test:
                                    is_safe = False
                                    print(f"Warning: Unexpected change in column {headers[idx]}")
                                    break
                        
                        if is_safe:
                            # Clear remaining rows
                            worksheet.batch_clear([
                                f"{notes_col}3:{notes_col}",
                                f"{subcat_col}3:{subcat_col}"
                            ])
                            print(f"Successfully cleared {sheet_name}")
                        else:
                            # Revert changes to row 2
                            worksheet.update(f"{notes_col}2", [[original_row[notes_idx]]])
                            worksheet.update(f"{subcat_col}2", [[original_row[subcat_idx]]])
                            print(f"Aborted clearing {sheet_name} - Unexpected column changes")
                        
                except Exception as e:
                    print(f"Error processing {sheet_name}: {str(e)}")
                    continue
            
            print("\nFinished processing all sheets")
            
        except Exception as e:
            raise Exception(f"Failed to clear categorization columns: {str(e)}")