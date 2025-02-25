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
        Initialize Google Sheets connection with enhanced configuration
        
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
        self.min_delay = 2.0  # Increased minimum delay between requests
        self.max_retries = 5   # Increased maximum retries
        self.base_wait_time = 5  # Base wait time for exponential backoff

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
        """Enhanced rate limit handling with random jitter"""
        now = datetime.now()
        elapsed = (now - self.last_request_time).total_seconds()
        if elapsed < self.min_delay:
            delay = self.min_delay - elapsed + random.uniform(0.5, 1.5)
            time.sleep(delay)
        self.last_request_time = datetime.now()

    def _handle_api_error(self, attempt, e, operation_name="operation"):
        """Enhanced API error handling with exponential backoff"""
        if 'RESOURCE_EXHAUSTED' in str(e) and attempt < self.max_retries - 1:
            wait_time = min(self.base_wait_time * (2 ** attempt) + random.uniform(0.1, 1.0), 60)
            print(f"Rate limit hit during {operation_name}, waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            return True
        return False

    def get_worksheet(self, spreadsheet_id, sheet_name):
        """Get worksheet with enhanced retry logic"""
        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()
                spreadsheet = self.client.open_by_key(spreadsheet_id)
                worksheet = spreadsheet.worksheet(sheet_name)
                return worksheet
            except gspread.exceptions.APIError as e:
                if self._handle_api_error(attempt, e, f"getting worksheet {sheet_name}"):
                    continue
                raise Exception(f"Failed to get worksheet {sheet_name}: {str(e)}")
            except Exception as e:
                raise Exception(f"Failed to get worksheet {sheet_name}: {str(e)}")

    def get_all_data(self, worksheet):
        """Get all data with enhanced retry logic"""
        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()
                data = worksheet.get_all_values()
                
                if not data:
                    raise ValueError(f"No data found in worksheet")
                if not all(isinstance(row, list) for row in data):
                    raise ValueError(f"Invalid data format in worksheet")
                    
                return data
            except gspread.exceptions.APIError as e:
                if self._handle_api_error(attempt, e, "getting data"):
                    continue
                raise Exception(f"Failed to get data from worksheet: {str(e)}")
            except Exception as e:
                raise Exception(f"Failed to get data from worksheet: {str(e)}")

    def get_sheet_data(self, spreadsheet_id, sheet_name):
        """Get data from sheet and return as DataFrame with enhanced error handling"""
        try:
            worksheet = self.get_worksheet(spreadsheet_id, sheet_name)
            data = self.get_all_data(worksheet)
            
            if not data:
                return pd.DataFrame()
                
            df = pd.DataFrame(data[1:], columns=data[0])
            # Add source sheet tracking
            df['Source Sheet'] = sheet_name
            
            return df
            
        except Exception as e:
            raise Exception(f"Failed to get sheet data: {str(e)}")

    def load_keyword_mapping(self, spreadsheet_id, keyword_sheet_name):
        """Load and process keyword mapping with enhanced error handling"""
        try:
            worksheet = self.get_worksheet(spreadsheet_id, keyword_sheet_name)
            data = self.get_all_data(worksheet)
            
            mapping_df = pd.DataFrame(data)
            
            if not mapping_df.empty:
                mapping_df.columns = mapping_df.iloc[0]
                mapping_df = mapping_df[1:]
                mapping_df = mapping_df.dropna(subset=['Keyword'])
                mapping_df['Keyword'] = mapping_df['Keyword'].str.lower()
            
            return mapping_df
        except Exception as e:
            raise Exception(f"Failed to load keyword mapping: {str(e)}")

    def clear_categorization_columns(self, spreadsheet_id, sheets_list_file):
        """Clear categorization columns with enhanced error handling and rate limiting"""
        try:
            with open(sheets_list_file, 'r') as f:
                sheet_names = [line.strip() for line in f if line.strip()]
            
            print("\nStarting to clear Notes and Subcategory columns...")
            for sheet_name in sheet_names:
                retry_count = 0
                while retry_count < self.max_retries:
                    try:
                        self._wait_for_rate_limit()
                        worksheet = self.get_worksheet(spreadsheet_id, sheet_name)
                        if worksheet:
                            headers = worksheet.row_values(1)
                            
                            # Find columns to clear
                            notes_idx = headers.index('Notes')
                            subcat_idx = headers.index('Subcategory')
                            notes_col = chr(65 + notes_idx)
                            subcat_col = chr(65 + subcat_idx)
                            
                            # Enhanced batch clearing with rate limiting
                            self._wait_for_rate_limit()
                            worksheet.batch_clear([
                                f"{notes_col}2:{notes_col}",
                                f"{subcat_col}2:{subcat_col}"
                            ])
                            
                            print(f"✓ Successfully cleared {sheet_name}")
                            break
                            
                    except gspread.exceptions.APIError as e:
                        if self._handle_api_error(retry_count, e, f"clearing {sheet_name}"):
                            retry_count += 1
                            continue
                        print(f"❌ Error processing {sheet_name}: {str(e)}")
                        break
                    
                    except Exception as e:
                        print(f"❌ Error processing {sheet_name}: {str(e)}")
                        break
                
                # Add delay between processing sheets
                time.sleep(random.uniform(1.0, 2.0))
            
            print("\n✅ Finished processing all sheets")
            
        except Exception as e:
            raise Exception(f"Failed to clear categorization columns: {str(e)}")