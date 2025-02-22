import pandas as pd
from datetime import datetime
import os

class BankStatementProcessor:
    def __init__(self, gs_connection):
        """
        Initialize BankStatementProcessor
        
        Parameters:
        gs_connection (GoogleSheetsConnection): Instance of GoogleSheetsConnection
        """
        self.gs_connection = gs_connection
        self.processed_data = None
        self.removed_rows = None

    def load_sheets_list(self, sheets_file):
        """Load list of sheets to process from text file"""
        try:
            with open(sheets_file, 'r') as f:
                sheets = [line.strip() for line in f.readlines()]
            return [sheet for sheet in sheets if sheet]  # Remove empty lines
        except Exception as e:
            raise Exception(f"Failed to load sheets list: {str(e)}")

    def process_sheet(self, worksheet_data, sheet_name):
        """Process individual bank statement worksheet"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(worksheet_data[1:], columns=worksheet_data[0])

            # Add source sheet column
            df['Source_Sheet'] = sheet_name  # Use the sheet name
            
            # Initialize DataFrame for removed rows
            removed_rows = pd.DataFrame(columns=[
                'Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)', 
                'Balance (£)', 'Notes', 'Subcategory', 'Source_Sheet',
                'Removal_Reason'
            ])
            
            # Remove header row if it exists in data
            if df['Date'].iloc[0].lower() == 'date':
                df = df.iloc[1:]
            
            # Define removal patterns
            removal_patterns = {
                'Balance Forward': [
                    'balance brought forward',
                    'brought forward',
                    'closing balance'
                ],
                'Metro Bank DD': [
                    'direct debit metro bank y65ys7p',
                    'direct debit metro bank'
                ]
            }
            
            # Handle rows to be removed
            for reason, patterns in removal_patterns.items():
                # Create mask for matching patterns
                pattern_mask = df['Transaction'].str.lower().str.contains(
                    '|'.join(patterns), 
                    na=False
                )
                
                # Store rows being removed
                if pattern_mask.any():
                    removed_df = df[pattern_mask].copy()
                    removed_df['Removal_Reason'] = reason
                    removed_rows = pd.concat([removed_rows, removed_df])
                    
                    print(f"Removing {pattern_mask.sum()} rows matching pattern: {reason}")
                    df = df[~pattern_mask]
            
            # Handle empty rows
            empty_mask = (
                (df['Date'].isna()) |
                (df['Date'].str.strip() == '') |
                (df['Transaction'].isna() & df['Paid In (£)'].isna() & 
                 df['Withdrawn (£)'].isna() & df['Balance (£)'].isna())
            )
            
            if empty_mask.any():
                removed_df = df[empty_mask].copy()
                removed_df['Removal_Reason'] = 'Empty Row'
                removed_rows = pd.concat([removed_rows, removed_df])
                
                print(f"Removing {empty_mask.sum()} empty rows")
                df = df[~empty_mask]
            
            # Rename columns to match expected format
            df.columns = ["Date", "Transaction", "Paid In (£)", "Withdrawn (£)", 
                         "Balance (£)", "Notes", "Subcategory", "Source_Sheet"]
            
            # Convert date with multiple format handling
            def parse_date(date_str):
                if pd.isna(date_str) or not isinstance(date_str, str):
                    return None
                    
                date_str = date_str.strip()
                date_formats = [
                    '%d %b %y',    # e.g., "03 Apr 23"
                    '%d %B %Y',    # e.g., "03 April 2023"
                    '%d/%m/%Y',    # e.g., "03/04/2023"
                    '%d-%m-%Y',    # e.g., "03-04-2023"
                    '%Y-%m-%d',    # e.g., "2023-04-03"
                ]
                
                for date_format in date_formats:
                    try:
                        return pd.to_datetime(date_str, format=date_format)
                    except:
                        continue
                
                try:
                    # Last resort: let pandas guess the format
                    return pd.to_datetime(date_str, dayfirst=True)
                except:
                    return None

            df['Date'] = df['Date'].apply(parse_date)
            
            # Handle rows with invalid dates
            invalid_dates = df['Date'].isna()
            if invalid_dates.any():
                removed_df = df[invalid_dates].copy()
                removed_df['Removal_Reason'] = 'Invalid Date'
                removed_rows = pd.concat([removed_rows, removed_df])
                
                print(f"Removing {invalid_dates.sum()} rows with invalid dates")
                df = df.dropna(subset=['Date'])
            
            # Convert monetary columns to float
            for col in ['Paid In (£)', 'Withdrawn (£)', 'Balance (£)']:
                # Remove currency symbols and commas
                df[col] = df[col].astype(str).str.replace('£', '').str.replace(',', '')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Ensure consistent dtypes
            df = df.astype({
                'Date': 'datetime64[ns]',
                'Transaction': str,
                'Paid In (£)': float,
                'Withdrawn (£)': float,
                'Balance (£)': float,
                'Notes': str,
                'Subcategory': str,
                'Source_Sheet': str
            })
            
            # Store removed rows
            if not removed_rows.empty:
                if self.removed_rows is None:
                    self.removed_rows = removed_rows
                else:
                    self.removed_rows = pd.concat([self.removed_rows, removed_rows])
            
            return df
                
        except Exception as e:
            raise Exception(f"Failed to process worksheet: {str(e)}")

    def process_all_statements(self, spreadsheet_id, sheets_file):
        try:
            # Load sheets list
            sheets_to_process = self.load_sheets_list(sheets_file)
            print(f"Found {len(sheets_to_process)} sheets to process")

            # Process each sheet
            all_data = []
            for sheet_name in sheets_to_process:
                print(f"\nProcessing sheet: {sheet_name}")

                # Get worksheet
                worksheet = self.gs_connection.get_worksheet(spreadsheet_id, sheet_name)
                data = self.gs_connection.get_all_data(worksheet)

                # Process sheet with sheet name
                df = self.process_sheet(data, sheet_name)  # Pass sheet_name

                # Ensure consistent dtypes across all dataframes
                df = df.astype({
                    'Date': 'datetime64[ns]',
                    'Transaction': str,
                    'Paid In (£)': float,
                    'Withdrawn (£)': float,
                    'Balance (£)': float,
                    'Notes': str,
                    'Subcategory': str,
                    'Source_Sheet': str
                })

                all_data.append(df)
                print(f"✓ Processed {len(df)} transactions from {sheet_name}")

            # Combine all processed data with consistent dtypes
            self.processed_data = pd.concat(all_data, ignore_index=True)

            # Sort by date
            self.processed_data = self.processed_data.sort_values('Date')

            # Remove duplicates if any
            initial_len = len(self.processed_data)
            self.processed_data = self.processed_data.drop_duplicates()
            if len(self.processed_data) < initial_len:
                print(f"\nRemoved {initial_len - len(self.processed_data)} duplicate transactions")

            print(f"\nTotal processed transactions: {len(self.processed_data)}")
            return self.processed_data

        except Exception as e:
            raise Exception(f"Failed to process bank statements: {str(e)}")

    def export_to_excel(self, output_file='processed_statements.xlsx'):
        """
        Export processed data to Excel with defined columns:
        
        Columns:
        - Date (DD/MM/YYYY): Transaction date
        - Transaction: Full transaction description
        - Paid In (£): Amount credited to account (positive value)
        - Withdrawn (£): Amount debited from account (positive value)
        - Balance (£): Running account balance
        - Notes: Keyword found in transaction (from keyword mapping)
        - Subcategory: Category assigned based on found keyword
        - Source_Sheet: Name of original sheet containing this transaction
        """
        if self.processed_data is None:
            raise Exception("No data has been processed yet")
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Create a copy for export
                export_data = self.processed_data.copy()
                
                # Format date to DD/MM/YYYY
                export_data['Date'] = pd.to_datetime(export_data['Date']).dt.strftime('%d/%m/%Y')
                
                # Write main data
                export_data.to_excel(writer, sheet_name='Transactions', index=False)
                
                # Write summary statistics
                summary = pd.DataFrame([
                    ['Total Transactions', len(export_data)],
                    ['Date Range Start', export_data['Date'].min()],
                    ['Date Range End', export_data['Date'].max()],
                    ['Total Income', f"£{export_data['Paid In (£)'].sum():,.2f}"],
                    ['Total Expenses', f"£{export_data['Withdrawn (£)'].sum():,.2f}"],
                    ['Net Position', f"£{(export_data['Paid In (£)'].sum() - export_data['Withdrawn (£)'].sum()):,.2f}"]
                ], columns=['Metric', 'Value'])
                summary.to_excel(writer, sheet_name='Summary', index=False)
                
                # Add sheet for transactions by source
                source_summary = export_data.groupby('Source_Sheet').agg({
                    'Transaction': 'count',
                    'Paid In (£)': 'sum',
                    'Withdrawn (£)': 'sum'
                }).reset_index()
                source_summary.columns = ['Sheet Name', 'Transaction Count', 'Total Paid In (£)', 'Total Withdrawn (£)']
                source_summary['Net Amount (£)'] = source_summary['Total Paid In (£)'] - source_summary['Total Withdrawn (£)']
                source_summary.to_excel(writer, sheet_name='Sheet_Summary', index=False)
                
                print(f"Data exported to {output_file}")
                
        except Exception as e:
            raise Exception(f"Failed to export data: {str(e)}")

    def export_removed_rows(self, output_file='removed_rows_analysis.xlsx'):
        """
        Export removed rows analysis with defined columns:
        
        Columns:
        - Date (DD/MM/YYYY): Transaction date
        - Transaction: Full transaction description
        - Paid In (£): Amount credited to account
        - Withdrawn (£): Amount debited from account
        - Balance (£): Account balance
        - Notes: Any notes associated with transaction
        - Subcategory: Any category assigned
        - Source_Sheet: Original sheet name
        - Removal_Reason: Why the row was removed
        """
        if self.removed_rows is None or self.removed_rows.empty:
            print("No removed rows to export")
            return
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Format date in removed rows
                export_removed = self.removed_rows.copy()
                export_removed['Date'] = pd.to_datetime(export_removed['Date']).dt.strftime('%d/%m/%Y')
                
                # Write all removed rows
                export_removed.to_excel(writer, sheet_name='All_Removed_Rows', index=False)
                
                # Write removal summary by reason
                removal_summary = export_removed['Removal_Reason'].value_counts().reset_index()
                removal_summary.columns = ['Reason', 'Count']
                removal_summary.to_excel(writer, sheet_name='Removal_Summary', index=False)
                
                # Write removal summary by sheet
                sheet_summary = export_removed.groupby(['Source_Sheet', 'Removal_Reason']).size().reset_index()
                sheet_summary.columns = ['Sheet', 'Reason', 'Count']
                sheet_summary.to_excel(writer, sheet_name='Sheet_Wise_Summary', index=False)
                
                # Write detailed summaries for each reason
                for reason in export_removed['Removal_Reason'].unique():
                    reason_df = export_removed[export_removed['Removal_Reason'] == reason]
                    sheet_name = f"{reason.replace(' ', '_')[:28]}"  # Excel sheet names limited to 31 chars
                    reason_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                print(f"Removed rows analysis exported to {output_file}")
                
        except Exception as e:
            raise Exception(f"Failed to export removed rows analysis: {str(e)}")

    def apply_keyword_mapping(self, spreadsheet_id, keyword_sheet_name):
        """Apply keyword mapping categorization before deposit processing"""
        try:
            # Load keyword mapping
            keyword_mapping = self.gs_connection.load_keyword_mapping(spreadsheet_id, keyword_sheet_name)
            
            if keyword_mapping.empty:
                raise ValueError("Keyword mapping sheet is empty")
            
            # Clear existing categorization
            self.processed_data['Notes'] = ''
            self.processed_data['Subcategory'] = ''
            
            # Convert transaction descriptions to lowercase for case-insensitive matching
            transactions = self.processed_data['Transaction'].str.lower()
            
            # Track matches for verification
            self.keyword_matches = pd.DataFrame(columns=[
                'Transaction', 'Matched_Keyword', 'Applied_Subcategory', 'Multiple_Matches'
            ])
            
            # Process each transaction
            for idx, transaction in transactions.items():
                matches = []
                
                # Check each keyword
                for _, mapping in keyword_mapping.iterrows():
                    keyword = str(mapping['Keyword']).lower().strip()
                    if keyword in str(transaction):
                        matches.append({
                            'keyword': keyword,
                            'subcategory': mapping['Subcategory']
                        })
                
                if matches:
                    # Get unique subcategories (in case multiple keywords map to same subcategory)
                    subcategories = sorted(set(m['subcategory'] for m in matches))
                    
                    # Apply subcategories
                    self.processed_data.at[idx, 'Subcategory'] = ', '.join(subcategories)
                    
                    # Track matches
                    self.keyword_matches = pd.concat([
                        self.keyword_matches,
                        pd.DataFrame([{
                            'Transaction': transaction,
                            'Matched_Keyword': ', '.join(m['keyword'] for m in matches),
                            'Applied_Subcategory': ', '.join(subcategories),
                            'Multiple_Matches': len(matches) > 1
                        }])
                    ], ignore_index=True)
            
            # Print summary
            print(f"\nKeyword Mapping Summary:")
            print(f"Total transactions processed: {len(self.processed_data)}")
            print(f"Transactions categorized: {len(self.keyword_matches)}")
            print(f"Transactions with multiple matches: {len(self.keyword_matches[self.keyword_matches['Multiple_Matches']])}")
            print(f"Uncategorized transactions: {len(self.processed_data) - len(self.keyword_matches)}")
            
            return self.processed_data
            
        except Exception as e:
            raise Exception(f"Failed to apply keyword mapping: {str(e)}")

    def export_keyword_mapping_analysis(self, output_file='keyword_mapping_analysis.xlsx'):
        """Export keyword mapping analysis to Excel"""
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write transactions with matches
                if hasattr(self, 'keyword_matches') and not self.keyword_matches.empty:
                    self.keyword_matches.to_excel(
                        writer, 
                        sheet_name='Keyword_Matches',
                        index=False
                    )
                    
                    # Write multiple matches for review
                    multiple_matches = self.keyword_matches[
                        self.keyword_matches['Multiple_Matches']
                    ].copy()
                    if not multiple_matches.empty:
                        multiple_matches.to_excel(
                            writer,
                            sheet_name='Multiple_Matches',
                            index=False
                        )
                
                # Write uncategorized transactions
                if hasattr(self, 'processed_data'):
                    uncategorized = self.processed_data[
                        self.processed_data['Subcategory'] == ''
                    ][['Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)']]
                    if not uncategorized.empty:
                        uncategorized.to_excel(
                            writer,
                            sheet_name='Uncategorized',
                            index=False
                        )
                
            print(f"Keyword mapping analysis exported to {output_file}")
            
        except Exception as e:
            raise Exception(f"Failed to export keyword mapping analysis: {str(e)}")