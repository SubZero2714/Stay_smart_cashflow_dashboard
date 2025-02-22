import pandas as pd
from src.utils.error_handler import ProcessingError, handle_error

class Categorisation:
    def __init__(self, bank_statement_processor):
        """
        Initialize Categorisation
        
        Parameters:
        bank_statement_processor (BankStatementProcessor): Processed bank statement data
        """
        self.processor = bank_statement_processor
        self.data = None
        self.categorization_issues = []
        
    def _get_keyword_mappings(self, spreadsheet_id, sheet_name):
        """
        Get keyword mappings from Google Sheet.
        
        Args:
            spreadsheet_id (str): Google Sheets ID
            sheet_name (str): Name of keyword mapping sheet
            
        Returns:
            pd.DataFrame: DataFrame containing keyword mappings
        """
        try:
            # Get keyword mappings from Google Sheets
            mappings_df = self.processor.gs_connection.get_sheet_data(
                spreadsheet_id,
                sheet_name
            )
            
            # Validate mappings
            if mappings_df is None or mappings_df.empty:
                raise ProcessingError(
                    "No keyword mappings found in sheet",
                    "_get_keyword_mappings",
                    "categorisation.py"
                )
                
            required_columns = ['Keyword', 'Subcategory']
            missing_columns = [col for col in required_columns if col not in mappings_df.columns]
            if missing_columns:
                raise ProcessingError(
                    f"Missing required columns in keyword mapping sheet: {missing_columns}",
                    "_get_keyword_mappings",
                    "categorisation.py"
                )
                
            # Clean up mappings
            mappings_df = mappings_df[['Keyword', 'Subcategory']].dropna()
            
            return mappings_df
            
        except Exception as e:
            handle_error(e, "_get_keyword_mappings", "categorisation.py")
            raise

    def apply_categorization(self, spreadsheet_id, sheet_name):
        """
        Apply keyword categorization to transactions.
        
        Args:
            spreadsheet_id (str): Google Sheets ID
            sheet_name (str): Name of keyword mapping sheet
        """
        try:
            print("\n📋 Starting transaction categorization...")
            
            # Make a copy of processed data
            self.data = self.processor.processed_data.copy()
            
            # Get keyword mappings
            keyword_mappings = self._get_keyword_mappings(spreadsheet_id, sheet_name)
            if keyword_mappings.empty:
                raise ProcessingError(
                    "No keyword mappings found",
                    "apply_categorization",
                    "categorisation.py"
                )
            
            # Track progress
            total_rows = len(self.data)
            print(f"\nProcessing {total_rows} transactions...")
            
            # Process each transaction
            for index, row in self.data.iterrows():
                try:
                    transaction = str(row['Transaction']).upper()
                    found_keywords = []
                    found_subcategories = set()
                    
                    for _, mapping in keyword_mappings.iterrows():
                        keyword = str(mapping['Keyword']).upper()
                        if keyword in transaction:
                            found_keywords.append(mapping['Keyword'])  # Original case
                            found_subcategories.add(mapping['Subcategory'])
                    
                    # Update data
                    if found_keywords:
                        self.data.at[index, 'Notes'] = ', '.join(found_keywords)
                        self.data.at[index, 'Subcategory'] = ', '.join(found_subcategories)
                        
                        # Track multiple matches
                        if len(found_subcategories) > 1:
                            self.categorization_issues.append({
                                'Row': index + 2,  # Excel row number
                                'Transaction': row['Transaction'],
                                'Found_Keywords': ', '.join(found_keywords),
                                'Multiple_Categories': ', '.join(found_subcategories),
                                'Issue': 'Multiple category matches'
                            })
                    
                    # Progress indicator every 100 rows
                    if (index + 1) % 100 == 0:
                        print(f"✓ Processed {index + 1}/{total_rows} transactions")
                        
                except Exception as e:
                    self.categorization_issues.append({
                        'Row': index + 2,
                        'Transaction': row['Transaction'],
                        'Issue': f"Processing error: {str(e)}"
                    })
            
            # Print summary
            print("\n📊 Categorization Summary:")
            print(f"Total Transactions: {total_rows}")
            categorized = len(self.data[self.data['Subcategory'].notna()])
            print(f"Categorized: {categorized}")
            print(f"Uncategorized: {total_rows - categorized}")
            print(f"Issues Found: {len(self.categorization_issues)}")
            
            return self.data
            
        except Exception as e:
            handle_error(e, "apply_categorization", "categorisation.py")
            raise
            
    def _validate_categorization(self):
        """Validate categorization and flag issues"""
        try:
            # Initialize DataFrame for categorization issues
            self.categorization_issues = pd.DataFrame(columns=[
                'Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)', 
                'Subcategory', 'Issue_Type', 'Description'
            ])
            
            # Check for uncategorized transactions
            uncategorized = self.data[
                (self.data['Subcategory'].isna()) | 
                (self.data['Subcategory'] == '')
            ].copy()
            
            if not uncategorized.empty:
                uncategorized['Issue_Type'] = 'Uncategorized'
                uncategorized['Description'] = 'No matching category found'
                self.categorization_issues = pd.concat([
                    self.categorization_issues, 
                    uncategorized
                ])
            
            # Check for multiple categories
            multiple_categories = self.data[
                self.data['Subcategory'].str.contains(',', na=False)
            ].copy()
            
            if not multiple_categories.empty:
                multiple_categories['Issue_Type'] = 'Multiple Categories'
                multiple_categories['Description'] = 'Transaction matches multiple categories'
                self.categorization_issues = pd.concat([
                    self.categorization_issues, 
                    multiple_categories
                ])
            
            # Print summary
            if not self.categorization_issues.empty:
                print("\nCategorization Issues Found:")
                issue_summary = self.categorization_issues.groupby('Issue_Type').size()
                for issue_type, count in issue_summary.items():
                    print(f"{issue_type}: {count} issues")
            else:
                print("\nNo categorization issues found")
                
        except Exception as e:
            raise Exception(f"Failed to validate categorization: {str(e)}")
            
    def export_categorization_analysis(self, output_file='categorization_analysis.xlsx'):
        """Export categorization analysis to Excel"""
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write all transactions
                self.data.to_excel(writer, sheet_name='All_Transactions', index=False)
                
                # Write categorization issues
                if not self.categorization_issues.empty:
                    self.categorization_issues.to_excel(
                        writer, 
                        sheet_name='Categorization_Issues',
                        index=False
                    )
                
                # Write category summary
                category_summary = self.data.groupby('Subcategory').agg({
                    'Paid In (£)': 'sum',
                    'Withdrawn (£)': 'sum',
                    'Transaction': 'count'
                }).reset_index()
                
                category_summary.columns = [
                    'Subcategory', 
                    'Total Paid In (£)', 
                    'Total Withdrawn (£)',
                    'Transaction Count'
                ]
                category_summary['Net Amount (£)'] = (
                    category_summary['Total Paid In (£)'] - 
                    category_summary['Total Withdrawn (£)']
                )
                
                category_summary.to_excel(
                    writer,
                    sheet_name='Category_Summary',
                    index=False
                )
                
            print(f"Categorization analysis exported to {output_file}")
            
        except Exception as e:
            raise Exception(f"Failed to export categorization analysis: {str(e)}")