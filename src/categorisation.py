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
            mappings_df = self.processor.gs_connection.load_keyword_mapping(spreadsheet_id, sheet_name)
            
            if mappings_df is None or mappings_df.empty:
                raise ProcessingError("No keyword mappings found in sheet", "_get_keyword_mappings", "categorisation.py")
            
            required_columns = ['Keyword', 'Subcategory']
            missing_columns = [col for col in required_columns if col not in mappings_df.columns]
            if missing_columns:
                raise ProcessingError(f"Missing required columns: {missing_columns}", "_get_keyword_mappings", "categorisation.py")
            
            return mappings_df[['Keyword', 'Subcategory']].dropna()
        
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
            self.data = self.processor.processed_data.copy()
            keyword_mappings = self._get_keyword_mappings(spreadsheet_id, sheet_name)
            
            if keyword_mappings.empty:
                raise ProcessingError("No keyword mappings found", "apply_categorization", "categorisation.py")
            
            total_rows = len(self.data)
            print(f"\nProcessing {total_rows} transactions...")
            
            for index, row in self.data.iterrows():
                try:
                    transaction = str(row['Transaction']).upper()
                    found_keywords, found_subcategories = [], set()
                    
                    for _, mapping in keyword_mappings.iterrows():
                        keyword = str(mapping['Keyword']).upper()
                        if keyword in transaction:
                            found_keywords.append(mapping['Keyword'])
                            found_subcategories.add(mapping['Subcategory'])
                    
                    if found_keywords:
                        self.data.at[index, 'Notes'] = ', '.join(found_keywords)
                        self.data.at[index, 'Subcategory'] = ', '.join(found_subcategories)
                        
                        if len(found_subcategories) > 1:
                            self.categorization_issues.append({
                                'Row': index + 2,
                                'Transaction': row['Transaction'],
                                'Found_Keywords': ', '.join(found_keywords),
                                'Multiple_Categories': ', '.join(found_subcategories),
                                'Issue': 'Multiple category matches'
                            })
                    
                    if (index + 1) % 100 == 0:
                        print(f"✓ Processed {index + 1}/{total_rows} transactions")
                
                except Exception as e:
                    self.categorization_issues.append({
                        'Row': index + 2,
                        'Transaction': row['Transaction'],
                        'Issue': f"Processing error: {str(e)}"
                    })
            
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
            self.categorization_issues = pd.DataFrame(columns=[
                'Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)', 'Subcategory', 'Issue_Type', 'Description'
            ])
            
            uncategorized = self.data[self.data['Subcategory'].isna() | (self.data['Subcategory'] == '')].copy()
            if not uncategorized.empty:
                uncategorized['Issue_Type'] = 'Uncategorized'
                uncategorized['Description'] = 'No matching category found'
                self.categorization_issues = pd.concat([self.categorization_issues, uncategorized])
            
            multiple_categories = self.data[self.data['Subcategory'].str.contains(',', na=False)].copy()
            if not multiple_categories.empty:
                multiple_categories['Issue_Type'] = 'Multiple Categories'
                multiple_categories['Description'] = 'Transaction matches multiple categories'
                self.categorization_issues = pd.concat([self.categorization_issues, multiple_categories])
            
            if not self.categorization_issues.empty:
                print("\nCategorization Issues Found:")
                for issue_type, count in self.categorization_issues.groupby('Issue_Type').size().items():
                    print(f"{issue_type}: {count} issues")
            else:
                print("\nNo categorization issues found")
        
        except Exception as e:
            raise Exception(f"Failed to validate categorization: {str(e)}")

    def export_categorization_analysis(self, output_file='categorization_analysis.xlsx'):
        """Export categorization analysis to Excel"""
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                self.data.to_excel(writer, sheet_name='All_Transactions', index=False)
                
                if not self.categorization_issues.empty:
                    self.categorization_issues.to_excel(writer, sheet_name='Categorization_Issues', index=False)
                
                category_summary = self.data.groupby('Subcategory').agg({
                    'Paid In (£)': 'sum',
                    'Withdrawn (£)': 'sum',
                    'Transaction': 'count'
                }).reset_index()
                
                category_summary.columns = ['Subcategory', 'Total Paid In (£)', 'Total Withdrawn (£)', 'Transaction Count']
                category_summary['Net Amount (£)'] = category_summary['Total Paid In (£)'] - category_summary['Total Withdrawn (£)']
                category_summary.to_excel(writer, sheet_name='Category_Summary', index=False)
            
            print(f"Categorization analysis exported to {output_file}")
        
        except Exception as e:
            raise Exception(f"Failed to export categorization analysis: {str(e)}")