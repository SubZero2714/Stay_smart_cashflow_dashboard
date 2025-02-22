from src.utils.error_handler import ProcessingError, handle_error
import pandas as pd
from datetime import datetime, timedelta

class DepositCategorizer:
    def __init__(self, bank_statement_processor):
        """Initialize DepositCategorizer with strict deposit rules and error tracking"""
        self.processor = bank_statement_processor
        self.data = bank_statement_processor.processed_data.copy()
        
        # Strict deposit amounts
        self.deposit_amounts = [50.0, 100.0]
        
        # Tracking containers
        self.deposit_issues = []
        self.matched_deposits = []
        self.unmatched_deposits = []
        self.unmatched_returns = []
        self.miscellaneous_transactions = []
        self.processing_errors = []
        
        # Keywords for identification
        self.deposit_keywords = [
            'deposit', 'dep', 'security deposit', 'damage deposit',
            'room deposit', 'booking deposit'
        ]
        self.return_keywords = [
            'deposit return', 'dep return', 'deposit refund', 'dep refund',
            'return deposit', 'refund deposit', 'deposit back'
        ]

    def categorize_deposits(self):
        """
        Categorize deposits with comprehensive error tracking and validation
        """
        try:
            print("\nStarting deposit categorization...")
            
            # Track transactions
            deposits = []
            returns = []
            
            # Process each transaction
            total_rows = len(self.data)
            processed_count = 0
            error_count = 0
            
            for index, row in self.data.iterrows():
                try:
                    # Progress tracking
                    processed_count += 1
                    if processed_count % 100 == 0:
                        print(f"✓ Processed {processed_count}/{total_rows} transactions")
                        print(f"  - Found {len(deposits)} deposits, {len(returns)} returns")
                        print(f"  - Issues: {len(self.deposit_issues)}")
                    
                    transaction = str(row['Transaction']).upper()
                    paid_in = row['Paid In (£)'] if pd.notnull(row['Paid In (£)']) else 0.0
                    withdrawn = row['Withdrawn (£)'] if pd.notnull(row['Withdrawn (£)']) else 0.0
                    
                    # Detailed transaction analysis
                    self._analyze_transaction(index, row, transaction, paid_in, withdrawn,
                                           deposits, returns)
                    
                except Exception as e:
                    error_count += 1
                    error_detail = {
                        'Row': index + 2,
                        'Transaction': row['Transaction'],
                        'Error': str(e),
                        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.processing_errors.append(error_detail)
                    handle_error(e, f"categorize_deposits - Row {index + 2}",
                               "deposit_categorizer.py")
            
            # Match deposits with returns
            print("\nMatching deposits with returns...")
            self._match_deposits_returns(deposits, returns)
            
            # Validate timing patterns
            print("\nValidating deposit timing patterns...")
            self._validate_timing_patterns()
            
            # Generate comprehensive summary
            self._generate_detailed_summary(total_rows, processed_count, error_count)
            
            return self.data
            
        except Exception as e:
            handle_error(e, "categorize_deposits", "deposit_categorizer.py")
            raise

    def _analyze_transaction(self, index, row, transaction, paid_in, withdrawn,
                           deposits, returns):
        """Detailed analysis of each transaction"""
        try:
            # Check keywords
            is_deposit_desc = any(keyword.upper() in transaction
                                for keyword in self.deposit_keywords)
            is_return_desc = any(keyword.upper() in transaction
                               for keyword in self.return_keywords)
            
            # Case analysis with detailed tracking
            if paid_in in self.deposit_amounts or withdrawn in self.deposit_amounts:
                if not (is_deposit_desc or is_return_desc):
                    self._track_miscellaneous(index, row, paid_in, withdrawn)
                elif paid_in in self.deposit_amounts and is_deposit_desc:
                    self._track_deposit(index, row, paid_in, deposits)
                elif withdrawn in self.deposit_amounts and is_return_desc:
                    self._track_return(index, row, withdrawn, returns)
            elif is_deposit_desc or is_return_desc:
                self._track_issue(index, row, paid_in, withdrawn,
                                "Non-standard amount with deposit keyword")
                
        except Exception as e:
            raise ProcessingError(
                f"Transaction analysis failed: {str(e)}",
                "_analyze_transaction",
                "deposit_categorizer.py"
            )

    def _track_miscellaneous(self, index, row, paid_in, withdrawn):
        """Track transactions needing review"""
        self.miscellaneous_transactions.append({
            'Row': index + 2,
            'Date': row['Date'],
            'Transaction': row['Transaction'],
            'Amount': paid_in if paid_in > 0 else withdrawn,
            'Type': 'Paid In' if paid_in > 0 else 'Withdrawn',
            'Issue': 'Standard amount but no deposit keyword'
        })

    def _track_deposit(self, index, row, amount, deposits):
        """Track valid deposits"""
        deposits.append({
            'Index': index,
            'Date': row['Date'],
            'Transaction': row['Transaction'],
            'Amount': amount
        })
        self.data.at[index, 'Notes'] = 'DEPOSIT'
        self.data.at[index, 'Subcategory'] = 'Deposit'

    def _track_return(self, index, row, amount, returns):
        """Track valid returns"""
        returns.append({
            'Index': index,
            'Date': row['Date'],
            'Transaction': row['Transaction'],
            'Amount': amount
        })
        self.data.at[index, 'Notes'] = 'DEPOSIT RETURN'
        self.data.at[index, 'Subcategory'] = 'Deposit Return'

    def _track_issue(self, index, row, paid_in, withdrawn, issue):
        """Track deposit-related issues"""
        self.deposit_issues.append({
            'Row': index + 2,
            'Date': row['Date'],
            'Transaction': row['Transaction'],
            'Amount': paid_in if paid_in > 0 else withdrawn,
            'Type': 'Paid In' if paid_in > 0 else 'Withdrawn',
            'Issue': issue
        })

    def _match_deposits_returns(self, deposits, returns):
        """
        Match deposits with their corresponding returns
        
        Parameters:
        deposits (list): List of deposit transactions
        returns (list): List of return transactions
        """
        try:
            # Sort by date
            deposits.sort(key=lambda x: pd.to_datetime(x['Date']))
            returns.sort(key=lambda x: pd.to_datetime(x['Date']))
            
            # Track matched transactions
            matched_deposits = []
            used_returns = set()
            
            # Match each deposit with a return
            for deposit in deposits:
                deposit_date = pd.to_datetime(deposit['Date'])
                deposit_amount = deposit['Amount']
                best_match = None
                min_days_diff = float('inf')
                
                # Find the best matching return
                for i, return_trans in enumerate(returns):
                    if i not in used_returns:
                        return_date = pd.to_datetime(return_trans['Date'])
                        return_amount = return_trans['Amount']
                        
                        # Check if amounts match
                        if return_amount == deposit_amount:
                            days_diff = (return_date - deposit_date).days
                            
                            # Return must be after deposit
                            if days_diff > 0 and days_diff < min_days_diff:
                                min_days_diff = days_diff
                                best_match = (i, return_trans)
                
                # Process the match if found
                if best_match:
                    return_index, return_trans = best_match
                    used_returns.add(return_index)
                    
                    matched_deposits.append({
                        'Deposit_Index': deposit['Index'],
                        'Deposit_Date': deposit['Date'],
                        'Deposit_Transaction': deposit['Transaction'],
                        'Deposit_Amount': deposit_amount,
                        'Return_Index': return_trans['Index'],
                        'Return_Date': return_trans['Date'],
                        'Return_Transaction': return_trans['Transaction'],
                        'Return_Amount': return_trans['Amount'],
                        'Days_Between': min_days_diff
                    })
                    
                    # Update transaction notes
                    self.data.at[deposit['Index'], 'Notes'] += ' (Matched)'
                    self.data.at[return_trans['Index'], 'Notes'] += ' (Matched)'
                else:
                    # Track unmatched deposit
                    self.unmatched_deposits.append(deposit)
            
            # Track unmatched returns
            self.unmatched_returns.extend(
                returns[i] for i in range(len(returns))
                if i not in used_returns
            )
            
            # Store matched deposits
            self.matched_deposits = matched_deposits
            
            # Print matching summary
            print(f"\nMatching Summary:")
            print(f"Matched Pairs: {len(matched_deposits)}")
            print(f"Unmatched Deposits: {len(self.unmatched_deposits)}")
            print(f"Unmatched Returns: {len(self.unmatched_returns)}")
            
        except Exception as e:
            handle_error(e, "_match_deposits_returns", "deposit_categorizer.py")
            raise

    def _validate_timing_patterns(self):
        """Validate timing patterns between deposits and returns"""
        try:
            for deposit in self.matched_deposits:
                deposit_date = pd.to_datetime(deposit['Deposit_Date'])
                return_date = pd.to_datetime(deposit['Return_Date'])
                
                # Flag suspicious timing patterns
                if return_date - deposit_date < timedelta(days=1):
                    self.deposit_issues.append({
                        'Transaction': deposit['Deposit_Transaction'],
                        'Issue': 'Return too quick (less than 1 day)',
                        'Deposit_Date': deposit_date,
                        'Return_Date': return_date
                    })
                elif return_date - deposit_date > timedelta(days=30):
                    self.deposit_issues.append({
                        'Transaction': deposit['Deposit_Transaction'],
                        'Issue': 'Return delayed (more than 30 days)',
                        'Deposit_Date': deposit_date,
                        'Return_Date': return_date
                    })
                    
        except Exception as e:
            handle_error(e, "_validate_timing_patterns", "deposit_categorizer.py")

    def _generate_detailed_summary(self, total_rows, processed_count, error_count):
        """Generate detailed processing summary"""
        print("\nDeposit Analysis Summary:")
        print(f"Total Transactions Processed: {processed_count}/{total_rows}")
        print(f"Processing Errors: {error_count}")
        print(f"Matched Deposits: {len(self.matched_deposits)}")
        print(f"Unmatched Deposits: {len(self.unmatched_deposits)}")
        print(f"Unmatched Returns: {len(self.unmatched_returns)}")
        print(f"Miscellaneous Transactions: {len(self.miscellaneous_transactions)}")
        print(f"Issues Found: {len(self.deposit_issues)}")

    def export_deposit_analysis(self, output_file):
        """Export comprehensive deposit analysis"""
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Export all tracking data
                if self.matched_deposits:
                    pd.DataFrame(self.matched_deposits).to_excel(
                        writer, sheet_name='Matched_Deposits', index=False)
                
                if self.unmatched_deposits:
                    pd.DataFrame(self.unmatched_deposits).to_excel(
                        writer, sheet_name='Unmatched_Deposits', index=False)
                
                if self.unmatched_returns:
                    pd.DataFrame(self.unmatched_returns).to_excel(
                        writer, sheet_name='Unmatched_Returns', index=False)
                
                if self.miscellaneous_transactions:
                    pd.DataFrame(self.miscellaneous_transactions).to_excel(
                        writer, sheet_name='Miscellaneous', index=False)
                
                if self.deposit_issues:
                    pd.DataFrame(self.deposit_issues).to_excel(
                        writer, sheet_name='Issues', index=False)
                
                if self.processing_errors:
                    pd.DataFrame(self.processing_errors).to_excel(
                        writer, sheet_name='Processing_Errors', index=False)
                
            print(f"\nComprehensive deposit analysis exported to {output_file}")
            
        except Exception as e:
            handle_error(e, "export_deposit_analysis", "deposit_categorizer.py")
            raise