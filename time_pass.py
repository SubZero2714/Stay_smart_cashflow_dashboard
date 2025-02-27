from pathlib import Path
import sys
import code
import gspread
import pandas as pd
from src.google_sheets_connection import GoogleSheetsConnection


print("hello world!")
#google sheet connection creds
# Use absolute path for credentials
credentials_file = r"C:\Users\Shushant Kumar\PycharmProjects\Stay_smart_cashflow_dashboard\creds\credentials.json"
spreadsheet_id = "1SpBtGBfcFwTJaXfj1_6A48ffKZXSRMLAHHn0fvqrWHo"
processed_data_spreadsheet_id = "1RZMKy1Z3xdZ9INBnbBvMXMjj7jh_JITbU91gOZNBLEo"

# print("🔄 Initializing Google Sheets connection...")
gs_connection = GoogleSheetsConnection(credentials_file)

# # Get all sheet names
# spreadsheet = gs_connection.client.open_by_key(spreadsheet_id)
# all_sheets = spreadsheet.worksheets()
#
# print("\n📑 Available sheets:")
# for idx, sheet in enumerate(all_sheets, 1):
#     print(f"{idx}. {sheet.title}")
#
# # Get user input for sheet selection
# selected_sheet = input("\n👉 Enter the sheet name you want to process: ")
#
# # Get the selected worksheet
# print(f"\n🔄 Accessing sheet: {selected_sheet}")
# worksheet = gs_connection.get_worksheet(spreadsheet_id, selected_sheet)
#
# if worksheet:
#     print("✅ Sheet accessed successfully!")
#
#     # Get data and convert to DataFrame
#     data = gs_connection.get_all_data(worksheet)
#     if data:
#         # Convert to DataFrame
#         df = pd.DataFrame(data[1:], columns=data[0])
#
#         # Display DataFrame info
#         print("\n📊 DataFrame Summary:")
#         print(f"Rows: {len(df)}")
#         print(f"Columns: {', '.join(df.columns)}")
#         print("\nFirst 5 rows:")
#         print(df.head())
#
#         # data_frame = df  # Return DataFrame for further use
#         df.to_pickle("data.pkl")
#
# Load the pickle file and convert it into a DataFrame
df = pd.read_pickle("data.pkl")
print("✅ Data loaded successfully!")

# Perform operations
print(df.head())  # View first 5 rows

df.loc[len(df)] = ["10 Jun 23", "Card Purchase GBP 10 JUN 23 XYZ linen", "", 20.00, 500.00, "", ""]
# Add new rows using df.loc
df.loc[len(df)] = ["2024-09-06", "Automated Credit H KAFI ABAD GOLDERGREEN STAYS FP", 100, "", 33615.18, "", ""]
df.loc[len(df)] = ["2024-09-19", "Automated Credit P ARANTES FP", 50, "", 8053.86, "", ""]
df.loc[len(df)] = ["2023-11-05", "FT23307ZDF78 Inward Payment CLARKE CR Christopher", 50, "", 38389.87, "", ""]
df.loc[len(df)] = ["2023-11-30", "FT23334JKRGM Inward Payment Abdullah Mohamed Sent from Revolut", 100, "", 13615.78, "", ""]
df.loc[len(df)] = ["2023-12-03", "FT2333591NS6 Account to Account Transfer MR B A ABDULRAHMAN", 50, "", 12878.19, "", ""]
df.loc[len(df)] = ["2023-12-03", "FT23335BHWH9 Inward Payment AL-NAJAFI H H R HASAN", 50, "", 12928.19, "", ""]
df.loc[len(df)] = ["2023-12-13", "FT233474S435 Inward Payment FESTUS OLUWAFEMI F ONASANYA EFFS OFFICIAL ROOM", 100, "", 593.38, "", ""]
df.loc[len(df)] = ["2023-12-19", "FT23353M8PQ4 Inward Payment KANTANKA F K S SK", 50, "", 9887.22, "", ""]
df.loc[len(df)] = ["2024-01-29", "FT240297HT0K Inward Payment JADIR M MAHMOUD JADIR", 50, "", 16514.2, "", ""]
df.loc[len(df)] = ["2023-09-18", "FT23261YD0GH Outward Faster Payment LD kelly 46 woodstock road", "", 50, 18120.85, "", ""]
df.loc[len(df)] = ["2024-04-19", "FT24110F49MP Outward Faster Payment Chinedu Owkha deposit return", "", 50, 4103.65, "", ""]
df.loc[len(df)] = ["2024-04-19", "Brought forward", 50, "", 4103.65, "", ""]
df.loc[len(df)] = ["2024-04-19", "FT24110F49MP Outward Faster Payment Chinedu Owkha deposit return", "", 50, 4103.65, "", ""]
df.loc[len(df)] = ["2024-04-19", "FT24110F49MP Outward Faster Payment Chinedu Owkha deposit return", "", 50, 4103.65, "", ""]
# # Convert date column to datetime
# df['Date'] = pd.to_datetime(df['Date'])

def create_or_update_sheet(gs_connection_creds, data_frame, sheet_name, spreadsheet_id_=processed_data_spreadsheet_id):
    """
    Creates a new sheet or updates an existing sheet in the Google Spreadsheet.

    Args:
        gs_connection_creds (gspread.Client): Authenticated Google Sheets connection.
        spreadsheet_id_ (str): The ID of the Google Spreadsheet.
        data_frame (pd.DataFrame): The DataFrame to write to the sheet.
        sheet_name (str): The name of the sheet to create or update.

    Returns:
        None
    """
    try:
        # Open the spreadsheet
        spreadsheet = gs_connection_creds.open_by_key(spreadsheet_id_)

        try:
            # Try to get the existing sheet
            sheet = spreadsheet.worksheet(sheet_name)
            print(f"📌 Sheet '{sheet_name}' already exists. Clearing and updating...")
            sheet.clear()  # Clear existing data
        except gspread.exceptions.WorksheetNotFound:
            # If the sheet doesn't exist, create a new one
            print(f"➕ Creating new sheet: {sheet_name}")
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=data_frame.shape[0] + 10, cols=data_frame.shape[1] + 10)

        # Convert DataFrame to list of lists (Google Sheets format)
        data_to_update = [data_frame.columns.tolist()] + data_frame.values.tolist()

        # Update the sheet with the new data
        sheet.update(data_to_update)
        print(f"✅ Successfully updated sheet: {sheet_name}")

    except Exception as e:
        print(f"❌ Error updating/creating sheet '{sheet_name}': {str(e)}")

# # Sort DataFrame by date
# df = df.sort_values('Date').reset_index(drop=True)

# Print confirmation
print(f"\n✅ Added {11} new rows")
print(f"📊 Total rows now: {len(df)}")


def apply_keyword_mapping(dataframe, gs_connection_creds, spreadsheet_id_, keyword_sheet_name):
    """Apply keyword mapping categorization before deposit processing"""
    try:
        # Load keyword mapping
        keyword_mapping = gs_connection_creds.load_keyword_mapping(spreadsheet_id_, keyword_sheet_name)

        if keyword_mapping.empty:
            raise ValueError("Keyword mapping sheet is empty")

        # Store original keywords with their lowercase versions
        keyword_case_map = {
            str(row['Keyword']).lower().strip(): str(row['Keyword']).strip()
            for _, row in keyword_mapping.iterrows()
        }

        # Clear existing categorization
        dataframe['Notes'] = ''
        dataframe['Subcategory'] = ''

        # Convert transaction descriptions to lowercase for case-insensitive matching
        transactions = dataframe['Transaction'].str.lower()

        # Track matches for verification
        keyword_matches = pd.DataFrame(columns=[
            'Transaction', 'Matched_Keyword', 'Applied_Subcategory', 'Multiple_Matches'
        ])

        # Process each transaction
        for idx, transaction in transactions.items():
            matches = []

            # Check each keyword
            for _, mapping in keyword_mapping.iterrows():
                keyword_lower = str(mapping['Keyword']).lower().strip()
                if keyword_lower in str(transaction):
                    # Only add if not already matched (avoid duplicates)
                    if not any(m['keyword'] == keyword_case_map[keyword_lower] for m in matches):
                        matches.append({
                            'keyword': keyword_case_map[keyword_lower],
                            'subcategory': mapping['Subcategory'].strip()
                        })

            if matches:
                # Get unique subcategories
                subcategories = sorted(set(m['subcategory'] for m in matches))

                # Get matched keywords in original case (unique)
                matched_keywords = sorted(set(m['keyword'] for m in matches))

                # Apply subcategories and keywords to DataFrame (clean formatting)
                dataframe.at[idx, 'Subcategory'] = ' | '.join(subcategories)
                dataframe.at[idx, 'Notes'] = ' | '.join(matched_keywords)

                # Track matches
                keyword_matches = pd.concat([
                    keyword_matches,
                    pd.DataFrame([{
                        'Transaction': dataframe.at[idx, 'Transaction'],
                        'Matched_Keyword': ' | '.join(matched_keywords),
                        'Applied_Subcategory': ' | '.join(subcategories),
                        'Multiple_Matches': len(matched_keywords) > 1
                    }])
                ], ignore_index=True)

        # Clean up any remaining formatting issues
        dataframe['Notes'] = dataframe['Notes'].str.strip()
        dataframe['Subcategory'] = dataframe['Subcategory'].str.strip()

        # Apply subcategory rules after initial categorization
        dataframe_1 = apply_subcategory_rules(dataframe)

        # Calculate accurate statistics
        total_transactions = len(dataframe)
        categorized_transactions = len(dataframe[dataframe['Subcategory'].str.strip() != ''])
        multiple_matches = len(dataframe[dataframe['Notes'].str.count('\|') > 0])
        uncategorized = total_transactions - categorized_transactions

        # Print summary with accurate counts
        print("\n📊 Keyword Mapping Summary:")
        print(f"✓ Total transactions processed: {total_transactions}")
        print(f"✓ Transactions categorized: {categorized_transactions}")
        print(f"ℹ️ Transactions with multiple matches: {multiple_matches}")
        print(f"⚠️ Uncategorized transactions: {uncategorized}")

        # Show uncategorized transactions if any
        if uncategorized > 0:
            print("\n⚠️ Sample of Uncategorized Transactions:")
            uncategorized_df = dataframe[dataframe['Subcategory'].str.strip() == ''][['Transaction']].head()
            print(uncategorized_df)

        return dataframe_1

    except Exception as e:
        raise Exception(f"Error in keyword mapping: {str(e)}")


# def apply_subcategory_rules(data_frame):
#     """
#     Apply prioritization rules for multiple subcategories
#     Args:
#         data_frame (pd.DataFrame): DataFrame containing transaction data
#     Returns:
#         pd.DataFrame: Processed DataFrame with applied subcategory rules
#     """
#     try:
#         # Define priority rules as pairs (higher_priority, lower_priority)
#         priority_rules = [
#             ('Air bnb', 'Ketan/ Management'),
#             ('Platform fee', 'Ketan/ Management'),
#             ('Booking.com', 'Ketan/ Management')
#         ]
#
#         # Process each row that has multiple subcategories
#         mask = data_frame['Subcategory'].str.contains('\|', na=False)
#         rows_to_process = data_frame[mask].copy()
#
#         for idx, row in rows_to_process.iterrows():
#             subcategories = set(cat.strip() for cat in row['Subcategory'].split('|'))
#             original_keywords = row['Notes'].split('|') if pd.notna(row['Notes']) else []
#
#             # Check each priority rule
#             for high_priority, low_priority in priority_rules:
#                 if high_priority in subcategories and low_priority in subcategories:
#                     # Keep high priority category and remove low priority
#                     subcategories.remove(low_priority)
#
#                     # Update the row
#                     data_frame.at[idx, 'Subcategory'] = ' | '.join(sorted(subcategories))
#                     print(f"\n📋 Applied rule for transaction: {row['Transaction']}")
#                     print(f"🔄 Changed categories from: {row['Subcategory']}")
#                     print(f"✓ To: {' | '.join(sorted(subcategories))}")
#
#         # Print summary of changes
#         print("\n📊 Subcategory Rule Application Summary:")
#         print(f"✓ Processed {len(rows_to_process)} transactions with multiple categories")
#
#         return data_frame
#
#     except Exception as e:
#         raise Exception(f"Error applying subcategory rules: {str(e)}")

# def apply_subcategory_rules(data_frame):
#     """Apply subcategory rules and handle deposits"""
#     try:
#         # 1. Priority rules for multiple subcategories
#         priority_rules = [
#             ('Air bnb', 'Ketan/ Management'),
#             ('Platform fee', 'Ketan/ Management'),
#             ('Booking.com', 'Ketan/ Management')
#         ]
#
#         # Handle multiple subcategories first
#         mask = data_frame['Subcategory'].str.contains('\|', na=False)
#         for idx, row in data_frame[mask].iterrows():
#             subcategories = set(cat.strip() for cat in row['Subcategory'].split('|'))
#             for high_priority, low_priority in priority_rules:
#                 if high_priority in subcategories and low_priority in subcategories:
#                     subcategories.remove(low_priority)
#                     data_frame.at[idx, 'Subcategory'] = ' | '.join(sorted(subcategories))
#
#         # 2. Handle deposits
#         # Define valid deposit amounts
#         valid_deposit_amounts = {50.0, 100.0}  # Set of acceptable deposit values
#
#         # Iterate through each row in DataFrame
#         for idx, row in data_frame.iterrows():
#             # Skip rows that already have categories
#             if pd.notna(row['Subcategory']) and row['Subcategory'].strip():
#                 continue
#
#             # Convert amount strings to floats with error handling
#             try:
#                 # Handle Paid In amount
#                 amount_paid = (
#                     float(row['Paid In (£)'])  # Convert to float if possible
#                     if pd.notna(row['Paid In (£)']) and str(row['Paid In (£)']).strip() != ''  # Check if value exists
#                     else 0.0  # Default to 0 if empty
#                 )
#
#                 # Handle Withdrawn amount
#                 amount_withdrawn = (
#                     float(row['Withdrawn (£)'])  # Convert to float if possible
#                     if pd.notna(row['Withdrawn (£)']) and str(
#                         row['Withdrawn (£)']).strip() != ''  # Check if value exists
#                     else 0.0  # Default to 0 if empty
#                 )
#             except (ValueError, TypeError):
#                 # If conversion fails, mark as invalid
#                 data_frame.at[idx, 'Notes'] = 'Invalid Amount Format'
#                 data_frame.at[idx, 'Subcategory'] = 'Miscellaneous'
#                 continue
#
#             # Convert transaction description to lowercase for comparison
#             description = str(row['Transaction']).lower()
#
#             # Process Paid In amounts (£50 case)
#             if amount_paid > 0:  # If there's a paid in amount
#                 if amount_paid in valid_deposit_amounts:  # If amount is £50 or £100
#                     if 'deposit' in description:  # If description contains 'deposit'
#                         data_frame.at[idx, 'Subcategory'] = 'Deposit'  # Clear deposit case
#                     else:
#                         data_frame.at[idx, 'Subcategory'] = 'Miscellaneous'  # Right amount, no deposit mention
#                         data_frame.at[idx, 'Notes'] = 'Possible Deposit - Need Review'
#                 elif 'deposit' in description:  # Wrong amount but mentions deposit
#                     data_frame.at[idx, 'Subcategory'] = 'Miscellaneous'
#                     data_frame.at[idx, 'Notes'] = 'Flagged Deposit - Invalid Amount'
#
#         # Generate summary
#         deposits = len(data_frame[data_frame['Subcategory'] == 'Deposit'])
#         returns = len(data_frame[data_frame['Subcategory'] == 'Deposit Return'])
#         flagged = len(data_frame[data_frame['Notes'].str.contains('Flagged|Review|Invalid', na=False)])
#
#         print("\n📊 Deposit Processing Summary:")
#         print(f"✓ Deposits collected: {deposits}")
#         print(f"✓ Deposits returned: {returns}")
#         print(f"⚠️ Flagged transactions: {flagged}")
#
#         if deposits != returns:
#             print(f"⚠️ Mismatch: {deposits} deposits vs {returns} returns")
#
#         if flagged > 0:
#             print("\n⚠️ Flagged Transactions:")
#             flagged_df = data_frame[data_frame['Notes'].str.contains('Flagged|Review|Invalid', na=False)]
#             print(flagged_df[['Transaction', 'Paid In (£)', 'Withdrawn (£)', 'Notes']].head())
#
#         return data_frame
#
#     except Exception as e:
#         raise Exception(f"Error applying rules: {str(e)}")

def apply_subcategory_rules(data_frame):
    """Apply subcategory rules, handle deposits, and detect duplicate transactions."""
    try:
        # 1. Priority rules for multiple subcategories (No changes here)
        priority_rules = [
            ('Air bnb', 'Ketan/ Management'),
            ('Platform fee', 'Ketan/ Management'),
            ('Booking.com', 'Ketan/ Management')
        ]

        # Handle multiple subcategories first (No changes here)
        mask = data_frame['Subcategory'].str.contains('\|', na=False)
        for idx, row in data_frame[mask].iterrows():
            subcategories = set(cat.strip() for cat in row['Subcategory'].split('|'))
            for high_priority, low_priority in priority_rules:
                if high_priority in subcategories and low_priority in subcategories:
                    subcategories.remove(low_priority)
                    data_frame.at[idx, 'Subcategory'] = ' | '.join(sorted(subcategories))

        # 2. Handle deposits (No changes here)
        valid_deposit_amounts = {50.0, 100.0}
        for idx, row in data_frame.iterrows():
            if pd.notna(row['Subcategory']) and row['Subcategory'].strip():
                continue  # Skip already categorized rows

            try:
                amount_paid = float(row['Paid In (£)']) if pd.notna(row['Paid In (£)']) and str(row['Paid In (£)']).strip() != '' else 0.0
                amount_withdrawn = float(row['Withdrawn (£)']) if pd.notna(row['Withdrawn (£)']) and str(row['Withdrawn (£)']).strip() != '' else 0.0
            except (ValueError, TypeError):
                data_frame.at[idx, 'Notes'] = 'Invalid Amount Format'
                data_frame.at[idx, 'Subcategory'] = 'Miscellaneous'
                continue

            description = str(row['Transaction']).lower()
            if amount_paid > 0:
                if amount_paid in valid_deposit_amounts:
                    if 'deposit' in description:
                        data_frame.at[idx, 'Subcategory'] = 'Deposit'
                    else:
                        data_frame.at[idx, 'Subcategory'] = 'Miscellaneous'
                        data_frame.at[idx, 'Notes'] = 'Possible Deposit - Need Review'
                elif 'deposit' in description:
                    data_frame.at[idx, 'Subcategory'] = 'Miscellaneous'
                    data_frame.at[idx, 'Notes'] = 'Flagged Deposit - Invalid Amount'

        # 3. ✅ Detect Duplicate Transactions (NEW FUNCTIONALITY)
        # Detect duplicate transactions (excluding the first occurrence)
        duplicate_mask = data_frame.duplicated(subset=['Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)'],
                                               keep='first')

        # Mark only duplicate rows (excluding the first occurrence)
        data_frame.loc[duplicate_mask, 'Notes'] = 'Duplicate Transaction'
        data_frame.loc[duplicate_mask, 'Subcategory'] = 'Ignore These'

        # Print summary
        if duplicate_mask.any():
            print(f"\n⚠️ Duplicate Transactions Found: {duplicate_mask.sum()}")
            print("\n🔍 Sample Duplicates:")
            print(data_frame[duplicate_mask][
                      ['Date', 'Transaction', 'Paid In (£)', 'Withdrawn (£)', 'Notes', 'Subcategory']].head())

        # 4. Generate final summary (No changes here)
        deposits = len(data_frame[data_frame['Subcategory'] == 'Deposit'])
        returns = len(data_frame[data_frame['Subcategory'] == 'Deposit Return'])
        flagged = len(data_frame[data_frame['Notes'].str.contains('Flagged|Review|Invalid', na=False)])

        print("\n📊 Deposit Processing Summary:")
        print(f"✓ Deposits collected: {deposits}")
        print(f"✓ Deposits returned: {returns}")
        print(f"⚠️ Flagged transactions: {flagged}")

        if deposits != returns:
            print(f"⚠️ Mismatch: {deposits} deposits vs {returns} returns")

        if flagged > 0:
            print("\n⚠️ Flagged Transactions:")
            flagged_df = data_frame[data_frame['Notes'].str.contains('Flagged|Review|Invalid', na=False)]
            print(flagged_df[['Transaction', 'Paid In (£)', 'Withdrawn (£)', 'Notes']].head())

        return data_frame

    except Exception as e:
        raise Exception(f"Error applying rules: {str(e)}")



def split_and_analyze_dataframe(data_frame, gs_connection_creds, spreadsheet_id_, source_sheet_name=""):
    """
    Enhanced split function with:
    1. More exclusion rules (Brought Forward, Closing Balance)
    2. Saving excluded rows in a separate Google Sheet
    3. Summarizing valid transactions
    """

    try:
        # Track the source sheet
        data_frame['Source_Sheet'] = source_sheet_name

        # Define exclusion conditions
        exclusion_conditions = {
            'Empty Transaction': data_frame['Transaction'].isna() | (data_frame['Transaction'].str.strip() == ''),
            'Marked for Ignore': data_frame['Subcategory'].str.contains('Ignore these', case=False, na=False),
            'Brought Forward': data_frame['Transaction'].str.contains('Brought Forward', case=False, na=False),
            'Closing Balance': data_frame['Transaction'].str.contains('Closing Balance', case=False, na=False)
        }

        # Create excluded DataFrame
        excluded_mask = pd.Series(False, index=data_frame.index)
        excluded_df = pd.DataFrame()

        for reason, mask in exclusion_conditions.items():
            matched_rows = data_frame[mask].copy()
            matched_rows['Exclusion_Reason'] = reason
            excluded_df = pd.concat([excluded_df, matched_rows])
            excluded_mask = excluded_mask | mask

        # Create valid DataFrame
        valid_df = data_frame[~excluded_mask].copy()

        # Generate summary
        print("\n📊 Split Analysis:")
        print(f"✓ Total rows: {len(data_frame)}")
        print(f"✓ Valid rows: {len(valid_df)}")
        print(f"⚠️ Excluded rows: {len(excluded_df)}")

        if not excluded_df.empty:
            print("\n📋 Exclusion Summary:")
            summary = excluded_df.groupby('Exclusion_Reason').size()
            for reason, count in summary.items():
                print(f"- {reason}: {count}")

            # Save excluded rows to Google Sheets
            try:
                worksheet = gs_connection_creds.get_worksheet(spreadsheet_id_, "Excluded Transactions")
                if worksheet:
                    worksheet.clear()
                    worksheet.update([excluded_df.columns.tolist()] + excluded_df.values.tolist())
                    print("✅ Excluded transactions saved in 'Excluded Transactions' sheet.")
            except Exception as e:
                print(f"⚠️ Error saving excluded transactions: {e}")

        # Generate category summary for valid transactions
        print("\n📊 Category Summary:")
        category_summary = valid_df['Subcategory'].value_counts().reset_index()
        category_summary.columns = ['Subcategory', 'Count']
        print(category_summary)

        return valid_df, excluded_df

    except Exception as e:
        raise Exception(f"❌ Error in split analysis: {str(e)}")




#main
df_0 = apply_keyword_mapping(df, gs_connection, spreadsheet_id, "Keyword Mapping")
create_or_update_sheet(gs_connection.client, df_0, "Level_1_keyword_applied", processed_data_spreadsheet_id)
needed_data, ignored_data = split_and_analyze_dataframe(df_0, gs_connection, spreadsheet_id)
create_or_update_sheet(gs_connection.client, needed_data, "Level_2_keyword_applied", processed_data_spreadsheet_id)
create_or_update_sheet(gs_connection.client, ignored_data, "Ignored_data", processed_data_spreadsheet_id)
print("I am executing this line")









