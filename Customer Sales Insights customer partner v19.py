import pandas as pd
import numpy as np
import sys

# --- Configuration ---
# IMPORTANT: Replace 'your_excel_file.xlsx' with the actual path to your Excel file
excel_file_path = 'EA-CUSTOMER-PURCHASE-INSIGHTS-SUMMARY.xlsx'
output_file_path = 'processed_sales_data.xlsx' # Output file name

# Define the columns that are NOT dates and should be kept as identifiers
# These names MUST exactly match the column headers in your Excel file (after stripping whitespace)
non_date_cols = [
    'SalesLevel1',
    'SalesLevel2',
    'CustomerName',
    'PartnerName',
    'Program',
    'InternalBusinessEntityName',
    'InternalSubBusinessEntityName',
    'SKU'
]

# Define the columns for the SKU-level grouping
sku_group_cols = ['CustomerName', 'PartnerName', 'SKU']

# Define the columns for the Customer/Partner-level grouping
cust_partner_group_cols = ['CustomerName', 'PartnerName']

# Define the program names as they appear in the 'Program' column
program_ea_trigger = 'EA 3.0' # Program to identify the first purchase trigger (assuming 'EA 3.0' based on prompt)
program_unknown = 'UNKNOWN' # Program for which to sum purchases after the trigger

# --- Load Data ---
print(f"Loading data from {excel_file_path}...")
try:
    df_original = pd.read_excel(excel_file_path)
    df_original.columns = df_original.columns.str.strip() # Strip whitespace from column names
    print("Actual columns read from Excel:", df_original.columns.tolist())
    print("Data loaded successfully.")
except FileNotFoundError:
    print(f"Error: File not found at {excel_file_path}")
    sys.exit(1)
except Exception as e:
    print(f"Error loading Excel file: {e}")
    sys.exit(1)

# --- Validate Required Columns ---
print("Validating required columns...")
required_cols = non_date_cols
missing_cols = [col for col in required_cols if col not in df_original.columns]
if missing_cols:
    print(f"Error: The following required columns are missing from the Excel file: {missing_cols}")
    print("Please check your Excel file headers and the 'non_date_cols' list in the script.")
    sys.exit(1)

missing_sku_group_cols = [col for col in sku_group_cols if col not in df_original.columns]
if missing_sku_group_cols:
     print(f"Error: The following SKU grouping columns are missing from the Excel file: {missing_sku_group_cols}")
     print("Please check your Excel file headers and the 'sku_group_cols' list in the script.")
     sys.exit(1)

missing_cp_group_cols = [col for col in cust_partner_group_cols if col not in df_original.columns]
if missing_cp_group_cols:
     print(f"Error: The following Customer/Partner grouping columns are missing from the Excel file: {missing_cp_group_cols}")
     print("Please check your Excel file headers and the 'cust_partner_group_cols' list in the script.")
     sys.exit(1)

print("Required columns found.")

# --- Identify Date Columns ---
date_col_names_str = [col for col in df_original.columns if col not in non_date_cols]
try:
    date_col_names_str_sorted = sorted(date_col_names_str, key=lambda x: pd.to_datetime(x))
except Exception as e:
     print(f"Warning: Could not sort date columns chronologically. Processing will continue but date order might be incorrect: {e}")
     date_col_names_str_sorted = date_col_names_str

if not date_col_names_str_sorted:
    print("Error: No date columns identified. Columns not in 'non_date_cols' were expected to be dates.")
    print("Please check your Excel file headers and the 'non_date_cols' list.")
    print(f"Columns found in file: {df_original.columns.tolist()}")
    print(f"Columns listed as non_date_cols: {non_date_cols}")
    sys.exit(1)
print(f"Identified date columns: {date_col_names_str_sorted}")

# --- Melt DataFrame ---
print("Melting data...")
try:
    df_melted = df_original.melt(
        id_vars=non_date_cols,
        value_vars=date_col_names_str_sorted,
        var_name='Date_Str',
        value_name='Amount'
    )
    df_melted['Date'] = pd.to_datetime(df_melted['Date_Str'], errors='coerce')
    df_melted['Amount'] = pd.to_numeric(df_melted['Amount'], errors='coerce').fillna(0)
    df_melted.dropna(subset=['Date'], inplace=True) # Drop rows where Date conversion failed
    # Sort by the most granular group (SKU) and date for accurate first purchase identification
    df_melted.sort_values(by=sku_group_cols + ['Date'], inplace=True)
    print("Data melted and sorted.")
except Exception as e:
    print(f"Error during data melting or preparation: {e}")
    sys.exit(1)

# --- Calculate SKU-Level Metrics ---
print("Calculating SKU-level metrics...")

def calculate_sku_metrics(group):
    # Find first EA 3.0 purchase date and amount for this SKU group
    ea_purchases = group[
        (group['Program'] == program_ea_trigger) & (group['Amount'] > 0)
    ].sort_values(by='Date')

    first_ea_date = pd.NaT # Use NaT for pandas datetime
    first_ea_amount = 0

    if not ea_purchases.empty:
        first_ea_row = ea_purchases.iloc[0]
        first_ea_date = first_ea_row['Date']
        first_ea_amount = first_ea_row['Amount']

    # Calculate total EA and UNKNOWN after the first EA purchase date for this SKU
    total_ea_after_first_sku = 0
    total_unknown_after_first_ea_sku = 0

    if pd.notna(first_ea_date):
         after_first_ea_df = group[group['Date'] > first_ea_date]

         total_ea_after_first_sku = after_first_ea_df[
             after_first_ea_df['Program'] == program_ea_trigger
         ]['Amount'].sum()

         total_unknown_after_first_ea_sku = after_first_ea_df[
             after_first_ea_df['Program'] == program_unknown
         ]['Amount'].sum()

    # Return SKU-level metrics
    return pd.Series({
        '_SKU_First_EA_Amount': first_ea_amount,
        '_SKU_Total_EA_After_First': total_ea_after_first_sku,
        '_SKU_Total_UNKNOWN_After_First_SKU': total_unknown_after_first_ea_sku, # Renamed for clarity
        '_SKU_First_EA_Date': first_ea_date # Keep date for potential future use or debugging
    })

# Apply SKU-level calculation
sku_results_df = df_melted.groupby(sku_group_cols).apply(calculate_sku_metrics).reset_index()
print("SKU-level metrics calculated.")

# --- Calculate Customer/Partner-Level Metrics ---
print("Calculating Customer/Partner-level metrics...")

def calculate_cust_partner_metrics(group):
    # Find the earliest EA 3.0 purchase date across ALL SKUs for this Customer/Partner
    ea_purchases_cp = group[
        (group['Program'] == program_ea_trigger) & (group['Amount'] > 0)
    ]

    first_ea_date_cp = pd.NaT
    if not ea_purchases_cp.empty:
        first_ea_date_cp = ea_purchases_cp['Date'].min()

    # Calculate total UNKNOWN after this earliest EA purchase date for this Customer/Partner
    total_unknown_after_first_ea_cp = 0
    if pd.notna(first_ea_date_cp):
        total_unknown_after_first_ea_cp = group[
            (group['Date'] > first_ea_date_cp) &
            (group['Program'] == program_unknown)
        ]['Amount'].sum()

    # Return Customer/Partner-level metric
    return pd.Series({
        '_CP_Total_UNKNOWN_After_First': total_unknown_after_first_ea_cp
    })

# Apply Customer/Partner-level calculation
# Note: We group the melted data by cust_partner_group_cols for this calculation
cp_results_df = df_melted.groupby(cust_partner_group_cols).apply(calculate_cust_partner_metrics).reset_index()
print("Customer/Partner-level metrics calculated.")


# --- Merge results back to original DataFrame ---
print("Merging calculated metrics back to original data...")

# Merge SKU-level results first
df_final = pd.merge(df_original, sku_results_df, on=sku_group_cols, how='left')

# Merge Customer/Partner-level results
df_final = pd.merge(df_final, cp_results_df, on=cust_partner_group_cols, how='left')

print("Merge complete.")

# --- Conditionally Apply Calculated Metrics to EA 3.0 Rows ---
print("Applying calculated metrics ONLY to EA 3.0 program rows...")

# Define the names for the final output columns
final_first_ea_sku_col = 'Calculated First EA Amount (SKU)'
final_total_ea_after_sku_col = 'Calculated Total EA After First (SKU)'
final_total_unknown_after_sku_col = 'Calculated Total UNKNOWN After First EA (SKU)'
final_total_unknown_after_cp_col = 'Calculated Total UNKNOWN After First EA (Cust/Partner)' # This one is from the CP group

# Initialize the new columns in df_final to 0.0 (or np.nan if you prefer blank cells)
# Using np.nan might be better if you want truly blank cells in Excel
df_final[final_first_ea_sku_col] = 0.0 # Or np.nan
df_final[final_total_ea_after_sku_col] = 0.0 # Or np.nan
df_final[final_total_unknown_after_sku_col] = 0.0 # Or np.nan
df_final[final_total_unknown_after_cp_col] = 0.0 # Or np.nan

# --- Apply SKU-level metrics to ALL EA 3.0 rows for that SKU group ---
# Create a boolean mask for rows where Program is EA 3.0
ea_rows_mask = df_final['Program'] == program_ea_trigger

df_final.loc[ea_rows_mask, final_first_ea_sku_col] = \
    df_final.loc[ea_rows_mask, '_SKU_First_EA_Amount']

df_final.loc[ea_rows_mask, final_total_ea_after_sku_col] = \
    df_final.loc[ea_rows_mask, '_SKU_Total_EA_After_First']

df_final.loc[ea_rows_mask, final_total_unknown_after_sku_col] = \
    df_final.loc[ea_rows_mask, '_SKU_Total_UNKNOWN_After_First_SKU'] # Assign the SKU UNKNOWN metric

print("SKU-level metrics applied to all relevant EA 3.0 rows.")

# --- Apply Customer/Partner-level UNKNOWN metric to ONLY ONE EA 3.0 row per CP group ---

# 1. Filter df_final to get only EA 3.0 rows
df_ea_rows = df_final[ea_rows_mask].copy() # Use .copy() to avoid SettingWithCopyWarning

# 2. Identify the index of the first EA 3.0 row for each Customer/Partner group
# 'keep="first"' ensures we get the row that appeared first in the original df_final order
first_ea_row_indices_cp = df_ea_rows.drop_duplicates(
    subset=cust_partner_group_cols,
    keep='first'
).index

# 3. Use the identified indices to update the final column in the original df_final
# We use .loc with the index list to target only those specific rows
df_final.loc[first_ea_row_indices_cp, final_total_unknown_after_cp_col] = \
    df_final.loc[first_ea_row_indices_cp, '_CP_Total_UNKNOWN_After_First']

print("Customer/Partner-level UNKNOWN metric applied to one EA 3.0 row per group.")


# --- Clean up temporary columns ---
# Drop the intermediate group metric columns that were added during the merges
temp_cols_to_drop = [
    '_SKU_First_EA_Amount',
    '_SKU_Total_EA_After_First',
    '_SKU_Total_UNKNOWN_After_First_SKU',
    '_SKU_First_EA_Date', # Drop the temporary date column
    '_CP_Total_UNKNOWN_After_First'
]
df_final.drop(columns=temp_cols_to_drop, errors='ignore', inplace=True)

print("Temporary columns removed.")

# --- Write Output ---
print(f"Writing results to {output_file_path}...")
try:
    df_final.to_excel(output_file_path, index=False)
    print("Processing complete. Results saved successfully.")
except Exception as e:
    print(f"Error writing output file: {e}")
    sys.exit(1)
