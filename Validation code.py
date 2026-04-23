import pandas as pd
import numpy as np
import os
from datetime import datetime
from tkinter import *
from tkcalendar import Calendar
from tkinter import filedialog
# number representation.
import locale
locale.setlocale(locale.LC_ALL, 'en_IN')

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# ============================================================================================================================================================================================================

starttime = datetime.now()
print(f"\nProcess started at: {starttime.strftime('%d-%m-%Y %H:%M:%S')}\n")

Valuation_Date = ''


def get_valuation_date():
    global Valuation_Date
    selected_date = datetime.strptime(cal.get_date(), '%m/%d/%y').strftime('%d/%m/%Y')
    Valuation_Date = selected_date
    root.destroy()


root = Tk()
root.title("Select Valuation Date")
root.geometry("300x350")
cal = Calendar(root, selectmode='day', year=2025, month=1, day=1)
cal.pack(pady=20)
Button(root, text="Get Valuation Date", command=get_valuation_date).pack(pady=10)
root.mainloop()
print(f"\nValuation Date: {Valuation_Date}")

# ============================================================================================================================================================================================================

# File selection with cancel handling
policyinputsheet_filepath = filedialog.askopenfilename(
    title="Policy input File Selection",
    filetypes=[("CSV and Excel files", "*.csv *.xlsx"),
               ("CSV files", "*.csv"),
               ("Excel files", "*.xlsx"),
               ("All files", "*.*")]
)

policyinputsheet_399_filepath = filedialog.askopenfilename(
    title="Policy input 399 File Selection",
    filetypes=[("CSV and Excel files", "*.csv *.xlsx"),
               ("CSV files", "*.csv"),
               ("Excel files", "*.xlsx"),
               ("All files", "*.*")]
)

policyinputsheet_savings_filepath = filedialog.askopenfilename(
    title="Policy input savings File Selection",
    filetypes=[("CSV and Excel files", "*.csv *.xlsx"),
               ("CSV files", "*.csv"),
               ("Excel files", "*.xlsx"),
               ("All files", "*.*")]
)

claims_filepath = filedialog.askopenfilename(title="Claims File Selection", filetypes=[("CSV and Excel files", "*.csv *.xlsx"), ("CSV files", "*.csv"), ("Excel files", "*.xlsx"), ("All files", "*.*")])
cancelled_filepath = filedialog.askopenfilename(title="Cancellations File Selection", filetypes=[("CSV and Excel files", "*.csv *.xlsx"), ("CSV files", "*.csv"), ("Excel files", "*.xlsx"), ("All files", "*.*")])
ReinsurancePremium_filepath = filedialog.askopenfilename(title=" Reinsurance Premium File Selection", filetypes=[("CSV and Excel files", "*.csv *.xlsx"), ("CSV files", "*.csv"), ("Excel files", "*.xlsx"), ("All files", "*.*")])

# Create DataFrames with cancel handling


def read_file_or_empty(filepath):
    if filepath:
        try:
            if filepath.endswith('.csv'):
                return pd.read_csv(filepath, low_memory=False)
            elif filepath.endswith(('.xlsx', '.xls')):
                return pd.read_excel(filepath)
            else:
                try:
                    return pd.read_csv(filepath, low_memory=False)
                except:
                    return pd.read_excel(filepath)
        except Exception as e:
            print(f"Error reading file {filepath}: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()


# Read files or create empty DataFrames
df = read_file_or_empty(policyinputsheet_filepath)
EmployerEmployee = read_file_or_empty(policyinputsheet_399_filepath)
rp = read_file_or_empty(policyinputsheet_savings_filepath)

# print the columns of all dataframes
# print(f"Policy input file columns: {list(df.columns)}\n")
# print(f"Policy 399 file columns: {list(EmployerEmployee.columns)}\n")
# print(f"Policy savings file columns: {list(rp.columns)}\n")

print(f"Policy input file: {'Loaded' if not df.empty else 'Empty'} ({len(df)} rows)\nNo of Columns: {len(df.columns)}\n")
print(f"Policy 399 file: {'Loaded' if not EmployerEmployee.empty else 'Empty'} ({len(EmployerEmployee)} rows)\nNo of Columns: {len(EmployerEmployee.columns)}\n")
print(f"Policy savings file: {'Loaded' if not rp.empty else 'Empty'} ({len(rp)} rows)\nNo of Columns: {len(rp.columns)}\n")

# ============================================================================================================================================================================================================

Output_directorypath = filedialog.askdirectory(title="Select Destination Folder for Output Files")

EmployerEmployee.rename(columns={'Product UIN': 'Base Product UIN'}, inplace=True)

# print(f"Final row count: {len(EmployerEmployee)}")
print(f"\nUnique Base Product UINs and counts in EmployerEmployee file:")
print(EmployerEmployee['Base Product UIN'].value_counts())



if policyinputsheet_filepath:
    policyinputsheet_filename, policyinputsheet_fileExtension = os.path.splitext(os.path.basename(policyinputsheet_filepath))
else:
    policyinputsheet_filename = ''
    policyinputsheet_fileExtension = ".csv"

new_folder_name = "Validation Output Files"
new_folder_path = os.path.join(Output_directorypath, new_folder_name)


if not os.path.exists(new_folder_path):
    os.makedirs(new_folder_path)
    print(f"\nFolder '{new_folder_name}' created at location: \n{new_folder_path}")
else:
    print(f"\nFolder '{new_folder_name}' already exists at location: \n{new_folder_path}")

# get EmployerEmployee data file as a csv file with all columns
EmployerEmployee.to_csv(os.path.join(new_folder_path, "EmployerEmployee_data.csv"), index=False)
print(f"\n✅✅✅ EmployerEmployee data saved at: {os.path.join(new_folder_path, 'EmployerEmployee_data.csv')}")
# ===================================================================================== Input File Combiner ===================================================================================================

def safe_concat(df1, df2):
    print("\nChecking column names...")
    
    # Find common columns
    common_cols = set(df1.columns) & set(df2.columns)
    df1_only_cols = set(df1.columns) - set(df2.columns)
    df2_only_cols = set(df2.columns) - set(df1.columns)
    
    print(f"Common columns: {len(common_cols)}")
    print(f"Columns only in df1: {len(df1_only_cols)}")
    print(f"Columns only in df2: {len(df2_only_cols)}")
    
    if common_cols:
        # print(f"Common columns: {list(common_cols)}")
        
        if df1_only_cols:
            print(f" ---> Columns only in df1 (will be dropped): \n{list(df1_only_cols)}")
        if df2_only_cols:
            print(f" ---> Columns only in df2 (will be dropped): \n{list(df2_only_cols)}")
        
        # Reorder df2 columns to match df1's order for common columns
        # This maintains consistent column order in the result
        common_cols_ordered = [col for col in df1.columns if col in common_cols]
        df2_common = df2[common_cols_ordered]
        
        try:
            # Use only common columns from both DataFrames
            result = pd.concat([df1[common_cols_ordered], df2_common], ignore_index=True)
            print(f"✅✅✅ DataFrames successfully concatenated using common columns.")
            print(f"Original df1 length: {len(df1)}, df2 length: {len(df2)}")
            # print(f"Final result length: {len(result)}")
            # print(f"Final column count: {len(result.columns)}")
            return result
        except Exception as e:
            print(f"Concatenation failed: {e}")
            return df1
    else:
        print("No common columns found. Concatenation not performed.")
        return df1

# Prepare list of valid DataFrames
valid_dfs = []

# Ensure all DataFrames have ['Regular Premium', 'Loan Term'] column
if not df.empty:
    if 'Regular Premium' not in df.columns:
        df['Regular Premium'] = ''
    if 'Loan Term' not in df.columns:
        df['Loan Term'] = ''
    valid_dfs.append(df)

if not EmployerEmployee.empty:
    if 'Regular Premium' not in EmployerEmployee.columns:
        EmployerEmployee['Regular Premium'] = ''
    if 'Loan Term' not in EmployerEmployee.columns:
        EmployerEmployee['Loan Term'] = ''
    valid_dfs.append(EmployerEmployee)

# Proceed only if any input was selected
if valid_dfs:
    base_df = valid_dfs[0]
    for next_df in valid_dfs[1:]:
        base_df = safe_concat(base_df, next_df)
else:
    base_df = pd.DataFrame()  # Empty fallback

# After combining df and EmployerEmployee, ensure the combined result also has 'Regular Premium', 'Loan Term' columns
if not base_df.empty and 'Regular Premium' not in base_df.columns:
    base_df['Regular Premium'] = ''
if not base_df.empty and 'Loan Term' not in base_df.columns:
    base_df['Loan Term'] = ''

# Now handle rp
if not rp.empty:
    rp.rename(columns={'Accumulated Premium': 'Premium'}, inplace=True)
    if 'Regular Premium' not in rp.columns:
        rp['Regular Premium'] = ''
    if 'Loan Term' not in rp.columns:
        rp['Loan Term'] = ''
    base_df = safe_concat(base_df, rp)

    # Final result
    df = base_df
else:
    print("\nNo valid input files selected. Skipping file combination.")

# print 'Loan Term' column unique values and count for all products
if 'Loan Term' in df.columns:
    print(f"\nUnique values in 'Loan Term' column and their counts:\n{df[['Base Product UIN', 'Loan Term']].value_counts()}")

# printing all the available columns in the final combined dataframe
print(f"\nColumns in the final combined DataFrame: {list(df.columns)}")

print("\nFinal Combined Length:", len(df))
print(df[['Base Product UIN', 'Premium Mode']].value_counts(),"\n")


print(f"\nPremium column rounding.")
before = df['Premium'].sum()
print(f"✅ Total Premium before conversion: {locale.format_string('%.2f', before, grouping=True)}")
df['Premium'] = df['Premium'].round(0)
after = df['Premium'].sum()
print(f"✅ Total Premium after conversion: {locale.format_string('%.2f', after, grouping=True)}\n")

def calculate_status_change_date(df):
    status_change_date = []
    for index, row in df.iterrows():
        coi_status = row['Status']
        if coi_status == 11:
            status_change_date.append(row['Issuance Date'])
        elif coi_status == 22:
            status_change_date.append(row['Date of Cancellation'])
        elif coi_status == 42:
            status_change_date.append(row['Date of Claim Intimation'])
        elif coi_status == 43:
            status_change_date.append(row['Date of Claim Settlement'])
        elif coi_status == 61:
            status_change_date.append(row['Expiry Date'])
        else:
            status_change_date.append(None)
    df['Modelled status change date'] = status_change_date
    return df

df = calculate_status_change_date(df)

# file_path = os.path.join(new_folder_path, "AllCombinedInput.csv")
# df.to_csv(file_path, index=False)
# print(f"\nCombined input saved at: {file_path}")


# ==========================================================

timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
error_file = os.path.join(new_folder_path, "Data_Validation_Report.csv")

with open(error_file, 'w') as file:
    file.write(f"Data Validation Error Report File.\n\n")
    file.write(f"Date of Valuation,{Valuation_Date}\n")
    file.write(
        f"Input File,{policyinputsheet_filename + policyinputsheet_fileExtension}\n")
    file.write(f"Created on,{timestamp}\n")

print(f"\nFile '{error_file}' created successfully.")

# ============================================================================================================================================================================================================

df = df.sort_values(by='COI Number')
# df = df.iloc[:, :df.columns.get_loc('Premium Payment Term') + 1]
df.insert(loc=14, column='Valuation Date', value=pd.to_datetime(
    Valuation_Date, dayfirst=True, format='%d/%m/%Y'))

print(f"\nTotal no of records : {df.shape[0]}")

with open(error_file, 'a') as file:
    file.write(f"\nTotal no of records,{df.shape[0]}\n")


# ============================================================================================================================================================================================================
# Remove UW status records
df = df[df["Status"] != "UW"]
# ============================================================================================================================================================================================================

print("Before Conversion Status dtype:", df['Status'].unique())

# convert the string numbers like '11' if exist, to actual numeric types for the column 'Status'
df['Status'] = pd.to_numeric(df['Status'], errors='coerce').astype('Int64')

# remove records with Status as NA
df = df[df['Status'].notna()]

print("After Conversion Status dtype:", df['Status'].unique())

# status_counts = df['Status'].value_counts()

# with open(error_file, 'a') as file:
#     file.write(f"\nThe status labels in the given input file\n")
# status_counts.to_csv(error_file, mode='a', header=False)

print(
    f"\nThe status labels in the given input file : {list(df['Status'].unique())}")


# ============================================================================================================================


result = df[df['Status'].isin([11, '11', 33, '33'])].groupby(['Status', 'Base Product UIN', 'Premium Mode']).agg({
    'Premium': 'sum',
    'COI Number': 'count',
    'Original SA': 'sum'
})

# Format Premium with commas, but keep COI Number count without commas
result['COI_Count'] = result['COI Number']  # This will be without commas
result['Premium'] = result['Premium'].apply(lambda x: locale.format_string('%.2f', x, grouping=True))
result['Original SA'] = result['Original SA'].apply(lambda x: locale.format_string('%.2f', x, grouping=True))

print(f"\nPremium and COI Count for statuses 11 and 33:\n{result[['COI_Count','Premium', 'Original SA']].to_string()}")

# ============================================================================================================================================================================================================
# Filter out NA values from the unique statuses
# for status in df['Status'].dropna().unique():
#     if status != 11:
#         status_df = df[df['Status'] == status]        
#         file_path = os.path.join(new_folder_path, f"status_{status}.csv")
#         status_df.to_csv(file_path, index=False)

# print(f"Files saved to: {new_folder_path}")

# ============================================================================================================================================================================================================

duplicate_policy_numbers = df[df.duplicated(subset=['COI Number'], keep=False)]
duplicate_values_list = duplicate_policy_numbers['COI Number'].tolist()

if len(duplicate_values_list) != 0:
    print(f"Duplicate COIs check: \n{duplicate_values_list}")
    with open(error_file, 'a') as file:
        file.write(f"\nDuplicate COIs check,{len(duplicate_values_list)}\n")
        # file.write("CoI Numbers,Error description \n")
        for policy_number in duplicate_values_list:
            file.write(f"{policy_number}, duplicate COI record.\n")
# ============================================================================================================================================================================================================

print(f"\nStatus wise counts...\n{df['Status'].value_counts()}\n")

# ============================================================================================================================================================================================================

columns_to_check = ['COI Number', 'Base Product UIN', 'Issuance Date', 'Coverage Effective Date', 'Expiry Date', 'PH DOB', 'PH Entry Age', 'Premium', 'Original SA', 'Current SA', 'Status']

# Filter DataFrame to include only records where Status == 11
filtered_df = df[df['Status'] == 11]

# Check for missing values in the specified columns of the filtered DataFrame
missing_values = filtered_df[[*columns_to_check,'PH Gender']].isnull()

# Print columns with missing values and their counts
print("\nMissing values by column (Status == 11 only):")
print(missing_values.sum())

# ============================================================================================================================================================================================================

Jointlife_columns_to_check = ['Joint Holder Gender', 'Joint Holder DOB']
df['Joint Holder DOB'] = pd.to_datetime(df['Joint Holder DOB'], format='mixed', dayfirst=True)
df['Joint Holder DOB'] = df['Joint Holder DOB'].dt.strftime('%d-%m-%Y')


# Filter DataFrame to include only records where Status == 11
filtered_df = df.loc[(df['Status'] == 11) & (df['Co-Borrower/Joint Life ID'] == 'Joint Life')]

# Check for missing values in the specified columns of the filtered DataFrame
missing_values = filtered_df[Jointlife_columns_to_check].isnull()

# Print columns with missing values and their counts
print("\nMissing values in Inforce by column ('Co-Borrower/Joint Life ID' == Joint Life only):")
print(missing_values.sum())

# add the details to error file
total_missing_values = filtered_df[Jointlife_columns_to_check].isnull().sum().sum()
missing_info = []
for index, row in filtered_df.iterrows():
    missing_fields = []
    for column in Jointlife_columns_to_check:
        if pd.isnull(row[column]):
            missing_fields.append(column)
    if missing_fields:
        coi_number = row['COI Number']
        missing_fields_str = ', '.join(missing_fields)
        missing_info.append(f"{coi_number},{missing_fields_str} missing")

with open(error_file, 'a') as file:
    file.write(f"\nMissing Values in Joint Life Columns, {total_missing_values}\n")
    for info in missing_info:
        file.write(f"{info}\n")
print(f"\nMissing values information for Joint Life appended to '{error_file}' successfully.")

# ============================================================================================================================================================================================================


# Filter for status 42 first
status_42_data = df[df['Status'] == 42]

print("\n\nMissing values analysis for Status 42:")
print("=" * 50)

for column in ['Date of Death', 'Date of Claim Intimation']:
    missing_mask = status_42_data[column].isnull()
    missing_count = missing_mask.sum()
    
    print(f"\nColumn: {column}")
    print(f"Missing values: {missing_count}")
    
    if missing_count > 0:
        missing_coi_numbers = status_42_data[missing_mask].index.tolist()
        print(f"\nCOI Numbers with missing '{column}': {missing_coi_numbers}")
        # You can also write this information to the error file if needed
        with open(error_file, 'a') as file:
            for coi in missing_coi_numbers:
                file.write(f"{coi}, missing {column}\n")

# add the details to error file
total_missing_values = status_42_data[['Date of Death', 'Date of Claim Intimation']].isnull().sum().sum()

# Overall summary
total_missing = status_42_data[['Date of Death', 'Date of Claim Intimation']].isnull().any(axis=1).sum()
print(f"\nTotal records with any missing value: {total_missing}")
# ============================================================================================================================================================================================================

# 'Date of Cancellation' should be only for Status 22. check which all other status have a 'Date of Cancellation' filled. print those COI numbers as list and then write them to error file.
other_status_with_cancellation = df[(df['Status'] != 22) & df['Date of Cancellation'].notnull()]
if not other_status_with_cancellation.empty:
    coi_numbers = other_status_with_cancellation['COI Number'].tolist()
    print(f"\nCOI Numbers with Date of Cancellation filled but Status not 22: {coi_numbers}")
    with open(error_file, 'a') as file:
        for coi in coi_numbers:
            file.write(f"{coi}, Date of Cancellation filled but Status is not 22\n")

# ============================================================================================================================================================================================================

# Filter for status 43 first
status_43_data = df[df['Status'] == 43]

print("\n\nMissing values analysis for Status 43:")
print("=" * 50)

for column in ['Date of Death', 'Date of Claim Intimation', 'Date of Claim Settlement']:
    missing_mask = status_43_data[column].isnull()
    missing_count = missing_mask.sum()
    
    print(f"\nColumn: {column}")
    print(f"Missing values: {missing_count}")
    
    if missing_count > 0:
        missing_coi_numbers = status_43_data[missing_mask].index.tolist()
        print(f"COI Numbers with missing '{column}': {missing_coi_numbers}")
        # You can also write this information to the error file if needed
        with open(error_file, 'a') as file:
            for coi in missing_coi_numbers:
                file.write(f"{coi}, missing {column}\n")

# Overall summary
total_missing = status_43_data[["Date of Death", "Date of Claim Intimation", "Date of Claim Settlement"]].isnull().any(axis=1).sum() 
print(f"\nTotal records with any missing value: {total_missing}")

# ============================================================================================================================================================================================================

# Filter for status 22
status_22_data = df[df['Status'] == 22]

print("\n\nMissing values analysis for Status 22:")
print("=" * 50)

for column in ['Date of Cancellation']:
    missing_mask = status_22_data[column].isnull()
    missing_count = missing_mask.sum()
    
    print(f"\nColumn: {column}")
    print(f"Missing values: {missing_count}")
    
    if missing_count > 0:
        missing_coi_numbers = status_22_data[missing_mask].index.tolist()
        print(f"\nCOI Numbers with missing '{column}': {missing_coi_numbers}")
        # You can also write this information to the error file if needed
        with open(error_file, 'a') as file:
            for coi in missing_coi_numbers:
                file.write(f"{coi}, missing {column}\n")

# ============================================================================================================================================================================================================


total_missing_values = df[df['Status'] == 11][columns_to_check].isnull().sum().sum()
missing_info = []

for index, row in df.iterrows():
    missing_fields = []
    for column in columns_to_check:
        if pd.isnull(row[column]):
            missing_fields.append(column)
    if missing_fields:
        coi_number = row['COI Number']
        missing_fields_str = ', '.join(missing_fields)
        missing_info.append(f"{coi_number},{missing_fields_str} missing")
with open(error_file, 'a') as file:
    file.write(f"\nMissing Values in Relevant Columns, {total_missing_values}\n")
    for info in missing_info:
        file.write(f"{info}\n")

print(f"\nMissing values information appended to '{error_file}' successfully.")

# ============================================================================================================================================================================================================

uin_limits = {

    '163N001V01': {                                                         # Raksha Kavach
        'age_limits': (18, 74),
        'sa_limits': (1000, 200000),
        'premium_limits': (1, 52982),
        'policy_term_limits': (1, 36)
    },
    '163N001V02': {
        'age_limits': (18, 74),
        'sa_limits': (5000, 200000),
        'premium_limits': (1, 52982),
        'policy_term_limits': (1, 120)
    },

    '163N002V01': {                                                         # suraksha Sukshm
        'age_limits': (18, 74),
        'sa_limits': (1000, 200000),
        'premium_limits': (1, 7560),
        'policy_term_limits': (12, 12)
    },
    '163N002V02': {
        'age_limits': (18, 74),
        'sa_limits': (1000, 200000),
        'premium_limits': (1, 7560),
        'policy_term_limits': (12, 12)
    },

    '163N003V01': {                                                         # Suraksha
        'age_limits': (18, 74),
        'sa_limits': (5000, 200000000),
        'premium_limits': (1, 492734),
        'policy_term_limits': (12, 12)
    },
    '163N003V02': {
        'age_limits': (18, 74),
        'sa_limits': (10000, 200000000),
        'premium_limits': (1, 492734),
        'policy_term_limits': (12, 12)
    },

    '163N004V01': {                                                         # Raksha Chakra
        'age_limits': (18, 62),
        'sa_limits': (1000, 2500000),
        'premium_limits': (1, 492734),
        'policy_term_limits': (1, 240)
    },
    '163N004V02': {
        'age_limits': (18, 74),
        'maturity_age': (18, 75),
        'sa_limits': (10000, 3000000),
        'premium_limits': (1, 492734),
        'policy_term_limits': (1, 240)
    },

    '163N012V01': {                                                         # Grameen Sanchay
        'age_limits': (18, 65),
        'sa_limits': (10000, 2500000),
        'premium_limits': (1, 100000),
        'policy_term_limits': (36, 60)
    },

    '163N007V02': {                                                         # Sarvochchah Nidhi
        'age_limits': (18, 74),
        'sa_limits': (10000, 10000),
        'premium_limits': (25, 25),
        'policy_term_limits': (1, 12)
    },

    '163N009V01': {                                                         # Anantha Suraksha Sukshm
        'age_limits': (18, 74),
        'sa_limits': (5000, 200000),
        'premium_limits': (0, 0),
        'policy_term_limits': (1, 120),
        'Maturity_Age': (18, 75)
    },


}

def assign_group(row):
    if row['Base Product UIN'] == '163N004V01' or row['Base Product UIN'] == "163N004V02" :
        return 'Raksha Chakra Group'
    elif row['MPH Code'] == 'TELAMPH615':
        return 'Telangana Group'
    elif row['POLICYNUMBER'] == 399:
        if row['Base Product UIN'] == '163N003V02':
            return '399 Base'
        elif row['Base Product UIN'] == '163B001V01':
            return '399 ADB'
        elif row['Base Product UIN'] == '163B002V01':
            return '399 ATPD'
    elif row['MPH Code'] == 'CREDMPH002' and row['POLICYNUMBER'] != 399:
        return 'Grameen Group'
    else:
        return 'All Others'

def find_boundary_errors(df, uin_limits):
    error_rows = []
    for index, row in df.iterrows():
        product_name = str(row['Base Product UIN']).strip()
        entry_age = int(row['PH Entry Age'])
        sa = float(row['Original SA'])
        premium = float(row['Premium'])
        policy_term = int(row['Policy Term_Month'])
        ph_gender = str(row['PH Gender']).strip()

        errors = []

        if product_name not in uin_limits:
            errors.append(f"Unknown Product UIN: {product_name}")
        else:
            limits = uin_limits[product_name]

            if not (limits['age_limits'][0] <= entry_age <= limits['age_limits'][1]):
                errors.append("Entry Age is out of bounds")

            if not (limits['sa_limits'][0] <= sa <= limits['sa_limits'][1]):
                errors.append("Sum Assured is out of bounds")

            if not (limits['premium_limits'][0] <= premium <= limits['premium_limits'][1]):
                errors.append("Premium is out of bounds")

            if not (limits['policy_term_limits'][0] <= policy_term <= limits['policy_term_limits'][1]):
                errors.append("Policy Term is out of bounds")

            if ph_gender not in ['Female', 'Male']:
                errors.append("PH Gender should be either Female or Male")

        if errors:
            error_rows.append({
                'index': index,
                'COI Number': row['COI Number'],
                'Base Product UIN': row['Base Product UIN'],
                'PH Entry Age': row['PH Entry Age'],
                'Sum Assured': row['Original SA'],
                'Status': row['Status'],
                'Premium': row['Premium'],
                'Policy Term_Month': row['Policy Term_Month'],
                'PH Gender': row['PH Gender'],
                'Errors': errors
            })
    return pd.DataFrame(error_rows)


error_df = find_boundary_errors(df[df['Status'] == 11], uin_limits)
file_path = os.path.join(new_folder_path, "Boundary_Error_records.csv")
error_df.to_csv(file_path, index=False)


missing_issuance_dates = df.loc[df['Issuance Date'].isnull(
), 'COI Number'].tolist()
if len(missing_issuance_dates) != 0:
    missing_dates_df = df[df['Issuance Date'].isnull()]
    missing_dates_df_file = new_folder_path + "/" + \
        policyinputsheet_filename+'_MissingIssuanceDates'
    print(f"\nTotal {len(missing_issuance_dates)} COI's with Missing Issuance Dates: \n\n{missing_issuance_dates}\n")
    missing_dates_df.to_csv(missing_dates_df_file +
                            policyinputsheet_fileExtension, index=False)

# ============================================================================================================================================================================================================

# Coverstart > Expiry

# Expiry_CoverageStart = df[pd.to_datetime(df['Coverage Effective Date'], format='mixed', dayfirst=True) > pd.to_datetime(df['Expiry Date'], format='%d/%m/%Y', dayfirst=True)]

# if len(Expiry_CoverageStart) != 0:
#     print(f"There are {len(Expiry_CoverageStart)} records where Coverage start date is greater than Expiry date.")
#     file_path = os.path.join(new_folder_path, "Coverstart_greaterthan_Expiry.csv")
#     Expiry_CoverageStart.to_csv(file_path, index=False)
# else:
#     print("nill")

Expiry_CoverageStart = df[pd.to_datetime(df['Coverage Effective Date'], format='mixed', dayfirst=True) > pd.to_datetime(
    df['Expiry Date'], format='mixed', dayfirst=True)]

if len(Expiry_CoverageStart) != 0:
    print(
        f"\nThere are {len(Expiry_CoverageStart)} records where Coverage start date is greater than Expiry date.")
    error_info = []
    for index, row in Expiry_CoverageStart.iterrows():
        coi_number = row['COI Number']
        error_info.append(
            f"{coi_number}, Coverage start date greater than Expiry date")
    with open(error_file, 'a') as file:
        file.write(
            f"\nCoverage Start Date Greater Than Expiry Date,{len(Expiry_CoverageStart)}\n")
        for info in error_info:
            file.write(f"{info}\n")
    file_path = os.path.join(
        new_folder_path, "Coverstart_greaterthan_Expiry.csv")
    Expiry_CoverageStart.to_csv(file_path, index=False)
    print(
        f"\nDetails of records with Coverage start date greater than Expiry date appended to '{error_file}' successfully.")
# ============================================================================================================================================================================================================

# Check the size of the MPH Code column and identify the MPH code having the COI Numbers count less than 5. (Minimum eligibility for group size is 5)
mph_code_counts = df['MPH Code'].value_counts()
small_groups = mph_code_counts[mph_code_counts < 5]
if len(small_groups) != 0:
    print(f"\nThere are {len(small_groups)} MPH Codes with less than 5 COI Numbers.")
    print(small_groups)

    with open(error_file, 'a') as file:
        file.write(f"\nMPH Codes with less than 5 COI Numbers,{len(small_groups)}\n")
        for mph_code, count in small_groups.items():
            file.write(f"{mph_code}, {count} COI Numbers\n")
    print(f"\nDetails of MPH Codes with less than 5 COI Numbers appended to '{error_file}' successfully.")
else:
    print("\nAll MPH Codes have 5 or more COI Numbers.")



# ============================================================================================================================================================================================================

# Policy Term validation
# for OYRTA products validation is expiry date should be same for the records with same POLICY NUMBER column
# for Non OYRTA products validation is calculated term which is  the difference between Expiry Date and Coverage Effective Date in months should be same as Policy Term_Month column
# the records not satisfying the above conditions will be filtered out and saved in a excel file and the count will be appended to error file
# the count by product will be printed in the console.

from dateutil.relativedelta import relativedelta

df['Coverage Effective Date'] = pd.to_datetime(df['Coverage Effective Date'], dayfirst=True, errors='coerce')
df['Expiry Date'] = pd.to_datetime(df['Expiry Date'], dayfirst=True, errors='coerce')

df['Calculated_Term'] = df.apply(
    lambda x: relativedelta(x['Expiry Date'], x['Coverage Effective Date']).years * 12 + 
              relativedelta(x['Expiry Date'], x['Coverage Effective Date']).months
    if pd.notnull(x['Expiry Date']) and pd.notnull(x['Coverage Effective Date']) else None,
    axis=1
)

OYRTA_products = ['163N002V01','163N002V02', '163N003V01', '163N003V02', '163N007V02','163B001V01','163B002V01']
Non_OYRTA_products = df[~df['Base Product UIN'].isin(OYRTA_products)]

# For OYRTA products
OYRTA_issues = df[df['Base Product UIN'].isin(OYRTA_products)]
OYRTA_issues = OYRTA_issues.groupby('COI Number').filter(lambda x: x['Expiry Date'].nunique() > 1)

if len(OYRTA_issues) != 0:
    print(f"There are {len(OYRTA_issues)} records in OYRTA products where Expiry dates are not same for the same COI Number.")
    file_path = os.path.join(new_folder_path, "OYRTA_PolicyTerm_Issues.xlsx")
    OYRTA_issues.to_excel(file_path, index=False)
    print("\nOYRTA Products Policy Term Issues Count by Product\n",OYRTA_issues['Base Product UIN'].value_counts())
else:
    print("\n✅✅✅ No issues found in OYRTA products for Policy Term validation.")

# For Non OYRTA products
Non_OYRTA_issues = Non_OYRTA_products[Non_OYRTA_products['Calculated_Term'] != Non_OYRTA_products['Policy Term_Month']]
if len(Non_OYRTA_issues) != 0:
    print(f"There are {len(Non_OYRTA_issues)} records in Non OYRTA products where Policy Term_Month is not matching with the calculated term.")
    file_path = os.path.join(new_folder_path, "Non_OYRTA_PolicyTerm_Issues.xlsx")
    Non_OYRTA_issues.to_excel(file_path, index=False)
    print("\nNon OYRTA Products Policy Term Issues Count by Product\n",Non_OYRTA_issues['Base Product UIN'].value_counts())
else:
    print("\n✅✅✅ No issues found in Non OYRTA products for Policy Term validation.")

# ============================================================================================================================================================================================================


Back_Dated_Issuances = df[
    (pd.to_datetime(df['Coverage Effective Date'], format='mixed', dayfirst=True) < pd.to_datetime(df['Issuance Date'], format='mixed', dayfirst=True)) &
    (pd.to_datetime(df['Issuance Date'], format='mixed', dayfirst=True) - pd.to_datetime(df['Coverage Effective Date'], format='mixed', dayfirst=True) > pd.Timedelta(days=45))
]

if len(Back_Dated_Issuances) != 0:
    print(f"\nThere are {len(Back_Dated_Issuances)} records where Issuance date is after 45 days from Coverage effective date.")

    file_path = os.path.join(new_folder_path, "Back_Dated_Issuances.xlsx")
    Back_Dated_Issuances.to_excel(file_path, index=False)

print("\nBackdated COIs Count by MPH\n",Back_Dated_Issuances['MPH Code'].value_counts())

# ============================================================================================================================================================================================================

Issuance_CoverageStart = df[pd.to_datetime(df['Coverage Effective Date'], format='mixed', dayfirst=True) > pd.to_datetime(
    df['Issuance Date'], format='mixed', dayfirst=True)]

if len(Issuance_CoverageStart) != 0:
    print(
        f"\nThere are {len(Issuance_CoverageStart)} records where Coverage start date is greater than Issuance date.")
    error_info = []
    for index, row in Issuance_CoverageStart.iterrows():
        coi_number = row['COI Number']
        error_info.append(
            f"{coi_number}, Coverage start date greater than Issuance date")
    with open(error_file, 'a') as file:
        file.write(
            f"\nCoverage Start Date Greater Than Issuance Date,{len(Issuance_CoverageStart)}\n")
        for info in error_info:
            file.write(f"{info}\n")
    file_path = os.path.join(
        new_folder_path, "Coverstart_greaterthan_Issuance.csv")
    Issuance_CoverageStart.to_csv(file_path, index=False)
    print(
        f"\nDetails of records with Coverage start date greater than Issuance date appended to '{error_file}' successfully.")

# ============================================================================================================================================================================================================

# Expiry > ValDate and status is 61

Expiry_Valuation = df[
    (pd.to_datetime(df['Expiry Date'], format='mixed', dayfirst=True) > pd.to_datetime(df['Valuation Date'], format='mixed', dayfirst=True)) &
    ((df['Status'] == '61') | (df['Status'] == 61))
]

if len(Expiry_Valuation) != 0:
    print(
        f"\nThere are {len(Expiry_Valuation)} records where Expiry date is greater than Valuation date with Status 61.")
    error_info = []
    for index, row in Expiry_Valuation.iterrows():
        coi_number = row['COI Number']
        error_info.append(
            f"{coi_number}, Expiry date > Valuation date with Status 61")
    with open(error_file, 'a') as file:
        file.write("\n")
        file.write(
            f"\nExpiry date > Valuation date with Status is 61,{len(Expiry_Valuation)}\n")
        file.write("CoI Numbers,Error description\n")
        for info in error_info:
            file.write(f"{info}\n")
    file_path = os.path.join(
        new_folder_path, "Expiry_greaterthan_Valuation_61.csv")
    Expiry_Valuation.to_csv(file_path, index=False)
    print(f"\nFiltered records saved to '{file_path}'.")


# print("\nStatus Counts in Expiry Valuation:")
# print(Expiry_Valuation['Status'].value_counts())

# ============================================================================================================================================================================================================

# Expiry < ValDate and status 11

Expiry_Valuation = df[
    (pd.to_datetime(df['Expiry Date'], format='mixed', dayfirst=True) < pd.to_datetime(df['Valuation Date'], format='mixed', dayfirst=True)) &
    ((df['Status'] == '11') | (df['Status'] == 11))
]

if len(Expiry_Valuation) != 0:
    print(
        f"\nThere are {len(Expiry_Valuation)} records where Expiry date is less than Valuation date and Status is 11.")
    error_info = []
    for index, row in Expiry_Valuation.iterrows():
        coi_number = row['COI Number']
        error_info.append(
            f"\n{coi_number}, Expiry date < Valuation date and Status is 11")
    with open(error_file, 'a') as file:
        file.write(
            f"\nExpiry date < Valuation date and Status is 11,{len(Expiry_Valuation)}\n")
        for info in error_info:
            file.write(f"{info}\n")
    file_path = os.path.join(
        new_folder_path, "Expiry_lessthan_Valuation_11.csv")
    Expiry_Valuation.to_csv(file_path, index=False)
    print(f"\nFiltered records saved to '{file_path}'.")


# print("\nStatus Counts in Expiry Valuation:")
# print(Expiry_Valuation['Status'].value_counts())


SumAssureds_issues = df[(df['Status'] == 11) & (
    df['Original SA'] < df['Current SA'])]
if len(SumAssureds_issues) != 0:
    print(
        f"\nThere are {len(SumAssureds_issues)} Inforce records where Original SA is lessthan current SA.")
    error_info = []
    for index, row in SumAssureds_issues.iterrows():
        coi_number = row['COI Number']
        error_info.append(f"{coi_number}, Original SA is lessthan current SA.")
    with open(error_file, 'a') as file:
        file.write(
            f"\nOriginal SA is lessthan current SA.,{len(SumAssureds_issues)}\n")
        for info in error_info:
            file.write(f"{info}\n")
    file_path = os.path.join(
        new_folder_path, "Original_SA_lessthan_current_SA.csv")
    SumAssureds_issues.to_csv(file_path, index=False)
    print(f"\nFiltered records saved to '{file_path}'.")


levelcover_issues = df[(df['Status'] == 11) & (
    df['SA_code'] == 'Level') & (df['Original SA'] != df['Current SA'])]
if len(levelcover_issues) != 0:
    print(
        f"\nThere are {len(levelcover_issues)} records where SA_code is Level but Original SA != current SA.")
    error_info = []
    for index, row in levelcover_issues.iterrows():
        coi_number = row['COI Number']
        error_info.append(
            f"\n{coi_number}, SA_code is Level but Original SA != current SA.")
    with open(error_file, 'a') as file:
        file.write(
            f"\nSA_code is Level but Original SA != current SA,{len(levelcover_issues)}\n")
        for info in error_info:
            file.write(f"{info}\n")
    file_path = os.path.join(
        new_folder_path, "Level_org_SA_notequal_cur_SA.csv")
    levelcover_issues.to_csv(file_path, index=False)
    print(f"\nFiltered records saved to '{file_path}'.")


# ============================================================================================================================================================================================================

# 01/01/0001  ?

date_columns = ['Issuance Date', 'Coverage Effective Date', 'Status Change Date',
                'Expiry Date', 'Maturity date', 'PH DOB', 'Valuation Date']

policy_numbers = set()

for column in date_columns:
    matched_indices = df[df[column] == '01/01/0001'].index
    policy_numbers.update(df.loc[matched_indices, 'COI Number'])

filtered_df = df[df['COI Number'].isin(policy_numbers)]

if len(policy_numbers) != 0:
    filtered_df.to_csv("Datawith_01010001_DateErrors.csv", index=False)
    print(
        f"\nThere are {len(filtered_df)} records with '01/01/0001' format in the provided input file.")

# ============================================================================================================================================================================================================

df[date_columns] = df[date_columns].apply(
    pd.to_datetime, dayfirst=True, format='mixed', errors='coerce')
df[date_columns] = df[date_columns].apply(lambda x: x.dt.strftime('%d-%m-%Y'))

# ============================================================================================================================================================================================================

first_date = pd.to_datetime(
    df['Issuance Date'], format='%d-%m-%Y', dayfirst=True).min()
last_date = pd.to_datetime(
    df['Issuance Date'], format='%d-%m-%Y', dayfirst=True).max()

print("\nIssuance date column - First date:", first_date.strftime('%d-%m-%Y'),
      " - Last date:", last_date.strftime('%d-%m-%Y'))

# ============================================================================================================================================================================================================

first_date = pd.to_datetime(
    df['Coverage Effective Date'], format='%d-%m-%Y', dayfirst=True).min()
last_date = pd.to_datetime(
    df['Coverage Effective Date'], format='%d-%m-%Y', dayfirst=True).max()

print("\nCoverage Effective Date - First date:", first_date.strftime('%d-%m-%Y'),
      " - Last date:", last_date.strftime('%d-%m-%Y'))

# ============================================================================================================================================================================================================

first_date = pd.to_datetime(
    df['Status Change Date'], format='%d-%m-%Y', dayfirst=True).min()
last_date = pd.to_datetime(
    df['Status Change Date'], format='%d-%m-%Y', dayfirst=True).max()

print("\nStatus Change Date - First date:", first_date.strftime('%d-%m-%Y'),
      " - Last date:", last_date.strftime('%d-%m-%Y'))

# ============================================================================================================================================================================================================

today = datetime.today()
future_records = df[pd.to_datetime(
    df['Coverage Effective Date'], format='%d-%m-%Y') > today]
if len(future_records) != 0:
    print(
        f"\nTotal of {len(future_records)} are found having future dates in Coverage Effective Date column.")

# ============================================================================================================================================================================================================

today = datetime.today()
future_records = df[pd.to_datetime(
    df['Issuance Date'], format='%d-%m-%Y') > today]
if len(future_records) != 0:
    print(
        f"\nTotal of {len(future_records)} are found having future dates in Issuance Date column.")

# ============================================================================================================================================================================================================

negativepremium = list(
    df[(df['Status'] == '11') & (df['Premium'] < 0)]['COI Number'])
if len(negativepremium) != 0:
    print(
        f"\nTotal of {len(future_records)} are found having negative premium with status inforce.")

# ============================================================================================================================================================================================================

print(df['Base Product UIN'].value_counts())
df['Group wise'] = df.apply(assign_group, axis=1)

print("\nGroup wise and PH Gender counts...\n", df[['Group wise','PH Gender']].value_counts(),"\n")

bins = [18, 31, 41, 51, 61, float('inf')]
labels = ['18-30', '31-40', '41-50', '51-60', 'Above 60']
df['Age_Bands'] = pd.cut(df['PH Entry Age'], bins=bins, labels=labels, right=False)
print("\nAge Bands counts...\n", df['Age_Bands'].value_counts(),"\n")

# ============================================================================================================================================================================================================

product_code_mapping = {'163N001V01' : 'Raksha Kavach',
                        '163N001V02' : 'Raksha Kavach',
                        '163N002V01' : 'Suraksha sukshm',
                        '163N002V02' : 'Suraksha sukshm',
                        '163N003V01' : 'Suraksha',
                        '163N003V02' : 'Suraksha',
                        '163B001V01' : 'ADB',
                        '163B002V01' : 'ATPD',
                        '163N004V01' : 'Raksha chakra',
                        '163N004V02' : 'Raksha chakra',
                        '163N007V02' : 'Sarvochchah Nidhi',
                        '163N009V01' : 'Anantha Suraksha',
                        '163N012V01' : 'Grameen Sanchay',
                        '163N013V01' : 'Raksha Chakra Poorna',                    
                        '163N014V01' : 'Anantha Suraksha Sukshm',
                    }

df['Product Name'] = df['Base Product UIN'].map(product_code_mapping)

df['Par/NonPar'] = 'Non-Par' # All products are Non-Par
df['LOB'] = 'Life'      # All products belong to Life LOB


class ReinsuredPolicy:
    def __init__(self, policy_number, retained_SA, reinsurance_type):
        self.policy_number = policy_number
        self.retained_SA = retained_SA
        self.reinsurance_type = reinsurance_type
    
    def __repr__(self):
        return f"Policy({self.policy_number}, Retained: {self.retained_SA}, Type: {self.reinsurance_type})"

# Create dictionary
Reinsured_policy_numbers = {
    399 : ReinsuredPolicy(399, 1500000, "Surplus"),
}

# Basic reinsurance flag
df['Reinsured/NonReinsured'] = df['POLICYNUMBER'].apply(
    lambda x: 'Reinsured' if x in Reinsured_policy_numbers else 'Non-Reinsured'
)



# Reinsurance premium for COI Numbers is in the file "ReinsurancePremium_filepath" is given, for the rest it is assumed to be zero. match the COI Number column in the both the dfs and assign the reinsurance premium accordingly.
if ReinsurancePremium_filepath:
    reinsurance_premium_dict = pd.read_excel(ReinsurancePremium_filepath).set_index('COI Number')['Reinsurance Premium'].to_dict()
    df['Reinsurance Premium'] = df['COI Number'].apply(
        lambda x: reinsurance_premium_dict[x] if x in reinsurance_premium_dict else 0
    )

    # Retained_SA column - use the minimum of reinsured retained SA and original SA
    df['Retained_SA'] = df.apply(
        lambda row: min(Reinsured_policy_numbers[row['POLICYNUMBER']].retained_SA, row['Original SA']) 
        if row['POLICYNUMBER'] in Reinsured_policy_numbers 
        else row['Original SA'], 
        axis=1
    )
    # print the confirmation message confirming the reinsurance premium for the COI Numbers is assigned successfully to all the COI numbers present in the Reinsurance Premium file.
    # how to check that all the COI Numbers present in the Reinsurance Premium file are assigned successfully in the main df.
    missing_reinsurance_premium_coi = [coi for coi in reinsurance_premium_dict.keys() if coi not in df['COI Number'].values]
    if len(missing_reinsurance_premium_coi) > 0:
        print(f"❌❌❌ Warning: The following COI Numbers from the Reinsurance Premium file were not found in the main data: {missing_reinsurance_premium_coi}\n")
    print("✅✅✅ Reinsurance premium assigned successfully for all COI Numbers present in the Reinsurance Premium file.\n")
else:
    df['Reinsurance Premium'] = 0
    df['Retained_SA'] = df['Original SA']
    print("ℹ️ℹ️ℹ️ No Reinsurance Premium file provided. Assigned Reinsurance Premium as 0 for all records.\n")

df['Grameen/NonGrameen'] = df['MPH Code'].apply(lambda x: 'Grameen' if x == 'CREDMPH002' else 'Non-Grameen')
print(f"{df[df['Status'] == 11]['Grameen/NonGrameen'].value_counts()}\n")



df['channel type'] = df['Channel'].apply(lambda x: x[4:-4])
print(f"{df[df['Status'] == 11]['channel type'].value_counts()}\n")



print(f"{df[df['Status'] == 11]['CLASS'].value_counts()}\n")



def get_issuance_year(date_str):
    date = pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce')
    if pd.isna(date):
        return 'Unknown'
    year = date.year
    if date.month >= 4:
        return f"{year}-{year + 1}"
    else:
        return f"{year - 1}-{year}"
df['Issuance year'] = df['Issuance Date'].apply(get_issuance_year)
print(f"{df[df['Status'] == 11]['Issuance year'].value_counts()}\n")


df['Issuance Date'] = pd.to_datetime(df['Issuance Date'], format='%d-%m-%Y', dayfirst=True, errors='coerce')
df['Valuation Date'] = pd.to_datetime(df['Valuation Date'], format='%d-%m-%Y', dayfirst=True, errors='coerce')

val_month = df['Valuation Date'].dt.month
val_year  = df['Valuation Date'].dt.year

is_matching = (
    (df['Issuance Date'].dt.month == val_month) &
    (df['Issuance Date'].dt.year == val_year)
)

total = df.groupby('POLICYNUMBER')['POLICYNUMBER'].transform('count')
matching = is_matching.groupby(df['POLICYNUMBER']).transform('sum')

df['Freelook Status'] = (
    (total == matching)
    .map({True: "New POLICY", False: "Existing POLICY"})
)


ValuationReadyFilename = new_folder_path + "/" + 'Valuation'

# summary.to_excel(ValuationReadyFilename + '_Summary.xlsx', index=False)
# print(f"Summary file saved to '{ValuationReadyFilename}_Summary.xlsx'.\n")

# ============================================================================================================================================================================================================

df.to_csv(ValuationReadyFilename + policyinputsheet_fileExtension, index=False)

# ============================================================================================================================================================================================================

Valuation_filepath = ValuationReadyFilename + policyinputsheet_fileExtension
df = pd.read_csv(Valuation_filepath, low_memory=False)

# ============================================================================================================================================================================================================

# df['Status'] all the values must be integer format except non-finite values (NA or inf) to integer
df['Status'] = pd.to_numeric(df['Status'], errors='coerce').astype('Int64')

# Check unique values
# unique_statuses = list(df['Status'].unique())
# print(unique_statuses)
# for status in unique_statuses:
#     output_file = f"{ValuationReadyFilename}_{status}{policyinputsheet_fileExtension}"
#     df[df['Status'] == status].to_csv(output_file, index=False)
#     print(f"\n'{status}' : \n'{output_file}'")

# ============================================================================================================================================================================================================

# type = list(df['Co-Borrower/Joint Life ID'].unique())
# print(type)
# for covertype in type:
#     output_file = f"{ValuationReadyFilename}_{covertype}{policyinputsheet_fileExtension}"
#     df[df['Co-Borrower/Joint Life ID'] == covertype].to_csv(output_file, index=False)
#     print(f"\n'{covertype}' : \n'{output_file}'")

# ============================================================================================================================================================================================================

def representative_sample(df, columns, sample_size, random_seed):
    """
    Generate representative sample based on selected columns.
    """
    if not columns:
        return df.sample(min(len(df), sample_size), random_state=random_seed)

    sample_df = (
        df.groupby(columns, dropna=False)
          .apply(lambda x: x.sample(min(len(x), sample_size), random_state=random_seed))
          .reset_index(drop=True)
    )

    return sample_df

def summary(df, sample_df, columns):
    print("\n===== SAMPLE SUMMARY =====\n")
    
    print(f"Original rows : {len(df):,}")
    print(f"Sample rows   : {len(sample_df):,}")
    print(f"Columns used  : {columns}")
    
    print("\nColumn Coverage:")
    print("-" * 50)
    
    for col in columns:
        orig_unique = df[col].nunique(dropna=True)
        sample_unique = sample_df[col].nunique(dropna=True)
        
        coverage = (sample_unique / orig_unique * 100) if orig_unique else 0
        
        print(f"{col:20} | {orig_unique:8} → {sample_unique:8} | {coverage:6.2f}%")
    
columns = [
    "Base Product UIN",
    "Co-Borrower/Joint Life ID",
    "Premium Mode",
    "SA_code",
    "PH Gender",
    "Status",
    "Group wise",
    "Reinsured/NonReinsured",
    "Grameen/NonGrameen",
] 

sample_size = 2   
random_seed = 42

sample_df = representative_sample(df, columns, sample_size, random_seed)
summary(df, sample_df, columns)

sample_df.to_csv(f"{ValuationReadyFilename}_sample.csv", index=False)
print("\n✅ Sample saved as sample_output.csv\n")

# ============================================================================================================================================================================================================


unique_product = list(df['Base Product UIN'].unique())
print(unique_product)
for product in unique_product:
    output_file = f"{ValuationReadyFilename}_{product}{policyinputsheet_fileExtension}"
    df[df['Base Product UIN'] == product].to_csv(output_file, index=False)
    print(f"\n'{product}' : \n'{output_file}'")


# ============================================================================================================================================================================================================

# Extract the records into a seperate file for all the policies where the issuance date falls in the month and year of the Valuation date
# which means extract the valuation month and year from Valuation date column and filter the records based on that month and year in Issuance date column.

df['Valuation Date'] = pd.to_datetime(df['Valuation Date'], format='%d-%m-%Y', dayfirst=True, errors='coerce')
valuation_month = df['Valuation Date'].dt.month.iloc[0]
valuation_year = df['Valuation Date'].dt.year.iloc[0]

# Get month name
month_name = df['Valuation Date'].dt.month_name().iloc[0]

issued_in_valuation_month = df[
    (pd.to_datetime(df['Issuance Date'], format='%d-%m-%Y', dayfirst=True).dt.month == valuation_month) &
    (pd.to_datetime(df['Issuance Date'], format='%d-%m-%Y', dayfirst=True).dt.year == valuation_year)
]
if len(issued_in_valuation_month) != 0:
    output_file = f"{ValuationReadyFilename}_{month_name}_{valuation_year}{policyinputsheet_fileExtension}"
    issued_in_valuation_month.to_csv(output_file, index=False)
    print(
        f"\nTotal of {len(issued_in_valuation_month)} records found with Issuance date in the {month_name} of {valuation_year}.")
    print(f"Records saved to '{output_file}'.")


# Extract the remaining records into a seperate file after removing the above extracted records from the main df as "NewEB""
NewEB = df[~df.index.isin(issued_in_valuation_month.index)]
NewEB_file = f"{ValuationReadyFilename}_NewEB{policyinputsheet_fileExtension}"
NewEB.to_csv(NewEB_file, index=False)
print(f"\nRemaining records after extracting Issuance month records saved to '{NewEB_file}'.")

# ============================================================================================================================================================================================================

# Monthly Claims Data

claims_directory_path = os.path.dirname(claims_filepath)

claims_filename, claims_fileExtension = os.path.splitext(
    os.path.basename(claims_filepath))

if claims_fileExtension == ".xlsx":
    claimed = pd.read_excel(claims_filepath)
else:
    claimed = pd.read_csv(claims_filepath, low_memory=False)

# Check for missing values in the specified columns
claims_missing_values = claimed.isnull()

# Claim amount should be a missing value and greater than 0
claims_missing_values['Claim Amount'] = claimed['Claim Amount'].isnull() | (claimed['Claim Amount'] <= 0)
# print the COI_nos where Claim Amount is missing or less than or equal to 0
invalid_claim_amount_cois = claimed[claims_missing_values['Claim Amount']]['COI_no'].tolist()
print(f"\nCOI_nos with missing or invalid Claim Amounts in claims sheet : {invalid_claim_amount_cois}")

# Print columns with missing values and their counts
print("\nMissing values by column in claims sheet :")
print(claims_missing_values.sum())

claimed['Coverage Effective Date'] = pd.to_datetime(
    claimed['Coverage Effective Date'], dayfirst=True)
claimed['Date of Death'] = pd.to_datetime(
    claimed['Date of Death'], dayfirst=True)
claimed['Date of claim intimation'] = pd.to_datetime(
    claimed['Date of claim intimation'], dayfirst=True)
claimed['Date of claim settlement'] = pd.to_datetime(
    claimed['Date of claim settlement'], dayfirst=True)

print(f"Date related Checks in {claims_filename}{claims_fileExtension}")

coi_list = claimed[claimed['Date of Death'] >
                   claimed['Date of claim intimation']]['COI_no'].tolist()
print(f"\n    Date of Death > Date of claim intimation : {coi_list}")

coi_list = claimed[claimed['Date of Death'] >
                   claimed['Date of claim settlement']]['COI_no'].tolist()
print(f"\n    Date of Death > Date of claim settlement : {coi_list}")

coi_list = claimed[claimed['Date of claim intimation'] >
                   claimed['Date of claim settlement']]['COI_no'].tolist()
print(
    f"\n    Date of claim intimation > Date of claim settlement : {coi_list}")

coi_list = claimed[claimed['Coverage Effective Date'] >
                   claimed['Date of Death']]['COI_no'].tolist()
print(f"\n    Coverage effective date > Date of Death : {coi_list}")

coi_list = claimed[claimed['Coverage Effective Date'] >
                   claimed['Date of claim intimation']]['COI_no'].tolist()
print(f"\n    Coverage effective date > Date of claim intimation : {coi_list}")

coi_list = claimed[claimed['Coverage Effective Date'] >
                   claimed['Date of claim settlement']]['COI_no'].tolist()
print(f"\n    Coverage effective date > Date of claim settlement : {coi_list}")

claimed = claimed.iloc[:, :claimed.columns.get_loc('Cause of Death') + 1]

date_columns = ['Coverage Effective Date', 'Date of Death',
                'Date of claim intimation', 'Date of claim settlement']
claimed[date_columns] = claimed[date_columns].apply(
    pd.to_datetime, dayfirst=True, errors='coerce')
claimed[date_columns] = claimed[date_columns].apply(
    lambda x: x.dt.strftime('%d-%m-%Y'))

duplicate_policy_numbers = claimed[claimed.duplicated(
    subset=['COI_no'], keep=False)]
duplicate_values_list = duplicate_policy_numbers['COI_no'].tolist()

print(duplicate_values_list)

grouped_df = claimed.groupby(['Channel', 'mphcode']).agg({
    'Claim Amount': 'sum',
    'COI_no': 'count'
}).reset_index()

grouped_df.rename(columns={'Claim Amount': 'Total Claim Amount',
                  'COI_no': 'Number of Claims'}, inplace=True)

pd.DataFrame(grouped_df)

claimed['Claim Amount'].sum()

claimedcoi = list(claimed['COI_no'])
print(f"\nThe No. of claims COIs in claimInputSheet : {len(claimedcoi)}")

# ============================================================================================================================================================================================================
# compare the PH Gender of completedata and Deseased Person Gender in Claims data
# column 'Co-Borrower/Joint Life ID' should also be included in the comparison output file.
# mismatched_genders = []
# for coi in claimedcoi:
#     policy_record = df[df['COI Number'] == coi]
#     if not policy_record.empty:
#         policy_gender = policy_record['PH Gender'].values[0]
#         claim_record = claimed[claimed['COI_no'] == coi]
#         if not claim_record.empty:
#             claim_gender = claim_record['Deseased Person Gender'].values[0]
#             if policy_gender != claim_gender:
#                 mismatched_genders.append((coi, policy_gender, claim_gender))
#                 # include Co-Borrower/Joint Life ID in the output
#                 co_borrower_id = policy_record['Co-Borrower/Joint Life ID'].values[0]
#                 mismatched_genders[-1] += (co_borrower_id,)
# if mismatched_genders:
#     print("\nMismatched Genders between Policy and Claims Data:")
#     for coi, policy_gender, claim_gender, co_borrower_id in mismatched_genders:
#         print(f"COI: {coi}, Policy Gender: {policy_gender}, Claim Gender: {claim_gender}, Co-Borrower/Joint Life ID: {co_borrower_id}")
#         # write these mismatches to the error file
#     with open(error_file, 'a') as file:
#         file.write("\nMismatched Genders between Policy and Claims Data\n")
#         file.write("COI Number, Policy Gender, Claim Gender, Co-Borrower/Joint Life ID\n")
#         for coi, policy_gender, claim_gender, co_borrower_id in mismatched_genders:
#             file.write(f"{coi}, {policy_gender}, {claim_gender}, {co_borrower_id}\n")
# else:
#     print("\nNo mismatched genders found between Policy and Claims Data.")  

# ============================================================================================================================================================================================================
# check the data of Claims and PolicyInputSheet Deaths.

PISclaims = list(df[(df['Status'] == 43)]['COI Number'])
print(f"\nThe No. of claim COIs(43) in PIS are : {len(PISclaims)}")

# Convert lists to sets for efficient comparison
set_claimedcoi = set(claimedcoi)
set_PISclaims = set(PISclaims)

# 1. COI numbers in claimedcoi but missing in PISclaims
missing_in_PISclaims = list(set_claimedcoi - set_PISclaims)

# 2. COI numbers in PISclaims but missing in claimedcoi
missing_in_claimedcoi = list(set_PISclaims - set_claimedcoi)

print(f"\nCOI numbers present in 'claimedcoi' but missing in 'PISclaims': {len(missing_in_PISclaims)}")
print(missing_in_PISclaims)

# Open the existing error file in append mode
with open(error_file, 'a') as f:
    f.write("\nCOI numbers present in 'ClaimInputSheet' but missing in 'PIS' - created the csv file.\n")

# write the records from claim sheet with COI numbers in missing_in_PISclaims to the error file and also make a seperate csv file with the details.
for coi in missing_in_PISclaims:
    claim_record = claimed[claimed['COI_no'] == coi]
    if not claim_record.empty:
        file_path = os.path.join(new_folder_path, "Claims_missing_in_PISclaims.csv")
        claim_record.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)
print(f"\nDetails of COI numbers present in 'ClaimInputSheet' but missing in 'PISclaims' saved to '{file_path}'.")


print(f"\nCOI numbers present in 'PISclaims' but missing in 'claimedcoi': {len(missing_in_claimedcoi)}")
print(missing_in_claimedcoi)

# Open the existing error file in append mode again
with open(error_file, 'a') as f:
    f.write("\nCOI numbers labelled as 43 in 'PIS' but missing in 'ClaimInputsheet' - created the csv file.\n")

for coi in missing_in_claimedcoi:
    policy_record = df[df['COI Number'] == coi]
    if not policy_record.empty:
        file_path = os.path.join(new_folder_path, "PISclaims_missing_in_ClaimInputSheet.csv")
        policy_record.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)       
print(f"\nDetails of COI numbers labelled as 43 in 'PIS' but missing in 'ClaimInputsheet' saved to '{file_path}'.")

# ============================================================================================================================================================================================================
# Monthly Cancelled Data

cancelled_directory_path = os.path.dirname(cancelled_filepath)

cancelled_filename, cancelled_fileExtension = os.path.splitext(
    os.path.basename(cancelled_filepath))

if cancelled_fileExtension == ".xlsx":
    cancelled = pd.read_excel(cancelled_filepath)
else:
    cancelled = pd.read_csv(cancelled_filepath, low_memory=False)

cancelledcoi = list(cancelled['COI_no'])

print(f"\nThe Total no of cancelled COIs are {len(cancelledcoi)}\n")

print("\nProcess completed successfully.")
endtime = datetime.now()
print(f"Process ended at: {endtime.strftime('%d-%m-%Y %H:%M:%S')}")
print(f"Total duration: {endtime - starttime}\n")   
