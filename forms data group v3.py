import pandas as pd
import numpy as np

print("\nLoading CSV with optimized dtypes...\n")
print(f"Time : {pd.Timestamp.now()}")

# Standard dictionary
dictionary_status_desc = {
    11: 'In-force', 22: 'Cancelled', 24: 'Cov closed', 
    32: 'Surrender', 33: 'Lapsed', 42: 'Claim intimated', 
    43: 'claim settled', 61: 'Expired',
}

# Read only necessary columns and set types if possible
df = pd.read_csv("Valuation_complete.csv", low_memory=False)

# Optimize dimensions
dim_cols = ['Reinsured/NonReinsured', 'Base Product UIN', 'Premium Mode', 'Status']
for col in dim_cols:
    df[col] = df[col].astype('category')

# Date Conversion (Highly Vectorized)
dt = pd.to_datetime(df['Issuance Date'], format='%d-%m-%Y', errors='coerce')
years = dt.dt.year
months = dt.dt.month
fy_start = np.where(months >= 4, years, years - 1)
df['Issuance year'] = np.where(dt.isna(), 'Unknown', (fy_start).astype(str) + '-' + (fy_start+1).astype(str))
df['Issuance year'] = df['Issuance year'].astype('category')

# Map status to description
df['Status_desc'] = df['Status'].map(dictionary_status_desc).fillna('Other').astype('category')

# Hierarchy Mapping
dictionary_segregation_hierarchy = {
    'In-force': 1, 'Cancelled': 2, 'claim settled': 3, 
    'Claim intimated': 4, 'Surrender': 5, 'Lapsed': 6, 
    'Cov closed': 7, 'Expired': 8, 'Other': 9
}

# ------------------------------------------------------------------------------------------------------------
# HIGH PERFORMANCE DOMINANT STATUS (TRANSFORM + AGG)
# ------------------------------------------------------------------------------------------------------------

# Count unique COIs per policy+status
counts = df.groupby(['POLICYNUMBER', 'Status_desc'], observed=True)['COI Number'].nunique().reset_index()
counts['priority'] = counts['Status_desc'].map(dictionary_segregation_hierarchy).fillna(9)

# Efficiently find max counts and priority using sorting and drop_duplicates
dominant = (counts.sort_values(by=['POLICYNUMBER', 'COI Number', 'priority'], 
                              ascending=[True, False, True])
                  .drop_duplicates('POLICYNUMBER'))

# Map dominant status back
policy_map = dict(zip(dominant['POLICYNUMBER'], dominant['Status_desc']))
df['Dominant_Status_Desc'] = df['POLICYNUMBER'].map(policy_map)

# Map back to status code
reverse_map = {v: k for k, v in dictionary_status_desc.items()}
# Dominant status code for reporting
df['Dominant_Status_Code'] = df['Dominant_Status_Desc'].apply(lambda x: reverse_map.get(x, 'Other'))

# ------------------------------------------------------------------------------------------------------------
# SINGLE-PASS AGGREGATION
# ------------------------------------------------------------------------------------------------------------
group_keys = ['Issuance year', 'Reinsured/NonReinsured', 'Base Product UIN', 'Premium Mode']

# Prepare Policy Data (unique policies)
df_policy = df.sort_values('Issuance Date').drop_duplicates('POLICYNUMBER')

# Aggregate Policy counts (by dominant status)
policy_agg = (df_policy.groupby(['Dominant_Status_Code'] + group_keys, observed=False)['POLICYNUMBER']
              .nunique().rename('POLICYNUMBER_count').reset_index())
policy_agg.rename(columns={'Dominant_Status_Code': 'Status'}, inplace=True)

# Actual Data aggregation
agg_df = df.groupby(['Status'] + group_keys, observed=False).agg(
    COINumber_count=('COI Number', 'nunique'),
    PremiumAmount_sum=('Premium', 'sum'),
    ReinsurancePremium_sum=('Reinsurance Premium', 'sum'),
    OriginalSA=('Original SA', 'sum'),
    Retained_SA=('Retained_SA', 'sum'),
    Gross_Reserve=('Gross Reserve\n(NPV)', 'sum'),
    Net_Reserve=('Net Reserve\n(NPV)', 'sum')
).reset_index()

# Calculations
agg_df['SumAssured_Gross_sum'] = agg_df['OriginalSA']
agg_df['SumAssured_Net_sum'] = agg_df['OriginalSA'] - agg_df['Retained_SA']
agg_df['Reserve_Gross_sum'] = agg_df['Gross_Reserve']
agg_df['Reserve_Net_sum'] = agg_df['Net_Reserve']
agg_df['SumAtRisk_Gross_sum'] = agg_df['SumAssured_Gross_sum'] - agg_df['Reserve_Gross_sum']
agg_df['SumAtRisk_Net_sum'] = agg_df['SumAssured_Net_sum'] - agg_df['Reserve_Net_sum']

# Merge Aggregation and Policy counts
final_df = agg_df.merge(policy_agg, on=['Status'] + group_keys, how='outer').fillna(0)

# ------------------------------------------------------------------------------------------------------------
# FULL GRID (PRESERVING ALL STATUSES)
# ------------------------------------------------------------------------------------------------------------
all_status_codes = list(dictionary_status_desc.keys()) + ['Other']
status_cat = pd.DataFrame({'Status': all_status_codes})

# Cross-join Dimensions with Status
unique_dims = final_df[group_keys].drop_duplicates()
unique_dims['tmp'] = 1
status_cat['tmp'] = 1
grid = unique_dims.merge(status_cat, on='tmp').drop('tmp', axis=1)

# Final Merge with full grid
final_report = grid.merge(final_df, on=['Status'] + group_keys, how='left').fillna(0)

# Reorder
summary = final_report[[
    'Base Product UIN', 'Premium Mode', 'Reinsured/NonReinsured',
    'Issuance year', 'Status', 'POLICYNUMBER_count', 'COINumber_count',
    'SumAssured_Gross_sum', 'SumAssured_Net_sum', 'PremiumAmount_sum',
    'ReinsurancePremium_sum', 'Reserve_Gross_sum', 'Reserve_Net_sum',
    'SumAtRisk_Gross_sum', 'SumAtRisk_Net_sum'
]]

print("\nValidation:")
print("Unique Policies:", df['POLICYNUMBER'].nunique())
print("Reporting Policies:", int(summary['POLICYNUMBER_count'].sum()))

# Save
output_excel = 'IRDAI Forms Grouping Data V3.xlsx'
with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
    summary.to_excel(writer, index=False, sheet_name='Summary')

print(f"\nFinal File: {output_excel}")
print(f"Time : {pd.Timestamp.now()}")
