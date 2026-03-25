import pandas as pd
import numpy as np

print("\nLoading CSV file...\n")
print(f"Time : {pd.Timestamp.now()}")

# Use low_memory=False for safety, but we can specify dtypes later for v3
df = pd.read_csv("Valuation_complete.csv", low_memory=False)
print("CSV file loaded successfully.\n")

# Vectorized Issuance Year calculation
def vectorize_issuance_year(dates):
    dt = pd.to_datetime(dates, format='%d-%m-%Y', errors='coerce')
    years = dt.dt.year
    months = dt.dt.month
    
    # Financial year logic: April to March
    # If month >= 4: YYYY-(YYYY+1)
    # Else: (YYYY-1)-YYYY
    fy_start = np.where(months >= 4, years, years - 1)
    fy_end = fy_start + 1
    
    # Format strings efficiently
    labels = np.where(
        dt.isna(), 
        'Unknown', 
        fy_start.astype(str) + '-' + fy_end.astype(str)
    )
    return labels

df['Issuance year'] = vectorize_issuance_year(df['Issuance Date'])

dictionary_status_desc = {
    11: 'In-force',
    22: 'Cancelled',
    24: 'Cov closed',
    32: 'Surrender',
    33: 'Lapsed',
    42: 'Claim intimated',
    43: 'claim settled',  
    61: 'Expired',
}

dictionary_segregation_hierarchy = {
    'In-force': 1,
    'Cancelled': 2,
    'claim settled': 3,
    'Claim intimated': 4,
    'Surrender': 5,
    'Lapsed': 6,
    'Cov closed': 7,
    'Expired': 8,
    'Other': 9
}

# Map status to description
df['Status_desc'] = df['Status'].map(dictionary_status_desc).fillna('Other')

# ------------------------------------------------------------------------------------------------------------
# STEP 1: VECTORIZED DOMINANT STATUS (FASTER VERSION)
# ------------------------------------------------------------------------------------------------------------

# 1. Count unique COI Number per Policy and Status
status_counts = df.groupby(['POLICYNUMBER', 'Status_desc'], as_index=False)['COI Number'].nunique()

# 2. Add priority for tie-breaking
status_counts['priority'] = status_counts['Status_desc'].map(dictionary_segregation_hierarchy).fillna(999)

# 3. Sort: Higher count first, then lower priority (higher in hierarchy)
# We want max count, then min priority
status_counts = status_counts.sort_values(
    ['POLICYNUMBER', 'COI Number', 'priority'], 
    ascending=[True, False, True]
)

# 4. Take the first row for each policy (the dominant one)
dominant_df = status_counts.drop_duplicates('POLICYNUMBER')

# 5. Map back to Status Code (reverse map)
reverse_map = {v: k for k, v in dictionary_status_desc.items()}
dominant_df['Dominant_Status'] = dominant_df['Status_desc'].map(reverse_map).fillna('Other')

# Broadcast dominant status back to main df (using map for speed)
policy_to_dominant = dominant_df.set_index('POLICYNUMBER')['Dominant_Status']
df['Dominant_Status'] = df['POLICYNUMBER'].map(policy_to_dominant)

# ------------------------------------------------------------------------------------------------------------
# STEP 2: POLICY LEVEL DATA
# ------------------------------------------------------------------------------------------------------------

# One row per policy, picking first occurrence (same as original logic: df.sort_values('Issuance Date').drop_duplicates('POLICYNUMBER'))
# But we already have the dominant status. Let's get the metadata.
df_policy = df.sort_values('Issuance Date').drop_duplicates('POLICYNUMBER').copy()

# Keep only necessary columns
group_cols = ['Issuance year', 'Reinsured/NonReinsured', 'Base Product UIN', 'Premium Mode']
df_policy = df_policy[['POLICYNUMBER', 'Dominant_Status'] + group_cols]

# ------------------------------------------------------------------------------------------------------------
# STEP 3: AGGREGATION & POLICY COUNTS
# ------------------------------------------------------------------------------------------------------------

# Count policies by their dominant characteristics
# We use Dominant_Status as the 'Status' for counting purposes
policy_counts = df_policy.groupby(['Dominant_Status'] + group_cols)['POLICYNUMBER'].nunique().reset_index()
policy_counts.columns = ['Status'] + group_cols + ['POLICYNUMBER_count']

# Normal Aggregation (Actual Status)
agg_df = df.groupby(['Status'] + group_cols).agg({
    'COI Number': 'nunique',
    'Premium': 'sum',
    'Reinsurance Premium': 'sum',
    'Original SA': 'sum',
    'Retained_SA': 'sum',
    'Gross Reserve\n(NPV)': 'sum',
    'Net Reserve\n(NPV)': 'sum'
}).reset_index()

agg_df.rename(columns={'COI Number': 'COINumber_count'}, inplace=True)

# Derive columns
agg_df['SumAssured_Gross_sum'] = agg_df['Original SA']
agg_df['SumAssured_Net_sum'] = agg_df['Original SA'] - agg_df['Retained_SA']
agg_df['Reserve_Gross_sum'] = agg_df['Gross Reserve\n(NPV)']
agg_df['Reserve_Net_sum'] = agg_df['Net Reserve\n(NPV)']
agg_df['SumAtRisk_Gross_sum'] = agg_df['SumAssured_Gross_sum'] - agg_df['Reserve_Gross_sum']
agg_df['SumAtRisk_Net_sum'] = agg_df['SumAssured_Net_sum'] - agg_df['Reserve_Net_sum']

# Cleaning
agg_df = agg_df.drop(columns=['Original SA', 'Retained_SA', 'Gross Reserve\n(NPV)', 'Net Reserve\n(NPV)'])

# ------------------------------------------------------------------------------------------------------------
# STEP 4: FULL GRID & MERGING
# ------------------------------------------------------------------------------------------------------------

all_status_codes = list(dictionary_status_desc.keys()) + ['Other']

# Create cross join grid
unique_dims = agg_df[group_cols].drop_duplicates()
status_df = pd.DataFrame({'Status': all_status_codes})

unique_dims['key'] = 1
status_df['key'] = 1
full_grid = unique_dims.merge(status_df, on='key').drop('key', axis=1)

# Merge Aggregation
final_df = full_grid.merge(agg_df, on=['Status'] + group_cols, how='left')

# Merge Policy Counts (Now vectorized instead of .apply)
final_df = final_df.merge(policy_counts, on=['Status'] + group_cols, how='left')

# Fill NaNs
numeric_cols = final_df.select_dtypes(include=[np.number]).columns
final_df[numeric_cols] = final_df[numeric_cols].fillna(0)

# Final formatting
final_df.rename(columns={
    'Premium': 'PremiumAmount_sum',
    'Reinsurance Premium': 'ReinsurancePremium_sum'
}, inplace=True)

# Selection
summary = final_df[[
    'Base Product UIN', 'Premium Mode', 'Reinsured/NonReinsured',
    'Issuance year', 'Status', 'POLICYNUMBER_count', 'COINumber_count',
    'SumAssured_Gross_sum', 'SumAssured_Net_sum', 'PremiumAmount_sum',
    'ReinsurancePremium_sum', 'Reserve_Gross_sum', 'Reserve_Net_sum',
    'SumAtRisk_Gross_sum', 'SumAtRisk_Net_sum'
]]

# VALIDATION
print("\nValidation Check:")
print("Total unique policies:", df['POLICYNUMBER'].nunique())
print("Total from summary:", int(summary['POLICYNUMBER_count'].sum()))

# SAVE
output_file = 'IRDAI Forms Grouping Data V2.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    summary.to_excel(writer, index=False, sheet_name='Summary')
    print(f"✓ Summary sheet saved to {output_file}")

print(f"\nFile saved successfully")
print(f"Time : {pd.Timestamp.now()}")
