import pandas as pd
import numpy as np

print("\nLoading CSV file...\n")
print(f"Time : {pd.Timestamp.now()}")

df = pd.read_csv("Valuation_complete.csv", low_memory=False)
print("CSV file loaded successfully.\n")

print(df.columns)

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
    1: 'In-force',
    2: 'Cancelled',
    3: 'Claim settled',
    4: 'Claim intimated',
    5: 'Surrender',
    6: 'Lapsed',
    7: 'Cov closed',
    8: 'Expired',
}

hierarchy_priority = {status: priority for priority, status in dictionary_segregation_hierarchy.items()}
hierarchy_priority['Other'] = 9


# ============================================================================================================
# SHEET 1 FINAL (NO DUPLICATION ANYWHERE)

df['Status_desc'] = df['Status'].map(dictionary_status_desc).fillna('Other')

all_status_codes = list(dictionary_status_desc.keys())
all_status_codes.append('Other')

# ------------------------------------------------------------------------------------------------------------
# STEP 1: DOMINANT STATUS

policy_dominant_status = {}

for policy_num in df['POLICYNUMBER'].unique():
    policy_df = df[df['POLICYNUMBER'] == policy_num]

    status_counts = policy_df.groupby('Status_desc')['COI Number'].nunique()

    if not status_counts.empty:
        max_coi_count = status_counts.max()
        max_statuses = status_counts[status_counts == max_coi_count].index.tolist()

        if len(max_statuses) == 1:
            dominant_status_desc = max_statuses[0]
        else:
            status_priorities = [(status, hierarchy_priority.get(status, 999)) for status in max_statuses]
            status_priorities.sort(key=lambda x: x[1])
            dominant_status_desc = status_priorities[0][0]

        reverse_map = {v: k for k, v in dictionary_status_desc.items()}
        policy_dominant_status[policy_num] = reverse_map.get(dominant_status_desc, 'Other')
    else:
        policy_dominant_status[policy_num] = 'Other'

df['Dominant_Status'] = df['POLICYNUMBER'].map(policy_dominant_status)

# ------------------------------------------------------------------------------------------------------------
# STEP 2: STRICT ONE ROW PER POLICY (FINAL FIX FOR ALL DUPLICATION)

df_policy = df.sort_values('Issuance Date').drop_duplicates('POLICYNUMBER')

df_policy = df_policy[['POLICYNUMBER', 'Dominant_Status',
                       'Issuance year',
                       'Reinsured/NonReinsured',
                       'Base Product UIN',
                       'Premium Mode']]

# ------------------------------------------------------------------------------------------------------------
# POLICY COUNT KEY

df_policy['target_key'] = df_policy['Dominant_Status'].astype(str) + '|' + \
                         df_policy['Issuance year'].astype(str) + '|' + \
                         df_policy['Reinsured/NonReinsured'].astype(str) + '|' + \
                         df_policy['Base Product UIN'].astype(str) + '|' + \
                         df_policy['Premium Mode'].astype(str)

policy_counts = df_policy.groupby('target_key')['POLICYNUMBER'].nunique().to_dict()

# ------------------------------------------------------------------------------------------------------------
# STEP 3: NORMAL AGGREGATION (UNCHANGED)

agg_df = df.groupby(['Status', 'Issuance year', 'Reinsured/NonReinsured',
                     'Base Product UIN', 'Premium Mode']).agg({
    'COI Number': 'nunique',
    'Premium': 'sum',
    'Reinsurance Premium': 'sum',
    'Original SA': 'sum',
    'Retained_SA': 'sum',
    'Gross Reserve\n(NPV)': 'sum',
    'Net Reserve\n(NPV)': 'sum'
}).reset_index()

agg_df.rename(columns={'COI Number': 'COINumber_count'}, inplace=True)

agg_df['SumAssured_Gross_sum'] = agg_df['Original SA']
agg_df['SumAssured_Net_sum'] = agg_df['Original SA'] - agg_df['Retained_SA']
agg_df['Reserve_Gross_sum'] = agg_df['Gross Reserve\n(NPV)']
agg_df['Reserve_Net_sum'] = agg_df['Net Reserve\n(NPV)']
agg_df['SumAtRisk_Gross_sum'] = agg_df['SumAssured_Gross_sum'] - agg_df['Reserve_Gross_sum']
agg_df['SumAtRisk_Net_sum'] = agg_df['SumAssured_Net_sum'] - agg_df['Reserve_Net_sum']

agg_df = agg_df.drop(columns=[
    'Original SA', 'Retained_SA',
    'Gross Reserve\n(NPV)', 'Net Reserve\n(NPV)'
])

# ------------------------------------------------------------------------------------------------------------
# STEP 4: FULL GRID

unique_dims = agg_df[['Issuance year', 'Reinsured/NonReinsured',
                      'Base Product UIN', 'Premium Mode']].drop_duplicates()

full_grid = unique_dims.assign(key=1).merge(
    pd.DataFrame({'Status': all_status_codes, 'key': 1}),
    on='key'
).drop('key', axis=1)

final_df = full_grid.merge(
    agg_df,
    on=['Status', 'Issuance year', 'Reinsured/NonReinsured',
        'Base Product UIN', 'Premium Mode'],
    how='left'
)

numeric_cols = final_df.columns.difference(['Status', 'Issuance year',
                                            'Reinsured/NonReinsured',
                                            'Base Product UIN', 'Premium Mode'])

final_df[numeric_cols] = final_df[numeric_cols].fillna(0)

# ------------------------------------------------------------------------------------------------------------
# STEP 5: POLICY COUNT ASSIGNMENT

def get_policy_count(row):
    key = str(row['Status']) + '|' + str(row['Issuance year']) + '|' + \
          str(row['Reinsured/NonReinsured']) + '|' + \
          str(row['Base Product UIN']) + '|' + str(row['Premium Mode'])
    return policy_counts.get(key, 0)

final_df['POLICYNUMBER_count'] = final_df.apply(get_policy_count, axis=1)

# ------------------------------------------------------------------------------------------------------------
# FINAL OUTPUT

summary = final_df[[
    'Base Product UIN',
    'Premium Mode',
    'Reinsured/NonReinsured',
    'Issuance year',
    'Status',
    'POLICYNUMBER_count',
    'COINumber_count',
    'SumAssured_Gross_sum',
    'SumAssured_Net_sum',
    'Premium',
    'Reinsurance Premium',
    'Reserve_Gross_sum',
    'Reserve_Net_sum',
    'SumAtRisk_Gross_sum',
    'SumAtRisk_Net_sum'
]].rename(columns={
    'Premium': 'PremiumAmount_sum',
    'Reinsurance Premium': 'ReinsurancePremium_sum'
})

# VALIDATION
print("\nValidation Check:")
print("Total unique policies:", df['POLICYNUMBER'].nunique())
print("Total from summary:", int(summary['POLICYNUMBER_count'].sum()))

# SAVE
with pd.ExcelWriter('IRDAI Forms Grouping Data FINAL.xlsx', engine='openpyxl') as writer:
    summary.to_excel(writer, index=False, sheet_name='Summary')
    print("✓ Summary sheet saved")

print(f"\nFile saved successfully")
print(f"Time : {pd.Timestamp.now()}")