import pandas as pd
import numpy as np
import time
from datetime import datetime

# ==========================================================================================
# CONFIGURATION & MAPPINGS
# ==========================================================================================
STATUS_MAP = {
    11: 'In-force', 22: 'Cancelled', 24: 'Cov closed', 
    32: 'Surrender', 33: 'Lapsed', 42: 'Claim intimated', 
    43: 'claim settled', 61: 'Expired'
}

HIERARCHY_PRIORITY = {
    'In-force': 1, 'Cancelled': 2, 'claim settled': 3, 
    'Claim intimated': 4, 'Surrender': 5, 'Lapsed': 6, 
    'Cov closed': 7, 'Expired': 8, 'Other': 9
}

GROUP_COLS = ['Issuance year', 'Reinsured/NonReinsured', 'Base Product UIN', 'Premium Mode']

def run_ultimate_processor(input_file: str, output_file: str):
    start_time = time.time()
    print(f"\n🚀 [START] Processing {input_file}...")
    
    # --------------------------------------------------------------------------------------
    # 1. LOAD DATA (OPTIMIZED DTYPES)
    # --------------------------------------------------------------------------------------
    # Loading with low_memory=False and specific columns if possible for speed
    df = pd.read_csv(input_file, low_memory=False)
    print(f"✅ Loaded {len(df):,} records | Time: {time.time() - start_time:.2f}s")

    # --------------------------------------------------------------------------------------
    # 2. VECTORIZED DATE & DIMENSION PROCESSING
    # --------------------------------------------------------------------------------------
    # Fast Date Conversion (April-March Financial Year)
    dt = pd.to_datetime(df['Issuance Date'], format='%d-%m-%Y', errors='coerce')
    years = dt.dt.year.values
    months = dt.dt.month.values
    
    # Calculate FY Start Year: If month >= 4 then year, else year-1
    fy_starts = np.where(months >= 4, years, years - 1)
    
    # Create labels efficiently (avoiding repeated string operations)
    fy_labels = np.where(
        np.isnan(years), 
        'Unknown', 
        pd.Series(fy_starts).astype(str) + '-' + pd.Series(fy_starts + 1).astype(str)
    )
    df['Issuance year'] = fy_labels
    
    # Map Status & Descriptions
    df['Status_desc'] = df['Status'].map(STATUS_MAP).fillna('Other')
    
    # Optimize memory & speed by converting to Categories
    cat_cols = ['Issuance year', 'Reinsured/NonReinsured', 'Base Product UIN', 'Premium Mode', 'Status_desc']
    for col in cat_cols:
        df[col] = df[col].astype('category')
    
    print(f"✅ Date & Dimension processing complete | Time: {time.time() - start_time:.2f}s")

    # --------------------------------------------------------------------------------------
    # 3. VECTORIZED DOMINANT STATUS (TIE-BREAKING)
    # --------------------------------------------------------------------------------------
    # Use GroupBy + nunique to get COI counts per policy/status
    counts = (df.groupby(['POLICYNUMBER', 'Status_desc'], observed=True)['COI Number']
                .nunique().reset_index())
    
    # Apply hierarchy priority for tie-breaking
    counts['priority'] = counts['Status_desc'].map(HIERARCHY_PRIORITY).fillna(9)

    # Sort and pick top row for each policy: Max COIs, then highest priority
    dominant = (counts.sort_values(by=['POLICYNUMBER', 'COI Number', 'priority'], 
                                  ascending=[True, False, True])
                      .drop_duplicates('POLICYNUMBER'))

    # Map dominant status back to unique Status Code
    reverse_map = {v: k for k, v in STATUS_MAP.items()}
    dominant['Dominant_Status'] = dominant['Status_desc'].apply(lambda x: reverse_map.get(x, 'Other'))
    
    # Broadcast dominant status to main df
    policy_dominant_map = dominant.set_index('POLICYNUMBER')['Dominant_Status']
    df['Dominant_Status_Code'] = df['POLICYNUMBER'].map(policy_dominant_map)
    
    print(f"✅ Dominant status calculation complete | Time: {time.time() - start_time:.2f}s")

    # --------------------------------------------------------------------------------------
    # 4. DATA AGGREGATION & REPORTING
    # --------------------------------------------------------------------------------------
    # A. Policy Counts (Unique policy level, based on Dominant status)
    df_policy_meta = df.sort_values('Issuance Date').drop_duplicates('POLICYNUMBER')
    policy_agg = (df_policy_meta.groupby(['Dominant_Status_Code'] + GROUP_COLS, observed=False)
                  ['POLICYNUMBER'].nunique()
                  .rename('POLICYNUMBER_count').reset_index()
                  .rename(columns={'Dominant_Status_Code': 'Status'}))

    # B. Main Metric Aggregation (By Actual Status for full accuracy)
    main_agg = df.groupby(['Status'] + GROUP_COLS, observed=False).agg(
        COINumber_count=('COI Number', 'nunique'),
        PremiumAmount_sum=('Premium', 'sum'),
        ReinsurancePremium_sum=('Reinsurance Premium', 'sum'),
        OriginalSA=('Original SA', 'sum'),
        Retained_SA=('Retained_SA', 'sum'),
        Gross_Reserve=('Gross Reserve\n(NPV)', 'sum'),
        Net_Reserve=('Net Reserve\n(NPV)', 'sum')
    ).reset_index()

    # C. Derived Calculations
    main_agg['SumAssured_Gross_sum'] = main_agg['OriginalSA']
    main_agg['SumAssured_Net_sum'] = main_agg['OriginalSA'] - main_agg['Retained_SA']
    main_agg['Reserve_Gross_sum'] = main_agg['Gross_Reserve']
    main_agg['Reserve_Net_sum'] = main_agg['Net_Reserve']
    main_agg['SumAtRisk_Gross_sum'] = main_agg['SumAssured_Gross_sum'] - main_agg['Reserve_Gross_sum']
    main_agg['SumAtRisk_Net_sum'] = main_agg['SumAssured_Net_sum'] - main_agg['Reserve_Net_sum']
    
    # Combine Metrics and Policy Counts
    combined = main_agg.merge(policy_agg, on=['Status'] + GROUP_COLS, how='outer').fillna(0)

    # --------------------------------------------------------------------------------------
    # 5. FULL GRID GENERATION (ENSURE NO MISSING STATUSES)
    # --------------------------------------------------------------------------------------
    all_statuses = list(STATUS_MAP.keys()) + ['Other']
    status_df = pd.DataFrame({'Status': all_statuses})
    
    # Multi-dimensional unique skeleton
    skeleton = combined[GROUP_COLS].drop_duplicates()
    skeleton['key'] = 1
    status_df['key'] = 1
    full_grid = skeleton.merge(status_df, on='key').drop('key', axis=1)
    
    report = full_grid.merge(combined, on=['Status'] + GROUP_COLS, how='left').fillna(0)
    
    print(f"✅ Aggregation & Grid complete | Time: {time.time() - start_time:.2f}s")

    # --------------------------------------------------------------------------------------
    # 6. OUTPUT & VALIDATION
    # --------------------------------------------------------------------------------------
    final_cols = [
        'Base Product UIN', 'Premium Mode', 'Reinsured/NonReinsured',
        'Issuance year', 'Status', 'POLICYNUMBER_count', 'COINumber_count',
        'SumAssured_Gross_sum', 'SumAssured_Net_sum', 'PremiumAmount_sum',
        'ReinsurancePremium_sum', 'Reserve_Gross_sum', 'Reserve_Net_sum',
        'SumAtRisk_Gross_sum', 'SumAtRisk_Net_sum'
    ]
    
    report = report[final_cols]
    
    # Validation Check
    unique_count = df['POLICYNUMBER'].nunique()
    reported_count = int(report['POLICYNUMBER_count'].sum())
    print("\n📊 [VALIDATION]")
    print(f"Total Unique Policies in Data: {unique_count}")
    print(f"Total Policies in Final Report: {reported_count}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        report.to_excel(writer, index=False, sheet_name='Summary')
    
    print(f"\n💾 [SUCCESS] File saved: {output_file}")
    print(f"🕒 Total Runtime: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    run_ultimate_processor("Valuation_complete.csv", "IRDAI Forms Grouping ULTIMATE.xlsx")
