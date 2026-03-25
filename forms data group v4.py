import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any

class IRDAIFormProcessor:
    def __init__(self, filename: str):
        self.filename = filename
        self.timestamp = datetime.now()
        self.status_map = {
            11: 'In-force', 22: 'Cancelled', 24: 'Cov closed', 
            32: 'Surrender', 33: 'Lapsed', 42: 'Claim intimated', 
            43: 'claim settled', 61: 'Expired'
        }
        self.hierarchy_priority = {
            'In-force': 1, 'Cancelled': 2, 'claim settled': 3, 
            'Claim intimated': 4, 'Surrender': 5, 'Lapsed': 6, 
            'Cov closed': 7, 'Expired': 8, 'Other': 9
        }
        self.group_cols = ['Issuance year', 'Reinsured/NonReinsured', 'Base Product UIN', 'Premium Mode']
        self.df = None
        self.summary = None

    def log(self, message: str):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def load_data(self):
        self.log(f"Loading {self.filename}...")
        try:
            # Read CSV with some column mapping for efficiency
            self.df = pd.read_csv(self.filename, low_memory=False)
            self.log("Data loaded successfully.")
        except Exception as e:
            self.log(f"Error loading file: {e}")
            raise

    def preprocess_dates(self):
        self.log("Preprocessing dates and financial years...")
        dt = pd.to_datetime(self.df['Issuance Date'], format='%d-%m-%Y', errors='coerce')
        years = dt.dt.year
        months = dt.dt.month
        fy_start = np.where(months >= 4, years, years - 1)
        
        # Vectorized financial year label
        fy_labels = np.where(
            dt.isna(), 
            'Unknown', 
            fy_start.astype(str) + '-' + (fy_start+1).astype(str)
        )
        self.df['Issuance year'] = fy_labels
        self.df['Issuance year'] = self.df['Issuance year'].astype('category')

        # Status Mapping
        self.df['Status_desc'] = self.df['Status'].map(self.status_map).fillna('Other').astype('category')

    def calculate_dominant_status(self):
        self.log("Calculating Dominant Status per Policy...")
        # Step 1: Count unique COIs by policy and status
        counts = (self.df.groupby(['POLICYNUMBER', 'Status_desc'], observed=True)['COI Number']
                  .nunique().reset_index())
        
        # Step 2: Assign Priority for Tie-Breaking
        counts['priority'] = counts['Status_desc'].map(self.hierarchy_priority).fillna(9)

        # Step 3: Determine Dominant Row
        # Priority: Highest COI Count -> Highest Status Hierarchy (Lowest Priority Score)
        dominant = (counts.sort_values(by=['POLICYNUMBER', 'COI Number', 'priority'], 
                                      ascending=[True, False, True])
                          .drop_duplicates('POLICYNUMBER'))

        # Step 4: Map back to Status Codes
        reverse_map = {v: k for k, v in self.status_map.items()}
        dominant['Dominant_Status'] = dominant['Status_desc'].apply(lambda x: reverse_map.get(x, 'Other'))
        
        # Step 5: Merge result into main dataframe
        policy_dominant_map = dominant.set_index('POLICYNUMBER')['Dominant_Status']
        self.df['Dominant_Status_Code'] = self.df['POLICYNUMBER'].map(policy_dominant_map)

    def aggregate_results(self):
        self.log("Aggregating results (Metrics & Policy Counts)...")
        
        # 1. Unique Policy level counts (for Dominant status reporting)
        df_policy_unique = self.df.sort_values('Issuance Date').drop_duplicates('POLICYNUMBER')
        policy_counts = (df_policy_unique.groupby(['Dominant_Status_Code'] + self.group_cols, observed=False)
                         ['POLICYNUMBER'].nunique()
                         .rename('POLICYNUMBER_count').reset_index()
                         .rename(columns={'Dominant_Status_Code': 'Status'}))

        # 2. Main Metric Aggregation (by Actual Status)
        main_agg = self.df.groupby(['Status'] + self.group_cols, observed=False).agg(
            COINumber_count=('COI Number', 'nunique'),
            PremiumAmount_sum=('Premium', 'sum'),
            ReinsurancePremium_sum=('Reinsurance Premium', 'sum'),
            OriginalSA=('Original SA', 'sum'),
            Retained_SA=('Retained_SA', 'sum'),
            Gross_Reserve=('Gross Reserve\n(NPV)', 'sum'),
            Net_Reserve=('Net Reserve\n(NPV)', 'sum')
        ).reset_index()

        # 3. Calculate Derived Metrics
        main_agg['SumAssured_Gross_sum'] = main_agg['OriginalSA']
        main_agg['SumAssured_Net_sum'] = main_agg['OriginalSA'] - main_agg['Retained_SA']
        main_agg['Reserve_Gross_sum'] = main_agg['Gross_Reserve']
        main_agg['Reserve_Net_sum'] = main_agg['Net_Reserve']
        main_agg['SumAtRisk_Gross_sum'] = main_agg['SumAssured_Gross_sum'] - main_agg['Reserve_Gross_sum']
        main_agg['SumAtRisk_Net_sum'] = main_agg['SumAssured_Net_sum'] - main_agg['Reserve_Net_sum']

        # 4. Integrate Policy Counts
        self.summary = main_agg.merge(policy_counts, on=['Status'] + self.group_cols, how='outer').fillna(0)

        # 5. Full Grid Completion (Ensures every status is present for every combination)
        self.log("Generating complete grid...")
        all_statuses = list(self.status_map.keys()) + ['Other']
        status_df = pd.DataFrame({'Status': all_statuses})
        
        unique_dims = self.summary[self.group_cols].drop_duplicates()
        unique_dims['tmp'] = 1
        status_df['tmp'] = 1
        grid = unique_dims.merge(status_df, on='tmp').drop('tmp', axis=1)
        
        self.summary = grid.merge(self.summary, on=['Status'] + self.group_cols, how='left').fillna(0)

    def save_output(self, output_file: str):
        self.log(f"Saving final output to {output_file}...")
        final_cols = [
            'Base Product UIN', 'Premium Mode', 'Reinsured/NonReinsured',
            'Issuance year', 'Status', 'POLICYNUMBER_count', 'COINumber_count',
            'SumAssured_Gross_sum', 'SumAssured_Net_sum', 'PremiumAmount_sum',
            'ReinsurancePremium_sum', 'Reserve_Gross_sum', 'Reserve_Net_sum',
            'SumAtRisk_Gross_sum', 'SumAtRisk_Net_sum'
        ]
        
        # Select and Reorder
        report = self.summary[final_cols].copy()
        
        # Validation
        policy_total = self.df['POLICYNUMBER'].nunique()
        report_total = int(report['POLICYNUMBER_count'].sum())
        self.log(f"Validation: Unique Policies ({policy_total}) | Reported Policies ({report_total})")
        
        if policy_total != report_total:
            self.log("WARNING: Policy count mismatch detected!")

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            report.to_excel(writer, index=False, sheet_name='Summary')
        
        self.log("File saved successfully.")

    def run(self):
        self.load_data()
        self.preprocess_dates()
        self.calculate_dominant_status()
        self.aggregate_results()
        self.save_output('IRDAI Forms Grouping Data V4.xlsx')
        self.log("Process completed.")

if __name__ == "__main__":
    processor = IRDAIFormProcessor("Valuation_complete.csv")
    processor.run()
