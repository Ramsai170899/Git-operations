import warnings
from dateutil import relativedelta
import datetime as dt
from datetime import datetime, timedelta
import time
import os
import pandas as pd
from typing import Dict, Tuple
import numpy as np
import numpy_financial as npf
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import csv
import builtins
input = builtins.input
from openpyxl.styles import Alignment

pd.set_option('display.max_columns', None)
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)


# Define output columns for individual policy results
OUTPUT_COLUMNS = [
    "COI Number", "Channel", "PH Gender", "JH Gender", "PH DOB", "JH DOB", "Age", "MPH Code", "Issuance Date",
    "Coverage Effective Date", "Expiry Date", "Net UPR_bom Per Policy", "Net UPR_eom Per Policy", "Gross UPR_bom Per Policy", "Gross UPR_eom Per Policy",
    "Outstanding Term(Months)", "Base Product UIN", "Policy Term_Month",
    "Premium before Reinsurance", "Premium after Reinsurance", "Commission","Sum Assured before Reinsurance", 
    "Sum Assured after Reinsurance", "Current SA", "Calculated", "Death Payment",
    "Gross_NPV DB", "Net GPV", "Gross GPV", "Gross Reserve\n(NPV)", "Net Reserve\n(NPV)", "Net SV Deficiency Reserve","Applicable Reserve", "Picked Up Reserve", "Status", "RSM",
    "Age at expiry", "Mortality", "SB", "Policy Duration Months", "POL NO",
    "Cover life", "SA_Code", "Total exp", "Total Benefit", "BEL\n(eom)", "DMT"
]

################################################################################
# Helper Functions

def calculate_age(birth_date, specific_date=None):
    birth_date = datetime.strptime(birth_date, "%d-%m-%Y")
    if specific_date is None:
        specific_date = datetime.now()
    else:
        specific_date = datetime.strptime(specific_date, "%d-%m-%Y")
    age = specific_date.year - birth_date.year - \
        ((specific_date.month, specific_date.day)
         < (birth_date.month, birth_date.day))
    return age

def add_months(date_str, months_to_add):
    date = datetime.strptime(date_str, "%d-%m-%Y")
    new_date = date + relativedelta.relativedelta(months=months_to_add)
    return new_date.strftime("%d-%m-%Y")

def get_next_montheversary_and_weeks_excel_logic(val_date, coverage_date_str):
    coverage_date = datetime.strptime(coverage_date_str, '%d-%m-%Y')
    target_day = coverage_date.day
    
    if val_date.month == 12:
        next_month_year = val_date.year + 1
        next_month_num = 1
    else:
        next_month_year = val_date.year
        next_month_num = val_date.month + 1
    
    if next_month_num == 12:
        month_after_next = datetime(next_month_year + 1, 1, 1)
    else:
        month_after_next = datetime(next_month_year, next_month_num + 1, 1)
    
    eomonth_next = month_after_next - timedelta(days=1)  
    
    montheversary_day = min(target_day, eomonth_next.day)
    montheversary_date = datetime(next_month_year, next_month_num, montheversary_day)
    
    days_diff = (montheversary_date - val_date).days
    weeks = int(days_diff / 7) + 1
    weeks_result = min(weeks, 4)
    
    return montheversary_date, weeks_result

def get_last_date_of_month(date_str):
    date_obj = datetime.strptime(date_str, '%d-%m-%Y')
    next_month = date_obj.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)
    return last_day.strftime('%d-%m-%Y')

def calculate_outstanding_sa(policy_months, sum_assured, term, interest_rate, moratorium_p_a):
    monthly_rate = ((1 + interest_rate) ** (1 / 12) -1)
    remaining_payments = term - policy_months

    pmt = -npf.pmt(monthly_rate, term - max(moratorium_p_a,policy_months), sum_assured)
    remaining_balance = sum_assured
    outstanding_sa = [0] * (policy_months-1)
    outstanding_sa.append(remaining_balance)

    for month in range(1, remaining_payments+1):
        ipmt = (remaining_balance*monthly_rate)
        remaining_balance = (remaining_balance + ipmt - pmt) if moratorium_p_a < (month+policy_months) else remaining_balance
        outstanding_sa.append(remaining_balance)

    # print("remaining payment and len(outstanding_sa) : ", remaining_payments, len(outstanding_sa))

    return outstanding_sa

def get_ae(df, product_code,  ph_gender):
    res = df[
        (df["Group wise"] == product_code) &
        (df["PH Gender"] == ph_gender) 
    ]
    if not res.empty:
        return res.iloc[0]["Mortality_loading"]
    else:
        return None 
    
def update_aggregated_cashflows(agg_cashflows, group_key: Tuple, components: Dict[str, float]):
    """Update aggregated cashflows with dynamic grouping."""
    if group_key not in agg_cashflows:
        agg_cashflows[group_key] = {
            "Premium" : 0.0,
            "Reinsurance Premium" : 0.0,
            "FY - Commission" : 0.0,
            "REN - Commission" : 0.0,
            "ACQ EXP - Prem" : 0.0,
            "ACQ EXP - PP" : 0.0,
            "ACQ EXP - SA" : 0.0,
            "REN EXP - Prem" : 0.0,
            "REN EXP - PP" : 0.0,
            "REN EXP - SA" : 0.0,
            "DB EXP" : 0.0,
            "SURR EXP" : 0.0,
            "MAT EXP" : 0.0,
            "Gross DB" : 0.0,
            "Net DB" : 0.0,
            "SURR Ben" : 0.0,
            "MAT Ben" : 0.0,
            "Probability - IF\n(bom)" : 0.0,
            "Probability - death\n(eom)" : 0.0,
            "Probability - lapse\n(eom)" : 0.0,
            "Probability - maturity\n(eom)" : 0.0,
            "IF Premium\n(bom)" : 0.0,
            "IF Reinsurance Premium\n(bom)" : 0.0,
            "IF Commission\n(bom)" : 0.0,
            "IF REN EXP\n(bom)" : 0.0,
            "IF DB EXP\n(eom)" : 0.0,
            "IF SURR EXP\n(eom)" : 0.0,
            "IF MAT EXP\n(eom)" : 0.0,
            "IF Gross DB\n(eom)" : 0.0,
            "IF Net DB\n(eom)" : 0.0,
            "IF SURR BEN\n(eom)" : 0.0,
            "IF MAT BEN\n(eom)" : 0.0,
            "PV Premium\n(bom)" : 0.0,
            "PV Reinsurance Premium\n(bom)" : 0.0,
            "PV Commission\n(bom)" : 0.0,
            "PV REN EXP\n(bom)" : 0.0,
            "PV DB EXP\n(bom)" : 0.0,
            "PV SURR EXP\n(bom)" : 0.0,
            "PV MAT EXP\n(bom)" : 0.0,
            "PV Total EXP\n(bom)" : 0.0,
            "PV Gross DB\n(bom)" : 0.0,
            "PV Net DB\n(bom)" : 0.0,
            "PV SURR BEN\n(bom)" : 0.0,
            "PV MAT BEN\n(bom)" : 0.0,
            "PV Gross Total BEN\n(bom)" : 0.0,
            "PV Net Total BEN\n(bom)" : 0.0,
            "Gross UPR\n(bom)" : 0.0,
            "Gross UPR\n(eom)" : 0.0,
            "Gross GPV\n(eom)" : 0.0,
            "Gross Final Reserve\n(eom)" : 0.0,
            "Gross SVDR\n(eom)" : 0.0,
            "Net UPR\n(bom)" : 0.0,
            "Net UPR\n(eom)" : 0.0,
            "Net GPV\n(eom)" : 0.0,
            "Net Final Reserve\n(eom)" : 0.0,
            "Net SVDR\n(eom)" : 0.0,

            "Probability - IF_p\n(bom)" : 0.0,
            "Probability - death_p\n(eom)" : 0.0,
            "Probability - lapse_p\n(eom)" : 0.0,
            "Probability - maturity_p\n(eom)" : 0.0,
            "IF FY Premium_p\n(bom)" : 0.0,
            "IF REN Premium_p\n(bom)" : 0.0,
            "IF Reinsurance Premium_p\n(bom)" : 0.0,
            "IF FY Commission_p\n(bom)" : 0.0,
            "IF REN Commission_p\n(bom)" : 0.0,
            "IF ACQ EXP - Prem_p\n(bom)" : 0.0,
            "IF ACQ EXP - PP_p\n(bom)" : 0.0,
            "IF ACQ EXP - SA_p\n(bom)" : 0.0,
            "IF REN EXP - Prem_p\n(bom)" : 0.0,
            "IF REN EXP - PP_p\n(bom)" : 0.0,
            "IF REN EXP - SA_p\n(bom)" : 0.0,
            "IF DB EXP_p\n(eom)" : 0.0,
            "IF SURR EXP_p\n(eom)" : 0.0,
            "IF MAT EXP_p\n(eom)" : 0.0,
            "IF DB_p\n(eom)" : 0.0,
            "IF SURR BEN_p\n(eom)" : 0.0,
            "IF MAT BEN_p\n(eom)" : 0.0,
            "BEL\n(eom)" : 0.0,
            "Gross Policy Liab_p\n(eom)" : 0.0,
            "Net Policy Liab_p\n(eom)" : 0.0,
            "Incr in Liab_p\n(eom)" : 0.0,
            "Int_p\n(eom)" : 0.0,
            "PBT_p\n(eom)" : 0.0,
            "Tax_p\n(eom)" : 0.0,
            "PAT_p\n(eom)" : 0.0,
            "Gross SAR_p\n(eom)" : 0.0,
            "Net SAR_p\n(eom)" : 0.0,
            "RSM_p\n(eom)" : 0.0,
            "Incr in RSM_p\n(eom)" : 0.0,
            "Int RSM_p\n(eom)" : 0.0,
            "Tax Int RSM_p\n(eom)" : 0.0,
            "SH Profit_p\n(eom)" : 0.0,
         }

    for k, v in components.items():
        agg_cashflows[group_key][k] += v

def write_policy_result(output_file, output_obj, write_header=False):
    """Write individual policy result to CSV file."""
    with open(output_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(output_obj)

def Reserve(input_row, selected_date_val, asum_data, GS_data, vri ,IALM_data, ADB, ATPD, GS_loading, lapse_table_data, agg_cashflows, mortalities, output_file, chunk_id, policy_index,results_dir,COIlevel_CashflowsRequired,ProfitabilityComponentsRequired,cashflowAggregation_type,IsAggregationRequired,GROUPING_COLUMNS,runcase):
    try:    
        
        policy_term = input_row['Policy Term_Month']
        loan_term = input_row['Loan Term']
        Age = input_row['PH Entry Age']
        Issuance_Date = input_row['Issuance Date']
        PH_Gender = input_row['PH Gender']

        JH_Gender = input_row['Joint Holder Gender']  
        JH_DOB = input_row['Joint Holder DOB']  

        if pd.isna(JH_Gender) and (input_row['Co-Borrower/Joint Life ID'] == 'Joint Life'): # where JH gender is missing 
            if PH_Gender == 'Male':
                JH_Gender = 'Female'
            else:
                JH_Gender = 'Male'

        if pd.isna(JH_DOB) and (input_row['Co-Borrower/Joint Life ID'] == 'Joint Life'):
            JH_DOB = input_row['PH DOB']    

        Modal_Premium = round(     ((input_row['Regular Premium'] * 52)/12 if input_row['Premium Mode'] == 'Weekly' else (input_row['Regular Premium'] * 26)/12 if input_row['Premium Mode'] == 'Fortnightly' else 0), 2 )

        Reinsurance = input_row['Reinsured/NonReinsured']

        Gross_Premium = input_row['Premium']

        months_covered = (Gross_Premium / Modal_Premium) if input_row['Premium Mode'] in ['Weekly', 'Fortnightly'] else 0              # total premium/monthly converted premium gives us the number of months for which premium has been paid. 
        full_months_paid = int(months_covered)
        lapse_triggered = False

        paidupSA = input_row['Original SA'] if input_row['Status'] == 33 else 0

        total_premium_payable = Modal_Premium * policy_term if input_row['Premium Mode'] in ['Weekly', 'Fortnightly'] else input_row['Regular Premium']

        newSA = paidupSA * full_months_paid
        # newSA is Inforce SA for polices which are currently in paidup status.

        currentSA = input_row['Current SA'] if input_row['Status'] == 11 else paidupSA if input_row['Status'] == 33 else 0
        Gross_SA = input_row['Original SA'] if input_row['Status'] == 11 else paidupSA if input_row['Status'] == 33 else 0
        
        # print(f"Gross_SA : {Gross_SA}")
        moratorium_p_a = input_row['Moratorium_p_m']


        Net_Premium = input_row['Premium'] - input_row['Reinsurance Premium'] if Reinsurance == 'Reinsured' else input_row['Premium']

                
        yr1_comm = input_row['Commission']
        Premium_mode = input_row['Premium Mode']
        UIN = input_row['Base Product UIN']
        no_of_lives = 1
        no_of_lives_p = 1


        # Load assumptions
        mortality_mad = asum_data.loc['mortality_mad_reserving', UIN]
        product_type = asum_data.loc['product_type', UIN]
        expense_mad = asum_data.loc['expense_mad_reserving', UIN]
        interest_mad = asum_data.loc['interest_mad_reserving', UIN]
        lapse_mad = asum_data.loc['lapse_mad_reserving', UIN]

        mortality_shock = asum_data.loc['mortality_shock', UIN]
        expense_shock = asum_data.loc['expense_shock', UIN]
        interest_shock = asum_data.loc['interest_shock', UIN]
        lapse_shock = asum_data.loc['lapse_shock', UIN]
        CAT_shock = asum_data.loc['CAT_shock',UIN]
        mass_lapse_shock = asum_data.loc['mass_lapse_shock',UIN]

        claim_expense = asum_data.loc['claim_expense', UIN]
        surrender_expense = asum_data.loc['surrender_expense', UIN]
        maturity_expense = asum_data.loc['maturity_exp', UIN]
        surr_charge = asum_data.loc['surrender_charge', UIN]
        Acq_prem = asum_data.loc['acq_prem', UIN]
        surr_factor = asum_data.loc['surr_factor', UIN]
        Acq_SA = asum_data.loc['acq_sa', UIN]
        ren_exp_pol = asum_data.loc['ren_exp_pol', UIN]
        exp_inf = asum_data.loc['exp_inf', UIN]

        # Lists for monthly cashflows
        Gross_Premium_list, Accumulated_premium, Reinsurance_Premium_list, Expected_Premium, Expected_Reinsurance_Premium, SB, Gross_DB, Net_DB, MB, death_benefit, Yr1_comm, acq_exp_prem, acq_exp_pol, exp_infl, cumulative_prem, clm_exp, surrender_exp, mat_exp,acq_exp_sa, ren_comm, ren_exp_prem, ren_per_pol, ren_sa, Expected_ren_exp, age, JH_age, qx, wx, deaths, Expected_comm, lapse, maturity_prob, monthly_interest_rate_list, Expected_clm_exp, Expected_surr_exp, Expected_mat_exp,val_int, Gross_Expected_DB, Net_Expected_DB, Expected_SB, Expected_MB, Net_UPR_eom, Net_UPR_bom, Gross_UPR_bom, Gross_UPR_eom, Gross_res_per_life, Net_res_per_life, exit_exp, Net_sv_deficiency_reserve, Gross_final_reserve, Net_final_reserve, Gross_GPV, Net_GPV, Overall_mortality_list = ([] for i in range(54))
        inf_prob = [1]
        inf_lives = [1]

        # Lists for projection cashflows 
        deaths_p, lapse_p, maturity_prob_p, int_earned, rdr_list, Yr1_prem, ren_prem_p, reins_prem_p, death_ben_p, surr_ben_p, maturity_ben_p ,yr1_comm_p, ren_comm_p, acq_exp_prem_p, acq_exp_pol_p, acq_sa_p, ren_exp_prem_p, ren_per_pol_p, ren_sa_p, clm_exp_p, maturity_expense_p,surr_exp_p, Gross_policy_liability_p, Net_policy_liability_p, increase_in_liability, inc_int, PBT, tax, PAT, Gross_sum_at_risk, Net_sum_at_risk, rsm, inc_in_rsm, int_on_rsm, tax_on_rsm_int, sh_profit, pv_profit, Accumulated_value_of_profit, k1, k2, Net_BEL_p  = ([] for i in range(41))
        month_list  = []

        d1 = selected_date_val if runcase != 'Profitability' else datetime.strptime(get_last_date_of_month(input_row['Coverage Effective Date']), '%d-%m-%Y')
        d2 = datetime.strptime(input_row['Expiry Date'], '%d-%m-%Y')
        d3 = datetime.strptime(input_row['Coverage Effective Date'], '%d-%m-%Y')

        datediff = relativedelta.relativedelta(d2, d1)
        outstanding_term_months = datediff.months + (datediff.years * 12)

        # valuation date - effective date
        policy_months = (d1.year - d3.year) * 12 + d1.month - d3.month
        if d1.day < d3.day:
            policy_months -= 1

        # print(f"COI : {input_row['COI Number']} --- Valuation Date : {d1.strftime('%d-%m-%Y')} --- Effective Date : {d3.strftime('%d-%m-%Y')} --- Expiry date : {d2.strftime('%d-%m-%Y')} --- Provided term : {policy_term} --- Policy Months : {policy_months} --- Outstanding Term (Months) : {outstanding_term_months} ")

        if input_row['SA_code'] == 'Reducing':
            outstanding_amount = calculate_outstanding_sa(policy_months, currentSA, int(loan_term), input_row['Loan_int_pa']/100, moratorium_p_a) if UIN == '163N013V01' else calculate_outstanding_sa(policy_months,currentSA, policy_term, input_row['Loan_int_pa']/100,moratorium_p_a)
        else:
            outstanding_amount = [Gross_SA] * policy_term          

          
# =================================================================================================================================================================
# Complete Policy Term Loop
# =================================================================================================================================================================

        for x in range(1, policy_term+1):
            policy_year = int((x-1)/12) + 1
            
            # Calculation of SA 
            SA = outstanding_amount[x-1]

            month_list.append(int(x))

            if (x == 1 and Premium_mode == 'Single/Member Premium'):
                Gross_Premium_list.append(Gross_Premium)
            elif (x != 1 and Premium_mode == 'Single/Member Premium'):
                Gross_Premium_list.append(0)

            elif(Premium_mode == 'Yearly'):    # append premium for every 12 months
                if (x-1)%12 == 0:
                    Gross_Premium_list.append(Gross_Premium) 
                else:
                    Gross_Premium_list.append(0)

            elif(Premium_mode == 'Fortnightly' or Premium_mode == 'Weekly'):
                if input_row['Status'] == 11 :
                    Gross_Premium_list.append(round(Modal_Premium, 10))
                elif input_row['Status'] == 33:
                    # Gross_Inforce_Premium_33.append(round(Modal_Premium, 10))
                    if not lapse_triggered:
                        if x <= full_months_paid:
                            Gross_Premium_list.append(round(Modal_Premium, 10))
                        elif x == full_months_paid + 1:
                            expected_if_full = full_months_paid * Modal_Premium
                            difference = expected_if_full - Gross_Premium
                            Gross_Premium_list.append(round(-difference, 10))
                            lapse_triggered = True
                        else:
                            Gross_Premium_list.append(0)
                    else:
                        Gross_Premium_list.append(0)


            # print(f" x : {x} --- Gross Premium : {Gross_Premium_list[x-1]} ")

            # Reinsurance Premium list
            if (x == 1 and Reinsurance == 'Reinsured'):
                Reinsurance_Premium_list.append(input_row['Reinsurance Premium'])
            else:
                Reinsurance_Premium_list.append(0)

            monthly_interest_rate = vri.loc[vri['Month'] == x , "VRI Monthly Rate"].values[0]
            monthly_interest_rate_list.append(monthly_interest_rate)                
            val_int.append(monthly_interest_rate * (1 - interest_mad))

            # Calculation of Accumulated Premium
            cumulative_prem.append(sum(Gross_Premium_list[0:x]))

            # =================================================================================================================================================
            # Calculation of PV Premium
            if x == 1:
                Accumulated_premium.append(Gross_Premium)
            else:
                Accumulated_premium.append(Accumulated_premium[x-2]*(1+vri.loc[vri['Month'] == x , "BE Rate"].values[0]))

            # =================================================================================================================================================
            # death_benefit.append(SA if Gross_Premium_list[x-1] > 0 else 0)   # for month where premium is 0, we are assuming that the policy is not inforce and hence DB is 0. This is relevant for paidup policies where after certain months premium becomes 0.
            death_benefit.append(SA)

            # Calculation of SB 
            if Premium_mode != 'Single/Member Premium' and UIN == '163N012V01':
                SB.append(sum(Gross_Premium_list[0:x])-surr_charge)
            elif Premium_mode != 'Single/Member Premium' and UIN == '163N004V02':
                if input_row['SA_code'] == 'Reducing':
                    SB.append((1-(x/policy_term))*cumulative_prem[x-1]*surr_factor*(death_benefit[x-1]/input_row['Original SA']))
                else:
                    SB.append((1-(x/policy_term))*cumulative_prem[x-1]*surr_factor)                
            elif (UIN != '163N002V01') and (UIN != '163N003V01'):
                if input_row['SA_code'] == 'Reducing':
                    SB.append((1-(x/policy_term))* Gross_Premium *surr_factor*(death_benefit[x-1]/input_row['Original SA']))
                else:
                    SB.append((1-(x/policy_term))* Gross_Premium *surr_factor)
            else:    
                SB.append(0)

            # Calculation of MB - maturity benefit
            if Premium_mode == 'Single/Member Premium' or Premium_mode == 'Yearly':
                MB.append(0)
            elif (x != policy_term) and Premium_mode != 'Single/Member Premium':
                MB.append(0)
            elif (x == policy_term) and Premium_mode != 'Single/Member Premium':
                MB.append(input_row['Original SA'])
            # print("MB done")

            # Calculation of DB 
            # Gross_DB.append(SA if Gross_Premium_list[x-1] > 0 else 0)   # for month where premium is 0, we are assuming that the policy is not inforce and hence DB is 0. This is relevant for paidup policies where after certain months premium becomes 0. 
            Gross_DB.append(SA)

            # print(f" x : {x} --- Gross Premium : {Gross_Premium_list[x-1]} --- Gross DB : {Gross_DB[x-1]}")

            Net_SA = min(input_row['Original SA'], input_row['Retained_SA']) if Reinsurance == 'Reinsured' else outstanding_amount[x-1]
            # Net_DB.append(Net_SA if Gross_Premium_list[x-1] > 0 else 0)   # for month where premium is 0, we are assuming that the policy is not inforce and hence DB is 0. This is relevant for paidup policies where after certain months premium becomes 0.
            Net_DB.append(Net_SA)            

            # Calculation of Commisions
            if(x == 1 and Premium_mode == 'Single/Member Premium'):
                Yr1_comm.append(yr1_comm)
            elif (Premium_mode == 'Single/Member Premium') and (x != 1):
                Yr1_comm.append(0)
            elif(Premium_mode != 'Single/Member Premium') and (policy_year == 1):
                Yr1_comm.append(yr1_comm/Gross_Premium * Gross_Premium_list[x-1])
            elif(Premium_mode != 'Single/Member Premium') and (policy_year != 1):
                Yr1_comm.append(0)                

            # print(f" x : {x} --- Policy Term : {policy_term} --- Policy Year : {policy_year} --- COI Number : {input_row['COI Number']} ")

            if (policy_year != 1) and (Premium_mode != 'Single/Member Premium'):
                ren_comm.append(yr1_comm/Gross_Premium*Gross_Premium_list[x-1])
            elif x != 0 and (Premium_mode == 'Single/Member Premium'):
                ren_comm.append(asum_data.loc['ren_comm', UIN]*Gross_Premium_list[x-1])
            else:
                ren_comm.append(0) 


            # for X lessthan the policy months (elapsed term) inflation is 1. from policy months onwards inflation is applied
                
            if x <= policy_months:
                exp_infl.append(1)
            else:
                months_from_valuation = x - policy_months
                if months_from_valuation <= 12:
                    exp_infl.append(1)
                else:
                    years_since_val_year = (months_from_valuation - 13) // 12 + 1
                    if cumulative_prem[x-1] > 0:
                        exp_infl.append((1 + exp_inf) ** years_since_val_year)
                    else:
                        exp_infl.append(1)

            # print(f" x : {x} --- exp_infl : {exp_infl[x-1]} ")
        

            if(x == 1 and ((Premium_mode == 'Single/Member Premium') or Premium_mode == 'Yearly')):
                acq_exp_prem.append(Acq_prem * Gross_Premium)
                acq_exp_sa.append(Acq_SA * SA)
                acq_exp_pol.append(asum_data.loc['acq_exp_pol', UIN])
                ren_exp_prem.append(0)
                ren_per_pol.append(0)
                ren_sa.append(0)
            elif ((Premium_mode == 'Single/Member Premium' or Premium_mode == 'Yearly') and x != 1):
                acq_exp_prem.append(0)
                acq_exp_sa.append(0)
                acq_exp_pol.append(0)
                ren_exp_prem.append(Gross_Premium_list[x-1]*exp_infl[x-1]*asum_data.loc['ren_exp_prem', UIN]*(1+expense_shock))
                ren_per_pol.append(int(cumulative_prem[x-1] > 0)*exp_infl[x-1]*asum_data.loc['ren_exp_pol', UIN]*(1+expense_shock))
                ren_sa.append(int(cumulative_prem[x-1] > 0)*exp_infl[x-1]*asum_data.loc['ren_exp_sa', UIN]*death_benefit[x-1]*(1+expense_shock))

            if(x == 1 and UIN == '163N012V01' and Premium_mode != 'Single/Member Premium'):
                acq_exp_prem.append(Acq_prem * Gross_Premium_list[x-1])
                acq_exp_sa.append(Acq_SA * SA)
                acq_exp_pol.append(asum_data.loc['acq_exp_pol', UIN]*(1+expense_shock))
                ren_exp_prem.append(0)
                ren_per_pol.append(0)
                ren_sa.append(0)
            elif (UIN == '163N012V01' and Premium_mode != 'Single/Member Premium' and x != 1):
                acq_exp_prem.append(0)
                acq_exp_sa.append(0)
                acq_exp_pol.append(0)
                ren_exp_prem.append(Gross_Premium_list[x-1]*exp_infl[x-1]*asum_data.loc['ren_exp_prem', UIN]*(1+expense_shock))
                ren_per_pol.append(int(cumulative_prem[x-1] > 0)*exp_infl[x-1]*asum_data.loc['ren_exp_pol', UIN]*(1+expense_shock))
                ren_sa.append(int(cumulative_prem[x-1] > 0)*exp_infl[x-1]*asum_data.loc['ren_exp_sa', UIN]*death_benefit[x-1]*(1+expense_shock))
            # print("Expenses done")
            # print(f"x : {x} --- Premium : {Gross_Premium_list[x-1]} --- ren_exp_prem : {ren_exp_prem[x-1]} --- ren_per_pol : {ren_per_pol[x-1]} --- ren_sa : {ren_sa[x-1]} ")

            # Calculation of claim expense with inflation
            clm_exp.append(claim_expense * exp_infl[x-1]*(1+expense_shock))
            surrender_exp.append(surrender_expense * exp_infl[x-1]*(1+expense_shock))
            mat_exp.append(maturity_expense * exp_infl[x-1]*(1+expense_shock))

            PH_int_age = calculate_age(input_row['PH DOB'], add_months(input_row['Coverage Effective Date'], x-1))
            age.append(PH_int_age)

            if (input_row['Co-Borrower/Joint Life ID'] == 'Joint Life'):  
                JH_int_age = calculate_age(JH_DOB , add_months(input_row['Coverage Effective Date'],x-1))  
                JH_age.append(JH_int_age)  
            else:
                JH_age.append(0)

            # print("Before qx calculation")
            # =================================================================================================================================================
            # Calculation of qx 
            if product_type == 'Rider' and UIN == '163B001V01':
                PH_mortality_base = ADB.loc[ADB['Age'] == PH_int_age, 'qx'].values[0]                           # get values from ADB table.
                # print(ADB.loc[ADB['Age'] == PH_int_age])                                                                   
                Overall_mortality = PH_mortality_base * get_ae(mortalities ,input_row['Group wise'], PH_Gender)

            elif product_type == 'Rider' and UIN == '163B002V01':
                PH_mortality_base = ATPD.loc[ATPD['Age'] == PH_int_age, 'qx'].values[0]                                      # get values from ATPD table.
                Overall_mortality = PH_mortality_base * get_ae(mortalities ,input_row['Group wise'], PH_Gender)

            elif UIN == '163N012V01':
                PH_mortality_base = IALM_data.loc[IALM_data['Age'] == PH_int_age, 'qx'].values[0]                                    
                Overall_mortality = PH_mortality_base * GS_loading.loc[GS_loading['Age'] == Age, 'Mortality_loading'].values[0]  
            
            else:

                PH_mortality_base = IALM_data.loc[IALM_data['Age'] == PH_int_age, 'qx'].values[0] 
                PH_overall_mortality = PH_mortality_base * get_ae(mortalities ,input_row['Group wise'], PH_Gender)
                                                     
                if (input_row['Co-Borrower/Joint Life ID'] == 'Joint Life'):
                    JH_mortality_base = IALM_data.loc[IALM_data['Age'] == JH_int_age, 'qx'].values[0]
                    JH_overall_mortality = JH_mortality_base * get_ae(mortalities ,input_row['Group wise'], JH_Gender)  
                    Overall_mortality = PH_overall_mortality + JH_overall_mortality - (PH_overall_mortality*JH_overall_mortality)
                else:
                    Overall_mortality = PH_overall_mortality

            Overall_mortality = Overall_mortality * (1+mortality_shock)    

            if CAT_shock == 1:
                if x >= policy_months and x < (policy_months+ 12 ):
                    Overall_mortality = Overall_mortality + (1.5/1000) 

            Overall_mortality_list.append(Overall_mortality)
            mortality_monthly = (1-(1-Overall_mortality)**(1/12))                                                        # convert to monthly mortality.


            qx.append(mortality_monthly)
            used_mortality = get_ae(mortalities, input_row['Group wise'], PH_Gender)                                            # Actual Loadings used for PH.

            # print(f"COI : {input_row['COI Number']} --- Month : {x} --- age : {age[x-1]} --- PH_mortality_base : {PH_mortality_base} --- used mortality : {used_mortality} --- Overall_mortality : {Overall_mortality} --- mortality_monthly : {mortality_monthly}") 


            # =================================================================================================================================================

            year = int((x - 1) / 12) + 1

            # if year == 1:
            #     inflation.append(1)
            # else:
            #     inflation.append((1+exp_inf)**((year-1)))

            lapse_m = (1 - (1 - (lapse_table_data.loc[year, UIN]) * (1+lapse_shock) )**(1/12))
            wx.append(lapse_m)

            if x == policy_months+1 and mass_lapse_shock == 1 :
                wx[policy_months] = 0.3
            # print(f"COI : {input_row['COI Number']} --- policy months : {policy_months} --- Month : {x} --- age : {age[x-1]} --- wx : {wx[x-1]}")

                
            ####################################################################################
            #                               Reserving Cashflows                                #
            ####################################################################################
            # Calculation of inf prob and deaths
            
            if x == 1:
                pass    
            elif x == policy_months + 1:
                no_of_lives = 1
                inf_prob.append(no_of_lives)
            elif x != 1:
                no_of_lives = inf_prob[x-2] - (inf_prob[x-2] * qx[x-2]*((1+mortality_mad))) - lapse[-1]
                inf_prob.append(no_of_lives)             
                
            # print("inf prob done")

            deaths.append(inf_prob[x-1]* qx[x-1]*((1+mortality_mad)))
            
            if x < policy_term:
                lapse.append((inf_prob[x-1]-deaths[-1]) * (1 + lapse_mad) * wx[x-1])
            else:
                lapse.append(0)

            if x < policy_term:
                maturity_prob.append(0)
            elif (x == policy_term and UIN == '163N012V01'):
                maturity_prob.append(inf_prob[x-1]-deaths[-1]-lapse[-1])
            elif (x == policy_term and ((Premium_mode == 'Single/Member Premium') or (Premium_mode == 'Yearly'))):
                maturity_prob.append(0)


            # projection probabilities
            if x == 1:
                pass    
            elif x == policy_months + 1:
                no_of_lives_p = 1
                inf_lives.append(no_of_lives_p) 
            elif x != 1:
                no_of_lives_p = (inf_lives[x-2]-deaths_p[x-2]-lapse_p[x-2])
                inf_lives.append(no_of_lives_p)

            deaths_p.append(inf_lives[x-1]*qx[x-1])     

            if x != policy_term:
                lapse_p.append((inf_lives[x-1]-deaths_p[x-1])*wx[x-1])
            else:
                lapse_p.append(0)

            if UIN == '163N012V01':
                maturity_prob_p.append(0) if x != policy_term else maturity_prob_p.append(inf_lives[x-1]-deaths_p[x-1]-lapse_p[x-1])    
            elif UIN != '163N012V01':
                maturity_prob_p.append(0)


            # print(f"x : {x} --- inf_prob : {inf_prob[x-1]} --- deaths : {deaths[x-1]} --- lapse : {lapse[x-1]} --- maturity_prob : {maturity_prob[x-1]}")

            if (x == 1 and Premium_mode == 'Single/Member Premium'):
                Expected_Premium.append(Gross_Premium*no_of_lives)
            elif (x != 1 and Premium_mode == 'Single/Member Premium'):
                Expected_Premium.append(0)
            elif (Premium_mode != 'Single/Member Premium'):
                Expected_Premium.append(Gross_Premium_list[x-1]*no_of_lives)

            if (x == 1 and Reinsurance == 'Reinsured'):
                Expected_Reinsurance_Premium.append(input_row['Reinsurance Premium']*no_of_lives)
            else:
                Expected_Reinsurance_Premium.append(0)


            # Calculation of Expected values...
            Gross_Expected_DB.append(deaths[x - 1] * (SA))

            Net_Expected_DB.append(deaths[x - 1] * (Net_SA))


            Expected_clm_exp.append(claim_expense * deaths[x - 1] * (1 + expense_mad) * exp_infl[x - 1])
            Expected_SB.append(lapse[x-1]*(SB[x-1]))
            Expected_surr_exp.append((surrender_expense*exp_infl[x-1] ))
            exit_exp.append((1 + expense_mad) *Expected_surr_exp[x-1] * lapse[x-1] )


            Expected_MB.append(maturity_prob[x-1]*(MB[x-1])) if UIN == '163N012V01' else Expected_MB.append(0)
            Expected_mat_exp.append(maturity_prob[x-1] * (mat_exp[x-1]) * (1 + expense_mad)) if UIN == '163N012V01' else Expected_mat_exp.append(0)
            # print(f"x : {x} --- inf_prob : {inf_prob[x-1]} --- deaths : {deaths[x-1]} --- Expected_DB : {Expected_DB[x-1]} --- Expected_clm_exp : {Expected_clm_exp[x-1]} --- Expected_MB : {Expected_MB[x-1]}")
            # print("Expected values done")

            Expected_comm.append(inf_prob[x-1]*(Yr1_comm[x-1]+ren_comm[x-1]))

            if x == 1:
                Expected_ren_exp.append(0)
            else:
                Expected_ren_exp.append((ren_exp_prem[x-1]+ren_per_pol[x-1]+ ren_sa[x-1])*(1+expense_mad)*inf_prob[x-1])
            # print(f"x : {x} --- Expected_ren_exp : {Expected_ren_exp[x-1]} ")
            
                
            stamp_duty = 0 if input_row['POLICYNUMBER'] == 399  else Acq_SA * input_row['Original SA']

            # Calculation of reserve    
            Net_UPR_bom.append(0) if Premium_mode != 'Single/Member Premium' else Net_UPR_bom.append(  ((1 - (( x-1 )/ policy_term)) * (SA/Gross_SA)         ) * (Net_Premium - Yr1_comm[0] - ren_comm[0] - stamp_duty))
            Net_UPR_eom.append(0) if Premium_mode != 'Single/Member Premium' else Net_UPR_eom.append(  ((1 - ((  x  ) / policy_term)) * (SA/Gross_SA)        ) * (Net_Premium - Yr1_comm[0] - ren_comm[0] - stamp_duty)) 

            Gross_UPR_bom.append(0) if Premium_mode != 'Single/Member Premium' else Gross_UPR_bom.append(  ((1 - (( x-1 )/ policy_term)) * (SA/Gross_SA)       ) * (Gross_Premium - Yr1_comm[0] - ren_comm[0] - stamp_duty))
            Gross_UPR_eom.append(0) if Premium_mode != 'Single/Member Premium' else Gross_UPR_eom.append(  ((1 - ((  x  ) / policy_term))  * (SA/Gross_SA)       ) * (Gross_Premium - Yr1_comm[0] - ren_comm[0] - stamp_duty))

            # print(f" x : {x} --- Gross_upr_bom : {Gross_UPR_bom[x-1]} --- policymonths : {policy_months} --- policy term : {policy_term}")  


# =================================================================================================================================================================
# NPV Variable Calculations
# =================================================================================================================================================================

        # inf_prob.append(no_of_lives-deaths[x-1])
        npv_premium = [0]
        npv_premium_infprob = [0]

        npv_reinsurance_premium = [0]
        npv_reinsurance_premium_infprob = [0]

        Gross_npv_DB = [0]
        Gross_npv_DB_infprob =[0]

        Net_npv_DB = [0]
        Net_npv_DB_infprob =[0]

        npv_clm_exp =[0]
        npv_clm_exp_infprob = [0]

        npv_SB = [0]
        npv_SB_infprob = [0]

        npv_mat_ben = [0]
        npv_mat_ben_infprob = [0]

        npv_mat_exp = [0]
        npv_mat_exp_infprob = [0]

        npv_exit_exp = [0]
        npv_exit_exp_infprob = [0]

        npv_ren_exp = [0]
        npv_ren_exp_infprob = [0]

        Gross_npv_total_ben = [0]
        npv_total_ben_infprob = [0]

        Net_npv_total_ben = [0]
        Net_npv_total_ben_infprob = [0]

        npv_total_exp = [0]
        npv_total_exp_infprob = [0]

        npv_ren_comm = [0]
        npv_comm_infprob = [0]


        Gross_reserves = [0]
        Net_reserves = [0]

        dpl = []
        rpl = []
        death_pv = [0]
        ren_pv = [0]

        for i in reversed(range(1, policy_term + 1)):
            x = i-1  

            # print(f"NPV Calculations - Month : {i} / {policy_term}")
            # Calculation of npv_premium
            npv_premium.append(Expected_Premium[x] + npv_premium[-1]/(1+val_int[x]))
            # npv_premium_infprob.append(   (Gross_Premium_list[x] + npv_premium[-1]/(1+val_int[x]))/inf_prob[x]    )

            npv_reinsurance_premium.append(Expected_Reinsurance_Premium[x] + npv_reinsurance_premium[-1]/(1+val_int[x]))
            # npv_reinsurance_premium_infprob.append(   (Reinsurance_Premium_list[x] + npv_reinsurance_premium[-1]/(1+val_int[x]))/inf_prob[x]    )

            # Calculation of npv_DB
            Gross_npv_DB.append((Gross_npv_DB[-1]+Gross_Expected_DB[x])/(1+val_int[x]))
            # npv_DB_infprob.append(   ((npv_DB[-1]+Expected_DB[x])/(1+val_int[x]))/inf_prob[x]   )

            Net_npv_DB.append((Net_npv_DB[-1]+Net_Expected_DB[x])/(1+val_int[x]))
            # npv_DB_infprob.append(   ((npv_DB[-1]+Expected_DB[x])/(1+val_int[x]))/inf_prob[x]   )

            npv_clm_exp.append((npv_clm_exp[-1]+Expected_clm_exp[x])/(1+val_int[x]))
            # npv_clm_exp_infprob.append(   ((npv_clm_exp[-1]+Expected_clm_exp[x])/(1+val_int[x]))/inf_prob[x]   )

            npv_SB.append((npv_SB[-1]+Expected_SB[x])/(1+val_int[x]))
            # npv_SB_infprob.append(   ((npv_SB[-1]+Expected_SB[x])/(1+val_int[x]))/inf_prob[x]   )

            npv_mat_ben.append((npv_mat_ben[-1]+Expected_MB[x])/(1+val_int[x]))
            # npv_mat_ben_infprob.append(   ((npv_mat_ben[-1]+Expected_MB[x])/(1+val_int[x]))/inf_prob[x]   )
            # print("npv_ben done")

            npv_mat_exp.append((npv_mat_exp[-1]+Expected_mat_exp[x])/(1+val_int[x]))
            # npv_mat_exp_infprob.append(   ((npv_mat_exp[-1]+Expected_mat_exp[x])/(1+val_int[x]))/inf_prob[x]   )

            npv_ren_exp.append( Expected_ren_exp[x] + npv_ren_exp[-1] / (1 + val_int[x]))
            # npv_ren_exp_infprob.append(   ((Expected_ren_exp[x] + npv_ren_exp[-1]) / (1 + val_int[x]))/inf_prob[x]     )

            npv_exit_exp.append( (npv_exit_exp[-1] + exit_exp[x] )/ (1 + val_int[x]))
            # npv_exit_exp_infprob.append(   ((exit_exp[x] + npv_exit_exp[-1] )/ (1 + val_int[x]))/inf_prob[x]     )

            npv_ren_comm.append( Expected_comm[x] + npv_ren_comm[-1] /(1 + val_int[x]))
            # npv_comm_infprob.append(   ((Expected_comm[x] + npv_ren_comm[-1])/(1 + val_int[x]))/inf_prob[x]     )
            # print("npv_exp done")

            # Calculation of npv_total_ben
            Gross_npv_total_ben.append(Gross_npv_DB[-1] + npv_SB[-1] + npv_mat_ben[-1])
            # npv_total_ben_infprob.append(   (npv_DB[-1] + npv_SB[-1] + npv_mat_ben[-1])/inf_prob[x]    )

            Net_npv_total_ben.append(Net_npv_DB[-1] + npv_SB[-1] + npv_mat_ben[-1])
            # Net_npv_total_ben_infprob.append(   (npv_DB[-1] + npv_SB[-1] + npv_mat_ben[-1])/inf_prob[x]    )

            npv_total_exp.append(npv_clm_exp[-1] + npv_ren_exp[-1] + npv_exit_exp[-1] + npv_mat_exp[-1])
            # npv_total_exp_infprob.append(   (npv_clm_exp[-1] +npv_ren_exp[-1] + npv_exit_exp[-1] + npv_mat_exp[-1])/inf_prob[x]    )

    
            Gross_reserves.append(Gross_npv_total_ben[-2]+npv_total_exp[-2]-npv_premium[-2]+npv_ren_comm[-2])
            Net_reserves.append(Net_npv_total_ben[-2]+npv_total_exp[-2]-npv_premium[-2]+npv_ren_comm[-2])
            # print("reserves : ", len(reserves), reserves)

            # Reserves per life
            death_pv.append(Gross_npv_total_ben[-2])
            ren_pv.append(npv_ren_exp[-2])

        npv_premium = npv_premium[::-1]
        # npv_premium_infprob = npv_premium_infprob[::-1]

        npv_reinsurance_premium = npv_reinsurance_premium[::-1]
        # npv_reinsurance_premium_infprob = npv_reinsurance_premium_infprob[::-1]

        Gross_npv_DB = Gross_npv_DB[::-1]
        # Gross_npv_DB_infprob = Gross_npv_DB_infprob[::-1]

        Net_npv_DB = Net_npv_DB[::-1]
        # Net_npv_DB_infprob = Net_npv_DB_infprob[::-1]

        npv_clm_exp = npv_clm_exp[::-1]
        # npv_clm_exp_infprob = npv_clm_exp_infprob[::-1]

        npv_ren_comm = npv_ren_comm[::-1]
        # npv_comm_infprob = npv_comm_infprob[::-1]

        npv_SB = npv_SB[::-1]
        # npv_SB_infprob = npv_SB_infprob[::-1]

        npv_mat_ben = npv_mat_ben[::-1]
        # npv_mat_ben_infprob = npv_mat_ben_infprob[::-1

        npv_mat_exp = npv_mat_exp[::-1]
        # npv_mat_exp_infprob = npv_mat_exp_infprob[::-1]

        npv_ren_exp = npv_ren_exp[::-1]
        # npv_ren_exp_infprob = npv_ren_exp_infprob[::-1]

        npv_exit_exp = npv_exit_exp[::-1]
        # npv_exit_exp_infprob = npv_exit_exp_infprob[::-1]

        Gross_npv_total_ben = Gross_npv_total_ben[::-1]
        # npv_total_ben_infprob = npv_total_ben_infprob[::-1]

        Net_npv_total_ben = Net_npv_total_ben[::-1]
        # Net_npv_total_ben_infprob = Net_npv_total_ben_infprob[::-1]

        npv_total_exp = npv_total_exp[::-1]
        # npv_total_exp_infprob = npv_total_exp_infprob[::-1]

        Gross_reserves = Gross_reserves[::-1]
        Net_reserves = Net_reserves[::-1]
        # print("\ninf prob : ", len(inf_prob),inf_prob)
        # print("\nreserves : ", len(reserves),reserves)
        # print("\nnpv_total_ben : ", len(npv_total_ben),npv_total_ben)

        for x in range(1, policy_term+1):
            if x < policy_term: 
                Gross_res_per_life.append( max(Gross_reserves[x-1] / inf_prob[x] * inf_lives[x-1], 0) )
                Net_res_per_life.append(max(Net_reserves[x-1] / inf_prob[x] * inf_lives[x-1], 0) )
            else:
                Gross_res_per_life.append(0)  
                Net_res_per_life.append(0)

        # print("\nres_per_life : ", len(res_per_life),res_per_life)


        for x in range(1, policy_term+1):
            Gross_GPV.append(Gross_res_per_life[x-1])
            Net_GPV.append(Net_res_per_life[x-1])
        # GPV.extend(res_per_life[:policy_term])

        # for x in range(1, policy_term+1):
            # print(f"x : {x} --- npv_premium : {npv_premium[x-1]} ")
            # print(f"x : {x} --- npv_ren_exp : {npv_ren_exp[x-1]} ")
            # print(f"x : {x} --- npv_DB : {npv_DB[x-1]} --- npv_SB : {npv_SB[x-1]} --- npv_mat_ben : {npv_mat_ben[x-1]} --- npv_total_ben : {npv_total_ben[x-1]} ")
            # print(f"x : {x} --- npv_clm_exp : {npv_clm_exp[x-1]} ---  npv_exit_exp : {npv_exit_exp[x-1]} --- npv_mat_exp : {npv_mat_exp[x-1]} --- npv_ren_exp : {npv_ren_exp[x-1]} --- npv_total_exp : {npv_total_exp[x-1]} ")
            # print(f"x : {x} --- res_per_life : {res_per_life[x-1]} --- GPV : {GPV[x-1]} ")


        X = ''
        Y = ''

        UPR_UINs = ['163N002V01', '163N002V02', '163N003V01', '163N003V02', '163N007V02']
        MAX_UPR_GPV_UINs = ['163N001V01', '163N001V02','163B001V01', '163B002V01']
        GPV_UINS = ['163N004V01', '163N004V02','163N009V01','163N013V01','163N014V01']


        # Determine reserve calculation method and set X, Y values
        if Premium_mode == 'Single/Member Premium' or Premium_mode == 'Yearly':
            if UIN in UPR_UINs:
                Gross_final_reserve = [Gross_UPR_eom[month] for month in range(policy_term)]
                Net_final_reserve = [Net_UPR_eom[month] for month in range(policy_term)]
                X = 'UPR'
                Y = 'UPR_eom'
                
            elif UIN in MAX_UPR_GPV_UINs:
                if policy_term <= 12:
                    Gross_final_reserve = [max(Gross_GPV[month], Gross_UPR_eom[month]) for month in range(policy_term)]
                    Net_final_reserve = [max(Net_GPV[month], Net_UPR_eom[month]) for month in range(policy_term)]
                    X = 'Max(U,G)'
                    Y = 'GPV' if Net_GPV[policy_months] > Net_UPR_eom[policy_months] else 'Net_UPR_eom'
                else:
                    Gross_final_reserve = [Gross_GPV[month] for month in range(policy_term)]
                    Net_final_reserve = [Net_GPV[month] for month in range(policy_term)]
                    X = 'G'
                    Y = 'GPV'
                    
            elif UIN in GPV_UINS:
                Gross_final_reserve = [Gross_GPV[month] for month in range(policy_term)]
                Net_final_reserve = [Net_GPV[month] for month in range(policy_term)]
                X = 'G'
                Y = 'GPV'

            # if (UIN not in UPR_UINs) and (x == policy_term): # and datediff.days > 0
            if (x == policy_term): # and datediff.days > 0
                Gross_final_reserve[-1]=Gross_UPR_bom[policy_term-1]*(d2.day/30)
                Net_final_reserve[-1]=Net_UPR_bom[policy_term-1]*(d2.day/30)
      

        elif Premium_mode != 'Single/Member Premium':
            if UIN == '163N012V01':
                X = 'G'
                Y = 'GPV'
                next_montheversary, noofweeks = get_next_montheversary_and_weeks_excel_logic(selected_date_val, input_row['Coverage Effective Date'])
                reservefactor = GS_data.loc[GS_data['Product UIN'] == UIN, noofweeks].values[0]

                Gross_final_reserve = [reservefactor*MB[x-1] if month == policy_term - 1 else reservefactor * Gross_GPV[month] for month in range(policy_term)]
                Net_final_reserve = [reservefactor*MB[x-1] if month == policy_term - 1 else reservefactor * Net_GPV[month] for month in range(policy_term)]

        Gross_sv_deficiency_reserve = [max(0, SB[i] - Gross_final_reserve[i]) for i in range(policy_term)]
        Net_sv_deficiency_reserve = [max(0, SB[i] - Net_final_reserve[i]) for i in range(policy_term)]

        # for x in range(1, policy_term+1):
        #     print(f"x : {x} --- Net_final_reserve : {Net_final_reserve[x-1]}  --- Net_sv_deficiency_reserve : {Net_sv_deficiency_reserve[x-1]} ")



# =================================================================================================================================================================
# Projection Cashflows Loop
# =================================================================================================================================================================

        if ProfitabilityComponentsRequired == 1:
            for x in range(1, policy_term+1):
                policy_year = int((x-1)/12) + 1 


                # if x == 1:
                #     pass    

                # elif x == policy_months + 1:
                #     no_of_lives_p = 1
                #     inf_lives.append(no_of_lives_p) 
                
                # elif x != 1:
                #     no_of_lives_p = (inf_lives[x-2]-deaths_p[x-2]-lapse_p[x-2])
                #     inf_lives.append(no_of_lives_p)

                # deaths_p.append(inf_lives[x-1]*qx[x-1])     

                # if x != policy_term:
                #     lapse_p.append((inf_lives[x-1]-deaths_p[x-1])*wx[x-1])
                # else:
                #     lapse_p.append(0)

                # if UIN == '163N012V01':
                #     maturity_prob_p.append(0) if x != policy_term else maturity_prob_p.append(inf_lives[x-1]-deaths_p[x-1]-lapse_p[x-1])    
                # elif UIN != '163N012V01':
                #     maturity_prob_p.append(0)
                        
                # print(f"x : {x} --- inf_lives : {inf_lives[x-1]} --- deaths_p : {deaths_p[x-1]} --- lapse_p : {lapse_p[x-1]} --- maturity_prob_p : {maturity_prob_p[x-1]}")

                int_earned.append(vri.loc[vri['Month'] == x, "BE Rate"].values[0])
                rdr_list.append(vri.loc[vri['Month'] == x, "BE Rate"].values[0]  +  (asum_data.loc['rdr monthly', UIN]) )  
                Yr1_prem.append(Gross_Premium_list[x-1]* int(policy_year == 1)*inf_lives[x-1])
                ren_prem_p.append(Gross_Premium_list[x-1]* int(policy_year != 1)*inf_lives[x-1])
                reins_prem_p.append(Reinsurance_Premium_list[x-1]* int(policy_year == 1)*inf_lives[x-1]) if (x == 1 and Reinsurance == 'Reinsured') else reins_prem_p.append(0)
                death_ben_p.append(deaths_p[x-1]*Net_DB[x-1])

                # print(f"x : {x} death_ben_p : {death_ben_p[x-1]} ")

                if x != policy_term:
                    surr_ben_p.append(SB[x-1]*lapse_p[x-1])
                else:
                    surr_ben_p.append(0)

                if x != policy_term:
                    maturity_ben_p.append(0)
                else:
                    maturity_ben_p.append(maturity_prob_p[x-1]*MB[x-1])

                # print(f"x : {x} death_ben_p : {death_ben_p[x-1]} --- surr_ben_p : {surr_ben_p[x-1]} --- maturity_ben_p : {maturity_ben_p[x-1]} ")

                yr1_comm_p.append(inf_lives[x-1] * Yr1_comm[x-1])
                ren_comm_p.append(inf_lives[x-1] * ren_comm[x-1])
                acq_exp_prem_p.append(inf_lives[x-1] * acq_exp_prem[x-1])
                acq_exp_pol_p.append(inf_lives[x-1] * acq_exp_pol[x-1])
                acq_sa_p.append(inf_lives[x-1] * acq_exp_sa[x-1])
                ren_exp_prem_p.append(inf_lives[x-1] * ren_exp_prem[x-1])
                ren_per_pol_p.append(inf_lives[x-1] * ren_per_pol[x-1])
                ren_sa_p.append(inf_lives[x-1] * ren_sa[x-1])
                clm_exp_p.append(deaths_p[x-1]*claim_expense* exp_infl[x - 1])
                surr_exp_p.append(lapse_p[x-1]*surrender_expense* exp_infl[x - 1])
                maturity_expense_p.append(maturity_prob_p[x-1]*maturity_expense* exp_infl[x - 1])

                # print(f"x : {x} death_ben_p : {death_ben_p[x-1]} --- surr_exp_p : {surr_exp_p[x-1]} --- maturity_expense_p : {maturity_expense_p[x-1]} ")                             
            # print(pd.DataFrame({ 'x': range(1, policy_term+1), 'inf_lives': inf_lives, 'deaths_p': deaths_p, 'lapse_p': lapse_p, 'maturity_prob_p': maturity_prob_p}).to_string(index=False, float_format='%.16f'))
                              
            # Initialize Net_BEL with policy_term+1 elements
            Net_BEL = [0] * (policy_term + 1)  

            # Calculate backwards from policy_term-1 down to 0
            for x in range(policy_term - 1, -1, -1):
                Net_BEL[x] = ((clm_exp_p[x] + surr_exp_p[x] + maturity_expense_p[x] + death_ben_p[x] + surr_ben_p[x] + maturity_ben_p[x] + Net_BEL[x+1]) / (1 + int_earned[x])) + (acq_exp_prem_p[x] + acq_exp_pol_p[x] + acq_sa_p[x] + ren_exp_prem_p[x] + ren_per_pol_p[x] + ren_sa_p[x]) - ((Yr1_prem[x] + ren_prem_p[x]) - (yr1_comm_p[x] + ren_comm_p[x] + reins_prem_p[x]))
            Net_BEL = Net_BEL[1:]

            for x in range(1, policy_term+1):     
                denominator = inf_lives[x-1] - deaths_p[x-1] -lapse_p[x-1] - maturity_prob_p[x-1]
                if abs(denominator) < 1e-10:
                    Net_BEL_p.append(0)  # or some other appropriate value
                else:
                    Net_BEL_p.append(Net_BEL[x-1]/denominator)

            # print(pd.DataFrame({'x': range(1, policy_term+1), 'maturity_expense_p': maturity_expense_p, 'deaths_p': deaths_p, 'death_ben_p': death_ben_p, 'surr_ben_p': surr_ben_p, 'maturity_ben_p': maturity_ben_p, 'Net_BEL': Net_BEL, 'Net_BEL_p': Net_BEL_p, 'int_earned': int_earned, 'ren_exp_total': [ren_exp_prem_p[i]+ren_per_pol_p[i]+ren_sa_p[i] for i in range(policy_term)], 'prem_total': [Yr1_prem[i]+ren_prem_p[i] for i in range(policy_term)], 'comm_total': [yr1_comm_p[i]+ren_comm_p[i] for i in range(policy_term)], 'reins_total': reins_prem_p}).to_string(index=False, float_format='%.12f'))

            for x in range(1, policy_term+1):
                if x != policy_term:
                    Gross_policy_liability_p.append(Gross_final_reserve[x-1]*inf_lives[x])
                    Net_policy_liability_p.append(Net_final_reserve[x-1]*inf_lives[x])
                else:
                    Gross_policy_liability_p.append(0)
                    Net_policy_liability_p.append(0)
                # print(f"x : {x} --- Net_policy_liability_p : {Net_policy_liability_p[x-1]} ")

            for x in range(1, policy_term+1):    
                if x == 1:
                    increase_in_liability.append(Net_policy_liability_p[x-1])
                else:
                    increase_in_liability.append(Net_policy_liability_p[x-1]-Net_policy_liability_p[x-2])

                if x == 1:
                    inc_int.append(int_earned[x-1]*(Yr1_prem[x-1]+ren_prem_p[x-1]
                                                    -yr1_comm_p[x-1]-ren_comm_p[x-1]-acq_exp_prem_p[x-1]-acq_exp_pol_p[x-1]-acq_sa_p[x-1]-
                                                    ren_exp_prem_p[x-1]-ren_per_pol_p[x-1]-ren_sa_p[x-1]-clm_exp_p[x-1]-surr_exp_p[x-1]-maturity_expense_p[x-1]))
                else:
                    inc_int.append(int_earned[x-1]*(Yr1_prem[x-1]+ren_prem_p[x-1]+Net_policy_liability_p[x-2]
                                                    -yr1_comm_p[x-1]-ren_comm_p[x-1]-acq_exp_prem_p[x-1]-acq_exp_pol_p[x-1]-acq_sa_p[x-1]-
                                                    ren_exp_prem_p[x-1]-ren_per_pol_p[x-1]-ren_sa_p[x-1]-clm_exp_p[x-1]-surr_exp_p[x-1]-maturity_expense_p[x-1]))
                    
                PBT.append(Yr1_prem[x-1]+ren_prem_p[x-1]+inc_int[x-1]
                            -reins_prem_p[x-1]-death_ben_p[x-1]-surr_ben_p[x-1]-maturity_ben_p[x-1]-yr1_comm_p[x-1]-ren_comm_p[x-1]-acq_exp_prem_p[x-1]-acq_exp_pol_p[x-1]-acq_sa_p[x-1]
                            -ren_exp_prem_p[x-1]-ren_per_pol_p[x-1]-ren_sa_p[x-1]-clm_exp_p[x-1]-surr_exp_p[x-1]-maturity_expense_p[x-1]-increase_in_liability[x-1])
                
                tax.append(PBT[x-1]*asum_data.loc['tax_rate', UIN]* int(PBT[x-1]>0))

                PAT.append(PBT[x-1]-tax[x-1])

                if x != policy_term:
                    Gross_sum_at_risk.append((Gross_DB[x-1]-Gross_final_reserve[x-1])*inf_lives[x])
                    Net_sum_at_risk.append((Net_DB[x-1]-Net_final_reserve[x-1])*inf_lives[x])
                else:
                    Gross_sum_at_risk.append(0)
                    Net_sum_at_risk.append(0)

                k1.append(max(0.85, Net_final_reserve[x-1]/Gross_final_reserve[x-1])) if Gross_final_reserve[x-1] !=0 else k1.append(0)
                k2.append(max(0.50, Net_sum_at_risk[x-1]/Gross_sum_at_risk[x-1])) if Gross_sum_at_risk[x-1] !=0 else k2.append(0)

                rsm.append(( Gross_policy_liability_p[x-1] * asum_data.loc['rsm_reserve', UIN] * k1[x-1] + Gross_sum_at_risk[x-1] * asum_data.loc['rsm_sar', UIN] * k2[x-1]) * asum_data.loc['solvency_ratio', UIN])
                
                if x == 1:
                    inc_in_rsm.append(rsm[x-1])
                else:
                    inc_in_rsm.append(rsm[x-1]-rsm[x-2])

                int_on_rsm.append(rsm[x-1]*int_earned[x-1])

                tax_on_rsm_int.append(int_on_rsm[x-1]*asum_data.loc['tax_rate', UIN])

                sh_profit.append(PAT[x-1]-inc_in_rsm[x-1]+int_on_rsm[x-1]-tax_on_rsm_int[x-1])

            # Net cashflow calculation list
            Net_cashflow = [0] * (policy_term +1)
            for x in range(1, policy_term+1):
                # inflows - expenses - benefits
                inflow = Yr1_prem[x-1] + ren_prem_p[x-1]
                expenses = reins_prem_p[x-1] + yr1_comm_p[x-1] + ren_comm_p[x-1] + acq_exp_prem_p[x-1] + acq_exp_pol_p[x-1] + acq_sa_p[x-1] + ren_exp_prem_p[x-1] + ren_per_pol_p[x-1] + ren_sa_p[x-1] + clm_exp_p[x-1] + surr_exp_p[x-1] + maturity_expense_p[x-1]
                benefits = death_ben_p[x-1] + surr_ben_p[x-1] + maturity_ben_p[x-1]
                Net_cashflow[x-1] = (expenses + benefits)


            # print(policy_months, outstanding_term_months,  policy_term)
            # DMT calculation
            DMT_numerator = 0
            DMT_denominator = 0
            for x in range(policy_months, policy_term+1):
                DMT_numerator += Net_cashflow[x-1] * x * (1 + int_earned[x-1])**(-x)
                DMT_denominator += Net_cashflow[x-1] * (1 + int_earned[x-1])**(-x)
                # print(f"x : {x} --- Net_cashflow : {Net_cashflow[x-1]} --- int_earned : {int_earned[x-1]} --- DMT_numerator : {DMT_numerator} --- DMT_denominator : {DMT_denominator} ")

            DMT = (np.abs(DMT_numerator / DMT_denominator) - policy_months)if DMT_denominator != 0 else 0
            # print(f"DMT : {DMT} ")
            # this DMT calculation is based on the definition of DMT as the weighted average time until cash flows are received, where the weights are the present value of the cash flows. 
            # The DMT value is a single value for the entire policy term, not a list of values for each month. 
            # It represents the average time until cash flows are received, taking into account the timing and magnitude of the cash flows.


        if(COIlevel_CashflowsRequired == 1):
            cashflow={
                "COI Number" : input_row['COI Number'],
                "Channel" : input_row['Channel'],
                "MPH" : input_row['MPH Code'],
                "Base Product UIN" : UIN,
                "PH Gender" : input_row['PH Gender'],
                "Issuance Date" : Issuance_Date,
                "Coverage Effective Date" : input_row['Coverage Effective Date'],
                "Expiry Date" : input_row['Expiry Date'],
                "DOB" : input_row['PH DOB'],
                "Policy Month" : month_list,
                "Per Policy ->": "",
                "Premium" : Gross_Premium_list,
                "Reinsurance Premium" : Reinsurance_Premium_list,
                "FY - Commission" : Yr1_comm,
                "REN - Commission" : ren_comm,
                "Infl Factor" : exp_infl,
                "ACQ EXP - Prem" : acq_exp_prem,
                "ACQ EXP - PP" : acq_exp_pol,
                "ACQ EXP - SA" : acq_exp_sa,
                "REN EXP - Prem" : ren_exp_prem,
                "REN EXP - PP" : ren_per_pol,
                "REN EXP - SA" : ren_sa,
                "DB EXP" : clm_exp,
                "SURR EXP" : surrender_exp,
                "MAT EXP" : mat_exp,
                "Gross DB" : Gross_DB,
                "Net DB" : Net_DB,
                "SURR BEN" : SB,
                "MAT BEN" : MB,
                "Age" : age,
                "Qx" : qx,
                "Wx" : wx,

                "Reserving ->":"",
                "Probability - IF\n(bom)" : inf_prob,
                "Probability - death\n(eom)" : deaths,
                "Probability - lapse\n(eom)" : lapse,
                "Probability - maturity\n(eom)" : maturity_prob,
                "VRI" : monthly_interest_rate_list,
                "IF Gross Premium\n(bom)" : Expected_Premium,
                "IF Reinsurance Premium\n(bom)" : Expected_Reinsurance_Premium,
                "IF Commission\n(bom)" : Expected_comm,
                "IF REN EXP\n(bom)" : Expected_ren_exp,
                "IF DB EXP\n(eom)" : Expected_clm_exp,
                "IF SURR EXP\n(eom)" : exit_exp,
                "IF MAT EXP\n(eom)" : Expected_mat_exp,
                "IF Gross DB\n(eom)" : Gross_Expected_DB,
                "IF Net DB\n(eom)" : Net_Expected_DB,
                "IF SURR BEN\n(eom)" : Expected_SB,
                "IF MAT BEN\n(eom)" : Expected_MB,
                "PV Premium\n(bom)" : npv_premium,
                "PV Reinsurance Premium\n(bom)" : npv_reinsurance_premium,
                "PV Commission\n(bom)" : npv_ren_comm,
                "PV REN EXP\n(bom)" : npv_ren_exp,
                "PV DB EXP\n(bom)" : npv_clm_exp,
                "PV SURR EXP\n(bom)" : npv_exit_exp,
                "PV MAT EXP\n(bom)" : npv_mat_exp,
                "PV Total EXP\n(bom)" : npv_total_exp,
                "PV Gross DB\n(bom)" : Gross_npv_DB,
                "PV Net DB\n(bom)" : Net_npv_DB,
                "PV SURR BEN\n(bom)" : npv_SB,
                "PV MAT BEN\n(bom)" : npv_mat_ben,
                "PV Gross Total BEN\n(bom)" : Gross_npv_total_ben,
                "PV Net Total BEN\n(bom)" : Net_npv_total_ben,
                "Gross UPR\n(bom)" : Gross_UPR_bom,
                "Gross UPR\n(eom)" : Gross_UPR_eom,
                "Gross GPV\n(eom)" : Gross_GPV,
                "Gross Final Reserve\n(eom)" : Gross_final_reserve,
                "Gross SVDR\n(eom)" : Gross_sv_deficiency_reserve,
                "Net UPR\n(bom)" : Net_UPR_bom,
                "Net UPR\n(eom)" : Net_UPR_eom,
                "Net GPV\n(eom)" : Net_GPV,
                "Net Final Reserve\n(eom)" : Net_final_reserve,
                "Net SVDR\n(eom)" : Net_sv_deficiency_reserve,

                "Profitability ->":"",
                "Probability - IF_p\n(bom)" : inf_lives,
                "Probability - death_p\n(eom)" : deaths_p,
                "Probability - lapse_p\n(eom)" : lapse_p,
                "Probability - maturity_p\n(eom)" : maturity_prob_p,
                "Int_PC" : int_earned,
                "RDR_PC" : rdr_list,
                "IF FY Premium_p\n(bom)" : Yr1_prem,
                "IF REN Premium_p\n(bom)" : ren_prem_p,
                "IF Reinsurance Premium_p\n(bom)" : reins_prem_p,
                "IF FY Commission_p\n(bom)" : yr1_comm_p,
                "IF REN Commission_p\n(bom)" : ren_comm_p,
                "IF ACQ EXP - Prem_p\n(bom)" : acq_exp_prem_p,
                "IF ACQ EXP - PP_p\n(bom)" : acq_exp_pol_p,
                "IF ACQ EXP - SA_p\n(bom)" : acq_sa_p,
                "IF REN EXP - Prem_p\n(bom)" : ren_exp_prem_p,
                "IF REN EXP - PP_p\n(bom)" : ren_per_pol_p,
                "IF REN EXP - SA_p\n(bom)" : ren_sa_p,
                "IF DB EXP_p\n(eom)" : clm_exp_p,
                "IF SURR EXP_p\n(eom)" : surr_exp_p,
                "IF MAT EXP_p\n(eom)" : maturity_expense_p,
                "IF DB_p\n(eom)" : death_ben_p,
                "IF SURR BEN_p\n(eom)" : surr_ben_p,
                "IF MAT BEN_p\n(eom)" : maturity_ben_p,
                "BEL\n(eom)" : Net_BEL_p,
                "Gross Policy Liab_p\n(eom)" : Gross_policy_liability_p,
                "Net Policy Liab_p\n(eom)" : Net_policy_liability_p,
                "Incr in Liab_p\n(eom)" : increase_in_liability,
                "Int_p\n(eom)" : inc_int,
                "PBT_p\n(eom)" : PBT,
                "Tax_p\n(eom)" : tax,
                "PAT_p\n(eom)" : PAT,
                "Gross SAR_p\n(eom)" : Gross_sum_at_risk,
                "Net SAR_p\n(eom)" : Net_sum_at_risk,
                "K1 Factor_p\n(eom)" : k1,
                "K2 Factor_p\n(eom)" : k2,
                "RSM_p\n(eom)" : rsm,
                "Incr in RSM_p\n(eom)" : inc_in_rsm,
                "Int RSM_p\n(eom)" : int_on_rsm,
                "Tax Int RSM_p\n(eom)" : tax_on_rsm_int,
                "SH Profit_p\n(eom)" : sh_profit,
                "Overall_mortality_list" : Overall_mortality_list,
                "Net_cashflow" : Net_cashflow,
                # "Accumulated Premium_p\n(eom)" : Accumulated_premium,
                # "Accumulated Profit_p\n(eom)" : Accumulated_value_of_profit,
                # "PV Profit_p\n(eom)" : pv_profit_final,
         
            }
        

            # First, determine the maximum length among all lists
            max_length = 0
            for key, value in cashflow.items():
                if isinstance(value, list):
                    max_length = max(max_length, len(value))

            # If there are no lists, set max_length to policy_term (or 1)
            if max_length == 0:
                max_length = policy_term

            # Extend scalar values to lists of appropriate length
            processed_cashflow = {}
            for key, value in cashflow.items():
                if isinstance(value, list):
                    # For lists, pad with None if shorter than max_length
                    if len(value) < max_length:
                        processed_cashflow[key] = value + [None] * (max_length - len(value))
                    else:
                        processed_cashflow[key] = value
                else:
                    # For scalar values, create a list with the value repeated max_length times
                    processed_cashflow[key] = [value] * max_length

            # Create DataFrame with month numbers as index (starting from 1)
            Cashflow = pd.DataFrame(processed_cashflow)
            # Cashflow.index = Cashflow.index + 1  # Make index start from 1 instead of 0

            # Filter from policy_months to policy_term
            OSCashflow = Cashflow.loc[policy_months : policy_term]

            seperating_cols_to_be_coloured = ["Per Policy ->", "Reserving ->", "Profitability ->"]

            # Save OSCashflows with colored columns
            os.makedirs(f"{results_dir}/OSCashflows", exist_ok=True)
            styled_OSCashflow = color_columns(OSCashflow, seperating_cols_to_be_coloured)
            styled_OSCashflow.to_excel(f"{results_dir}/OSCashflows/{input_row['COI Number']}_{UIN}_OS_Cashflow.xlsx", index=False)

            # Save CompleteCashflows with colored columns
            os.makedirs(f"{results_dir}/CompleteCashflows", exist_ok=True)
            styled_Cashflow = color_columns(Cashflow, seperating_cols_to_be_coloured)
            styled_Cashflow.to_excel(f"{results_dir}/CompleteCashflows/{input_row['COI Number']}_{UIN}_Complete_cashflow.xlsx", index=False)

# =================================================================================================================================================================
# Output Object Creation
# =================================================================================================================================================================

        output_obj = {
            "COI Number": input_row['COI Number'],
            "Base Product UIN": UIN,
            "Channel": input_row['Channel'],
            "PH Gender": input_row['PH Gender'],
            "JH Gender": JH_Gender,
            "PH DOB": input_row['PH DOB'],
            "JH DOB": JH_DOB,
            "Age": input_row['PH Entry Age'],
            "MPH Code": input_row['MPH Code'],
            "Issuance Date": Issuance_Date,
            "Coverage Effective Date": input_row['Coverage Effective Date'],
            "Expiry Date": input_row['Expiry Date'],
            "Gross UPR_bom Per Policy": Gross_UPR_bom[policy_months],
            "Gross UPR_eom Per Policy": Gross_UPR_eom[policy_months],
            "Net UPR_bom Per Policy": Net_UPR_bom[policy_months],
            "Net UPR_eom Per Policy": Net_UPR_eom[policy_months],
            "Outstanding Term(Months)": outstanding_term_months,
            "Policy Term_Month": policy_term,
            "Premium before Reinsurance": Gross_Premium,
            "Premium after Reinsurance": Net_Premium,
            "Commission": input_row['Commission'],
            "Sum Assured before Reinsurance": input_row['Original SA'],
            "Sum Assured after Reinsurance": min(input_row['Original SA'], input_row['Retained_SA']),
            "Current SA": input_row['Current SA'],
            "Calculated": outstanding_amount[policy_months],
            "Death Payment": Gross_Expected_DB[policy_months],
            "Gross_NPV DB": Gross_npv_DB[policy_months],
            "Gross GPV": Gross_GPV[policy_months],
            "Net GPV": Net_GPV[policy_months],
            "Gross Reserve\n(NPV)": Gross_final_reserve[policy_months],
            "Net Reserve\n(NPV)": Net_final_reserve[policy_months],
            "Net SV Deficiency Reserve": Net_sv_deficiency_reserve[policy_months],
            "Applicable Reserve": X,  
            "Picked Up Reserve": Y,       
            "Status": input_row['Status'],
            "RSM": (0.01 * Net_final_reserve[policy_months]) + (0.001*(SA-Net_final_reserve[policy_months])),
            "Age at expiry": PH_int_age,
            "Mortality": used_mortality,
            "SB": SB[policy_months],
            "Policy Duration Months": policy_months,
            "POL NO": input_row['POLICYNUMBER'],
            "Cover life": input_row['Co-Borrower/Joint Life ID'],
            "SA_Code": input_row['SA_code'],
            "Total exp": ren_pv[policy_months],
            "Total Benefit": death_pv[policy_months],
            "BEL\n(eom)" : Net_BEL[policy_months],
            "DMT" : DMT,
        }

        # Write individual policy result to CSV file
        write_header = (policy_index == 0)  # Write header only for first policy in chunk
        write_policy_result(output_file, output_obj, write_header)

# =================================================================================================================================================================
# Aggregation Creation
# =================================================================================================================================================================
        # print("aggregationrequirement------")
        if(IsAggregationRequired == 1):

            # Generate Monthly Output for aggregation    
            typeOfCashflow = cashflowAggregation_type
            if typeOfCashflow == 0:
                z = policy_months
            elif typeOfCashflow == 1:
                z = 0

            for x in range(z, policy_term):   
                if typeOfCashflow == 0:
                    os_month = x - policy_months          # use this incase of OS cashflows only
                elif typeOfCashflow == 1:
                    os_month = x                            # use this incase of full cashflows

                group_key = tuple(
                    (
                        os_month if col == "Outstanding Month"
                        else Y if col == "Picked Up Reserve"
                        else input_row[col]
                    )
                    for col in GROUPING_COLUMNS
                )
                
                # Build the base components dictionary
                components_dict = {
                    "Premium": Gross_Premium_list[x],
                    "Reinsurance Premium": Reinsurance_Premium_list[x],
                    "FY - Commission": Yr1_comm[x],
                    "REN - Commission": ren_comm[x],
                    "ACQ EXP - Prem": acq_exp_prem[x],
                    "ACQ EXP - PP": acq_exp_pol[x],
                    "ACQ EXP - SA": acq_exp_sa[x],
                    "REN EXP - Prem": ren_exp_prem[x],
                    "REN EXP - PP": ren_per_pol[x],
                    "REN EXP - SA": ren_sa[x],
                    "DB EXP": clm_exp[x],
                    "SURR EXP": surrender_exp[x],
                    "MAT EXP": mat_exp[x],
                    "Gross DB": Gross_DB[x],
                    "Net DB": Net_DB[x],
                    "SURR Ben": SB[x],
                    "MAT Ben": MB[x],
                    "Probability - IF\n(bom)": inf_prob[x],
                    "Probability - death\n(eom)": deaths[x],
                    "Probability - lapse\n(eom)": lapse[x],
                    "Probability - maturity\n(eom)": maturity_prob[x],
                    "IF Premium\n(bom)": Expected_Premium[x],
                    "IF Reinsurance Premium\n(bom)": Expected_Reinsurance_Premium[x],
                    "IF Commission\n(bom)": Expected_comm[x],
                    "IF REN EXP\n(bom)": Expected_ren_exp[x],
                    "IF DB EXP\n(eom)": Expected_clm_exp[x],
                    "IF SURR EXP\n(eom)": exit_exp[x],
                    "IF MAT EXP\n(eom)": Expected_mat_exp[x],
                    "IF Gross DB\n(eom)": Gross_Expected_DB[x],
                    "IF Net DB\n(eom)": Net_Expected_DB[x],
                    "IF SURR BEN\n(eom)": Expected_SB[x],
                    "IF MAT BEN\n(eom)": Expected_MB[x],
                    "PV Premium\n(bom)": npv_premium[x],
                    "PV Reinsurance Premium\n(bom)": npv_reinsurance_premium[x],
                    "PV Commission\n(bom)": npv_ren_comm[x],
                    "PV REN EXP\n(bom)": npv_ren_exp[x],
                    "PV DB EXP\n(bom)": npv_clm_exp[x],
                    "PV SURR EXP\n(bom)": npv_exit_exp[x],
                    "PV MAT EXP\n(bom)": npv_mat_exp[x],
                    "PV Total EXP\n(bom)": npv_total_exp[x],
                    "PV Gross DB\n(bom)": Gross_npv_DB[x],
                    "PV Net DB\n(bom)": Net_npv_DB[x],
                    "PV SURR BEN\n(bom)": npv_SB[x],
                    "PV MAT BEN\n(bom)": npv_mat_ben[x],
                    "PV Gross Total BEN\n(bom)": Gross_npv_total_ben[x],
                    "PV Net Total BEN\n(bom)": Net_npv_total_ben[x],
                    "Gross UPR\n(bom)" : Gross_UPR_bom[x],
                    "Gross UPR\n(eom)" : Gross_UPR_eom[x],
                    "Gross GPV\n(eom)": Gross_GPV[x],
                    "Gross Final Reserve\n(eom)": Gross_final_reserve[x],
                    "Gross SVDR\n(eom)": Gross_sv_deficiency_reserve[x],
                    "Net UPR\n(bom)": Net_UPR_bom[x],
                    "Net UPR\n(eom)": Net_UPR_eom[x],
                    "Net GPV\n(eom)": Net_GPV[x],
                    "Net Final Reserve\n(eom)": Net_final_reserve[x],
                    "Net SVDR\n(eom)": Net_sv_deficiency_reserve[x],
                }

                # Add profitability components if required
                if ProfitabilityComponentsRequired == 1:
                    components_dict.update({
                        "Probability - IF_p\n(bom)": inf_lives[x],
                        "Probability - death_p\n(eom)": deaths_p[x],
                        "Probability - lapse_p\n(eom)": lapse_p[x],
                        "Probability - maturity_p\n(eom)": maturity_prob_p[x],
                        "IF FY Premium_p\n(bom)": Yr1_prem[x],
                        "IF REN Premium_p\n(bom)": ren_prem_p[x],
                        "IF Reinsurance Premium_p\n(bom)": reins_prem_p[x],
                        "IF FY Commission_p\n(bom)": yr1_comm_p[x],
                        "IF REN Commission_p\n(bom)": ren_comm_p[x],
                        "IF ACQ EXP - Prem_p\n(bom)": acq_exp_prem_p[x],
                        "IF ACQ EXP - PP_p\n(bom)": acq_exp_pol_p[x],
                        "IF ACQ EXP - SA_p\n(bom)": acq_sa_p[x],
                        "IF REN EXP - Prem_p\n(bom)": ren_exp_prem_p[x],
                        "IF REN EXP - PP_p\n(bom)": ren_per_pol_p[x],
                        "IF REN EXP - SA_p\n(bom)": ren_sa_p[x],
                        "IF DB EXP_p\n(eom)": clm_exp_p[x],
                        "IF SURR EXP_p\n(eom)": surr_exp_p[x],
                        "IF MAT EXP_p\n(eom)": maturity_expense_p[x],
                        "IF DB_p\n(eom)": death_ben_p[x],
                        "IF SURR BEN_p\n(eom)": surr_ben_p[x],
                        "IF MAT BEN_p\n(eom)": maturity_ben_p[x],
                        "BEL\n(eom)": Net_BEL_p[x],
                        "Gross Policy Liab_p\n(eom)": Gross_policy_liability_p[x],
                        "Net Policy Liab_p\n(eom)": Net_policy_liability_p[x],
                        "Incr in Liab_p\n(eom)": increase_in_liability[x],
                        "Int_p\n(eom)": inc_int[x],
                        "PBT_p\n(eom)": PBT[x],
                        "Tax_p\n(eom)": tax[x],
                        "PAT_p\n(eom)": PAT[x],
                        "Gross SAR_p\n(eom)": Gross_sum_at_risk[x],
                        "Net SAR_p\n(eom)": Net_sum_at_risk[x],
                        "RSM_p\n(eom)": rsm[x],
                        "Incr in RSM_p\n(eom)": inc_in_rsm[x],
                        "Int RSM_p\n(eom)": int_on_rsm[x],
                        "Tax Int RSM_p\n(eom)": tax_on_rsm_int[x],
                        "SH Profit_p\n(eom)": sh_profit[x],
                    })

                # Now make the function call
                update_aggregated_cashflows(
                    agg_cashflows=agg_cashflows,
                    group_key=group_key,
                    components=components_dict,
                )

        return True      
    
    except Exception as e:
        # print(f"\n\n❌ Error in COI: {input_row.get('COI Number', 'Unknown')}")
        # print(f"❌ Error Type: {type(e).__name__}")
        # print(f"❌ Error Message: {e}")
        
        # import traceback
        # traceback.print_exc()
        # print("\n")
        return e

def process_chunk(chunk_data, chunk_id, selected_date_val, asum_data, GS_data, vri ,IALM_data, ADB, ATPD, GS_loading, lapse_table_data, mortalities,results_dir,COIlevel_CashflowsRequired,ProfitabilityComponentsRequired,cashflowAggregation_type,IsAggregationRequired,GROUPING_COLUMNS,runcase):
    """Process a chunk of data and return results."""
    chunk_skipped_policies = []
    chunk_reason = []
    chunk_aggregated_cashflows = {}
    
    # Create output file for this chunk
    chunk_output_path = f"{results_dir}/chunk_{chunk_id}_output.csv"
    
    # Initialize the output file with header
    with open(chunk_output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
    
    for index, input_row in chunk_data.iterrows():
        try:
            # Process policy and write result to file
            result = Reserve(input_row, selected_date_val, asum_data, GS_data,vri, IALM_data, ADB, ATPD,GS_loading,
                            lapse_table_data, chunk_aggregated_cashflows, 
                            mortalities, chunk_output_path, chunk_id, index, results_dir,COIlevel_CashflowsRequired,ProfitabilityComponentsRequired,cashflowAggregation_type,IsAggregationRequired,GROUPING_COLUMNS, runcase)
            if isinstance(result, Exception):
                chunk_skipped_policies.append(input_row['COI Number'])
                chunk_reason.append(str(result))
                
        except Exception as e:
            chunk_skipped_policies.append(input_row['COI Number'])
            chunk_reason.append(str(e))
    
    # Write aggregated data for this chunk
    chunk_agg_path = f"{results_dir}/chunk_{chunk_id}_aggregated.csv"
    
    if chunk_aggregated_cashflows:
        aggregated_df = pd.DataFrame.from_dict(chunk_aggregated_cashflows, orient='index')
        keys_df = pd.DataFrame(aggregated_df.index.tolist(), columns=GROUPING_COLUMNS)
        aggregated_df = pd.concat([keys_df, aggregated_df.reset_index(drop=True)], axis=1)
        aggregated_df.sort_values(GROUPING_COLUMNS, inplace=True)
        aggregated_df.to_csv(chunk_agg_path, index=False)
    
    return chunk_output_path, chunk_agg_path, chunk_skipped_policies, chunk_reason

def combine_csv_files(file_paths, output_path, results_dir, current_scenario,chunk_size=1000000 ):
    """Combine multiple CSV files efficiently."""
    if not file_paths:
        return
    
    print(f"Combining {len(file_paths)} files...")
    
    # Use pandas to read and concatenate files in chunks
    chunks = []
    for i, file_path in enumerate(file_paths):
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # try:
                # Read file in chunks to manage memory
                chunk_reader = pd.read_csv(file_path, chunksize=chunk_size)
                for chunk in chunk_reader:
                    chunks.append(chunk)                
                # Remove temporary file
                os.remove(file_path)                
                if (i + 1) % 10 == 0:
                    print(f"  Processed {i + 1}/{len(file_paths)} files")                    
            # except Exception as e:
            #     print(f"❌ Error processing file {file_path}: {e}")
    if chunks:
        final_df = pd.concat(chunks, ignore_index=True)

        # Find and remove any row that exactly matches the header
        header_values = final_df.columns.tolist()
        mask = final_df.apply(lambda row: row.tolist() == header_values, axis=1)
        if mask.any():
            # Keep only rows where mask is False
            final_df = final_df[~mask].reset_index(drop=True)

        final_df.to_csv(output_path, index=False)
        print(f"✅ Combined output saved to: {output_path}")
        print(f"\tTotal records: {len(final_df):,}")
        ResultAggregator(final_df, results_dir,current_scenario)  # Call grouping function
        SVDR_Aggregator(final_df, results_dir,current_scenario)  # Call Net SVDR grouping function
        return final_df
    else:
        print("❌ No data to combine")

def process_grouped_data(df):
    # try:    
        df['Status'] = pd.to_numeric(df['Status'], errors='coerce').dropna().astype(int)

        # Convert relevant columns to numeric to ensure proper aggregation
        numeric_columns = [
            'COI Number',
            'Premium before Reinsurance',
            'Premium after Reinsurance', 
            'Sum Assured before Reinsurance',
            'Sum Assured after Reinsurance',
            'Gross UPR_bom Per Policy',
            'Gross UPR_eom Per Policy',
            'Net UPR_bom Per Policy',
            'Net UPR_eom Per Policy',
            'Gross GPV',
            'Net GPV',
            'Gross Reserve\n(NPV)',
            'Net Reserve\n(NPV)',
            'BEL\n(eom)'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Perform aggregation with proper numeric handling
        result = (
            df.groupby(['Status', 'Base Product UIN'])
            .agg(
                COI_no_count=('COI Number', 'count'),
                Gross_premium_sum=('Premium before Reinsurance', 'sum'),
                Net_premium_sum=('Premium after Reinsurance', 'sum'),
                Gross_SA_sum=('Sum Assured before Reinsurance', 'sum'),
                Net_SA_sum=('Sum Assured after Reinsurance', 'sum'),
                Gross_UPR_bom_sum=('Gross UPR_bom Per Policy', 'sum'),
                Gross_UPR_eom_sum=('Gross UPR_eom Per Policy', 'sum'),
                Net_UPR_bom_sum=('Net UPR_bom Per Policy', 'sum'),
                Net_UPR_eom_sum=('Net UPR_eom Per Policy', 'sum'),
                Gross_GPV_sum=('Gross GPV', 'sum'),
                Net_GPV_sum=('Net GPV', 'sum'),
                Gross_reserve_sum=('Gross Reserve\n(NPV)', 'sum'),
                Net_reserve_sum=('Net Reserve\n(NPV)', 'sum'),
                BEL_sum=('BEL\n(eom)','sum')
            )
            .reset_index()
        )
        
        output = []
        for status, group in result.groupby('Status'):
            output.append([status] + [None] * 8)  # status followed by 8 None values
            
            for _, row in group.iterrows():
                output.append([
                    f"{row['Base Product UIN']}",  
                    row['COI_no_count'],
                    row['Gross_premium_sum'],
                    row['Net_premium_sum'],  
                    row['Gross_SA_sum'],
                    row['Net_SA_sum'],
                    row['Gross_UPR_bom_sum'],
                    row['Gross_UPR_eom_sum'],
                    row['Net_UPR_bom_sum'],
                    row['Net_UPR_eom_sum'],
                    row['Gross_GPV_sum'],
                    row['Net_GPV_sum'],
                    row['Gross_reserve_sum'],
                    row['Net_reserve_sum'],
                    row['BEL_sum']
                ])
        
        # Return DataFrame with all 9 columns
        return pd.DataFrame(output, columns=[
            'Category', 
            'COI_no_count', 
            'premium\n(Gross)', 
            'premium\n(Net)', 
            'SA\n(Gross)', 
            'SA\n(Net)', 
            'UPR_bom\n(Gross)',
            'UPR_eom\n(Gross)',
            'UPR_bom\n(Net)',
            'UPR_eom\n(Net)', 
            'GPV\n(Gross)',
            'GPV\n(Net)', 
            'Reserve\n(Gross)',
            'Reserve\n(Net)',
            'BEL\n(eom)'

        ])
    # except Exception as e:
    #     print(f"\n❌ Error in process_grouped_data: {e}")
    #     return pd.DataFrame()  # Return empty DataFrame on error

def ResultAggregator(df, results_dir,current_scenario):    
    # try:
        hierarchical_total = process_grouped_data(df)
        output_file = f"{results_dir}/Valuation_Results_Grouped_{current_scenario}.xlsx"
        output_file2 = f"Results_Scenarios_Grouped.xlsx"

        with pd.ExcelWriter(output_file) as writer:
            hierarchical_total.to_excel(writer, sheet_name='Total', index=False)

        # Check if file exists
        if os.path.exists(output_file2):
            with pd.ExcelWriter(output_file2, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                hierarchical_total.to_excel(writer, sheet_name=current_scenario, index=False)
        else:
            # Create new workbook if file doesn't exist
            with pd.ExcelWriter(output_file2, engine='openpyxl') as writer:
                hierarchical_total.to_excel(writer, sheet_name=current_scenario, index=False)

        print(f"✅ Data has been exported to {output_file}")
    # except Exception as e:
    #     print(f"❌ Error in Result Aggregator : {e} !")

def SVDR_Aggregator(df, results_dir,current_scenario):
    # try:
        df['Status'] = pd.to_numeric(df['Status'], errors='coerce').dropna().astype(int)
        df['SB'] = pd.to_numeric(df['SB'], errors='coerce')
        df['Net Reserve\n(NPV)'] = pd.to_numeric(df['Net Reserve\n(NPV)'], errors='coerce')
        df['Net SV Deficiency Reserve'] = pd.to_numeric(df['Net SV Deficiency Reserve'], errors='coerce')
        
        # Initial filtering
        svdr_filtered = df[(df['Status'] == 11) & (df['MPH Code'] != 'TELAMPH615') & (df['POL NO'] != 399)]
        
        # Group by Base Product UIN and MPH Code
        svdr_grouped = (
            svdr_filtered
            .groupby(['Base Product UIN', 'MPH Code'])
            .agg(
                SB_sum=('SB', 'sum'),
                reserve_sum=('Net Reserve\n(NPV)', 'sum'),
                svdr_sum=('Net SV Deficiency Reserve', 'sum')
            )
            .reset_index()
        )
        svdr_grouped.columns = ['Base Product UIN', 'MPH Code', 'SB', 'Net Reserve(NPV)', 'Net SVDR\n(COI level)']

        # Define special products
        special_products = ['163N001V01', '163N001V02']
        special_condition = svdr_grouped['Base Product UIN'].isin(special_products) & (svdr_grouped['MPH Code'] == 'CREDMPH002')

        # Calculate Net SVDR (Group level)
        svdr_grouped['Net SVDR\n(Group level)'] = svdr_grouped['SB'] - svdr_grouped['Net Reserve(NPV)']
        svdr_grouped.loc[~special_condition, 'Net SVDR\n(Group level)'] = svdr_grouped.loc[~special_condition, 'Net SVDR\n(Group level)'].clip(lower=0)
        
        # Calculate SVDR_Final
        svdr_grouped['SVDR_Final'] = svdr_grouped.apply(
            # lambda row: row['Net SVDR\n(COI level)'] if row['Base Product UIN'] in ['163N004V01', '163N004V02'] else row['Net SVDR\n(Group level)'],
            lambda row: row['Net SVDR\n(Group level)'],
            axis=1
        )
        
        # Create Net SVDR Final summary
        svdr_final_df = svdr_grouped.copy()
        
        # Create a flag to identify special products with CREDMPH002
        svdr_final_df['Is_Special_CREDMPH002'] = svdr_final_df['Base Product UIN'].isin(special_products) & (svdr_final_df['MPH Code'] == 'CREDMPH002')

        # Separate special CREDMPH002 rows
        special_credmph = svdr_final_df[svdr_final_df['Is_Special_CREDMPH002']].copy()
        special_credmph_summary = (
            special_credmph.groupby(['Base Product UIN', 'MPH Code'])
            .agg(SVDR_Final_sum=('SVDR_Final', 'sum'))
            .reset_index()
        )
        
        # Create display name for special CREDMPH002 cases
        special_credmph_summary['Display_Name'] = special_credmph_summary['Base Product UIN'] + ' - ' + special_credmph_summary['MPH Code']
        special_credmph_display = special_credmph_summary[['Display_Name', 'Base Product UIN', 'MPH Code', 'SVDR_Final_sum']].copy()
        
        # Apply special logic for CREDMPH002 records ONLY if they exist
        # Get the two special CREDMPH002 records
        credmph_163N001V01 = special_credmph_display[special_credmph_display['Base Product UIN'] == '163N001V01']
        credmph_163N001V02 = special_credmph_display[special_credmph_display['Base Product UIN'] == '163N001V02']
        
        # Initialize adjusted values with the original values
        adjusted_163N001V01 = 0
        adjusted_163N001V02 = 0
        
        # Only apply adjustment logic if both records exist
        if not credmph_163N001V01.empty and not credmph_163N001V02.empty:
            # Extract values
            value_163N001V01 = credmph_163N001V01['SVDR_Final_sum'].iloc[0]
            value_163N001V02 = credmph_163N001V02['SVDR_Final_sum'].iloc[0]
            
            # Apply the special logic
            if value_163N001V01 >= 0 and value_163N001V02 >= 0:
                # Both positive - remain same
                adjusted_163N001V01 = value_163N001V01
                adjusted_163N001V02 = value_163N001V02
            elif value_163N001V01 + value_163N001V02 < 0:
                # Sum is negative - both become 0
                adjusted_163N001V01 = 0
                adjusted_163N001V02 = 0
            else:
                # One negative, one positive, sum positive
                if value_163N001V01 < 0:
                    # 163N001V01 is negative, 163N001V02 is positive
                    adjusted_163N001V01 = 0
                    adjusted_163N001V02 = value_163N001V01 + value_163N001V02
                else:
                    # 163N001V02 is negative, 163N001V01 is positive
                    adjusted_163N001V01 = value_163N001V01 + value_163N001V02
                    adjusted_163N001V02 = 0
            
            # Update the values in the display DataFrame
            for idx, row in special_credmph_display.iterrows():
                if row['Base Product UIN'] == '163N001V01':
                    special_credmph_display.at[idx, 'SVDR_Final_Adjusted'] = adjusted_163N001V01
                elif row['Base Product UIN'] == '163N001V02':
                    special_credmph_display.at[idx, 'SVDR_Final_Adjusted'] = adjusted_163N001V02
        elif not credmph_163N001V01.empty or not credmph_163N001V02.empty:
            # If only one exists, keep its original value
            for idx, row in special_credmph_display.iterrows():
                special_credmph_display.at[idx, 'SVDR_Final_Adjusted'] = row['SVDR_Final_sum']
                if row['Base Product UIN'] == '163N001V01':
                    adjusted_163N001V01 = row['SVDR_Final_sum']
                elif row['Base Product UIN'] == '163N001V02':
                    adjusted_163N001V02 = row['SVDR_Final_sum']
        
        # For other rows (special products with non-CREDMPH002 and all non-special products)
        other_rows = svdr_final_df[~svdr_final_df['Is_Special_CREDMPH002']].copy()
        other_summary = (
            other_rows.groupby(['Base Product UIN'])
            .agg(SVDR_Final_sum=('SVDR_Final', 'sum'))
            .reset_index()
        )
        other_summary['SVDR_Final_Adjusted'] = other_summary['SVDR_Final_sum']  # Same for others
        
        # Create final display for special CREDMPH002
        special_credmph_final = pd.DataFrame()
        if not special_credmph_display.empty:
            special_credmph_final = special_credmph_display[['Display_Name', 'SVDR_Final_sum', 'SVDR_Final_Adjusted']].rename(columns={'Display_Name': 'Base Product UIN'})
        
        # Combine both summaries
        svdr_final_combined = pd.concat([
            special_credmph_final,
            other_summary[['Base Product UIN', 'SVDR_Final_sum', 'SVDR_Final_Adjusted']]
        ], ignore_index=True)
        
        # Sort for better readability - special products first, then others
        def sort_key(x):
            if x.startswith(tuple(special_products)):
                return (0, x)  # Special products first
            else:
                return (1, x)  # Others after
        
        if not svdr_final_combined.empty:
            svdr_final_combined = svdr_final_combined.sort_values(
                by='Base Product UIN', 
                key=lambda x: x.map(lambda y: (0 if str(y).startswith(tuple(special_products)) else 1, str(y)))
            )

        # Add the adjusted column to the original svdr_grouped DataFrame as well
        svdr_grouped['SVDR_Final_Adjusted'] = svdr_grouped['SVDR_Final']
        
        # Apply the same adjustment logic to svdr_grouped for the detailed sheet
        for idx, row in svdr_grouped.iterrows():
            if row['Base Product UIN'] == '163N001V01' and row['MPH Code'] == 'CREDMPH002':
                svdr_grouped.at[idx, 'SVDR_Final_Adjusted'] = adjusted_163N001V01
            elif row['Base Product UIN'] == '163N001V02' and row['MPH Code'] == 'CREDMPH002':
                svdr_grouped.at[idx, 'SVDR_Final_Adjusted'] = adjusted_163N001V02
             
        # Print summary of adjustments only if both special products exist
        print("\nSpecial CREDMPH002 Adjustments:")
        print("=" * 50)
        if not credmph_163N001V01.empty and not credmph_163N001V02.empty:
            print(f"163N001V01 - CREDMPH002: {credmph_163N001V01['SVDR_Final_sum'].iloc[0]:,.2f} → {adjusted_163N001V01:,.2f}")
            print(f"163N001V02 - CREDMPH002: {credmph_163N001V02['SVDR_Final_sum'].iloc[0]:,.2f} → {adjusted_163N001V02:,.2f}")
        elif not credmph_163N001V01.empty:
            print(f"163N001V01 - CREDMPH002: {credmph_163N001V01['SVDR_Final_sum'].iloc[0]:,.2f} (No adjustment - only one special product found)")
        elif not credmph_163N001V02.empty:
            print(f"163N001V02 - CREDMPH002: {credmph_163N001V02['SVDR_Final_sum'].iloc[0]:,.2f} (No adjustment - only one special product found)")
        else:
            print("No special CREDMPH002 products found in the data")
        print("=" * 50 + "\n")

        output_file = f"{results_dir}/Net SVDR_{current_scenario}.xlsx"
        with pd.ExcelWriter(output_file) as writer:
            svdr_grouped.to_excel(writer, sheet_name='Net SVDR Summary', index=False)
            svdr_final_combined.to_excel(writer, sheet_name='Net SVDR Final', index=False)
        print(f"✅ Net SVDR data has been exported to {output_file}")

    # except Exception as e:
    #     print(f"❌ Error in Net SVDR Aggregation: {e}")

def color_columns(df, columns_to_color):
    """Apply background color to specified columns"""
    def highlight_cols(x):
        if x.name in columns_to_color:
            return ['background-color: #ffff00'] * len(x)
        return [''] * len(x)
    return df.style.apply(highlight_cols, axis=0)

def color_blank_columns(styler, dict_new_headers, color="#ffff00"):
    """Apply background color to columns with blank/whitespace names AND columns with headers in dict_new_headers values"""
    # Get all column headers that should be colored from the dictionary values
    headers_to_color = list(dict_new_headers.values())
    
    for i, col in enumerate(styler.columns):
        # Color blank columns
        if col.strip() == "":  
            styler = styler.set_properties(subset=[col], **{'background-color': color})
            
            # Color the next column's HEADER if it exists
            if i + 1 < len(styler.columns):
                next_col = styler.columns[i + 1]
                styler = styler.set_table_styles([
                    {'selector': f'th.col_heading.level0.col{i+1}', 
                     'props': [('background-color', color)]}
                ], overwrite=False)
        
        # Color columns whose headers are in the dictionary values
        if col in headers_to_color:
            styler = styler.set_properties(subset=[col], **{'background-color': color})
    
    return styler

################################################################################
def run_valuation(user_name, config_path, selected_scenarios, progress_callback=None):
    """Main valuation function extracted to be callable from UI."""
    def log_msg(msg):
        if progress_callback:
            progress_callback(msg, 0, 0)
    
    try:
        config_df = pd.read_excel(config_path)
        
        # Filter configuration based on requirements
        config_df = config_df[config_df['Requirement'].isin(['Yes', '1', 1])].reset_index(drop=True)
        
        # Filter to only selected scenarios
        if selected_scenarios:
            config_df = config_df[config_df['Scenario name'].isin(selected_scenarios)].reset_index(drop=True)
        
        log_file_name = f"Run Logs/Reserve_Log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        os.makedirs(os.path.dirname(log_file_name), exist_ok=True)
        with open(log_file_name, 'a') as log_file:
            log_file.write(f"User: {user_name}\n")
            log_file.write(f"Code File: {os.path.basename(__file__)}\n")
            log_file.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write(f"Available Scenarios: {config_df['Scenario name'].tolist()}\n")
        
        total_scenarios = len(config_df)
        for scenario_idx, each_scenario in enumerate(range(total_scenarios)):
            log_msg(f"\n\n\n\nProcessing scenario {scenario_idx + 1}/{total_scenarios}...")
            
            current_scenario = config_df['Scenario name'].iloc[each_scenario]
            input_file_path = config_df['Input file path'].iloc[each_scenario] or "Valuation.csv"
            results_dir = f"Results_{config_df['Results Directory'].iloc[each_scenario]}" or 'Results'
            os.makedirs(results_dir, exist_ok=True)
            selected_date = pd.to_datetime(config_df['Date of Valuation'].iloc[each_scenario], dayfirst=True)
            Assumptions_path = config_df['Assumptions_path'].iloc[each_scenario] or 'Assumptions/BaseAssumptions.xlsx'
            
            Decrement_file_path = config_df['Decrements file path'].iloc[each_scenario] or 'Assumptions/Decrements Table.xlsx'
            lapse_file_path = config_df['Lapse file path'].iloc[each_scenario] or 'Assumptions/Lapse Table.xlsx'
            COIlevel_CashflowsRequired = int(config_df['COIlevel_CashflowsRequired'].iloc[each_scenario])
            IsAggregationRequired = int(config_df['IsAggregationRequired'].iloc[each_scenario])
            
            if IsAggregationRequired == 1:
                cashflowAggregation_type = int(config_df['CashflowAggregationType'].iloc[each_scenario])
            else:
                cashflowAggregation_type = " "
            
            runcase = config_df['Run Case'].iloc[each_scenario] or "Others"
            ProfitabilityComponentsRequired = int(config_df['ProfitabilityComponentsRequired'].iloc[each_scenario])
            
            hardcoded_grouping_columns = ["Status", "Outstanding Month"]
            additional_assumptions_groupings = pd.read_excel(Assumptions_path, sheet_name='Grouping columns')
            assumptions_grouping_columns = additional_assumptions_groupings['Column name'].tolist()
            GROUPING_COLUMNS = hardcoded_grouping_columns + assumptions_grouping_columns if assumptions_grouping_columns else hardcoded_grouping_columns
            
            print(f"\n\t\t\t\t\t\t\tCode version used: {os.path.basename(__file__)}")     
            print(f"\t\t\t\t\t\t\tRunning scenario: {current_scenario}")
            print(f"\t\t\t\t\t\t\tInput file path: {input_file_path}")    
            print(f"\t\t\t\t\t\t\tAssumptions file path: {Assumptions_path}")
            print(f"\t\t\t\t\t\t\tAre individual cashflows required: {COIlevel_CashflowsRequired}")
            print(f"\t\t\t\t\t\t\tAre profitability components required: {ProfitabilityComponentsRequired}")
            print(f"\t\t\t\t\t\t\tIs Aggregation Required: {IsAggregationRequired}")
            print(f"\t\t\t\t\t\t\tAssumptions grouping columns: {GROUPING_COLUMNS}")
            print(f"\t\t\t\t\t\t\tResults directory: {results_dir}")
            print(f"\t\t\t\t\t\t\tDate of Valuation: {selected_date.strftime('%d-%m-%Y')}")
            print(f"\t\t\t\t\t\t\tRun Case: {runcase}\n\t\t\t\t\t\t\t================================================================================")


            log_msg(f"Loading data for scenario: {current_scenario}")
            log_msg(f"\t\t\t\tInput file path: {input_file_path}")    
            log_msg(f"\t\t\t\tAssumptions file path: {Assumptions_path}")
            log_msg(f"\t\t\t\tAre individual cashflows required: {COIlevel_CashflowsRequired}")
            log_msg(f"\t\t\t\tAre profitability components required: {ProfitabilityComponentsRequired}")
            log_msg(f"\t\t\t\tIs Aggregation Required: {IsAggregationRequired}")
            log_msg(f"\t\t\t\tAssumptions grouping columns: {GROUPING_COLUMNS}")
            log_msg(f"\t\t\t\tResults directory: {results_dir}")
            log_msg(f"\t\t\t\tDate of Valuation: {selected_date.strftime('%d-%m-%Y')}")
            log_msg(f"\t\t\t\tRun Case: {runcase}\n\t\t\t\t\t\t\t================================================================================")


            df = pd.read_csv(input_file_path, low_memory=False)
            df = df[df['Issuance Date'].notna() & df['Coverage Effective Date'].notna()]
            
            valid_status_codes = {11: 'Yes', '11': 'Yes', 22: 'No', 24: 'No', 32: 'No', 33: 'Yes', '33': 'Yes', 42: 'Yes', '42': 'Yes', 43: 'No', 61: 'No'}
            df = df[df['Status'].isin([code for code, reserve in valid_status_codes.items() if reserve == 'Yes'])]
            
            output_date = dt.date.today()
            output_report_path = f"{results_dir}/Valuation-Output_{output_date}_{current_scenario}.csv"
            
            if cashflowAggregation_type == 0:
                aggregated_output_path = f"{results_dir}/Outstanding_Aggregated_Cashflows_{current_scenario}.xlsx"
            else:
                aggregated_output_path = f"{results_dir}/Complete_Aggregated_Cashflows_{current_scenario}.xlsx"
            
            skipped_policies_path = f"{results_dir}/Expired_records_{current_scenario}.csv"
            
            asum = pd.read_excel(Assumptions_path, sheet_name='main')
            additional_assumptions_groupings = pd.read_excel(Assumptions_path, sheet_name='Grouping columns')
            assumptions_grouping_columns = additional_assumptions_groupings['Column name'].tolist()
            asum.set_index('Assumptions', inplace=True)
            mortalities = pd.read_excel(Assumptions_path, sheet_name='mortalities')
            GS_loading = pd.read_excel(Assumptions_path, sheet_name='GS loading')
            GS_data = pd.read_excel(Assumptions_path, sheet_name='GS_data')
            vri = pd.read_excel(Assumptions_path, sheet_name='vri')
            lapse_table = pd.read_excel(lapse_file_path)
            lapse_table.set_index('Year', inplace=True)
            
            # Load decrement data
            IALM = pd.read_excel(Decrement_file_path, sheet_name='IALM1214')
            ADB = pd.read_excel(Decrement_file_path, sheet_name='ADB')
            ATPD = pd.read_excel(Decrement_file_path, sheet_name='ATPD')
            
            log_msg(f"Total records to process: {len(df):,}")
            
            # Process data in parallel
            dfbegin = time.time()
            start_time = datetime.now()
            
            num_processes = max(1, mp.cpu_count() - 1)
            total_records = len(df)
            log_msg(f"Using {num_processes} processes for parallel computation")
            
            chunk_size = len(df) // num_processes + 1
            chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
            
            all_skipped = []
            all_reasons = []
            output_files = []
            aggregated_files = []
            
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                future_to_chunk = {
                    executor.submit(process_chunk, chunk, i, selected_date, asum, GS_data, vri, IALM, ADB, ATPD, GS_loading, lapse_table, mortalities, results_dir, COIlevel_CashflowsRequired, ProfitabilityComponentsRequired, cashflowAggregation_type, IsAggregationRequired, GROUPING_COLUMNS, runcase): i 
                    for i, chunk in enumerate(chunks)
                }
                
                log_msg(f"Submitted {len(chunks)} chunks for processing...")
                processed_chunks = 0
                
                for future in as_completed(future_to_chunk):
                    chunk_id = future_to_chunk[future]
                    try:
                        chunk_output_path, chunk_agg_path, chunk_skipped, chunk_reason = future.result()
                        output_files.append(chunk_output_path)
                        aggregated_files.append(chunk_agg_path)
                        all_skipped.extend(chunk_skipped)
                        all_reasons.extend(chunk_reason)
                        
                        processed_chunks += 1
                        progress_percent = (processed_chunks / len(chunks)) * 100
                        log_msg(f"Progress: {processed_chunks}/{len(chunks)} chunks ({progress_percent:.1f}%)")
                    except Exception as e:
                        log_msg(f"Error processing chunk {chunk_id + 1}: {e}")
                        processed_chunks += 1
            
            # Combine output files
            log_msg("Combining output files...")
            resultsdf = combine_csv_files(output_files, output_report_path, results_dir, current_scenario)
            
            # Combine aggregated files
            if aggregated_files:
                log_msg("Combining aggregated files...")
                try:
                    aggregated_dfs = []
                    for agg_file in aggregated_files:
                        if os.path.exists(agg_file) and os.path.getsize(agg_file) > 0:
                            agg_df = pd.read_csv(agg_file)
                            if not agg_df.empty:
                                aggregated_dfs.append(agg_df)
                            os.remove(agg_file)
                    
                    if aggregated_dfs:
                        aggregated_final = pd.concat(aggregated_dfs, ignore_index=True)
                        numeric_cols = [col for col in aggregated_final.columns 
                                      if col not in GROUPING_COLUMNS and 
                                      pd.api.types.is_numeric_dtype(aggregated_final[col])]
                        aggregated_final = aggregated_final.groupby(GROUPING_COLUMNS, as_index=False)[numeric_cols].sum()
                        aggregated_final.sort_values(GROUPING_COLUMNS, inplace=True)
                        aggregated_final.to_excel(aggregated_output_path, index=False)
                        log_msg(f"Aggregated output saved to: {aggregated_output_path}")
                except Exception as e:
                    log_msg(f"Error combining aggregated files: {e}")
            
            # Write skipped policies
            if all_skipped:
                skipped_df = pd.DataFrame({'COI Number': all_skipped, 'Reason': all_reasons})
                skipped_df.to_csv(skipped_policies_path, index=False)
                log_msg(f"Skipped {len(all_skipped):,} policies")
            
            total_time_minutes = (time.time() - dfbegin) / 60
            log_msg(f"Scenario completed in {total_time_minutes:.2f} minutes")
        
        log_msg("✅ All scenarios completed successfully!")
        
    except Exception as e:
        log_msg(f"❌ Error in valuation: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    mp.freeze_support()

    # Try to launch UI, fallback to CLI if not available
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
        import threading
        import platform

        class ModernValuationUI:
            def __init__(self, root):
                self.root = root
                self.root.title("CPVM - CALI Python Valuation Model")
                self.root.geometry("1150x850")
                self.root.minsize(1050, 750)
                
                self.scenario_logs = {}
                self.current_scen = None
                
                # Professional Color Palette
                self.colors = {
                    "primary": "#1e3a8a",    # Deep Blue
                    "secondary": "#3b82f6",  # Blue
                    "bg_light": "#f8fafc",   # Slate 50
                    "bg_dark": "#1e293b",    # Slate 800
                    "card": "#ffffff",       # White
                    "border": "#e2e8f0",     # Slate 200
                    "text_main": "#0f172a",  # Slate 900
                    "text_sub": "#64748b",   # Slate 500
                    "success": "#10b981",    # Emerald 500
                    "error": "#ef4444",      # Red 500
                    "info": "#0ea5e9"        # Sky 500
                }
                
                self.setup_styles()
                self.create_widgets()
                self.log_header()
                
            def setup_styles(self):
                style = ttk.Style()
                style.theme_use('clam')
                
                # Frame Styles
                style.configure("Main.TFrame", background=self.colors["bg_light"])
                style.configure("Card.TFrame", background=self.colors["card"], relief="flat")
                style.configure("Header.TFrame", background=self.colors["primary"])
                style.configure("Sidebar.TFrame", background=self.colors["bg_dark"])
                
                # Label Styles
                style.configure("TLabel", background=self.colors["bg_light"], foreground=self.colors["text_main"], font=("Segoe UI", 10))
                style.configure("Card.TLabel", background=self.colors["card"], foreground=self.colors["text_main"], font=("Segoe UI", 10))
                style.configure("Title.TLabel", background=self.colors["primary"], foreground="white", font=("Segoe UI", 14, "bold"))
                style.configure("Heading.TLabel", background=self.colors["card"], foreground=self.colors["primary"], font=("Segoe UI", 12, "bold"))
                style.configure("Stat.TLabel", background=self.colors["bg_dark"], foreground="#cbd5e1", font=("Segoe UI", 9))
                
                # Button Styles
                style.configure("Action.TButton", font=("Segoe UI", 10, "bold"))
                style.configure("Secondary.TButton", font=("Segoe UI", 10))
                
                # Progressbar
                style.configure("Custom.Horizontal.TProgressbar", thickness=10, troughcolor=self.colors["border"], background=self.colors["secondary"])

            def create_widgets(self):
                # Header
                header = ttk.Frame(self.root, style="Header.TFrame", height=60)
                header.pack(side=tk.TOP, fill=tk.X)
                header.pack_propagate(False)
                
                ttk.Label(header, text="  📊 CreditAccess Life Insurance - CPVM", style="Title.TLabel").pack(side=tk.LEFT, padx=20, pady=15)
                
                # Main Container
                main_container = ttk.Frame(self.root, style="Main.TFrame")
                main_container.pack(fill=tk.BOTH, expand=True)
                
                # Sidebar (System Info & Scenarios)
                sidebar = ttk.Frame(main_container, style="Sidebar.TFrame", width=300)
                sidebar.pack(side=tk.LEFT, fill=tk.Y)
                sidebar.pack_propagate(False)
                
                self.create_sidebar_content(sidebar)
                
                # Content Area
                content = ttk.Frame(main_container, style="Main.TFrame", padding=25)
                content.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                
                # Top Controls Row (Config & Execution side-by-side)
                controls_row = ttk.Frame(content, style="Main.TFrame")
                controls_row.pack(fill=tk.X, pady=(0, 20))
                
                # Configuration Card (Left)
                config_card = ttk.Frame(controls_row, style="Card.TFrame", padding=15, borderwidth=1, relief="solid")
                config_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
                
                ttk.Label(config_card, text="Run Configuration", style="Heading.TLabel").pack(anchor=tk.W, pady=(0, 15))
                
                grid_frame = ttk.Frame(config_card, style="Card.TFrame")
                grid_frame.pack(fill=tk.X)
                grid_frame.columnconfigure(1, weight=1)
                
                ttk.Label(grid_frame, text="Name:", style="Card.TLabel").grid(row=0, column=0, sticky=tk.W, pady=5)
                self.user_name_var = tk.StringVar()
                self.user_name_entry = ttk.Entry(grid_frame, textvariable=self.user_name_var)
                self.user_name_entry.grid(row=0, column=1, columnspan=2, sticky=tk.EW, padx=(10, 0), pady=5)
                
                ttk.Label(grid_frame, text="Config File:", style="Card.TLabel").grid(row=1, column=0, sticky=tk.W, pady=5)
                self.config_path_var = tk.StringVar()
                self.config_entry = ttk.Entry(grid_frame, textvariable=self.config_path_var)
                self.config_entry.grid(row=1, column=1, sticky=tk.EW, padx=(10, 5), pady=5)
                ttk.Button(grid_frame, text="📁", command=self.browse_config, width=3).grid(row=1, column=2, sticky=tk.E, pady=5)
                
                ttk.Button(config_card, text="🔍 Load Scenarios", command=self.load_scenarios, style="Action.TButton").pack(fill=tk.X, pady=(15, 0))
                
                # Execution Card (Right)
                exec_card = ttk.Frame(controls_row, style="Card.TFrame", padding=15, borderwidth=1, relief="solid")
                exec_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0))
                
                ttk.Label(exec_card, text="Engine Control & Progress", style="Heading.TLabel").pack(anchor=tk.W, pady=(0, 5))
                
                ctrl_inner = ttk.Frame(exec_card, style="Card.TFrame")
                ctrl_inner.pack(fill=tk.X, pady=5)
                
                self.run_btn = ttk.Button(ctrl_inner, text="🚀 Run", command=self.run_scenarios, style="Action.TButton")
                self.run_btn.pack(side=tk.LEFT, padx=(0, 5))
                
                ttk.Button(ctrl_inner, text="🧹 Clear", command=self.clear_log).pack(side=tk.LEFT, padx=5)
                ttk.Button(ctrl_inner, text="⏹ Stop", command=self.root.quit).pack(side=tk.LEFT, padx=5)
                
                prog_container = ttk.Frame(exec_card, style="Card.TFrame")
                prog_container.pack(fill=tk.X, pady=(10, 0))
                
                self.progress_msg = ttk.Label(prog_container, text="Status: Idle", style="Card.TLabel")
                self.progress_msg.pack(anchor=tk.W)
                
                self.progress_var = tk.DoubleVar()
                self.progress_bar = ttk.Progressbar(prog_container, variable=self.progress_var, maximum=100, style="Custom.Horizontal.TProgressbar")
                self.progress_bar.pack(fill=tk.X, pady=(5, 0))
                
                # Bottom Row: Logs
                log_card = ttk.Frame(content, style="Card.TFrame", padding=20, borderwidth=1, relief="solid")
                log_card.pack(fill=tk.BOTH, expand=True, pady=(20, 0))
                
                ttk.Label(log_card, text="Detailed Process Logs", style="Heading.TLabel").pack(anchor=tk.W, pady=(0, 10))
                
                self.status_text = tk.Text(
                    log_card, 
                    height=8, 
                    font=("Consolas", 10), 
                    bg="#fafafa", 
                    fg="#334155",
                    relief="flat",
                    padx=10,
                    pady=10
                )
                self.status_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                
                log_sb = ttk.Scrollbar(log_card, orient=tk.VERTICAL, command=self.status_text.yview)
                log_sb.pack(side=tk.RIGHT, fill=tk.Y)
                self.status_text.config(yscrollcommand=log_sb.set)
                
                self.status_text.tag_configure("success", foreground=self.colors["success"])
                self.status_text.tag_configure("error", foreground=self.colors["error"])
                self.status_text.tag_configure("info", foreground=self.colors["info"])
                self.status_text.tag_configure("bold", font=("Consolas", 10, "bold"))

            def create_sidebar_content(self, parent):
                ttk.Label(parent, text="SYSTEM DIAGNOSTICS", style="Stat.TLabel", font=("Segoe UI", 10, "bold")).pack(pady=(30, 20), padx=20, anchor=tk.W)
                
                # CPU Info
                cpu_count = os.cpu_count()
                self.add_stat(parent, "Processors", f"{cpu_count} Cores")
                
                # OS Info
                os_info = f"{platform.system()} {platform.release()}"
                self.add_stat(parent, "OS Platform", os_info[:20])
                
                # Runtime status indicator
                ttk.Label(parent, text="ENGINE STATUS", style="Stat.TLabel", font=("Segoe UI", 10, "bold")).pack(pady=(40, 10), padx=20, anchor=tk.W)
                
                status_frame = ttk.Frame(parent, style="Sidebar.TFrame")
                status_frame.pack(fill=tk.X, padx=20)
                
                self.status_indicator = tk.Canvas(status_frame, width=15, height=15, bg=self.colors["bg_dark"], highlightthickness=0)
                self.status_indicator.pack(side=tk.LEFT, pady=5)
                self.status_dot = self.status_indicator.create_oval(2, 2, 13, 13, fill="#94a3b8")
                
                self.engine_status_lbl = ttk.Label(status_frame, text="Standby", style="Stat.TLabel")
                self.engine_status_lbl.pack(side=tk.LEFT, padx=10)
                
                # Queued Scenarios section
                ttk.Label(parent, text="QUEUED SCENARIOS", style="Stat.TLabel", font=("Segoe UI", 10, "bold")).pack(pady=(40, 10), padx=20, anchor=tk.W)
                
                list_container = ttk.Frame(parent, style="Sidebar.TFrame")
                list_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=5)
                
                self.scenario_listbox = tk.Listbox(
                    list_container, 
                    font=("Segoe UI", 9), 
                    bg=self.colors["bg_dark"], 
                    fg="#cbd5e1",
                    borderwidth=0,
                    highlightthickness=0,
                    selectbackground=self.colors["secondary"],
                    activestyle='none'
                )
                self.scenario_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                self.scenario_listbox.bind("<Double-1>", self.on_scenario_click)
                
                sb = ttk.Scrollbar(list_container, orient=tk.VERTICAL, command=self.scenario_listbox.yview)
                sb.pack(side=tk.RIGHT, fill=tk.Y)
                self.scenario_listbox.config(yscrollcommand=sb.set)

            def add_stat(self, parent, label, value):
                f = ttk.Frame(parent, style="Sidebar.TFrame")
                f.pack(fill=tk.X, padx=20, pady=5)
                ttk.Label(f, text=f"{label}:", style="Stat.TLabel", foreground="#94a3b8").pack(side=tk.LEFT)
                ttk.Label(f, text=value, style="Stat.TLabel", font=("Segoe UI", 9, "bold")).pack(side=tk.RIGHT)

            def set_status(self, mode):
                if mode == "running":
                    self.status_indicator.itemconfig(self.status_dot, fill=self.colors["success"])
                    self.engine_status_lbl.config(text="Processing...")
                elif mode == "error":
                    self.status_indicator.itemconfig(self.status_dot, fill=self.colors["error"])
                    self.engine_status_lbl.config(text="Halted")
                else:
                    self.status_indicator.itemconfig(self.status_dot, fill="#94a3b8")
                    self.engine_status_lbl.config(text="Standby")

            def browse_config(self):
                file_path = filedialog.askopenfilename(
                    title="Open Engine Configuration",
                    filetypes=[("Excel Files", "*.xlsx"), ("Modern Excel", "*.xlsm"), ("All Files", "*.*")]
                )
                if file_path:
                    self.config_path_var.set(file_path)
                    self.log_message(f"📂 Loaded configuration: {os.path.basename(file_path)}", "info")

            def load_scenarios(self):
                config_path = self.config_path_var.get()
                if not config_path:
                    messagebox.showwarning("Incomplete Setup", "Please specify a configuration file path.")
                    return
                
                try:
                    config_df = pd.read_excel(config_path)
                    config_df = config_df[config_df['Requirement'].isin(['Yes', '1', 1])].reset_index(drop=True)
                    scenarios = config_df['Scenario name'].tolist()
                    
                    self.scenario_listbox.delete(0, tk.END)
                    for i, s in enumerate(scenarios):
                        self.scenario_listbox.insert(tk.END, f"{i+1}. {s}")
                    
                    self.log_message(f"✅ Successfully extracted {len(scenarios)} active scenarios from registry", "success")
                except Exception as e:
                    self.log_message(f"❌ Registry Error: {str(e)}", "error")
                    messagebox.showerror("Registry Error", f"Failed to parse configuration: {str(e)}")

            def run_scenarios(self):
                if not self.user_name_var.get():
                    messagebox.showwarning("User Identity Missing", "Please enter your Name before execution.")
                    return
                if not self.config_path_var.get():
                    messagebox.showwarning("Missing Config", "Please select a valid configuration file.")
                    return
                    
                self.run_btn.config(state=tk.DISABLED)
                self.set_status("running")
                self.log_message("🚀 Initializing Valuation Engine Pipeline...", "bold")
                
                thread = threading.Thread(target=self.work_thread)
                thread.daemon = True
                thread.start()

            def work_thread(self):
                try:
                    self.progress_var.set(0)
                    user = self.user_name_var.get()
                    cfg = self.config_path_var.get()
                    
                    config_df = pd.read_excel(cfg)
                    config_df = config_df[config_df['Requirement'].isin(['Yes', '1', 1])].reset_index(drop=True)
                    all_scenarios = config_df['Scenario name'].tolist()
                    
                    def ui_callback(msg, curr, total):
                        timestamp = datetime.now().strftime("[%H:%M:%S] ")
                        
                        # Detect scenario start to associate logs
                        if "Loading data for scenario:" in msg:
                            self.current_scen = msg.split("scenario:")[1].strip()
                            if self.current_scen not in self.scenario_logs:
                                self.scenario_logs[self.current_scen] = []
                        
                        # Save log line to specific scenario
                        if self.current_scen:
                            self.scenario_logs[self.current_scen].append(f"{timestamp} {msg}")

                        self.log_message(msg)
                        if "completed" in msg.lower() or "success" in msg.lower():
                            self.log_strip(msg, "success")
                        if "error" in msg.lower() or "failed" in msg.lower():
                            self.log_strip(msg, "error")
                        
                        if total > 0:
                            p = (curr / total) * 100
                            self.progress_var.set(p)
                        self.progress_msg.config(text=f"Engine Status: {msg[:60]}...")
                        self.root.update_idletasks()

                    run_valuation(user, cfg, all_scenarios, ui_callback)
                    
                    self.progress_var.set(100)
                    self.progress_msg.config(text="Engine Status: Task Complete")
                    self.set_status("idle")
                    self.log_message("✨ Pipeline Execution Synchronized & Completed", "success")
                    messagebox.showinfo("Success", "Valuation run completed successfully!")
                except Exception as e:
                    self.log_message(f"⛔ Critical Pipeline Failure: {str(e)}", "error")
                    self.set_status("error")
                    messagebox.showerror("Error", f"An error occurred: {str(e)}")
                finally:
                    self.run_btn.config(state=tk.NORMAL)

            def log_message(self, message, tag=None):
                timestamp = datetime.now().strftime("[%H:%M:%S] ")
                self.status_text.insert(tk.END, timestamp, "bold")
                self.status_text.insert(tk.END, f"{message}\n", tag)
                self.status_text.see(tk.END)

            def log_strip(self, msg, tag):
                pass
                
            def log_header(self):
                self.status_text.insert(tk.END, "╔══════════════════════════════════════════════════════╗\n", "info")
                self.status_text.insert(tk.END, "║       VALUATION & PROFITABILITY ENGINE               ║\n", "info")
                self.status_text.insert(tk.END, "╚══════════════════════════════════════════════════════╝\n", "info")
                self.status_text.insert(tk.END, f"SYSTEM READY. Waiting for configuration...\n\n")

            def clear_log(self):
                self.status_text.delete(1.0, tk.END)
                self.scenario_logs = {}
                self.log_header()

            def on_scenario_click(self, event):
                selection = self.scenario_listbox.curselection()
                if not selection: return
                
                item_text = self.scenario_listbox.get(selection[0])
                # Extract scenario name from the "1. Name" format
                scen_name = item_text.split(". ", 1)[1] if ". " in item_text else item_text
                
                if scen_name in self.scenario_logs:
                    self.show_log_popup(scen_name)
                else:
                    messagebox.showinfo("Log Pending", f"Detailed logs for '{scen_name}' will be available once it starts processing.")

            def show_log_popup(self, scen_name):
                popup = tk.Toplevel(self.root)
                popup.title(f"Scenario Log: {scen_name}")
                popup.geometry("900x600")
                popup.configure(bg="#1e293b")
                
                # Header in popup
                p_header = ttk.Frame(popup, style="Header.TFrame", height=40)
                p_header.pack(fill=tk.X)
                ttk.Label(p_header, text=f"  Execution Details: {scen_name}", style="Title.TLabel", font=("Segoe UI", 11, "bold")).pack(pady=10)
                
                # Log Area
                log_frame = ttk.Frame(popup, style="Main.TFrame", padding=10)
                log_frame.pack(fill=tk.BOTH, expand=True)
                
                p_text = tk.Text(
                    log_frame, 
                    bg="#1e1e1e", 
                    fg="#e2e8f0", 
                    font=("Consolas", 10), 
                    padx=15, 
                    pady=15,
                    relief="flat"
                )
                p_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                
                p_sb = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=p_text.yview)
                p_sb.pack(side=tk.RIGHT, fill=tk.Y)
                p_text.config(yscrollcommand=p_sb.set)
                
                # Insert gathered logs
                for line in self.scenario_logs[scen_name]:
                    p_text.insert(tk.END, f"{line}\n")
                
                p_text.config(state=tk.DISABLED)

        root = tk.Tk()
        app = ModernValuationUI(root)
        root.mainloop()
    
    except ImportError:
        # Fallback to command-line interface
        print("\n\n================= Welcome to the Reserve Calculation Module =================\n\n")
        
        user_name = input("Please enter your name: ")
        print(f"Hello, {user_name}! Let's get started with the Reserve Calculation.")
        print("==============================================================================\n\n")
        
        config_path = input("Please provide the path to the config file: ")
        config_df = pd.read_excel(config_path)
        
        config_df = config_df[config_df['Requirement'].isin(['Yes', '1', 1])].reset_index(drop=True)
        
        print(f"\n\n\nAvailable Scenarios in the config file: \n{config_df['Scenario name'].tolist()}\n")
        
        log_file_name = f"Run Logs/Reserve_Log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        os.makedirs(os.path.dirname(log_file_name), exist_ok=True)
        with open(log_file_name, 'a') as log_file:
            log_file.write(f"User: {user_name}\n")
            log_file.write(f"Code File: {os.path.basename(__file__)}\n")
            log_file.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write(f"Available Scenarios: {config_df['Scenario name'].tolist()}\n")
        
        all_scenarios = config_df['Scenario name'].tolist()
        print(f"\nRunning all {len(all_scenarios)} scenarios in order...\n")
        
        def cli_progress_callback(message, current, total):
            print(message)
        
        run_valuation(user_name, config_path, all_scenarios, cli_progress_callback)


