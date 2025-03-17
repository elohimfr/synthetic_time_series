import pandas as pd
import os
import glob
from typing import Union, List


YEAR_LIST = [2006, 2007, 2008, 2009, 2010]
QUARTER_LIST = [1, 2, 3, 4]
DATA_PATH = '../datasets/FannieMae/'
AGG_PATH = DATA_PATH+'aggregate_years/'

# Columns from Fannie Mae glossary
glossary = pd.read_excel(DATA_PATH+'crt-file-layout-and-glossary_0.xlsx')
glossary = glossary[~glossary.isna().all(axis=1)] # Remove nan rows
GLOSSARY_COLS =  glossary['Field Name'].loc[~glossary['Field Name'].isnull()].reset_index(drop=True)

# Fields to be included
SELECTED_FIELDS=[
'Loan Identifier',
'Monthly Reporting Period',
'Channel',
'Original Interest Rate',
'Current Interest Rate',
'Original UPB',
'Current Actual UPB',
'Original Loan Term',
'Loan Age',
'Remaining Months To Maturity',
'Original Loan to Value Ratio (LTV)',
'Original Combined Loan to Value Ratio (CLTV)',
'Number of Borrowers',
'Debt-To-Income (DTI)',
'Borrower Credit Score at Origination',
'Co-Borrower Credit Score at Origination',
'First Time Home Buyer Indicator',
'Loan Purpose ',
'Property Type',
'Number of Units',
'Occupancy Status',
'Property State',
'Metropolitan Statistical Area (MSA)',
'Zip Code Short',
'Mortgage Insurance Percentage',
'Amortization Type',
'Prepayment Penalty Indicator',
'Interest Only Loan Indicator',
'Interest Only First Principal And Interest Payment Date',
'Months to Amortization',
'Current Loan Delinquency Status',
'Zero Balance Code',
'Zero Balance Effective Date',
'UPB at the Time of Removal',
'Total Principal Current',
'Last Paid Installment Date',
'Foreclosure Date',
'Special Eligibility Program',
'Property Valuation Method ',
'High Balance Loan Indicator ',
'Borrower Assistance Plan',
'Alternative Delinquency Resolution',
'Alternative Delinquency  Resolution Count']

def load_fanniemae_sf(path: str,
                      Y: Union[int, list]=None,
                      Q: Union[int, str]=None,
                      cols: List[str]=None,
                      selected_features: List[str]=None) -> pd.DataFrame:
    # Files to load
    if isinstance(Y, int):
        Y = [Y]  # Convert single year to list
    if isinstance(Q, int):
        Q = [Q]  # Convert single quarter to list
    files = []
    for year in Y:
        if Q == 'all':
            pattern = os.path.join(path, f"{year}Q*.csv")
            files.extend(glob.glob(pattern))
        else:
            for quarter in Q:
                pattern = os.path.join(path, f"{year}Q{quarter}.csv")
                files.extend(glob.glob(pattern))
    print("Files to load:", files)
    df = pd.concat([pd.read_csv(f, sep='|', names=cols) for f in files], ignore_index=True)

    # Select Features
    if selected_features is None:
        sf_fields = glossary[~glossary['Single-Family (SF) Loan Performance'].isna()]['Field Name'].to_list()
        df = df[sf_fields]
    else:
        df = df[selected_features]

    # Format column names
    df.rename(columns={s:'_'.join(s.strip().lower().split()).replace('-', '_') for s in selected_features}, inplace=True)

    # Time features
    df.insert(1, 'time', pd.to_datetime(df['monthly_reporting_period'], format='%m%Y'))
    df.insert(2, 'year', df.time.dt.year)
    df.drop('monthly_reporting_period', axis=1, inplace=True)

    # Rename columns
    df.rename(columns={
        'current_loan_delinquency_status':'delinquency_status',
                }, inplace=True)
    return df



def split_years(save_path: str, df: pd.DataFrame):
    ''' Split years and save each year in a separate csv file.'''
    for y in df.year.unique():
        df_year = df.loc[df.year == y]  # Filter year
        # Save path
        file_path = f'{save_path}agg_{y}.csv'
        meta_path = f'{save_path}agg_{y}_meta.csv'
        
        # Check if file exists before reading
        if os.path.exists(file_path):
            df_agg = pd.read_csv(file_path)  # Load existing data
            df_agg = pd.concat([df_agg, df_year], ignore_index=True)  # Append new data
        else:
            df_agg = df_year  # Start a new DataFrame if the file doesn’t exist

        # Save the updated DataFrame
        print(f'Saving {y}')
        df_agg.sort_values(by=['time', 'loan_identifier'])
        df_agg.to_csv(file_path, index=False)
        # Meta data
        df_meta = pd.DataFrame(df_agg['acquisition'].unique(), columns=['acquisition'])
        df_meta.to_csv(meta_path, index=False)



def aggregate_fanniemae(input_path: str, 
                        output_path: str, 
                        years: List[int], 
                        quarters: List[int], 
                        cols: pd.Series, 
                        selected_features: List[str]) -> None:
    for y in years:
        if quarters == 'all':
            quarters = [1, 2, 3, 4]
        for q in quarters:
            df = load_fanniemae_sf(input_path, Y=y, Q=q, cols=cols, selected_features=selected_features)
            acq = f'{y}Q{q}'
            df['acquisition'] = acq
            print(f'Splitting {acq}')
            split_years(output_path, df)


if __name__ == "__main__":
    print(f'Processing:')
    print(f'Years: {YEAR_LIST}')
    print(f'Quarters: {QUARTER_LIST}')
    aggregate_fanniemae(DATA_PATH, 
                        AGG_PATH, 
                        YEAR_LIST, 
                        QUARTER_LIST, 
                        cols=GLOSSARY_COLS, 
                        selected_features=SELECTED_FIELDS)


