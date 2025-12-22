# initial_data_processor.py (FINAL FIX)
import pandas as pd
import os
import datetime
import numpy as np

PROJECT_ROOT = os.getcwd() 
INPUT_CSV_PATH = os.path.join(PROJECT_ROOT, 'query-results.csv') 
OUTPUT_CSV_PATH = os.path.join(PROJECT_ROOT, 'Initial_Processed_Data.csv')

def process_initial_data():
    print("\n--- Running Initial Data Processor ---")
    
    try:
        df = pd.read_csv(INPUT_CSV_PATH)
    except FileNotFoundError:
        raise
    
    df.columns = df.columns.str.strip()
    df_temp = df.copy() 
    
    # 1. Add 'Date' column (FIX: Use clean format DD-MM-YYYY)
    df_temp['Date'] = datetime.date.today().strftime('%d-%m-%Y')

    # 2. Rename Q1 columns
    rename_map = {
        'FII holding': 'FII holdingQ1', 
        'Promoter holding': 'Promoter holdingQ1', 
        'DII holding': 'DII holdingQ1',
    }
    df_temp.rename(columns=rename_map, inplace=True)
    
    # 3. Calculate Holding Trend Metrics (Q2 and holgrowth)
    for holding_upper, holding_lower in zip(['FII', 'DII', 'Promoter'], ['FII', 'DII', 'promoter']):
        
        q1_col = f'{holding_upper} holdingQ1'
        chg_col_expected = f'Change in {holding_lower} holding' 
        q2_col = f'{holding_upper} holdingQ2'
        growth_col = f'{holding_upper} holgrowth'
        
        if chg_col_expected not in df_temp.columns:
            continue
            
        df_temp[q1_col] = pd.to_numeric(df_temp[q1_col], errors='coerce')
        df_temp[chg_col_expected] = pd.to_numeric(df_temp[chg_col_expected], errors='coerce')
        df_temp[q2_col] = df_temp[q1_col] - df_temp[chg_col_expected]
        
        mask = df_temp[q2_col].ne(0) & df_temp[q2_col].notna() & df_temp[q1_col].notna()
        df_temp.loc[mask, growth_col] = ((df_temp.loc[mask, q1_col] / df_temp.loc[mask, q2_col]) - 1) * 100
        df_temp.loc[~mask, growth_col] = np.nan 

    # 4. Save the processed data
    df_temp.to_csv(OUTPUT_CSV_PATH, index=False)
    
    print(f"✅ Initial data processed and saved to {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    process_initial_data()