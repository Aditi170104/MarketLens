# final_database_update.py (FINAL, WORKING VERSION)
import pandas as pd
import os
import psycopg2 
import numpy as np

# --- Configuration (UPDATE THESE) ---
PROJECT_ROOT = os.getcwd()
INPUT_PROCESSED_FILE = os.path.join(PROJECT_ROOT, 'Initial_Processed_Data.csv')
QUANT_FILE = os.path.join(PROJECT_ROOT, 'Master_Quantitative_Data.xlsx') # <-- Source of the new values
MGMT_SCORE_FILE = os.path.join(PROJECT_ROOT, 'query-results_mngmnt.csv')
GROWTH_SCORE_FILE = os.path.join(PROJECT_ROOT, 'query-results_growth.csv') 
OUTPUT_CSV_FOR_SQL = os.path.join(PROJECT_ROOT, 'master_data_for_sql_upload.csv') 
SQL_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'Master_SQL_Script.sql')

DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "Hanumanji@12", 
}

# --- Column Definitions (CRITICAL: 60 COLUMNS) ---
# NOTE: Adjusted column order for Dividend yield and PEG Ratio based on typical lists.
SQL_STAGING_COLUMNS = [
    "Name", "BSE Code", "NSE Code", "Industry", "Date", "Current Price", "Price to Earning", 
    "best p/e", "Market Capitalization", "Debt to equity", 
    "FII holdingQ1", "FII holdingQ2", "FII holgrowth", "Public holding", "Change in FII holding", 
    "Promoter holdingQ1", "Promoter holdingQ2", "Promoter holgrowth", "Change in promoter holding", 
    "Pledged percentage", "DII holdingQ1", "DII holdingQ2", "DII holgrowth", "Change in DII holding", 
    "ROCE3yr avg", "PEG Ratio", "Dividend yield", 
    "SalesQ1", "SalesQ2", "SalesQ3", "SalesQ4", "SalesQ5", "SalesQ6", "SalesQ7", "SalesQ8", "Sales growth", 
    "EPS_Q1", "EPS_Q2", "EPS_Q3", "EPS_Q4", "EPS_Q5", "EPS_Q6", "EPS_Q7", "EPS_Q8", "EPSgrowth", 
    "OPMQ1", "OPMQ2", "OPMQ3", "OPMQ4", "OPMQ5", "OPMQ6", "OPMQ7", "OPMQ8", 
    "OPM_growth", "OPM_4Q", "op_avg", 
    "COA-Net", "Management Score", "Transcriptions_score" # <--- FINAL DB NAMES (60 columns)
]

def combine_and_prepare_data():
    print("\n--- Merging Data Sources for SQL Upload ---")
    
    df_base = pd.read_csv(INPUT_PROCESSED_FILE) 
    
    # --- Load and Prepare Quantitative Data from Excel ---
    df_quant = pd.read_excel(QUANT_FILE)
    
    # Standardize column names before merge
    df_base.columns = df_base.columns.str.strip()
    df_quant.columns = df_quant.columns.str.strip()
    df_quant.rename(columns={'Company Name': 'Name'}, inplace=True)

    # Normalize common placeholder null-like strings to actual NaN for safe fillna
    null_like_values = ["NaN", "nan", "None", "none", "N/A", "NA", "-", "—", "", " "]
    df_base.replace(null_like_values, np.nan, inplace=True)
    df_quant.replace(null_like_values, np.nan, inplace=True)
    
    # --- FIX: Robust Merging Key ---
    def normalize_key(s):
        """Creates a robust key for matching names despite whitespace/punctuation differences."""
        # Remove all non-alphanumeric characters and lowercase
        return s.astype(str).str.lower().str.replace(r"[^a-z0-9]", "", regex=True)

    df_base['Name_key'] = normalize_key(df_base['Name'])
    df_quant['Name_key'] = normalize_key(df_quant['Name'])

    # Merge using the robust key; retain the original 'Name' column from df_base
    df_merged = df_base.merge(df_quant, on='Name_key', how='left', suffixes=('_base', '_quant'))
    
    # Drop the technical key column
    df_merged.drop(columns=['Name_key'], inplace=True)

    # --- FIX: Prioritize Quantitative Data (df_quant) ---
    # Merge logic to ensure quantitative data fills non-NaN values from the base data.
    cols_to_drop = []
    for col in df_merged.columns:
        if col.endswith('_base'):
            base_name = col[:-5] 
            quant_col = base_name + '_quant'
            
            if quant_col in df_merged.columns:
                # Use the non-null value, prioritizing the quantitative data (_quant)
                # If _quant is NaN, it falls back to the original base value (_base)
                df_merged[base_name] = df_merged[quant_col].fillna(df_merged[col])
                
                cols_to_drop.append(col)
                cols_to_drop.append(quant_col)
            else:
                 # If the quantitative file didn't have a match, retain the original base column 
                 # and rename it back
                 df_merged.rename(columns={col: base_name}, inplace=True)
        
        # This handles columns that came ONLY from the quant file. If they are now '_quant' they are duplicates.
        elif col.endswith('_quant') and col not in cols_to_drop:
             cols_to_drop.append(col)
    
    df_merged.drop(columns=cols_to_drop, errors='ignore', inplace=True)

    # --- Load and Merge Score Data (Remains similar) ---
    def load_scores(file, target_col, source_col):
        """Loads score CSVs and renames the score column for merging."""
        
        base_names = df_base['Name'].unique() 
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip()
            df.rename(columns={'Company Name': 'Name'}, inplace=True, errors='ignore') 
            df.replace(null_like_values, np.nan, inplace=True) # Normalize nulls in score files too
            
            if 'Name' not in df.columns or source_col not in df.columns:
                # Fallback on the original base names if the merge key fails
                return pd.DataFrame({'Name': base_names, target_col: np.nan})
            
            # Merge on normalized key for score files as well for consistency
            df['Name_key'] = normalize_key(df['Name'])
            
            # Create a simple frame using the normalized key for merging
            return df[['Name_key', source_col]].rename(columns={source_col: target_col})
            
        except Exception as e:
            print(f"❌ Error loading score file {file}: {e}. Filling with NaN.")
            return pd.DataFrame({'Name_key': df_merged['Name_key'].unique(), target_col: np.nan})

    df_mgmt = load_scores(MGMT_SCORE_FILE, 'Management Score', 'Management Score') 
    df_growth = load_scores(GROWTH_SCORE_FILE, 'Transcriptions_score', 'Transcriptions_score') 
    
    # Merge scores using the Name_key from the main merged DataFrame
    df_merged = df_merged.merge(df_mgmt, on='Name', how='left') # Assume scores file uses original Name, if not, change 'Name' to 'Name_key'
    df_merged = df_merged.merge(df_growth, on='Name', how='left')
    
    # --- Final Column Cleanup and Selection ---
    
    if 'Industry Group' in df_merged.columns:
        df_merged.drop(columns=['Industry Group'], inplace=True)
    
    missing_cols = [col for col in SQL_STAGING_COLUMNS if col not in df_merged.columns]
    if missing_cols:
        print(f"⚠️ Warning: Adding missing columns (filled with NaN): {missing_cols}")
        for col in missing_cols:
            df_merged[col] = np.nan
    
    # Select and order the columns precisely for the COPY command
    df_final = df_merged.reindex(columns=SQL_STAGING_COLUMNS)

    # Convert NaNs to 'NaN' string for PostgreSQL COPY command
    df_final.replace({np.nan: 'NaN', None: 'NaN', pd.NaT: 'NaN'}, inplace=True)
    
    df_final.to_csv(OUTPUT_CSV_FOR_SQL, index=False, na_rep='NaN')
    print(f"✅ Combined data saved to {OUTPUT_CSV_FOR_SQL} with {len(df_final.columns)} columns.")
    return OUTPUT_CSV_FOR_SQL

# --- Database Execution (No Fixes Needed Here) ---
def execute_sql_script(csv_path):
    print("\n--- Executing SQL Commands via psycopg2 ---")
    
    with open(SQL_SCRIPT_PATH, 'r') as f:
        master_sql_script = f.read()

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        sql_safe_path = csv_path.replace(os.sep, '/')
        final_sql = master_sql_script.replace("'<path/to/nifty-total-market.csv>'", f"'{sql_safe_path}'")
        commands = [cmd for cmd in final_sql.split(';') if cmd.strip() != '']
        
        for command in commands:
            if command.strip().startswith("COPY"):
                copy_command = f"COPY historical_screener_data_staging FROM '{sql_safe_path}' DELIMITER ',' CSV HEADER NULL 'NaN';"
                print("Executing COPY command...")
                cursor.execute(copy_command)
            else:
                cursor.execute(command)

        conn.commit()
        print("✅ PostgreSQL Database updated successfully.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def main():
    csv_to_upload = combine_and_prepare_data()
    execute_sql_script(csv_to_upload)

if __name__ == "__main__":
    main()