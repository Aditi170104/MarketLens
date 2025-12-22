# quantitative_extraction.py (FINAL CORRECTED - Simplified Growth)
import os
import xlwings as xw
import pandas as pd
import numpy as np

# --- Configuration ---
# NOTE: Directory name uses 31Oct to match previous successful run path
DOWNLOAD_DIRECTORY = 'StocksExportsConsolidated1Nov' 
OUTPUT_FILENAME = "Master_Quantitative_Data.xlsx"

# --- Helper Functions ---
def calculate_ttm_metrics(quarterly_values):
    """Calculates TTM1, TTM2, and Growth percentage (TTM/Prev TTM - 1) using simple math."""
    numeric_values = [pd.to_numeric(q, errors='coerce') for q in quarterly_values]
    ttm_2 = np.nansum(numeric_values[0:4]) # Previous TTM Sum
    ttm_1 = np.nansum(numeric_values[4:8]) # Latest TTM Sum
    growth = np.nan 
    
    # Simple, direct application of the formula using np.divide for safety
    if ttm_2 != 0: 
        growth = ((ttm_1 / ttm_2) - 1) * 100
    # If ttm_2 is 0, growth remains np.nan (initialized) or becomes inf/NaN if TTM1 != 0

    return ttm_1, ttm_2, growth

def calculate_eps_metrics(net_profits, equity_shares):
    """Calculates EPS values and growth using simple math."""
    equity_shares = pd.to_numeric(equity_shares, errors='coerce') if equity_shares else 0.0
    conversion_factor = 10_000_000 
    eps_values = []
    for np_value in net_profits:
        np_value = pd.to_numeric(np_value, errors='coerce')
        if not pd.isna(np_value) and equity_shares > 0:
            eps_values.append((np_value * conversion_factor) / equity_shares)
        else: eps_values.append(np.nan)
    numeric_eps = [pd.to_numeric(e, errors='coerce') for e in eps_values]
    eps_2_ttm = np.nansum(numeric_eps[0:4])
    eps_1_ttm = np.nansum(numeric_eps[4:8])
    eps_growth = np.nan
    
    if eps_2_ttm != 0: 
        eps_growth = ((eps_1_ttm / eps_2_ttm) - 1) * 100
        
    return eps_values, eps_1_ttm, eps_2_ttm, eps_growth

def calculate_opm_metrics(opm_values):
    """Calculates OPM Growth percentage (TTM Sum/Prev TTM Sum - 1) and OPM_4Q Sum using simple math."""
    numeric_opm = [pd.to_numeric(q, errors='coerce') for q in opm_values]
    opm_2_sum = np.nansum(numeric_opm[0:4]) 
    opm_1_sum = np.nansum(numeric_opm[4:8]) 
    opm_4q_sum = opm_1_sum 
    opm_growth = np.nan 
    
    if opm_2_sum != 0: 
        opm_growth = ((opm_1_sum / opm_2_sum) - 1) * 100
        
    return opm_values, opm_4q_sum, opm_growth

def calculate_annual_growth(annual_values):
    """Calculates YoY growth factor (Curr / Prev * 100) for 3 years, using simple math."""
    numeric_values = [pd.to_numeric(v, errors='coerce') for v in annual_values]
    growth_rates = []
    for i in range(len(numeric_values) - 1):
        prev = numeric_values[i]; curr = numeric_values[i+1]
        growth = np.nan
        if prev != 0: 
            growth = (curr / prev) * 100 
        growth_rates.append(growth)
    return growth_rates 

def calculate_coa_net_indicator(cfo_growth, sales_growth):
    """COA-Net Indicator: Sum of (1) if CFO Growth Factor >= 70% * Sales Growth Factor."""
    coa_net_indicator = 0
    for cfo_g, sales_g in zip(cfo_growth, sales_growth):
        cfo_g = pd.to_numeric(cfo_g, errors='coerce')
        sales_g = pd.to_numeric(sales_g, errors='coerce')
        
        # We must explicitly convert any inf (from division by zero) to NaN to ensure the comparison works
        if np.isinf(cfo_g): cfo_g = np.nan
        if np.isinf(sales_g): sales_g = np.nan

        if not pd.isna(cfo_g) and not pd.isna(sales_g):
            if cfo_g >= (0.70 * sales_g):
                 coa_net_indicator += 1
    return coa_net_indicator

# --- Main Extraction Logic ---
def main():
    if not os.path.isdir(DOWNLOAD_DIRECTORY):
        print(f"❌ Error: Folder '{DOWNLOAD_DIRECTORY}' not found. Did bulk_downloader run?")
        return
    
    output_data = []
    app = None
    
    for filename in os.listdir(DOWNLOAD_DIRECTORY):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(DOWNLOAD_DIRECTORY, filename)
            
            try:
                if app is None:
                    app = xw.App(visible=False, add_book=False)
                    
                wb = app.books.open(file_path)
                stock_data = {"File Name": filename}
                
                # A. Data Extraction (Quarterly)
                sheet_q = wb.sheets['Quarters']
                stock_data["Name"] = sheet_q.range('A1').value
                sales_q_raw = sheet_q.range('D4:K4').options(ndim=1).value
                op_q_raw = sheet_q.range('D6:K6').options(ndim=1).value
                opm_q_raw, opm_4q_sum, opm_growth = calculate_opm_metrics(sheet_q.range('D14:K14').options(ndim=1).value)
                
                sheet_data = wb.sheets['Data Sheet']
                eps_q_values, _, _, eps_growth = calculate_eps_metrics(
                    sheet_data.range('D49:K49').options(ndim=1).value, sheet_data.range('K70').value
                )
                
                # B. Calculations (Growth & Indicators)
                _, _, sales_growth = calculate_ttm_metrics(sales_q_raw)
                _, _, op_growth = calculate_ttm_metrics(op_q_raw) 
                
                sheet_pnl = wb.sheets['Profit & Loss']
                sales_yoy_growth_rates = calculate_annual_growth(sheet_pnl.range('H4:K4').options(ndim=1).value)
                
                sheet_cf = wb.sheets['Cash Flow']
                cfo_yoy_growth_rates = calculate_annual_growth(sheet_cf.range('H4:K4').options(ndim=1).value)
                
                coa_net_indicator = calculate_coa_net_indicator(cfo_yoy_growth_rates, sales_yoy_growth_rates)

                # C. Storage
                stock_data.update({f"SalesQ{i+1}": v for i, v in enumerate(sales_q_raw)})
                stock_data["Sales growth"] = sales_growth
                
                stock_data.update({f"OPMQ{i+1}": v for i, v in enumerate(opm_q_raw)}) 
                stock_data["op_avg"] = op_growth
                stock_data["OPM_growth"] = opm_growth
                stock_data["OPM_4Q"] = opm_4q_sum
                
                stock_data.update({f"EPS_Q{i+1}": v for i, v in enumerate(eps_q_values)})
                stock_data["EPSgrowth"] = eps_growth
                
                stock_data["COA-Net"] = coa_net_indicator
                stock_data["best p/e"] = sheet_pnl.range('M25').value

                output_data.append(stock_data)
            
            except Exception as e:
                print(f"🔥 Critical error processing {filename}. Error: {e}")
            finally:
                if 'wb' in locals() and wb: wb.close()
                
    if app:
        app.quit() 

    df = pd.DataFrame(output_data)
    
    # Final column filtering and renaming
    required_cols_to_keep = [
        "Name", "best p/e", "Sales growth", "op_avg", "OPM_growth", "OPM_4Q", "EPSgrowth", "COA-Net",
        *[f"SalesQ{i+1}" for i in range(8)], 
        *[f"OPMQ{i+1}" for i in range(8)], 
        *[f"EPS_Q{i+1}" for i in range(8)],
    ]
    
    df_final = df.reindex(columns=required_cols_to_keep)
    df_final.rename(columns={'Name': 'Company Name'}, inplace=True) 
    
    df_final.to_excel(OUTPUT_FILENAME, index=False)

if __name__ == "__main__":
    main()