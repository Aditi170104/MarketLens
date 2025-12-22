# pipeline_main.py (UPDATED for two score files)
import subprocess
import time
import os
import sys

# --- Configuration (UPDATE THIS BASE PATH) ---
PROJECT_ROOT = r'C:\StockModelPipeline' 
os.chdir(PROJECT_ROOT)

# --- List of scripts to run sequentially ---
SCRIPTS = [
    "initial_data_processor.py",
    "bulk_downloader.py",
    "quantitative_extraction.py",
    "quality_mngmnt.py",    # <--- NEW MANAGEMENT SCORE SCRIPT
    "transcriptions.py",    # <--- NEW GROWTH SCORE SCRIPT
    "final_database_update.py",
]

def run_script(script_name):
    """Executes a single Python script."""
    script_path = os.path.join(PROJECT_ROOT, script_name)
    print(f"\n--- Starting {script_name} ---")
    
    if not os.path.exists(script_path):
        print(f"❌ Error: Script not found at {script_path}. Skipping.")
        return False
        
    try:
        subprocess.run([sys.executable, script_path], check=True, cwd=PROJECT_ROOT)
        print(f"✅ {script_name} finished successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"🛑 {script_name} FAILED. Pipeline stopped.")
        print(f"Error details: {e}")
        return False
    except Exception as e:
        print(f"🔥 Critical error running {script_name}: {e}")
        return False

def main_pipeline():
    print("#######################################################")
    print("🚀 Starting Stock Model Automation Pipeline")
    print("#######################################################")
    
    if not os.path.exists(os.path.join(PROJECT_ROOT, 'query-results.csv')):
         print("🛑 CRITICAL: Initial data file 'query-results.csv' not found.")
         return

    for script in SCRIPTS:
        success = run_script(script)
        if not success:
            break
        time.sleep(5) 
    
    print("\n\n#######################################################")
    print("🎉 PIPELINE RUN COMPLETE. Check PostgreSQL for 'final_score' table.")
    print("#######################################################")

if __name__ == "__main__":
    main_pipeline()