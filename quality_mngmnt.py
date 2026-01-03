import pandas as pd
import time
import os
import re
import random
from google import genai 
from google.genai.errors import APIError 

# ==============================
# 🔑 ENTER YOUR GEMINI API KEY HERE
# ==============================
API_KEY = "API KEY"  # <-- Ensure valid

# API details
MODEL = "gemini-2.5-flash"
MAX_OUTPUT_TOKENS = 256

# ==============================
# 📘 FILE PATHS
# ==============================
INPUT_FILE = r"C:\StockModelPipeline\query-results.csv"
OUTPUT_FILE = r"C:\StockModelPipeline\query-results_mngmnt.csv"
FAILED_FILE = r"C:\StockModelPipeline\query-results_failed.csv"

# Initialize Gemini client
try:
    client = genai.Client(api_key=API_KEY)
    print("✨ Gemini Client Initialized.")
except Exception as e:
    print(f"❌ Error initializing Gemini Client: {e}. Check installation/key.")
    client = None

# ==============================
# 🔍 Function to call Gemini API
# ==============================
def get_management_quality_score(company_name: str):
    if client is None:
        return None

    prompt = (
        f"Analyze the quality of management for the Indian listed company {company_name} "
        "(listed on the NSE, India) using publicly available financial data, management commentary, "
        "governance practices, and performance indicators. Evaluate based on capital allocation, "
        "corporate governance, return consistency, debt management, promoter integrity, and strategic execution. "
        "Based on this analysis, provide only the final **Management Quality Score out of 10** "
        "(10 = exceptional, 1 = very poor). Return strictly a single numeric value between 1 and 10 "
        "with one decimal place, e.g., 7.5. Do not include any text, explanations, or symbols."
    )

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=dict(
                    temperature=0.0,
                    max_output_tokens=MAX_OUTPUT_TOKENS,
                    thinking_config=dict(thinking_budget=0, include_thoughts=False),
                ),
            )

            text_content = response.text
            if not text_content:
                print(f"❌ EMPTY response for {company_name}")
                time.sleep(10 + random.uniform(0, 5))
                continue

            match = re.search(r"(\d+(\.\d+)?)", text_content.strip())
            if match:
                score = float(match.group(1))
                if 1 <= score <= 10:
                    return score

            print(f"⚠️ Invalid score for {company_name}: {text_content}")
            return None

        except APIError as e:
            print(f"❌ API Error ({attempt+1}/3) for {company_name}: {e}")
            time.sleep(15 + random.uniform(0, 5))
        except Exception as e:
            print(f"❌ General Error ({attempt+1}/3) for {company_name}: {e}")
            time.sleep(10 + random.uniform(0, 5))

    return None

# ==============================
# 📊 MAIN EXECUTION (Resume Mode)
# ==============================
def main():
    if client is None:
        print("\n🚫 Cannot run main() due to client initialization failure.")
        return

    df = pd.read_csv(INPUT_FILE)
    if "Name" not in df.columns:
        raise ValueError("❌ CSV must have a 'Name' column.")

    df["Name"] = df["Name"].astype(str).str.strip()
    df = df[df["Name"] != ""]

    # Determine which companies are already processed
    processed_names = []
    if os.path.exists(OUTPUT_FILE):
        try:
            df_done = pd.read_csv(OUTPUT_FILE)
            processed_names = df_done["Name"].dropna().astype(str).tolist()
            print(f"📂 Found {len(processed_names)} processed companies in {OUTPUT_FILE}.")
        except Exception as e:
            print(f"⚠️ Error reading output file: {e}. Proceeding fresh.")

    # Filter only unprocessed companies (last ~250)
    df_remaining = df[~df["Name"].isin(processed_names)].reset_index(drop=True)
    print(f"➡️ {len(df_remaining)} companies remaining to process.")

    if df_remaining.empty:
        print("✅ All companies already processed!")
        return

    failed = []
    for idx, name in enumerate(df_remaining["Name"], start=1):
        total_remaining = len(df_remaining) - idx + 1
        print(f"\n({idx}/{len(df_remaining)}, {total_remaining} left) → Getting score for: {name}")

        score = get_management_quality_score(name)

        pd.DataFrame([{"Name": name, "Management Score": score}]).to_csv(
            OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False
        )

        if score is None:
            failed.append(name)

        # Delay for free-tier safety
        time.sleep(8 + random.uniform(0, 4))

    if failed:
        pd.DataFrame({"Failed_Companies": failed}).to_csv(FAILED_FILE, index=False)
        print(f"\n⚠️ Failed companies saved to {FAILED_FILE}")

    print(f"\n✅ Completed! Remaining results saved to: {OUTPUT_FILE}")

# ==============================
# 🚀 RUN
# ==============================
if __name__ == "__main__":
    main()

