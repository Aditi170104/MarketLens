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
API_KEY = "AIzaSyA_t8TwZH4mwG56B42uCFjNvg2q5-jFmGw"  # <-- your valid Gemini API key here

# API details
MODEL = "gemini-2.5-flash"
MAX_OUTPUT_TOKENS = 256

# ==============================
# 📘 FILE PATHS
# ==============================
INPUT_FILE = r"C:\StockModelPipeline\query-results.csv"
OUTPUT_FILE = r"C:\StockModelPipeline\query-results_growth.csv"
FAILED_FILE = r"C:\StockModelPipeline\query-results_growthfailed.csv"

# ==============================
# ⚙️ Initialize Gemini Client
# ==============================
try:
    client = genai.Client(api_key=API_KEY)
    print("✨ Gemini Client Initialized.")
except Exception as e:
    print(f"❌ Error initializing Gemini Client: {e}. Check installation/key.")
    client = None


# ==============================
# 🔍 Gemini API Function
# ==============================
def get_growth_guidance_score(company_name: str):
    """
    Get projected growth guidance score for a company between 0–10.
    """
    if client is None:
        return None

    prompt = f"""
You are an equity research analyst.

Check if the Indian listed company **{company_name}** (listed on the NSE)
has held or participated in **any investor meet, analyst call, or management interaction**
in the **current financial year (FY 2025–26)**.

If yes, analyze the management commentary or guidance from that interaction
to determine their **projected business growth outlook** — particularly on revenue, EBITDA,
and profit growth expectations.

Then, provide a **Projected Growth Guidance Score out of 10**, where:
- 10 = very strong growth guidance (>15% expected growth)
- 5 = moderate guidance (~5–10% growth)
- 1 = weak or negative guidance (<0% or no growth)

If no investor meet or analyst call is found, still infer a reasonable score
from financial performance, sentiment, or recent business trends.

Return **only the numeric score** (e.g. 8.4). Do not include any text or explanation.
"""

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

            text_content = response.text.strip() if response.text else ""
            if not text_content:
                print(f"⚠️ Empty response for {company_name}. Retrying...")
                time.sleep(10 + random.uniform(0, 5))
                continue

            # Extract a number between 0–10
            match = re.search(r"\b(?:[0-9]|10)(?:\.\d{1,2})?\b", text_content)
            if match:
                score = float(match.group(0))
                if 0 <= score <= 10:
                    return score

            print(f"⚠️ Invalid response for {company_name}: {text_content}")
            return None

        except APIError as e:
            print(f"❌ API Error ({attempt+1}/3) for {company_name}: {e}")
            time.sleep(15 + random.uniform(0, 5))
        except Exception as e:
            print(f"❌ General Error ({attempt+1}/3) for {company_name}: {e}")
            time.sleep(10 + random.uniform(0, 5))

    return None


# ==============================
# 📊 MAIN EXECUTION (ALL COMPANIES)
# ==============================
def main():
    if client is None:
        print("\n🚫 Cannot run main() due to client initialization failure.")
        return

    df = pd.read_csv(INPUT_FILE)
    if "Name" not in df.columns:
        raise ValueError("❌ CSV must have a 'Name' column.")

    df["Name"] = df["Name"].astype(str).str.strip()

    # ✅ Process ALL companies
    df_target = df.reset_index(drop=True)
    print(f"➡️ Processing all {len(df_target)} companies.\n")

    failed = []
    for idx, name in enumerate(df_target["Name"], start=1):
        print(f"\n({idx}/{len(df_target)}) → Getting score for: {name}")

        score = get_growth_guidance_score(name)

        pd.DataFrame([{"Name": name, "Transcriptions_score": score}]).to_csv(
            OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False
        )

        if score is None:
            failed.append(name)

        # Respect rate limits
        time.sleep(8 + random.uniform(0, 4))

    if failed:
        pd.DataFrame({"Failed_Companies": failed}).to_csv(FAILED_FILE, index=False)
        print(f"\n⚠️ Failed companies saved to {FAILED_FILE}")

    print(f"\n✅ Completed! Results saved to: {OUTPUT_FILE}")


# ==============================
# 🚀 RUN
# ==============================
if __name__ == "__main__":
    main()