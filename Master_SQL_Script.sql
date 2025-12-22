-- Master_SQL_Script.sql

-- 1. CLEANUP 
DROP TABLE IF EXISTS historical_screener_data;
DROP TABLE IF EXISTS historical_screener_data_staging;
DROP TABLE IF EXISTS screener_indicators;
DROP TABLE IF EXISTS indicator_weights;
DROP TABLE IF EXISTS final_score;

--------------------------------------------------------------------------------
-- 2. CREATE STAGING TABLE
--------------------------------------------------------------------------------
CREATE TABLE historical_screener_data_staging (
    "Name" TEXT, "BSE Code" TEXT, "NSE Code" TEXT, "Industry" TEXT, "Date" TEXT, 
    "Current Price" TEXT, "Price to Earning" TEXT, "best p/e" TEXT, "Market Capitalization" TEXT, 
    "Debt to equity" TEXT, "FII holdingQ1" TEXT, "FII holdingQ2" TEXT, "FII holgrowth" TEXT, 
    "Public holding" TEXT, "Change in FII holding" TEXT, "Promoter holdingQ1" TEXT, 
    "Promoter holdingQ2" TEXT, "Promoter holgrowth" TEXT, "Change in promoter holding" TEXT, 
    "Pledged percentage" TEXT, "DII holdingQ1" TEXT, "DII holdingQ2" TEXT, "DII holgrowth" TEXT, 
    "Change in DII holding" TEXT, "ROCE3yr avg" TEXT, "PEG Ratio" TEXT, 
    "SalesQ1" TEXT, "SalesQ2" TEXT, "SalesQ3" TEXT, "SalesQ4" TEXT, "SalesQ5" TEXT, "SalesQ6" TEXT, 
    "SalesQ7" TEXT, "SalesQ8" TEXT, "Sales growth" TEXT, 
    "EPS_Q1" TEXT, "EPS_Q2" TEXT, "EPS_Q3" TEXT, "EPS_Q4" TEXT, "EPS_Q5" TEXT, "EPS_Q6" TEXT, 
    "EPS_Q7" TEXT, "EPS_Q8" TEXT, "EPSgrowth" TEXT, "Dividend yield" TEXT, 
    "OPMQ1" TEXT, "OPMQ2" TEXT, "OPMQ3" TEXT, "OPMQ4" TEXT, "OPMQ5" TEXT, "OPMQ6" TEXT, 
    "OPMQ7" TEXT, "OPMQ8" TEXT, "OPM_growth" TEXT, "OPM_4Q" TEXT, "op_avg" TEXT, 
    "COA-Net" TEXT, "Management Score" TEXT, "Transcriptions_score" TEXT
);

--------------------------------------------------------------------------------
-- 3. IMPORT RAW DATA USING COPY (Path replaced by Python)
--------------------------------------------------------------------------------
COPY historical_screener_data_staging FROM '<path/to/nifty-total-market.csv>'
DELIMITER ','
CSV HEADER
NULL 'NaN'; 

--------------------------------------------------------------------------------
-- 4. CREATE FINAL TABLE (Target Table)
--------------------------------------------------------------------------------
CREATE TABLE historical_screener_data (
    stock_name TEXT, bse_code BIGINT, nse_code TEXT, industry TEXT, snapshot_date DATE,
    current_price NUMERIC, price_to_earning NUMERIC, best_p_e NUMERIC, market_capitalization NUMERIC,
    debt_to_equity NUMERIC, fii_holding_q1 NUMERIC, fii_holding_q2 NUMERIC, fii_holgrowth NUMERIC,
    public_holding NUMERIC, change_in_fii_holding NUMERIC, promoter_holding_q1 NUMERIC, promoter_holding_q2 NUMERIC,
    promoter_holgrowth NUMERIC, change_in_promoter_holding NUMERIC, pledged_percentage NUMERIC, dii_holding_q1 NUMERIC,
    dii_holding_q2 NUMERIC, dii_holgrowth NUMERIC, change_in_dii_holding NUMERIC, roce_3yr_avg NUMERIC,
    peg_ratio NUMERIC, dividend_yield NUMERIC, coa_net NUMERIC, management_score NUMERIC, transcriptions_score NUMERIC,
    
    -- Quarterly Values
    sales_q1 NUMERIC, sales_q2 NUMERIC, sales_q3 NUMERIC, sales_q4 NUMERIC, sales_q5 NUMERIC, sales_q6 NUMERIC, sales_q7 NUMERIC, sales_q8 NUMERIC, sales_growth NUMERIC, 
    eps_q1 NUMERIC, eps_q2 NUMERIC, eps_q3 NUMERIC, eps_q4 NUMERIC, eps_q5 NUMERIC, eps_q6 NUMERIC, eps_q7 NUMERIC, eps_q8 NUMERIC, eps_growth NUMERIC, 
    opm_q1 NUMERIC, opm_q2 NUMERIC, opm_q3 NUMERIC, opm_q4 NUMERIC, opm_q5 NUMERIC, opm_q6 NUMERIC, opm_q7 NUMERIC, opm_q8 NUMERIC, opm_growth NUMERIC, 
    opm_4q NUMERIC, op_avg NUMERIC
);

--------------------------------------------------------------------------------
-- 5. INSERT DATA WITH ROBUST TRANSFORMATION
--------------------------------------------------------------------------------
INSERT INTO historical_screener_data (
    stock_name, bse_code, nse_code, industry, snapshot_date, current_price, price_to_earning, best_p_e, market_capitalization, debt_to_equity, fii_holding_q1, fii_holding_q2, fii_holgrowth, public_holding, change_in_fii_holding, promoter_holding_q1, promoter_holding_q2, promoter_holgrowth, change_in_promoter_holding, pledged_percentage, dii_holding_q1, dii_holding_q2, dii_holgrowth, change_in_dii_holding, roce_3yr_avg, peg_ratio, dividend_yield, coa_net, management_score, transcriptions_score,
    sales_q1, sales_q2, sales_q3, sales_q4, sales_q5, sales_q6, sales_q7, sales_q8, sales_growth, 
    eps_q1, eps_q2, eps_q3, eps_q4, eps_q5, eps_q6, eps_q7, eps_q8, eps_growth, 
    opm_q1, opm_q2, opm_q3, opm_q4, opm_q5, opm_q6, opm_q7, opm_q8, opm_growth, opm_4q, op_avg
)
SELECT
    "Name"::TEXT, NULLIF("BSE Code", '')::BIGINT, NULLIF("NSE Code", '')::TEXT, "Industry"::TEXT, 
    TO_DATE("Date", 'DD-MM-YYYY'), NULLIF("Current Price", '')::NUMERIC, NULLIF("Price to Earning", '')::NUMERIC, 
    NULLIF("best p/e", '')::NUMERIC, NULLIF("Market Capitalization", '')::NUMERIC, NULLIF("Debt to equity", '')::NUMERIC, 
    NULLIF("FII holdingQ1", '')::NUMERIC, NULLIF("FII holdingQ2", '')::NUMERIC, NULLIF("FII holgrowth", '')::NUMERIC, 
    NULLIF("Public holding", '')::NUMERIC, NULLIF("Change in FII holding", '')::NUMERIC, NULLIF("Promoter holdingQ1", '')::NUMERIC, 
    NULLIF("Promoter holdingQ2", '')::NUMERIC, NULLIF("Promoter holgrowth", '')::NUMERIC, NULLIF("Change in promoter holding", '')::NUMERIC, 
    NULLIF("Pledged percentage", '')::NUMERIC, NULLIF("DII holdingQ1", '')::NUMERIC, NULLIF("DII holdingQ2", '')::NUMERIC, 
    NULLIF("DII holgrowth", '')::NUMERIC, NULLIF("Change in DII holding", '')::NUMERIC, NULLIF("ROCE3yr avg", '')::NUMERIC, 
    NULLIF("PEG Ratio", '')::NUMERIC, NULLIF("Dividend yield", '')::NUMERIC, NULLIF("COA-Net", '')::NUMERIC, 
    NULLIF("Management Score", '')::NUMERIC, NULLIF("Transcriptions_score", '')::NUMERIC,

    NULLIF("SalesQ1", '')::NUMERIC, NULLIF("SalesQ2", '')::NUMERIC, NULLIF("SalesQ3", '')::NUMERIC, NULLIF("SalesQ4", '')::NUMERIC, 
    NULLIF("SalesQ5", '')::NUMERIC, NULLIF("SalesQ6", '')::NUMERIC, NULLIF("SalesQ7", '')::NUMERIC, NULLIF("SalesQ8", '')::NUMERIC, 
    NULLIF("Sales growth", '')::NUMERIC, 
    
    NULLIF("EPS_Q1", '')::NUMERIC, NULLIF("EPS_Q2", '')::NUMERIC, NULLIF("EPS_Q3", '')::NUMERIC, NULLIF("EPS_Q4", '')::NUMERIC, 
    NULLIF("EPS_Q5", '')::NUMERIC, NULLIF("EPS_Q6", '')::NUMERIC, NULLIF("EPS_Q7", '')::NUMERIC, NULLIF("EPS_Q8", '')::NUMERIC, 
    NULLIF("EPSgrowth", '')::NUMERIC, 
    
    NULLIF("OPMQ1", '')::NUMERIC, NULLIF("OPMQ2", '')::NUMERIC, NULLIF("OPMQ3", '')::NUMERIC, NULLIF("OPMQ4", '')::NUMERIC, 
    NULLIF("OPMQ5", '')::NUMERIC, NULLIF("OPMQ6", '')::NUMERIC, NULLIF("OPMQ7", '')::NUMERIC, NULLIF("OPMQ8", '')::NUMERIC, 
    NULLIF("OPM_growth", '')::NUMERIC, NULLIF("OPM_4Q", '')::NUMERIC, NULLIF("op_avg", '')::NUMERIC

FROM historical_screener_data_staging;

--------------------------------------------------------------------------------
-- 6. CLEAN UP STAGING TABLE
--------------------------------------------------------------------------------
DROP TABLE historical_screener_data_staging;

-- F. Drop and Create the screener_indicators table
DROP TABLE IF EXISTS screener_indicators;

CREATE TABLE screener_indicators (
    nse_code TEXT NOT NULL, name VARCHAR(255), industry VARCHAR(255), data_date DATE NOT NULL,
    d_to_e_indicator SMALLINT, ph_indicator SMALLINT, ph_trend_indicator SMALLINT, dii_trend_indicator SMALLINT,
    fii_trend_indicator SMALLINT, peg_indicator SMALLINT, roce_indicator SMALLINT, sales_growth_indicator SMALLINT,
    eps_growth_indicator SMALLINT, div_yield_indicator SMALLINT, opm_trend_indicator SMALLINT,
    op_avggrowth_indicator SMALLINT, best_p_e_indicator SMALLINT, coa_net_indicator SMALLINT,
    management_score_indicator SMALLINT, transcriptions_indicator SMALLINT,

    PRIMARY KEY (nse_code, data_date)
);

-- G. Final Indicator Calculation and Population
WITH LatestData AS (
    SELECT
        h.nse_code, h.stock_name AS name, h.industry, h.snapshot_date AS data_date,
        h.debt_to_equity, h.promoter_holding_q2 AS promoter_holding, h.change_in_promoter_holding,
        h.dii_holding_q2 AS dii_holding, h.change_in_dii_holding, h.fii_holding_q2 AS fii_holding, 
        h.change_in_fii_holding, h.price_to_earning, h.best_p_e, h.peg_ratio, h.roce_3yr_avg AS roce3yr_avg,
        h.sales_growth, h.eps_growth, h.dividend_yield, h.opm_q1, h.opm_q2, h.opm_q3, h.opm_q4, 
        h.opm_q5, h.opm_q6, h.opm_q7, h.opm_q8, h.opm_4q, h.op_avg,
        h.management_score AS management_score_numeric,
        h.transcriptions_score AS transcriptions_score_numeric,
        h.coa_net AS coa_net_numeric
    FROM historical_screener_data h
)
INSERT INTO screener_indicators (
    nse_code, name, industry, data_date, d_to_e_indicator, ph_indicator, ph_trend_indicator, dii_trend_indicator,
    fii_trend_indicator, peg_indicator, roce_indicator, sales_growth_indicator, eps_growth_indicator, 
    div_yield_indicator, opm_trend_indicator, op_avggrowth_indicator, best_p_e_indicator, coa_net_indicator,
    management_score_indicator, transcriptions_indicator
)
SELECT
    h.nse_code, h.name, h.industry, h.data_date,
    -- Debt to Equity
    CASE
        WHEN h.industry IN ('NBFC', 'Banks', 'Finance', 'Financial Technology (Fintech)', 'Insurance', 'Capital Markets')
            THEN NULL
        WHEN h.debt_to_equity < 0.5 THEN 1
        ELSE 0
    END AS d_to_e_indicator,
    -- Promoter Holding
    CASE WHEN h.promoter_holding > 0.33 OR h.promoter_holding = 0 THEN 1 ELSE 0 END AS ph_indicator,
    -- Promoter Holding Trend
    CASE WHEN h.change_in_promoter_holding >= (-0.05 * h.promoter_holding) THEN 1 ELSE 0 END AS ph_trend_indicator,
    -- DII Trend 
    CASE WHEN h.change_in_dii_holding >= (-0.05 * h.dii_holding) THEN 1 ELSE 0 END AS dii_trend_indicator,
    -- FII Trend 
    CASE WHEN h.change_in_fii_holding >= (-0.05 * h.fii_holding) THEN 1 ELSE 0 END AS fii_trend_indicator,
    -- PEG Ratio
    CASE WHEN h.peg_ratio < 2.0 AND h.peg_ratio IS NOT NULL THEN 1 ELSE 0 END AS peg_indicator,
    -- ROCE
    CASE WHEN h.roce3yr_avg >= 15 THEN 1 ELSE 0 END AS roce_indicator,
    -- Sales Growth
    CASE WHEN h.sales_growth > 110 THEN 1 ELSE 0 END AS sales_growth_indicator,
    -- EPS Growth
    CASE WHEN h.eps_growth > 10 THEN 1 ELSE 0 END AS eps_growth_indicator,
    -- Dividend Yield
    CASE WHEN h.dividend_yield > 2 THEN 1 ELSE 0 END AS div_yield_indicator,
    -- OPM Trend
    CASE
        WHEN (h.opm_q2 >= h.opm_q1 AND h.opm_q3 >= h.opm_q2 AND h.opm_q4 >= h.opm_q3 AND
              h.opm_q5 >= h.opm_q4 AND h.opm_q6 >= h.opm_q5 AND h.opm_q7 >= h.opm_q6 AND
              h.opm_q8 >= h.opm_q7) AND (h.opm_4q > 40)
        THEN 1
        ELSE 0
    END AS opm_trend_indicator,
    -- Operating Profit Average Growth 
    CASE WHEN h.op_avg > 110 THEN 1 ELSE 0 END AS op_avggrowth_indicator,
    -- P/E vs Best P/E
    CASE WHEN h.price_to_earning < (h.best_p_e * 0.80) THEN 1 ELSE 0 END AS best_p_e_indicator,
    -- COA Net
    CASE
        WHEN h.industry IN ('NBFC', 'Banks', 'Finance', 'Financial Technology (Fintech)', 'Insurance', 'Capital Markets')
            THEN NULL
        WHEN h.coa_net_numeric >= 2 THEN 1
        ELSE 0
    END AS coa_net_indicator,
    -- Management Score
    CASE WHEN h.management_score_numeric > 6 THEN 1 ELSE 0 END AS management_score_indicator,
    -- Transcriptions Score
    CASE WHEN h.transcriptions_score_numeric > 6 THEN 1 ELSE 0 END AS transcriptions_indicator
FROM LatestData h
ON CONFLICT (nse_code, data_date) DO UPDATE SET
    name = EXCLUDED.name, industry = EXCLUDED.industry, d_to_e_indicator = EXCLUDED.d_to_e_indicator, 
    ph_indicator = EXCLUDED.ph_indicator, ph_trend_indicator = EXCLUDED.ph_trend_indicator, 
    dii_trend_indicator = EXCLUDED.dii_trend_indicator, fii_trend_indicator = EXCLUDED.fii_trend_indicator, 
    peg_indicator = EXCLUDED.peg_indicator, roce_indicator = EXCLUDED.roce_indicator, 
    sales_growth_indicator = EXCLUDED.sales_growth_indicator, eps_growth_indicator = EXCLUDED.eps_growth_indicator, 
    div_yield_indicator = EXCLUDED.div_yield_indicator, opm_trend_indicator = EXCLUDED.opm_trend_indicator, 
    op_avggrowth_indicator = EXCLUDED.op_avggrowth_indicator, best_p_e_indicator = EXCLUDED.best_p_e_indicator, 
    coa_net_indicator = EXCLUDED.coa_net_indicator, management_score_indicator = EXCLUDED.management_score_indicator, 
    transcriptions_indicator = EXCLUDED.transcriptions_indicator;

-- Create the indicator_weights table
CREATE TABLE indicator_weights (
    name TEXT NOT NULL, valid_from DATE NOT NULL, valid_upto DATE, weight DOUBLE PRECISION NOT NULL
);

-- Insert the indicator weights
INSERT INTO indicator_weights (name, valid_from, valid_upto, weight) VALUES
('Debt to Equity', '2025-10-15', NULL, 0.06),
('ROCE (last 3 years)', '2025-10-15', NULL, 0.11),
('Cash from Operating Activity Trend', '2025-10-15', NULL, 0.07),
('Price to Earning', '2025-10-15', NULL, 0.05),
('PEG Ratio', '2025-10-15', NULL, 0.06),
('Management Quality', '2025-10-15', NULL, 0.11),
('High Dividend Yield%', '2025-10-15', NULL, 0.04),
('Promoter Holding', '2025-10-15', NULL, 0.05),
('Promoter Holding Trend', '2025-10-15', NULL, 0.08),
('DII Holding Trend', '2025-10-15', NULL, 0.06),
('FII Holding Trend', '2025-10-15', NULL, 0.06),
('Sales Trend', '2025-10-15', NULL, 0.05),
('Operating Profit Trend', '2025-10-15', NULL, 0.04),
('EPS Trend', '2025-10-15', NULL, 0.06),
('OPM Trend', '2025-10-15', NULL, 0.06),
('Transcriptions', '2025-10-15', NULL, 0.04);

-- Drop existing final_score table if it exists
DROP TABLE IF EXISTS final_score;

-- Create the final_score table and calculate the final score
CREATE TABLE final_score AS
SELECT
    si.nse_code, si.name, si.industry, si.data_date,
    ROUND((
        COALESCE(si.d_to_e_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Debt to Equity') +
        COALESCE(si.roce_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'ROCE (last 3 years)') +
        COALESCE(si.coa_net_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Cash from Operating Activity Trend') +
        COALESCE(si.best_p_e_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Price to Earning') +
        COALESCE(si.peg_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'PEG Ratio') +
        COALESCE(si.management_score_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Management Quality') +
        COALESCE(si.div_yield_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'High Dividend Yield%') +
        COALESCE(si.ph_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Promoter Holding') +
        COALESCE(si.ph_trend_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Promoter Holding Trend') +
        COALESCE(si.dii_trend_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'DII Holding Trend') +
        COALESCE(si.fii_trend_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'FII Holding Trend') +
        COALESCE(si.sales_growth_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Sales Trend') +
        COALESCE(si.opm_trend_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'OPM Trend') +
        COALESCE(si.eps_growth_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'EPS Trend') +
        COALESCE(si.op_avggrowth_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Operating Profit Trend') +
        COALESCE(si.transcriptions_indicator, 0) * (SELECT weight FROM indicator_weights WHERE name = 'Transcriptions')
    )::numeric, 4) AS final_score
FROM screener_indicators si;