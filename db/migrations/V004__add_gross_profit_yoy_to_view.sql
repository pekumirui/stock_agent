-- v_financials_yoy ビューに gross_profit_yoy_pct を追加
DROP VIEW IF EXISTS v_financials_yoy;

CREATE VIEW IF NOT EXISTS v_financials_yoy AS
SELECT
    f.id,
    f.ticker_code,
    c.company_name,
    f.fiscal_year,
    f.fiscal_quarter,
    f.fiscal_end_date,
    f.announcement_date,
    -- 当期の値
    f.revenue,
    f.gross_profit,
    f.operating_income,
    f.ordinary_income,
    f.net_income,
    f.eps,
    -- 前年同期の値
    LAG(f.revenue, 1) OVER w AS revenue_prev_year,
    LAG(f.gross_profit, 1) OVER w AS gross_profit_prev_year,
    LAG(f.operating_income, 1) OVER w AS operating_income_prev_year,
    LAG(f.ordinary_income, 1) OVER w AS ordinary_income_prev_year,
    LAG(f.net_income, 1) OVER w AS net_income_prev_year,
    LAG(f.eps, 1) OVER w AS eps_prev_year,
    -- YoY変化額
    f.revenue - LAG(f.revenue, 1) OVER w AS revenue_yoy_change,
    f.operating_income - LAG(f.operating_income, 1) OVER w AS operating_income_yoy_change,
    f.net_income - LAG(f.net_income, 1) OVER w AS net_income_yoy_change,
    -- YoY変化率（%）
    CASE
        WHEN LAG(f.revenue, 1) OVER w IS NOT NULL AND LAG(f.revenue, 1) OVER w != 0
        THEN ROUND((f.revenue - LAG(f.revenue, 1) OVER w) * 100.0 / ABS(LAG(f.revenue, 1) OVER w), 2)
        ELSE NULL
    END AS revenue_yoy_pct,
    CASE
        WHEN LAG(f.gross_profit, 1) OVER w IS NOT NULL AND LAG(f.gross_profit, 1) OVER w != 0
        THEN ROUND((f.gross_profit - LAG(f.gross_profit, 1) OVER w) * 100.0 / ABS(LAG(f.gross_profit, 1) OVER w), 2)
        ELSE NULL
    END AS gross_profit_yoy_pct,
    CASE
        WHEN LAG(f.operating_income, 1) OVER w IS NOT NULL AND LAG(f.operating_income, 1) OVER w != 0
        THEN ROUND((f.operating_income - LAG(f.operating_income, 1) OVER w) * 100.0 / ABS(LAG(f.operating_income, 1) OVER w), 2)
        ELSE NULL
    END AS operating_income_yoy_pct,
    CASE
        WHEN LAG(f.net_income, 1) OVER w IS NOT NULL AND LAG(f.net_income, 1) OVER w != 0
        THEN ROUND((f.net_income - LAG(f.net_income, 1) OVER w) * 100.0 / ABS(LAG(f.net_income, 1) OVER w), 2)
        ELSE NULL
    END AS net_income_yoy_pct
FROM financials f
INNER JOIN companies c ON f.ticker_code = c.ticker_code
WINDOW w AS (PARTITION BY f.ticker_code, f.fiscal_quarter ORDER BY f.fiscal_year)
ORDER BY f.ticker_code, f.fiscal_year, f.fiscal_quarter;
