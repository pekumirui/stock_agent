-- 決算データ欠損フィールド確認ビュー
CREATE VIEW IF NOT EXISTS v_missing_financials AS
SELECT
    f.ticker_code,
    c.company_name,
    f.fiscal_year,
    f.fiscal_quarter,
    f.source,
    f.announcement_date,
    CASE WHEN f.revenue IS NULL THEN 1 ELSE 0 END AS missing_revenue,
    CASE WHEN f.gross_profit IS NULL THEN 1 ELSE 0 END AS missing_gross_profit,
    CASE WHEN f.operating_income IS NULL THEN 1 ELSE 0 END AS missing_operating_income,
    CASE WHEN f.ordinary_income IS NULL THEN 1 ELSE 0 END AS missing_ordinary_income,
    CASE WHEN f.net_income IS NULL THEN 1 ELSE 0 END AS missing_net_income,
    CASE WHEN f.eps IS NULL THEN 1 ELSE 0 END AS missing_eps
FROM financials f
INNER JOIN companies c ON f.ticker_code = c.ticker_code
WHERE f.revenue IS NULL
   OR f.gross_profit IS NULL
   OR f.operating_income IS NULL
   OR f.ordinary_income IS NULL
   OR f.net_income IS NULL
   OR f.eps IS NULL
ORDER BY f.announcement_date DESC, f.ticker_code;
