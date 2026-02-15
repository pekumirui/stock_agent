-- V005: v_financials_standalone_quarter に has_prev_quarter フラグ追加
DROP VIEW IF EXISTS v_financials_standalone_quarter;

CREATE VIEW IF NOT EXISTS v_financials_standalone_quarter AS
SELECT
    f.id,
    f.ticker_code,
    c.company_name,
    f.fiscal_year,
    f.fiscal_quarter,
    f.announcement_date,
    f.announcement_time,
    CASE f.fiscal_quarter
        WHEN 'Q1' THEN f.revenue
        ELSE f.revenue - COALESCE(prev.revenue, 0)
    END AS revenue_standalone,
    CASE f.fiscal_quarter
        WHEN 'Q1' THEN f.gross_profit
        ELSE f.gross_profit - COALESCE(prev.gross_profit, 0)
    END AS gross_profit_standalone,
    CASE f.fiscal_quarter
        WHEN 'Q1' THEN f.operating_income
        ELSE f.operating_income - COALESCE(prev.operating_income, 0)
    END AS operating_income_standalone,
    CASE f.fiscal_quarter
        WHEN 'Q1' THEN f.ordinary_income
        ELSE f.ordinary_income - COALESCE(prev.ordinary_income, 0)
    END AS ordinary_income_standalone,
    CASE f.fiscal_quarter
        WHEN 'Q1' THEN f.net_income
        ELSE f.net_income - COALESCE(prev.net_income, 0)
    END AS net_income_standalone,
    f.revenue AS revenue_cumulative,
    f.gross_profit AS gross_profit_cumulative,
    f.operating_income AS operating_income_cumulative,
    f.ordinary_income AS ordinary_income_cumulative,
    f.net_income AS net_income_cumulative,
    CASE f.fiscal_quarter
        WHEN 'Q1' THEN 1
        ELSE CASE WHEN prev.id IS NOT NULL THEN 1 ELSE 0 END
    END AS has_prev_quarter
FROM financials f
INNER JOIN companies c ON f.ticker_code = c.ticker_code
LEFT JOIN financials prev ON (
    f.ticker_code = prev.ticker_code
    AND f.fiscal_year = prev.fiscal_year
    AND prev.fiscal_quarter = CASE f.fiscal_quarter
        WHEN 'Q2' THEN 'Q1'
        WHEN 'Q3' THEN 'Q2'
        WHEN 'Q4' THEN 'Q3'
        ELSE NULL
    END
)
WHERE f.fiscal_quarter != 'FY';
