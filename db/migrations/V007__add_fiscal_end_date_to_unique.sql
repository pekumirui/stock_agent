-- V007: fiscal_end_date を UNIQUE 制約に追加（決算期変更対応）
-- SQLite は ALTER TABLE で制約変更不可のため、テーブル再作成方式

-- ステップ0: ビューを先に削除（テーブル削除前に必要）
DROP VIEW IF EXISTS v_financials_yoy;
DROP VIEW IF EXISTS v_financials_qoq;
DROP VIEW IF EXISTS v_financials_standalone_quarter;
DROP VIEW IF EXISTS v_latest_financials;
DROP VIEW IF EXISTS v_missing_financials;

-- ステップ1: 既存NULLのfiscal_end_dateを補完
UPDATE financials
SET fiscal_end_date = fiscal_year || '-12-31'
WHERE fiscal_end_date IS NULL;

-- ステップ2: 新テーブル作成（NOT NULL + 4カラムUNIQUE）
CREATE TABLE financials_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,
    fiscal_year TEXT NOT NULL,
    fiscal_quarter TEXT,
    fiscal_end_date TEXT NOT NULL,  -- NOT NULL に変更
    announcement_date TEXT,
    announcement_time TEXT,
    revenue REAL,
    gross_profit REAL,
    operating_income REAL,
    ordinary_income REAL,
    net_income REAL,
    eps REAL,
    currency TEXT DEFAULT 'JPY',
    unit TEXT DEFAULT 'million',
    source TEXT,
    edinet_doc_id TEXT,
    pdf_path TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

-- ステップ3: データコピー
INSERT INTO financials_new (
    id, ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date,
    announcement_date, announcement_time,
    revenue, gross_profit, operating_income, ordinary_income, net_income, eps,
    currency, unit, source, edinet_doc_id, pdf_path,
    created_at, updated_at
)
SELECT
    id, ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date,
    announcement_date, announcement_time,
    revenue, gross_profit, operating_income, ordinary_income, net_income, eps,
    currency, unit, source, edinet_doc_id, pdf_path,
    created_at, updated_at
FROM financials;

-- ステップ4: 入替
DROP TABLE financials;
ALTER TABLE financials_new RENAME TO financials;

-- ステップ5: インデックス再作成
CREATE INDEX IF NOT EXISTS idx_fin_ticker ON financials(ticker_code);
CREATE INDEX IF NOT EXISTS idx_fin_year ON financials(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_fin_announce ON financials(announcement_date);
CREATE INDEX IF NOT EXISTS idx_fin_end_date ON financials(fiscal_end_date);

-- ステップ6: ビュー再作成
-- v_financials_yoy: ORDER BY を fiscal_end_date に変更
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
WINDOW w AS (PARTITION BY f.ticker_code, f.fiscal_quarter ORDER BY f.fiscal_end_date)
ORDER BY f.ticker_code, f.fiscal_end_date, f.fiscal_quarter;

-- v_financials_qoq: ORDER BY を fiscal_end_date に変更
CREATE VIEW IF NOT EXISTS v_financials_qoq AS
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
    f.operating_income,
    f.net_income,
    f.eps,
    -- 前四半期の値
    LAG(f.revenue, 1) OVER w AS revenue_prev_quarter,
    LAG(f.operating_income, 1) OVER w AS operating_income_prev_quarter,
    LAG(f.net_income, 1) OVER w AS net_income_prev_quarter,
    LAG(f.eps, 1) OVER w AS eps_prev_quarter,
    -- QoQ変化額
    f.revenue - LAG(f.revenue, 1) OVER w AS revenue_qoq_change,
    f.operating_income - LAG(f.operating_income, 1) OVER w AS operating_income_qoq_change,
    f.net_income - LAG(f.net_income, 1) OVER w AS net_income_qoq_change,
    -- QoQ変化率（%）
    CASE
        WHEN LAG(f.revenue, 1) OVER w IS NOT NULL AND LAG(f.revenue, 1) OVER w != 0
        THEN ROUND((f.revenue - LAG(f.revenue, 1) OVER w) * 100.0 / ABS(LAG(f.revenue, 1) OVER w), 2)
        ELSE NULL
    END AS revenue_qoq_pct,
    CASE
        WHEN LAG(f.operating_income, 1) OVER w IS NOT NULL AND LAG(f.operating_income, 1) OVER w != 0
        THEN ROUND((f.operating_income - LAG(f.operating_income, 1) OVER w) * 100.0 / ABS(LAG(f.operating_income, 1) OVER w), 2)
        ELSE NULL
    END AS operating_income_qoq_pct
FROM financials f
INNER JOIN companies c ON f.ticker_code = c.ticker_code
WHERE f.fiscal_quarter != 'FY'
WINDOW w AS (
    PARTITION BY f.ticker_code
    ORDER BY f.fiscal_end_date
)
ORDER BY f.ticker_code, f.fiscal_end_date, f.fiscal_quarter;

-- v_financials_standalone_quarter: V006の定義をそのまま再作成
CREATE VIEW IF NOT EXISTS v_financials_standalone_quarter AS
SELECT
    f.id,
    f.ticker_code,
    c.company_name,
    f.fiscal_year,
    CASE f.fiscal_quarter
        WHEN 'FY' THEN 'Q4'
        ELSE f.fiscal_quarter
    END AS fiscal_quarter,
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
        WHEN 'FY' THEN 'Q3'
        ELSE NULL
    END
)
WHERE f.fiscal_quarter IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY');

-- v_latest_financials: 再作成
CREATE VIEW IF NOT EXISTS v_latest_financials AS
SELECT
    f.*,
    c.company_name,
    c.market_segment,
    c.sector_33
FROM financials f
INNER JOIN companies c ON f.ticker_code = c.ticker_code
WHERE f.announcement_date = (
    SELECT MAX(announcement_date) FROM financials WHERE ticker_code = f.ticker_code
);

-- v_missing_financials: 再作成
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
