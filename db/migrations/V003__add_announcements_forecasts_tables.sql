-- V003: add_announcements_forecasts_tables
--
-- 決算ビューア機能のデータ層:
-- - financials テーブルに announcement_time カラム追加
-- - announcements テーブル新規作成
-- - management_forecasts テーブル新規作成
-- - consensus_estimates テーブル新規作成
-- - v_financials_standalone_quarter ビュー新規作成
--

-- 1. financials テーブルにカラム追加
ALTER TABLE financials ADD COLUMN announcement_time TEXT;

-- 2. announcements テーブル
CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,
    announcement_date TEXT NOT NULL,
    announcement_time TEXT,
    announcement_type TEXT NOT NULL,  -- 'earnings', 'revision', 'dividend', 'other'
    title TEXT,
    fiscal_year TEXT,
    fiscal_quarter TEXT,
    document_url TEXT,
    source TEXT DEFAULT 'TDnet',
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, announcement_date, announcement_type, fiscal_year, fiscal_quarter),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);
CREATE INDEX IF NOT EXISTS idx_announce_date ON announcements(announcement_date);
CREATE INDEX IF NOT EXISTS idx_announce_ticker ON announcements(ticker_code);
CREATE INDEX IF NOT EXISTS idx_announce_type ON announcements(announcement_type);

-- 3. management_forecasts テーブル
CREATE TABLE IF NOT EXISTS management_forecasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,
    fiscal_year TEXT NOT NULL,
    fiscal_quarter TEXT NOT NULL,
    announced_date TEXT NOT NULL,
    forecast_type TEXT NOT NULL,  -- 'initial' / 'revised'
    revenue REAL,
    operating_income REAL,
    ordinary_income REAL,
    net_income REAL,
    eps REAL,
    dividend_per_share REAL,
    revision_direction TEXT,  -- 'up' / 'down' / NULL
    revision_reason TEXT,
    unit TEXT DEFAULT 'million',
    source TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, fiscal_year, fiscal_quarter, announced_date),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);
CREATE INDEX IF NOT EXISTS idx_forecast_ticker ON management_forecasts(ticker_code);
CREATE INDEX IF NOT EXISTS idx_forecast_date ON management_forecasts(announced_date);
CREATE INDEX IF NOT EXISTS idx_forecast_target ON management_forecasts(fiscal_year, fiscal_quarter);

-- 4. consensus_estimates テーブル（スキーマのみ）
CREATE TABLE IF NOT EXISTS consensus_estimates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,
    fiscal_year TEXT NOT NULL,
    fiscal_quarter TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    revenue_mean REAL, revenue_median REAL, revenue_high REAL, revenue_low REAL, revenue_analyst_count INTEGER,
    operating_income_mean REAL, operating_income_median REAL, operating_income_high REAL, operating_income_low REAL, operating_income_analyst_count INTEGER,
    ordinary_income_mean REAL, ordinary_income_median REAL, ordinary_income_high REAL, ordinary_income_low REAL, ordinary_income_analyst_count INTEGER,
    net_income_mean REAL, net_income_median REAL, net_income_high REAL, net_income_low REAL, net_income_analyst_count INTEGER,
    source TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, fiscal_year, fiscal_quarter, as_of_date),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);
CREATE INDEX IF NOT EXISTS idx_consensus_ticker ON consensus_estimates(ticker_code);
CREATE INDEX IF NOT EXISTS idx_consensus_target ON consensus_estimates(fiscal_year, fiscal_quarter);

-- 5. v_financials_standalone_quarter ビュー
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
    f.net_income AS net_income_cumulative
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
