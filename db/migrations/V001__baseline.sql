-- V001: ベースラインスキーマ
--
-- 既存のschema.sqlを元にしたベースラインマイグレーション
-- 全テーブル・インデックス・ビューを含む
--

-- 株式調査AIエージェント用データベーススキーマ
-- SQLite3

-- ============================================
-- 1. 銘柄マスタ
-- ============================================
CREATE TABLE IF NOT EXISTS companies (
    ticker_code TEXT PRIMARY KEY,           -- 証券コード（4-5桁）
    company_name TEXT NOT NULL,             -- 会社名
    company_name_en TEXT,                   -- 会社名（英語）
    market_segment TEXT,                    -- 市場区分（プライム/スタンダード/グロース）
    sector_33 TEXT,                         -- 33業種区分
    sector_17 TEXT,                         -- 17業種区分
    edinet_code TEXT,                       -- EDINETコード（決算取得用）
    is_active INTEGER DEFAULT 1,            -- 上場中フラグ（1=上場中, 0=上場廃止）
    listed_date TEXT,                       -- 上場日
    delisted_date TEXT,                     -- 上場廃止日
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_companies_market ON companies(market_segment);
CREATE INDEX IF NOT EXISTS idx_companies_sector ON companies(sector_33);
CREATE INDEX IF NOT EXISTS idx_companies_edinet ON companies(edinet_code);

-- ============================================
-- 2. 日次株価テーブル
-- ============================================
CREATE TABLE IF NOT EXISTS daily_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,              -- 証券コード
    trade_date TEXT NOT NULL,               -- 取引日（YYYY-MM-DD）
    open_price REAL,                        -- 始値
    high_price REAL,                        -- 高値
    low_price REAL,                         -- 安値
    close_price REAL,                       -- 終値
    volume INTEGER,                         -- 出来高
    adjusted_close REAL,                    -- 調整後終値（分割考慮）
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, trade_date),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

CREATE INDEX IF NOT EXISTS idx_prices_ticker ON daily_prices(ticker_code);
CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(trade_date);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON daily_prices(ticker_code, trade_date);

-- ============================================
-- 3. 株式分割テーブル
-- ============================================
CREATE TABLE IF NOT EXISTS stock_splits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,              -- 証券コード
    split_date TEXT NOT NULL,               -- 分割実施日
    split_ratio_from REAL NOT NULL,         -- 分割前（例: 1）
    split_ratio_to REAL NOT NULL,           -- 分割後（例: 2 → 1:2分割）
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, split_date),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

CREATE INDEX IF NOT EXISTS idx_splits_ticker ON stock_splits(ticker_code);
CREATE INDEX IF NOT EXISTS idx_splits_date ON stock_splits(split_date);

-- ============================================
-- 4. 決算テーブル
-- ============================================
CREATE TABLE IF NOT EXISTS financials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,              -- 証券コード
    fiscal_year TEXT NOT NULL,              -- 決算年度（例: 2024）
    fiscal_quarter TEXT,                    -- 四半期（Q1/Q2/Q3/Q4/FY）
    fiscal_end_date TEXT,                   -- 決算期末日
    announcement_date TEXT,                 -- 決算発表日

    -- 損益計算書項目
    revenue REAL,                           -- 売上高
    gross_profit REAL,                      -- 売上総利益
    operating_income REAL,                  -- 営業利益
    ordinary_income REAL,                   -- 経常利益
    net_income REAL,                        -- 当期純利益
    eps REAL,                               -- 1株当たり利益

    -- メタ情報
    currency TEXT DEFAULT 'JPY',            -- 通貨
    unit TEXT DEFAULT 'million',            -- 単位（million=百万円）
    source TEXT,                            -- データソース（EDINET等）
    edinet_doc_id TEXT,                     -- EDINET書類ID
    pdf_path TEXT,                          -- 決算短信PDFの保存パス

    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, fiscal_year, fiscal_quarter),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

CREATE INDEX IF NOT EXISTS idx_fin_ticker ON financials(ticker_code);
CREATE INDEX IF NOT EXISTS idx_fin_year ON financials(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_fin_announce ON financials(announcement_date);

-- ============================================
-- 5. バッチ実行ログテーブル
-- ============================================
CREATE TABLE IF NOT EXISTS batch_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_name TEXT NOT NULL,               -- バッチ名
    execution_date TEXT NOT NULL,           -- 実行日
    start_time TEXT NOT NULL,               -- 開始時刻
    end_time TEXT,                          -- 終了時刻
    status TEXT NOT NULL,                   -- 状態（running/success/failed）
    records_processed INTEGER DEFAULT 0,    -- 処理件数
    error_message TEXT,                     -- エラーメッセージ
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_batch_name ON batch_logs(batch_name);
CREATE INDEX IF NOT EXISTS idx_batch_date ON batch_logs(execution_date);

-- ============================================
-- 6. 決算資料分析テーブル（将来のAI分析用）
-- ============================================
CREATE TABLE IF NOT EXISTS document_analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,              -- 証券コード
    edinet_doc_id TEXT,                     -- EDINET書類ID
    doc_type TEXT,                          -- 書類種別（'tanshin', 'yuho' 等）
    pdf_path TEXT,                          -- PDFファイルパス
    analysis_type TEXT,                     -- 分析種別（'wording_change', 'sentiment' 等）
    analysis_result TEXT,                   -- 分析結果（JSON形式）
    model_name TEXT,                        -- 使用したAIモデル名
    analyzed_at TEXT,                       -- 分析実行日時
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

CREATE INDEX IF NOT EXISTS idx_doc_analysis_ticker ON document_analyses(ticker_code);
CREATE INDEX IF NOT EXISTS idx_doc_analysis_doc ON document_analyses(edinet_doc_id);

-- ============================================
-- ビュー: 前年同期比較（YoY）
-- ============================================
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

-- ============================================
-- ビュー: 前四半期比較（QoQ）
-- ============================================
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
    ORDER BY f.fiscal_year,
             CASE f.fiscal_quarter
                WHEN 'Q1' THEN 1
                WHEN 'Q2' THEN 2
                WHEN 'Q3' THEN 3
                WHEN 'Q4' THEN 4
             END
)
ORDER BY f.ticker_code, f.fiscal_year, f.fiscal_quarter;

-- ============================================
-- ビュー: 最新株価
-- ============================================
CREATE VIEW IF NOT EXISTS v_latest_prices AS
SELECT
    dp.*,
    c.company_name,
    c.market_segment,
    c.sector_33
FROM daily_prices dp
INNER JOIN companies c ON dp.ticker_code = c.ticker_code
WHERE dp.trade_date = (
    SELECT MAX(trade_date) FROM daily_prices WHERE ticker_code = dp.ticker_code
);

-- ============================================
-- ビュー: 最新決算
-- ============================================
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
