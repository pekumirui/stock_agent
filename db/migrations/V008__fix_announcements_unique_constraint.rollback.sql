-- V008 ロールバック: announcements UNIQUE制約を元に戻す
-- (ticker_code, announcement_date, announcement_type, fiscal_year, fiscal_quarter) の5カラム制約に戻す

-- ステップ1: 旧テーブル構造を再作成
CREATE TABLE IF NOT EXISTS announcements_old (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,
    announcement_date TEXT NOT NULL,
    announcement_time TEXT,
    announcement_type TEXT NOT NULL,
    title TEXT,
    fiscal_year TEXT,
    fiscal_quarter TEXT,
    document_url TEXT,
    source TEXT DEFAULT 'TDnet',
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, announcement_date, announcement_type, fiscal_year, fiscal_quarter),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

-- ステップ2: データコピー（title='' を NULL に戻す）
INSERT OR IGNORE INTO announcements_old
    (ticker_code, announcement_date, announcement_time, announcement_type,
     title, fiscal_year, fiscal_quarter, document_url, source, created_at)
SELECT ticker_code, announcement_date, announcement_time, announcement_type,
       NULLIF(title, ''), fiscal_year, fiscal_quarter, document_url, source, created_at
FROM announcements;

-- ステップ3: 入替
DROP TABLE IF EXISTS announcements;
ALTER TABLE announcements_old RENAME TO announcements;

-- ステップ4: インデックス再作成
CREATE INDEX IF NOT EXISTS idx_announce_date ON announcements(announcement_date);
CREATE INDEX IF NOT EXISTS idx_announce_ticker ON announcements(ticker_code);
CREATE INDEX IF NOT EXISTS idx_announce_type ON announcements(announcement_type);
