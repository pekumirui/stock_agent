-- V008: announcements UNIQUE制約修正
-- fiscal_year/fiscal_quarterのNULL問題を解消し、titleベースの制約に変更
-- SQLite は ALTER TABLE で制約変更不可のため、テーブル再作成方式
-- 各ステップは冪等（再実行時にも安全）

-- ステップ0: 途中失敗時の残骸を除去
DROP TABLE IF EXISTS announcements_new;

-- ステップ1: 新テーブル作成（title NOT NULL + 4カラムUNIQUE）
CREATE TABLE announcements_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_code TEXT NOT NULL,
    announcement_date TEXT NOT NULL,
    announcement_time TEXT,
    announcement_type TEXT NOT NULL,
    title TEXT NOT NULL,
    fiscal_year TEXT,
    fiscal_quarter TEXT,
    document_url TEXT,
    source TEXT DEFAULT 'TDnet',
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker_code, announcement_date, announcement_type, title),
    FOREIGN KEY (ticker_code) REFERENCES companies(ticker_code)
);

-- ステップ2: 重複を除いたデータコピー（同一キーで最古の created_at を採用）
-- NULL titleは空文字に変換（title NOT NULL制約に適合させる）
INSERT OR IGNORE INTO announcements_new
    (ticker_code, announcement_date, announcement_time, announcement_type,
     title, fiscal_year, fiscal_quarter, document_url, source, created_at)
SELECT ticker_code, announcement_date, announcement_time, announcement_type,
       COALESCE(title, ''), fiscal_year, fiscal_quarter, document_url, source, MIN(created_at)
FROM announcements
GROUP BY ticker_code, announcement_date, announcement_type, COALESCE(title, '');

-- ステップ3: 入替
DROP TABLE IF EXISTS announcements;
ALTER TABLE announcements_new RENAME TO announcements;

-- ステップ4: インデックス再作成
CREATE INDEX IF NOT EXISTS idx_announce_date ON announcements(announcement_date);
CREATE INDEX IF NOT EXISTS idx_announce_ticker ON announcements(ticker_code);
CREATE INDEX IF NOT EXISTS idx_announce_type ON announcements(announcement_type);
