"""
株式調査AIエージェント - データベースユーティリティ
"""
import sqlite3
import os
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

# パス設定
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "db" / "stock_agent.db"
SCHEMA_PATH = BASE_DIR / "db" / "schema.sql"


@contextmanager
def get_connection():
    """データベース接続のコンテキストマネージャ"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # 辞書形式でアクセス可能に
    conn.execute("PRAGMA foreign_keys = ON")  # 外部キー制約を有効化
    try:
        yield conn
    finally:
        conn.close()


def init_database():
    """データベースを初期化（マイグレーション適用）"""
    # DBディレクトリがなければ作成
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # マイグレーションディレクトリ
    MIGRATIONS_DIR = BASE_DIR / 'db' / 'migrations'

    # マイグレーション適用
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # マイグレーション履歴テーブル作成
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _yoyo_migration (
            migration_hash TEXT,
            migration_id TEXT NOT NULL PRIMARY KEY,
            applied_at_utc TIMESTAMP
        )
    """)

    # 既存の適用済みマイグレーションを取得
    cursor.execute("SELECT migration_id FROM _yoyo_migration")
    applied_ids = {row[0] for row in cursor.fetchall()}

    # マイグレーションファイルを順番に適用
    migration_files = sorted(MIGRATIONS_DIR.glob('V*.sql'))
    migration_files = [f for f in migration_files if not f.stem.endswith('.rollback')]

    applied_count = 0
    for migration_file in migration_files:
        migration_id = migration_file.stem

        if migration_id in applied_ids:
            continue  # 既に適用済み

        # マイグレーションSQL実行
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql = f.read()
            conn.executescript(sql)

        # マイグレーション履歴に記録
        applied_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            INSERT INTO _yoyo_migration (migration_hash, migration_id, applied_at_utc)
            VALUES ('', ?, ?)
        """, (migration_id, applied_at))

        applied_count += 1
        print(f"  マイグレーション適用: {migration_id}")

    conn.commit()
    conn.close()

    if applied_count > 0:
        print(f"マイグレーション適用: {applied_count}件")
    else:
        print("マイグレーション: すべて適用済み")

    print(f"データベースを初期化しました: {DB_PATH}")


def log_batch_start(batch_name: str) -> int:
    """バッチ開始をログに記録"""
    with get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO batch_logs (batch_name, execution_date, start_time, status)
            VALUES (?, date('now', 'localtime'), datetime('now', 'localtime'), 'running')
        """, (batch_name,))
        conn.commit()
        return cursor.lastrowid


def log_batch_end(log_id: int, status: str, records_processed: int = 0, error_message: str = None):
    """バッチ終了をログに記録"""
    with get_connection() as conn:
        conn.execute("""
            UPDATE batch_logs
            SET end_time = datetime('now', 'localtime'),
                status = ?,
                records_processed = ?,
                error_message = ?
            WHERE id = ?
        """, (status, records_processed, error_message, log_id))
        conn.commit()


def get_all_tickers(active_only: bool = True) -> list:
    """全銘柄コードを取得"""
    with get_connection() as conn:
        if active_only:
            cursor = conn.execute("SELECT ticker_code FROM companies WHERE is_active = 1")
        else:
            cursor = conn.execute("SELECT ticker_code FROM companies")
        return [row['ticker_code'] for row in cursor.fetchall()]


def get_last_price_date(ticker_code: str = None) -> str:
    """最後に取得した株価の日付を取得"""
    with get_connection() as conn:
        if ticker_code:
            cursor = conn.execute(
                "SELECT MAX(trade_date) as last_date FROM daily_prices WHERE ticker_code = ?",
                (ticker_code,)
            )
        else:
            cursor = conn.execute("SELECT MAX(trade_date) as last_date FROM daily_prices")
        row = cursor.fetchone()
        return row['last_date'] if row else None


def upsert_company(ticker_code: str, company_name: str, **kwargs):
    """銘柄マスタをupsert"""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO companies (ticker_code, company_name, company_name_en, market_segment, 
                                   sector_33, sector_17, edinet_code, is_active, listed_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker_code) DO UPDATE SET
                company_name = excluded.company_name,
                company_name_en = COALESCE(excluded.company_name_en, company_name_en),
                market_segment = COALESCE(excluded.market_segment, market_segment),
                sector_33 = COALESCE(excluded.sector_33, sector_33),
                sector_17 = COALESCE(excluded.sector_17, sector_17),
                edinet_code = COALESCE(excluded.edinet_code, edinet_code),
                is_active = COALESCE(excluded.is_active, is_active),
                updated_at = datetime('now', 'localtime')
        """, (
            ticker_code,
            company_name,
            kwargs.get('company_name_en'),
            kwargs.get('market_segment'),
            kwargs.get('sector_33'),
            kwargs.get('sector_17'),
            kwargs.get('edinet_code'),
            kwargs.get('is_active', 1),
            kwargs.get('listed_date')
        ))
        conn.commit()


def insert_daily_price(ticker_code: str, trade_date: str, open_price: float,
                       high_price: float, low_price: float, close_price: float,
                       volume: int, adjusted_close: float = None):
    """日次株価を挿入（重複時は無視）"""
    with get_connection() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO daily_prices 
            (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close))
        conn.commit()


def bulk_insert_prices(prices: list):
    """株価を一括挿入"""
    with get_connection() as conn:
        conn.executemany("""
            INSERT OR IGNORE INTO daily_prices 
            (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, prices)
        conn.commit()
        return conn.total_changes


def insert_stock_split(ticker_code: str, split_date: str, ratio_from: float, ratio_to: float):
    """株式分割情報を挿入"""
    with get_connection() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO stock_splits 
            (ticker_code, split_date, split_ratio_from, split_ratio_to)
            VALUES (?, ?, ?, ?)
        """, (ticker_code, split_date, ratio_from, ratio_to))
        conn.commit()


def insert_financial(ticker_code: str, fiscal_year: str, fiscal_quarter: str, **kwargs):
    """
    決算データを挿入（データソース優先度を考慮）

    上書きルール:
    - source='EDINET': 常に保存（正式版優先）
    - source='TDnet': 既存が EDINET の場合はスキップ

    Returns:
        bool: 保存した場合 True、スキップした場合 False
    """
    source = kwargs.get('source')

    with get_connection() as conn:
        # 既存データの source を確認
        cursor = conn.execute("""
            SELECT source FROM financials
            WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
        """, (ticker_code, fiscal_year, fiscal_quarter))
        existing = cursor.fetchone()

        # スキップ判定: TDnet データで既存が EDINET
        if existing and existing['source'] == 'EDINET' and source == 'TDnet':
            print(f"    [SKIP] EDINETデータ存在のためスキップ: {ticker_code} {fiscal_year} {fiscal_quarter}")
            return False

        # 通常の INSERT/UPDATE 処理
        conn.execute("""
            INSERT INTO financials
            (ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date, announcement_date,
             revenue, gross_profit, operating_income, ordinary_income, net_income, eps,
             currency, unit, source, edinet_doc_id, pdf_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker_code, fiscal_year, fiscal_quarter) DO UPDATE SET
                fiscal_end_date = excluded.fiscal_end_date,
                announcement_date = excluded.announcement_date,
                revenue = excluded.revenue,
                gross_profit = excluded.gross_profit,
                operating_income = excluded.operating_income,
                ordinary_income = excluded.ordinary_income,
                net_income = excluded.net_income,
                eps = excluded.eps,
                source = excluded.source,
                edinet_doc_id = COALESCE(excluded.edinet_doc_id, edinet_doc_id),
                pdf_path = COALESCE(excluded.pdf_path, pdf_path),
                updated_at = datetime('now', 'localtime')
        """, (
            ticker_code, fiscal_year, fiscal_quarter,
            kwargs.get('fiscal_end_date'),
            kwargs.get('announcement_date'),
            kwargs.get('revenue'),
            kwargs.get('gross_profit'),
            kwargs.get('operating_income'),
            kwargs.get('ordinary_income'),
            kwargs.get('net_income'),
            kwargs.get('eps'),
            kwargs.get('currency', 'JPY'),
            kwargs.get('unit', 'million'),
            source,
            kwargs.get('edinet_doc_id'),
            kwargs.get('pdf_path')
        ))
        conn.commit()
        return True


def get_financials_yoy(ticker_code: str = None) -> list:
    """YoY比較付き決算データを取得"""
    with get_connection() as conn:
        if ticker_code:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy WHERE ticker_code = ? ORDER BY fiscal_year DESC, fiscal_quarter",
                (ticker_code,)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy ORDER BY ticker_code, fiscal_year DESC, fiscal_quarter"
            )
        return [dict(row) for row in cursor.fetchall()]


def get_financials_qoq(ticker_code: str = None) -> list:
    """QoQ比較付き決算データを取得"""
    with get_connection() as conn:
        if ticker_code:
            cursor = conn.execute(
                "SELECT * FROM v_financials_qoq WHERE ticker_code = ? ORDER BY fiscal_year DESC, fiscal_quarter",
                (ticker_code,)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM v_financials_qoq ORDER BY ticker_code, fiscal_year DESC, fiscal_quarter"
            )
        return [dict(row) for row in cursor.fetchall()]


def is_valid_ticker_code(ticker_code: str) -> bool:
    """
    証券コードの妥当性を検証

    有効なパターン:
    - 4桁数字: 7203, 6758
    - 5桁数字: 12345
    - 4桁数字+英字1文字: 285A, 200A, 346A

    Args:
        ticker_code: 証券コード

    Returns:
        bool: 有効な場合 True

    Examples:
        >>> is_valid_ticker_code("7203")
        True
        >>> is_valid_ticker_code("285A")
        True
        >>> is_valid_ticker_code("ABC")
        False
        >>> is_valid_ticker_code("12345A")
        False
    """
    if not ticker_code:
        return False

    ticker_clean = ticker_code.strip()
    length = len(ticker_clean)

    # 4-5桁の範囲チェック
    if not (4 <= length <= 5):
        return False

    # パターン1: 4-5桁の数字のみ
    if ticker_clean.isdigit():
        return True

    # パターン2: 4文字（3桁数字 + 1文字英字）例: 285A
    if length == 4:
        return ticker_clean[:3].isdigit() and ticker_clean[3].isalpha()

    # パターン3: 5文字（4桁数字 + 1文字英字）例: 1234A
    if length == 5:
        return ticker_clean[:4].isdigit() and ticker_clean[4].isalpha()

    return False


if __name__ == "__main__":
    # 直接実行時はDB初期化
    init_database()
