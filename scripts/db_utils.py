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
            VALUES (?, ?, ?)
        """, (migration_id, migration_id, applied_at))

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
    """
    日次株価を挿入（重複時は無視）

    【NEW】IntegrityError ハンドリング追加
    """
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO daily_prices
                (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close))
            conn.commit()
    except sqlite3.IntegrityError:
        # スキップ（ログ出力なし、正常な除外として扱う）
        pass


def bulk_insert_prices(prices: list):
    """
    株価を一括挿入（JPXリスト外の銘柄はスキップ）

    【NEW】IntegrityError ハンドリング追加

    Returns:
        int: 挿入成功した件数
    """
    if not prices:
        return 0

    inserted_count = 0
    skipped_tickers = set()

    with get_connection() as conn:
        for price_data in prices:
            ticker_code = price_data[0]
            try:
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO daily_prices
                    (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, price_data)
                if cursor.rowcount > 0:
                    inserted_count += 1
            except sqlite3.IntegrityError:
                if ticker_code not in skipped_tickers:
                    skipped_tickers.add(ticker_code)

        conn.commit()

    if skipped_tickers:
        print(f"    [INFO] JPXリスト外のためスキップ: {sorted(skipped_tickers)}")

    return inserted_count


def insert_stock_split(ticker_code: str, split_date: str, ratio_from: float, ratio_to: float):
    """株式分割情報を挿入"""
    with get_connection() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO stock_splits 
            (ticker_code, split_date, split_ratio_from, split_ratio_to)
            VALUES (?, ?, ?, ?)
        """, (ticker_code, split_date, ratio_from, ratio_to))
        conn.commit()


# データソース優先度（値が大きいほど優先）
SOURCE_PRIORITY = {'EDINET': 3, 'TDnet': 2, 'JQuants': 2, 'yfinance': 1}


def insert_financial(ticker_code: str, fiscal_year: str, fiscal_quarter: str,
                     skip_priority_check: bool = False, **kwargs):
    """
    決算データを挿入（データソース優先度を考慮）

    上書きルール（優先度: EDINET > TDnet > yfinance）:
    - 低優先度ソースで既に高優先度データが存在する場合はスキップ
    - 同一優先度または高優先度ソースは上書き
    - skip_priority_check=True の場合は優先度チェックをスキップ

    Returns:
        bool: 保存した場合 True、スキップした場合 False
    """
    if not ticker_exists(ticker_code):
        return False

    fiscal_end_date = kwargs.get('fiscal_end_date')
    if not fiscal_end_date:
        print(f"    [ERROR] fiscal_end_date is required: {ticker_code} {fiscal_year} {fiscal_quarter}")
        return False

    source = kwargs.get('source')

    try:
        with get_connection() as conn:
            # 既存データの source を確認
            cursor = conn.execute("""
                SELECT source FROM financials
                WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                  AND fiscal_end_date = ?
            """, (ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date))
            existing = cursor.fetchone()

            # スキップ判定: 低優先度ソースが高優先度データを上書きしない
            if existing and not skip_priority_check:
                existing_priority = SOURCE_PRIORITY.get(existing['source'], 0)
                new_priority = SOURCE_PRIORITY.get(source, 0)
                if new_priority < existing_priority:
                    print(f"    [SKIP] 優先度の高いデータが存在: {ticker_code} {fiscal_year} {fiscal_quarter} "
                          f"(既存: {existing['source']}, 新規: {source})")
                    return False

            # 通常の INSERT/UPDATE 処理
            conn.execute("""
            INSERT INTO financials
            (ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date, announcement_date,
             announcement_time,
             revenue, gross_profit, operating_income, ordinary_income, net_income, eps,
             currency, unit, source, edinet_doc_id, pdf_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date) DO UPDATE SET
                announcement_date = CASE
                    WHEN announcement_date IS NULL THEN excluded.announcement_date
                    WHEN excluded.announcement_date IS NULL THEN announcement_date
                    WHEN excluded.announcement_date < announcement_date THEN excluded.announcement_date
                    ELSE announcement_date
                END,
                announcement_time = COALESCE(excluded.announcement_time, announcement_time),
                revenue = COALESCE(excluded.revenue, revenue),
                gross_profit = COALESCE(excluded.gross_profit, gross_profit),
                operating_income = COALESCE(excluded.operating_income, operating_income),
                ordinary_income = COALESCE(excluded.ordinary_income, ordinary_income),
                net_income = COALESCE(excluded.net_income, net_income),
                eps = COALESCE(excluded.eps, eps),
                source = excluded.source,
                edinet_doc_id = COALESCE(excluded.edinet_doc_id, edinet_doc_id),
                pdf_path = COALESCE(excluded.pdf_path, pdf_path),
                updated_at = datetime('now', 'localtime')
        """, (
            ticker_code, fiscal_year, fiscal_quarter,
            kwargs.get('fiscal_end_date'),
            kwargs.get('announcement_date'),
            kwargs.get('announcement_time'),
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
    except sqlite3.IntegrityError as e:
        # 【NEW】フォールバック（事前チェックをすり抜けた場合）
        print(f"    [ERROR] FOREIGN KEY制約違反: {ticker_code} - {e}")
        return False


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


def ticker_exists(ticker_code: str) -> bool:
    """
    証券コードがcompaniesテーブルに存在するか確認

    JPXリスト（東証銘柄）に登録されているかをチェックします。
    名証M、札証、福証など地方市場の銘柄は False を返します。

    Args:
        ticker_code: 証券コード

    Returns:
        bool: companiesテーブルに存在する場合 True

    Examples:
        >>> ticker_exists("7203")  # トヨタ（東証）
        True
        >>> ticker_exists("6655")  # 洋電機（名証M）
        False
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT 1 FROM companies WHERE ticker_code = ? LIMIT 1",
            (ticker_code,)
        )
        return cursor.fetchone() is not None


def get_edinet_ticker_map(active_only: bool = True) -> dict:
    """EDINETコード→証券コードのマッピングを一括取得"""
    with get_connection() as conn:
        sql = "SELECT edinet_code, ticker_code FROM companies WHERE edinet_code IS NOT NULL"
        if active_only:
            sql += " AND is_active = 1"
        cursor = conn.execute(sql)
        return {row['edinet_code']: row['ticker_code'] for row in cursor.fetchall()}


def get_processed_doc_ids() -> set:
    """処理済みのEDINET書類IDの集合を取得"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT DISTINCT edinet_doc_id FROM financials WHERE edinet_doc_id IS NOT NULL"
        )
        return {row['edinet_doc_id'] for row in cursor.fetchall()}


def insert_announcement(ticker_code: str, announcement_date: str, announcement_time: str,
                        announcement_type: str, title: str, fiscal_year: str = None,
                        fiscal_quarter: str = None, document_url: str = None,
                        source: str = 'TDnet'):
    """
    適時開示情報を挿入（重複時は更新: UPSERT）

    UNIQUE制約 (ticker_code, announcement_date, announcement_type, title) に
    基づいて重複を検出し、既存行の可変フィールドを最新値で上書きする。

    Args:
        ticker_code: 証券コード
        announcement_date: 開示日（YYYY-MM-DD）
        announcement_time: 開示時刻（HH:MM）
        announcement_type: 種別（'earnings', 'revision', 'dividend', 'other'）
        title: 開示タイトル（NOT NULL）
        fiscal_year: 決算年度
        fiscal_quarter: 四半期
        document_url: 書類URL
        source: データソース（デフォルト'TDnet'）
    """
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO announcements
                (ticker_code, announcement_date, announcement_time, announcement_type,
                 title, fiscal_year, fiscal_quarter, document_url, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker_code, announcement_date, announcement_type, title)
                DO UPDATE SET
                    announcement_time = COALESCE(excluded.announcement_time, announcements.announcement_time),
                    fiscal_year = COALESCE(excluded.fiscal_year, announcements.fiscal_year),
                    fiscal_quarter = COALESCE(excluded.fiscal_quarter, announcements.fiscal_quarter),
                    document_url = COALESCE(excluded.document_url, announcements.document_url)
            """, (ticker_code, announcement_date, announcement_time, announcement_type,
                  title, fiscal_year, fiscal_quarter, document_url, source))
            conn.commit()
    except sqlite3.IntegrityError:
        pass


def insert_management_forecast(ticker_code: str, fiscal_year: str, fiscal_quarter: str,
                               announced_date: str, forecast_type: str, revenue: float = None,
                               operating_income: float = None, ordinary_income: float = None,
                               net_income: float = None, eps: float = None,
                               dividend_per_share: float = None, revision_direction: str = None,
                               revision_reason: str = None, source: str = None,
                               skip_priority_check: bool = False) -> bool:
    """
    会社業績予想を挿入（重複時は更新、データソース優先度を考慮）

    上書きルール（insert_financial() と同じ優先度辞書を使用）:
    - 低優先度ソースが高優先度データを上書きしない
    - 同一優先度または高優先度ソースは上書き
    - skip_priority_check=True の場合は優先度チェックをスキップ

    Args:
        ticker_code: 証券コード
        fiscal_year: 対象決算年度
        fiscal_quarter: 対象四半期
        announced_date: 発表日
        forecast_type: 予想種別（'initial' / 'revised'）
        revenue: 売上高予想
        operating_income: 営業利益予想
        ordinary_income: 経常利益予想
        net_income: 純利益予想
        eps: EPS予想
        dividend_per_share: 配当予想
        revision_direction: 修正方向（'up' / 'down' / None）
        revision_reason: 修正理由
        source: データソース
        skip_priority_check: Trueなら優先度チェックをスキップして上書き

    Returns:
        bool: 保存した場合 True、スキップした場合 False
    """
    try:
        with get_connection() as conn:
            if not skip_priority_check:
                # 既存データの source を確認（優先度チェック）
                cursor = conn.execute("""
                    SELECT source FROM management_forecasts
                    WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                      AND announced_date = ?
                """, (ticker_code, fiscal_year, fiscal_quarter, announced_date))
                existing = cursor.fetchone()

                if existing:
                    existing_priority = SOURCE_PRIORITY.get(existing['source'], 0)
                    new_priority = SOURCE_PRIORITY.get(source, 0)
                    if new_priority < existing_priority:
                        print(f"    [SKIP] 予想: 優先度の高いデータが存在: {ticker_code} {fiscal_year} {fiscal_quarter} "
                              f"(既存: {existing['source']}, 新規: {source})")
                        return False

            conn.execute("""
                INSERT INTO management_forecasts
                (ticker_code, fiscal_year, fiscal_quarter, announced_date, forecast_type,
                 revenue, operating_income, ordinary_income, net_income, eps,
                 dividend_per_share, revision_direction, revision_reason, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker_code, fiscal_year, fiscal_quarter, announced_date) DO UPDATE SET
                    forecast_type = excluded.forecast_type,
                    revenue = COALESCE(excluded.revenue, management_forecasts.revenue),
                    operating_income = COALESCE(excluded.operating_income, management_forecasts.operating_income),
                    ordinary_income = COALESCE(excluded.ordinary_income, management_forecasts.ordinary_income),
                    net_income = COALESCE(excluded.net_income, management_forecasts.net_income),
                    eps = COALESCE(excluded.eps, management_forecasts.eps),
                    dividend_per_share = COALESCE(excluded.dividend_per_share, management_forecasts.dividend_per_share),
                    revision_direction = excluded.revision_direction,
                    revision_reason = excluded.revision_reason,
                    source = excluded.source
            """, (ticker_code, fiscal_year, fiscal_quarter, announced_date, forecast_type,
                  revenue, operating_income, ordinary_income, net_income, eps,
                  dividend_per_share, revision_direction, revision_reason, source))
            conn.commit()
            return True
    except sqlite3.IntegrityError as e:
        print(f"    [ERROR] management_forecast挿入失敗: {ticker_code} - {e}")
        return False


def get_announcements_by_date(date: str, types: list = None) -> list:
    """
    指定日の適時開示一覧を取得

    companiesテーブルとJOINしてcompany_nameも返す。
    typesでannouncement_typeをフィルタ可能。

    Args:
        date: 対象日（YYYY-MM-DD）
        types: フィルタするannouncement_typeのリスト（例: ['earnings', 'revision']）

    Returns:
        list[dict]: 適時開示情報のリスト
    """
    with get_connection() as conn:
        sql = """
            SELECT a.*, c.company_name
            FROM announcements a
            INNER JOIN companies c ON a.ticker_code = c.ticker_code
            WHERE a.announcement_date = ?
        """
        params = [date]

        if types:
            placeholders = ','.join(['?' for _ in types])
            sql += f" AND a.announcement_type IN ({placeholders})"
            params.extend(types)

        sql += " ORDER BY a.announcement_time DESC, a.ticker_code"

        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def get_management_forecast(ticker_code: str, fiscal_year: str, fiscal_quarter: str = None) -> list:
    """
    指定銘柄の最新業績予想を取得

    Args:
        ticker_code: 証券コード
        fiscal_year: 決算年度
        fiscal_quarter: 四半期（Noneの場合は全四半期）

    Returns:
        list[dict]: 業績予想データのリスト（announced_date降順）
    """
    with get_connection() as conn:
        sql = """
            SELECT * FROM management_forecasts
            WHERE ticker_code = ? AND fiscal_year = ?
        """
        params = [ticker_code, fiscal_year]

        if fiscal_quarter:
            sql += " AND fiscal_quarter = ?"
            params.append(fiscal_quarter)

        sql += " ORDER BY announced_date DESC"

        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def get_standalone_quarter(ticker_code: str, fiscal_year: str, fiscal_quarter: str) -> dict:
    """
    v_financials_standalone_quarterビューから単独四半期データを取得

    Args:
        ticker_code: 証券コード
        fiscal_year: 決算年度
        fiscal_quarter: 四半期（Q1/Q2/Q3/Q4）

    Returns:
        dict: 単独四半期データ（該当なしの場合はNone）
    """
    with get_connection() as conn:
        cursor = conn.execute("""
            SELECT * FROM v_financials_standalone_quarter
            WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
        """, (ticker_code, fiscal_year, fiscal_quarter))
        row = cursor.fetchone()
        return dict(row) if row else None


if __name__ == "__main__":
    # 直接実行時はDB初期化
    init_database()
