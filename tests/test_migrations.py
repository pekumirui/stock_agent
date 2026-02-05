"""マイグレーションテスト"""
import pytest
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime

# プロジェクトルートからマイグレーション適用関数を取得
BASE_DIR = Path(__file__).parent.parent
MIGRATIONS_DIR = BASE_DIR / 'db' / 'migrations'


def apply_migrations_to_db(db_path):
    """テスト用DB にマイグレーションを適用"""
    conn = sqlite3.connect(db_path)
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

    conn.commit()
    conn.close()


def test_apply_all_migrations():
    """全マイグレーションが正常に適用できること"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        test_db_path = tmp.name

    try:
        # マイグレーション適用
        apply_migrations_to_db(test_db_path)

        # テーブルが作成されていることを確認
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN (
                'companies', 'daily_prices', 'stock_splits',
                'financials', 'batch_logs', 'document_analyses',
                '_yoyo_migration'
            )
        """)
        tables = {row[0] for row in cursor.fetchall()}

        conn.close()

        # 全テーブルが存在することを確認
        expected_tables = {
            'companies', 'daily_prices', 'stock_splits',
            'financials', 'batch_logs', 'document_analyses',
            '_yoyo_migration'
        }
        assert tables == expected_tables, f"Expected {expected_tables}, got {tables}"

    finally:
        # クリーンアップ
        Path(test_db_path).unlink(missing_ok=True)


def test_migration_preserves_data():
    """マイグレーション適用後もデータが保持されること"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        test_db_path = tmp.name

    try:
        # マイグレーション適用
        apply_migrations_to_db(test_db_path)

        # テストデータ投入
        conn = sqlite3.connect(test_db_path)
        conn.execute("""
            INSERT INTO companies (ticker_code, company_name)
            VALUES ('9999', 'テスト株式会社')
        """)
        conn.commit()
        conn.close()

        # データが保持されていることを確認
        conn = sqlite3.connect(test_db_path)
        cursor = conn.execute("SELECT * FROM companies WHERE ticker_code = '9999'")
        result = cursor.fetchone()
        conn.close()

        assert result is not None
        assert result[1] == 'テスト株式会社'

    finally:
        # クリーンアップ
        Path(test_db_path).unlink(missing_ok=True)


def test_foreign_key_constraints_after_migration():
    """マイグレーション後も外部キー制約が機能すること"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        test_db_path = tmp.name

    try:
        # マイグレーション適用
        apply_migrations_to_db(test_db_path)

        # 外部キー制約有効化
        conn = sqlite3.connect(test_db_path)
        conn.execute("PRAGMA foreign_keys = ON")

        # 存在しない ticker_code へのINSERTは失敗すべき
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("""
                INSERT INTO daily_prices (ticker_code, trade_date, close_price)
                VALUES ('9999', '2024-01-01', 1000.0)
            """)

        conn.close()

    finally:
        # クリーンアップ
        Path(test_db_path).unlink(missing_ok=True)


def test_migration_idempotency():
    """マイグレーションの冪等性確認（複数回適用してもエラーなし）"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        test_db_path = tmp.name

    try:
        # 1回目適用
        apply_migrations_to_db(test_db_path)

        # 2回目適用（すべてスキップされる）
        apply_migrations_to_db(test_db_path)

        # マイグレーション履歴確認
        conn = sqlite3.connect(test_db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM _yoyo_migration")
        count = cursor.fetchone()[0]
        conn.close()

        # マイグレーション数が適切（重複なし）
        migration_files = sorted(MIGRATIONS_DIR.glob('V*.sql'))
        migration_files = [f for f in migration_files if not f.stem.endswith('.rollback')]
        assert count == len(migration_files)

    finally:
        # クリーンアップ
        Path(test_db_path).unlink(missing_ok=True)


def test_views_exist_after_migration():
    """マイグレーション後にビューが存在すること"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        test_db_path = tmp.name

    try:
        # マイグレーション適用
        apply_migrations_to_db(test_db_path)

        # ビューが作成されていることを確認
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='view' AND name IN (
                'v_financials_yoy', 'v_financials_qoq',
                'v_latest_prices', 'v_latest_financials'
            )
        """)
        views = {row[0] for row in cursor.fetchall()}

        conn.close()

        # 全ビューが存在することを確認
        expected_views = {
            'v_financials_yoy', 'v_financials_qoq',
            'v_latest_prices', 'v_latest_financials'
        }
        assert views == expected_views, f"Expected {expected_views}, got {views}"

    finally:
        # クリーンアップ
        Path(test_db_path).unlink(missing_ok=True)
