#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""マイグレーション管理CLIツール"""
import sys
import io
from pathlib import Path
from yoyo import read_migrations, get_backend

# Windows環境でのUnicode出力対応
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# パス設定
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / 'db' / 'stock_agent.db'
MIGRATIONS_DIR = PROJECT_ROOT / 'db' / 'migrations'

def apply_migrations():
    """マイグレーション適用"""
    backend = get_backend(f'sqlite:///{DB_PATH}')
    migrations = read_migrations(str(MIGRATIONS_DIR))

    with backend.lock():
        to_apply = backend.to_apply(migrations)
        if not to_apply:
            print("[OK] すべてのマイグレーションが適用済みです")
            return

        print(f"適用するマイグレーション: {len(to_apply)}件")
        for m in to_apply:
            print(f"  - {m.id}")

        backend.apply_migrations(to_apply)
        print("[OK] マイグレーション適用完了")

def rollback_migrations(steps=1):
    """マイグレーションロールバック"""
    backend = get_backend(f'sqlite:///{DB_PATH}')
    migrations = read_migrations(str(MIGRATIONS_DIR))

    with backend.lock():
        to_rollback = backend.to_rollback(migrations)[:steps]
        if not to_rollback:
            print("[OK] ロールバック可能なマイグレーションはありません")
            return

        print(f"ロールバックするマイグレーション: {len(to_rollback)}件")
        for m in to_rollback:
            print(f"  - {m.id}")

        backend.rollback_migrations(to_rollback)
        print("[OK] ロールバック完了")

def show_status():
    """マイグレーション状態表示"""
    import sqlite3

    # マイグレーションファイル一覧を取得
    migration_files = sorted(MIGRATIONS_DIR.glob('V*.sql'))
    migration_files = [f for f in migration_files if not f.stem.endswith('.rollback')]
    all_migration_ids = [f.stem for f in migration_files]

    # 適用済みマイグレーションを取得
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # テーブルが存在するか確認
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='_yoyo_migration'
    """)
    table_exists = cursor.fetchone() is not None

    if table_exists:
        cursor.execute("SELECT migration_id FROM _yoyo_migration")
        applied_ids = {row[0] for row in cursor.fetchall()}
    else:
        applied_ids = set()

    conn.close()

    # 統計表示
    applied_count = len(applied_ids)
    unapplied_count = len(all_migration_ids) - applied_count

    print(f"データベース: {DB_PATH}")
    print(f"マイグレーション総数: {len(all_migration_ids)}件")
    print(f"適用済み: {applied_count}件")
    print(f"未適用: {unapplied_count}件")
    print()

    # 各マイグレーションの状態表示
    for migration_id in all_migration_ids:
        status = "[OK] 適用済み" if migration_id in applied_ids else "[  ] 未適用"
        print(f"{status} {migration_id}")

def create_migration(description):
    """新規マイグレーション作成"""
    # 既存マイグレーションから最大番号を取得
    existing = list(MIGRATIONS_DIR.glob('V*.sql'))
    max_version = 0
    for f in existing:
        if f.stem.startswith('V') and '__' in f.stem:
            version_str = f.stem.split('__')[0][1:]
            try:
                max_version = max(max_version, int(version_str))
            except ValueError:
                pass

    next_version = max_version + 1
    version_str = f"V{next_version:03d}"

    # ファイル名生成
    filename = f"{version_str}__{description}.sql"
    rollback_filename = f"{version_str}__{description}.rollback.sql"

    migration_path = MIGRATIONS_DIR / filename
    rollback_path = MIGRATIONS_DIR / rollback_filename

    # テンプレート作成
    migration_template = f"""-- {version_str}: {description}
--
-- 説明: [ここに変更内容を記述]
--

-- TODO: マイグレーションSQLを記述

"""

    rollback_template = f"""-- {version_str} Rollback: {description}
--
-- ロールバック処理
--

-- TODO: ロールバックSQLを記述

"""

    migration_path.write_text(migration_template, encoding='utf-8')
    rollback_path.write_text(rollback_template, encoding='utf-8')

    print(f"[OK] 新規マイグレーション作成:")
    print(f"  {migration_path}")
    print(f"  {rollback_path}")

def mark_baseline():
    """既存DBにベースラインマークを適用"""
    import sqlite3
    from datetime import datetime

    # ベースラインマイグレーションのIDを取得
    baseline_files = list(MIGRATIONS_DIR.glob('V001__baseline.sql'))
    if not baseline_files:
        print("[ERROR] V001__baseline.sql が見つかりません")
        return

    baseline_id = baseline_files[0].stem

    # SQLiteに直接接続してマイグレーション履歴を記録
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # マイグレーション履歴テーブルが存在しない場合は作成
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _yoyo_migration (
            migration_hash TEXT,
            migration_id TEXT NOT NULL PRIMARY KEY,
            applied_at_utc TIMESTAMP
        )
    """)

    # 既にマーク済みか確認
    cursor.execute("SELECT migration_id FROM _yoyo_migration WHERE migration_id = ?", (baseline_id,))
    existing = cursor.fetchone()

    if existing:
        print(f"[OK] ベースラインは既に適用済みです: {baseline_id}")
    else:
        # ベースラインマークを追加
        applied_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            INSERT INTO _yoyo_migration (migration_hash, migration_id, applied_at_utc)
            VALUES ('', ?, ?)
        """, (baseline_id, applied_at))
        conn.commit()
        print(f"[OK] ベースラインマーク適用: {baseline_id}")

    conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python scripts/migrate.py apply           - マイグレーション適用")
        print("  python scripts/migrate.py rollback [N]    - ロールバック（デフォルト1件）")
        print("  python scripts/migrate.py status          - マイグレーション状態確認")
        print('  python scripts/migrate.py new "description" - 新規マイグレーション作成')
        print("  python scripts/migrate.py mark-baseline   - 既存DBにベースラインマークを適用")
        sys.exit(1)

    command = sys.argv[1]

    if command == 'apply':
        apply_migrations()
    elif command == 'rollback':
        steps = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        rollback_migrations(steps)
    elif command == 'status':
        show_status()
    elif command == 'new':
        if len(sys.argv) < 3:
            print("[ERROR] 説明を指定してください")
            sys.exit(1)
        create_migration(sys.argv[2])
    elif command == 'mark-baseline':
        mark_baseline()
    else:
        print(f"[ERROR] 不明なコマンド '{command}'")
        sys.exit(1)
