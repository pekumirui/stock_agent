# コーディング規約・注意事項

## 基本ルール

- Python 3.10+
- 型ヒント推奨
- docstring必須（日本語OK）
- DB操作は`db_utils.py`の関数を使用
- エラーハンドリングはtry-exceptで、batch_logsに記録

## 外部API制約

- Yahoo Finance API: 大量アクセス時はsleep入れる（0.3秒以上）
- EDINET API: APIキーは任意だが推奨
- SQLite: 同時書き込みに弱いので注意（日次バッチは単一プロセス想定）

## SQLiteマイグレーション注意事項

- `executescript()` はDDLに対してアトミックでない。テーブル再作成（CREATE新→INSERT→DROP旧→RENAME）が途中で失敗するとDBが壊れる
- マイグレーションSQLでテーブル再作成する場合は、各ステップの冪等性を確保すること（例: `DROP TABLE IF EXISTS`, `CREATE TABLE IF NOT EXISTS`）
- `get_connection()` は自動commitしない。テストで直接SQLを実行する場合は明示的に `conn.commit()` が必要（`insert_financial()` 等のdb_utils関数は内部でcommitする）

## テストのDB分離

- テストでは必ず `test_db` フィクスチャを使い、本番DBに触れないこと
- `test_db` を使わないテスト（`test_init_database` 等）は本番DBに対して実行され、未適用マイグレーションを発火させる危険がある
- テストで `insert_financial()` を使う場合は `fiscal_end_date` 必須

## Python環境
- `python -m venv` で仮想環境を作成し、venv内で `pip install` する
- 'externally-managed-environment' エラーが出たらvenvを作成・有効化してから再実行
- 作業前に `python --version`, `pip --version` で環境を確認する
