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
