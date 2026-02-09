# 株式データ収集バッチ - Claude Code用プロジェクト設定

## プロジェクト概要

日本株の株価・決算データを収集・蓄積するバッチシステム。
詳細なガイドラインは `.claude/rules/` を参照
アーキテクチャやドキュメントは `docs/` を参照

## プロジェクト構造

- `scripts/` - バッチスクリプト（fetch_financials.py, update_edinet_codes.py等）
- `web/` - 決算分析Webビューア（FastAPI + Jinja2 + htmx + Alpine.js）
- `lib/xbrlp/` - XBRLパーサーライブラリ（ローカル、pip管理外）
- `db/` - SQLiteデータベース（`stock_agent.db`）・スキーマ・マイグレーション
- `tests/` - テストコード
- `logs/` - バッチ実行ログ
- `.env` - 環境変数（EDINET_API_KEY等）

## Web機能の起動

```bash
cd /home/pekumirui/stock_agent && venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
```

アクセス: http://localhost:8000/viewer
