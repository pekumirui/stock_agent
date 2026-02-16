# 株式データ収集バッチ - Claude Code用プロジェクト設定

## プロジェクト概要

日本株の株価・決算データを収集・蓄積するバッチシステム。
本プロジェクトは主にPython。DBはSQLite。Web UIはhtmxとAlpine.js。テストはpytest。コアロジックファイル（fetch_*.py、models、parsers）を修正した後は、必ず `pytest` を実行すること。
詳細なガイドラインは `.claude/rules/` を参照
アーキテクチャやドキュメントは `docs/` を参照

## プロジェクト構造

- `scripts/` - バッチスクリプト（fetch_financials.py, update_edinet_codes.py等）
- `web/` - 決算分析Webビューア（FastAPI + Jinja2 + htmx + Alpine.js）
- `lib/xbrlp/` - XBRLパーサーライブラリ（ローカル、pip管理外）
- `db/` - SQLiteデータベース（`stock_agent.db`）・スキーマ・マイグレーション
- `tests/` - テストコード
- `logs/` - バッチ実行ログ
- `data/` - データキャッシュ（xbrl_cache, edinet_cache, tdnet_xbrl_cache, csv）
- `infra/` - cron設定・ログクリーンアップ
- `.env` - 環境変数（EDINET_API_KEY等）

## 注意事項

- システムに`python`コマンドは無い。必ず`venv/bin/python`を使う
- xbrlpライブラリはpip管理外（`lib/xbrlp/`をsys.path.insertで読み込み）
- `.env`の読み込み: スクリプト内は`_load_env()`を使用。bashから直接使う場合は`set -a && source .env && set +a`

## Web機能の起動

```bash
cd /home/pekumirui/stock_agent && venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
```

アクセス: http://localhost:8000/viewer

## テスト

```bash
cd /home/pekumirui/stock_agent && venv/bin/python -m pytest
```

## マイグレーション

```bash
cd /home/pekumirui/stock_agent && venv/bin/python scripts/migrate.py
```

## セットアップ

```bash
cd /home/pekumirui/stock_agent
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```
