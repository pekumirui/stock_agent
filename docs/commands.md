# 開発コマンドリファレンス

## 依存パッケージ

```bash
pip install yfinance pandas requests openpyxl pytest pytest-cov beautifulsoup4 fastapi uvicorn jinja2 python-multipart
```

## Webビューアの起動

```bash
# Webサーバー起動
cd /home/pekumirui/stock_agent && venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload

# アクセス
# http://localhost:8000/viewer
```

## 開発時のコマンド

```bash
# 初回セットアップ（サンプル銘柄でテスト）
python scripts/run_daily_batch.py --init --sample

# 銘柄マスタのみ初期化
python scripts/init_companies.py --sample

# 特定銘柄の株価取得
python scripts/fetch_prices.py --ticker 7203,6758 --days 30

# TDnet決算短信取得（本日発表分）
python scripts/fetch_tdnet.py

# TDnet決算短信取得（過去7日分）
python scripts/fetch_tdnet.py --days 7

# TDnet決算短信取得（特定銘柄のみ）
python scripts/fetch_tdnet.py --ticker 7203,6758

# TDnet決算短信取得（日付範囲指定）
python scripts/fetch_tdnet.py --date-from 2024-02-01 --date-to 2024-02-05

# EDINETコード一括更新（初回セットアップ時に実行）
python scripts/update_edinet_codes.py

# EDINETコード更新（過去30日分から収集）
python scripts/update_edinet_codes.py --days 30

# 日次バッチ実行（株価 + EDINET + TDnet）
python scripts/run_daily_batch.py

# TDnetスキップ
python scripts/run_daily_batch.py --skip-tdnet

# EDINET決算取得（処理済みスキップ付き）
python scripts/fetch_financials.py --days 30

# EDINET決算取得（処理済みも再取得）
python scripts/fetch_financials.py --days 30 --force

# 四半期報告書も取得（法改正前のQ1/Q3初期投入用）
python scripts/fetch_financials.py --include-quarterly --days 1095

# 四半期報告書取得（特定銘柄のみ）
python scripts/fetch_financials.py --include-quarterly --ticker 7203 --days 1095
```

## テスト

```bash
# 全テスト実行
python3 -m pytest tests/ -v

# カバレッジ付き
python3 -m pytest tests/ -v --cov=scripts --cov-report=term-missing

# 特定テストのみ
python3 -m pytest tests/test_db_utils.py -v
```

## データ品質管理

```bash
# EDINETコード欠損分析
python3 scripts/analyze_missing_edinet.py --include-stats

# スキーマ検証
python3 scripts/validate_schema.py

# スキーマ検証（特定銘柄）
python3 scripts/validate_schema.py --ticker 7203

#デバッグ用
-uを付けることでリアルタイムに出力される
python3 -u scripts/update_edinet_codes.py --days 720 --api-key <APIキー> > logs/update_edinet_codes.log 2>&1
```

