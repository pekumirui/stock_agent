# 開発コマンドリファレンス

## 依存パッケージ

```bash
pip install yfinance pandas requests openpyxl pytest pytest-cov beautifulsoup4
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

# 日次バッチ実行
python scripts/run_daily_batch.py
```

## テスト

```bash
# 全テスト実行（Anaconda Python使用）
C:/Users/pekum/anaconda3/python.exe -m pytest tests/ -v

# カバレッジ付き
C:/Users/pekum/anaconda3/python.exe -m pytest tests/ -v --cov=scripts --cov-report=term-missing

# 特定テストのみ
C:/Users/pekum/anaconda3/python.exe -m pytest tests/test_db_utils.py -v
```
