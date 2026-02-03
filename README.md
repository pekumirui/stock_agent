# 株式調査AIエージェント - データベース基盤

## 概要

日本株の株価・決算データを収集・管理するためのSQLiteデータベースと日次バッチシステム。

## ディレクトリ構成

```
stock_agent/
├── db/
│   ├── schema.sql          # DBスキーマ定義
│   └── stock_agent.db      # SQLiteデータベース（自動生成）
├── scripts/
│   ├── db_utils.py         # DB操作ユーティリティ
│   ├── init_companies.py   # 銘柄マスタ初期化
│   ├── fetch_prices.py     # 株価取得（Yahoo Finance）
│   ├── fetch_financials.py # 決算取得（EDINET）
│   └── run_daily_batch.py  # 日次バッチメイン
├── logs/                   # ログ出力用
└── README.md
```

## セットアップ

### 1. 必要なパッケージをインストール

```bash
pip install yfinance pandas requests openpyxl　xlrd
python -m pip install yfinance pandas requests openpyxl　xlrd
```

### 2. 初回セットアップ（銘柄マスタ + サンプルデータ）

```bash
# サンプル銘柄（約30社）でテスト
python scripts/run_daily_batch.py --init --sample

# または全銘柄（JPXから約4000社取得）
python scripts/run_daily_batch.py --init
```

### 3. 全履歴取得（初回のみ）

```bash
# 株価の全履歴を取得（時間がかかります）
python scripts/fetch_prices.py --full
```

## 日次運用

### 手動実行

```bash
python scripts/run_daily_batch.py
```

### cron設定（毎日18:00に実行）

```bash
0 18 * * 1-5 cd /path/to/stock_agent && python scripts/run_daily_batch.py >> logs/batch.log 2>&1
```

## 各スクリプトの使い方

### 株価取得 (fetch_prices.py)

```bash
# 全銘柄の最新データ
python scripts/fetch_prices.py

# 特定銘柄のみ
python scripts/fetch_prices.py --ticker 7203,6758,9984

# 過去30日分
python scripts/fetch_prices.py --days 30

# 全履歴
python scripts/fetch_prices.py --full
```

### 決算取得 (fetch_financials.py)

```bash
# 直近7日分
python scripts/fetch_financials.py

# 過去30日分
python scripts/fetch_financials.py --days 30

# 特定銘柄のみ
python scripts/fetch_financials.py --ticker 7203
```

### 銘柄マスタ (init_companies.py)

```bash
# JPXから全銘柄取得
python scripts/init_companies.py

# サンプル銘柄のみ
python scripts/init_companies.py --sample

# CSVから読み込み
python scripts/init_companies.py --csv path/to/companies.csv
```

## データベーススキーマ

### テーブル一覧

| テーブル | 説明 |
|---------|------|
| companies | 銘柄マスタ |
| daily_prices | 日次株価 |
| stock_splits | 株式分割情報 |
| financials | 決算データ |
| batch_logs | バッチ実行ログ |

### 主なビュー

- `v_latest_prices` - 各銘柄の最新株価
- `v_latest_financials` - 各銘柄の最新決算

## SQLiteでの確認

```bash
sqlite3 db/stock_agent.db

-- 銘柄一覧
SELECT ticker_code, company_name, market_segment FROM companies LIMIT 10;

-- 最新株価
SELECT * FROM v_latest_prices LIMIT 10;

-- 決算データ
SELECT ticker_code, fiscal_year, revenue, operating_income, net_income 
FROM financials ORDER BY announcement_date DESC LIMIT 10;
```

## 注意事項

1. **Yahoo Finance API**: 大量アクセスは制限される可能性あり。sleepを入れて対策済み。
2. **EDINET API**: 無料利用可能。APIキーは任意だが設定推奨。
3. **株式分割**: adjusted_close（調整後終値）を使えば分割考慮済みの価格を取得可能。

## 次のステップ（エージェント構築）

1. 上昇トレンド検出エージェント
2. テーマ・関連銘柄抽出エージェント
3. ニュース収集エージェント
4. 決算評価エージェント
5. 統合レポートエージェント

これらはDBが構築されてから順次開発予定。

## CI/CD（Azure VMへの自動デプロイ）

GitHub Actionsを使用してAzure VMへ自動デプロイします。

### セットアップ手順

#### 1. GitHubリポジトリの作成

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/stock_agent.git
git push -u origin main
```

#### 2. GitHub SecretsにSSH秘密鍵を登録

1. GitHubリポジトリの **Settings** → **Secrets and variables** → **Actions** へ移動
2. **New repository secret** をクリック
3. 以下を登録:
   - **Name**: `SSH_PRIVATE_KEY`
   - **Value**: `AI-bot.key.pem`の内容をコピー&ペースト

#### 3. Azure VM側の準備

```bash
# VMにSSH接続
ssh -i AI-bot.key.pem azureuser@20.222.241.22

# デプロイ先ディレクトリ作成
mkdir -p ~/stock_agent

# Python3とsqlite3がインストールされているか確認
python3 --version
sqlite3 --version

# 必要に応じてインストール
sudo apt update
sudo apt install python3 python3-venv python3-pip sqlite3
```

#### 4. デプロイの実行

- `main`ブランチにpushすると自動デプロイ
- GitHubの **Actions** タブから手動実行も可能

### デプロイ内容

- ソースコードをrsyncで転送
- Python仮想環境を作成（初回のみ）
- 依存パッケージをインストール
- DBを初期化（初回のみ）

### 除外されるファイル

- `.pem`ファイル（秘密鍵）
- `db/stock_agent.db`（DBファイル）
- `__pycache__/`
- `logs/`の中身
