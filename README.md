# 株式調査AIエージェント - データベース基盤

## 概要

日本株の株価・決算データを収集・管理するためのSQLiteデータベースと日次バッチシステム。

## ディレクトリ構成

```
stock_agent/
├── db/
│   ├── schema.sql          # DBスキーマ定義
│   ├── stock_agent.db      # SQLiteデータベース（自動生成）
│   ├── yoyo.ini            # マイグレーション設定
│   └── migrations/         # DBマイグレーション
├── lib/
│   └── xbrlp/              # XBRLパーサーライブラリ
├── scripts/
│   ├── db_utils.py         # DB操作ユーティリティ
│   ├── init_companies.py   # 銘柄マスタ初期化
│   ├── fetch_prices.py     # 株価取得（Yahoo Finance）
│   ├── fetch_financials.py # 決算取得（EDINET）
│   ├── fetch_tdnet.py      # 決算短信取得（TDnet）
│   ├── update_edinet_codes.py  # EDINETコード一括更新
│   ├── analyze_missing_edinet.py  # EDINETコード欠損分析
│   ├── validate_schema.py  # スキーマ検証ツール
│   ├── migrate.py          # DBマイグレーション管理
│   └── run_daily_batch.py  # 日次バッチメイン
├── web/                    # Webビューア
│   ├── app.py              # FastAPIメインアプリ
│   ├── routers/            # APIルーター
│   ├── services/           # ビジネスロジック
│   ├── templates/          # Jinja2テンプレート
│   └── static/             # CSS/JS
├── tests/                  # テストコード
├── logs/                   # ログ出力用
└── README.md
```

## セットアップ

### 1. 必要なパッケージをインストール

```bash
pip install yfinance pandas requests openpyxl beautifulsoup4 yoyo-migrations fastapi uvicorn jinja2 python-multipart
```

### 2. 初回セットアップ（銘柄マスタ + サンプルデータ）

```bash
# サンプル銘柄（約30社）でテスト
python3 scripts/run_daily_batch.py --init --sample

# または全銘柄（JPXから約4000社取得）
python3 scripts/run_daily_batch.py --init
```

### 3. 全履歴取得（初回のみ）

```bash
# 株価の全履歴を取得（時間がかかります）
python3 scripts/fetch_prices.py --full
```

## 日次運用

### 手動実行

```bash
# 通常実行（株価 + EDINET決算 + TDnet決算短信）
python3 scripts/run_daily_batch.py

# TDnet決算短信をスキップ
python3 scripts/run_daily_batch.py --skip-tdnet

# EDINET決算をスキップ
python3 scripts/run_daily_batch.py --skip-financials

# 株価のみ取得
python3 scripts/run_daily_batch.py --skip-financials --skip-tdnet
```

**実行ステップ（5段階）**:
1. データベース初期化
2. 銘柄マスタ確認
3. 株価取得（Yahoo Finance）
4. 決算データ取得（EDINET: 有報・半期報）
5. 決算短信取得（TDnet: Q1-Q4決算短信）

**TDnet統合の背景**:
2024年4月の金商法改正により、Q1/Q3四半期報告書のEDINET提出が廃止されました。そのため、Q1/Q3の決算データはTDnet決算短信からしか取得できません。TDnetは過去30日分しか取得できないため、取りこぼしを防ぐために日次バッチに統合しています。

### cron設定（毎日18:00に実行）

```bash
0 18 * * 1-5 cd /path/to/stock_agent && python3 scripts/run_daily_batch.py >> logs/batch.log 2>&1
```

## サポートする証券コード

本システムは以下の形式の証券コードに対応しています：

| パターン | 例 | 説明 |
|---------|---|------|
| **4桁数字** | 7203, 6758 | 一般的な株式（トヨタ、ソニー等） |
| **5桁数字** | 12345 | 特定の銘柄 |
| **4桁数字+英字** | 285A, 200A, 346A | キオクシア、NEXT FUNDS等 |

### 実行例

```bash
# 数字のみ
python3 scripts/fetch_prices.py --ticker 7203

# 英字付き
python3 scripts/fetch_prices.py --ticker 285A

# 混在
python3 scripts/fetch_prices.py --ticker 7203,285A,200A,6758
```

**バリデーション**: すべての証券コードは `db_utils.is_valid_ticker_code()` で検証されます。

## Webビューア

決算データをリアルタイムに参照できるWebインターフェースを提供します。

### 起動方法

```bash
cd /home/pekumirui/stock_agent && venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
```

ブラウザで http://localhost:8000/viewer にアクセス。

### 機能

- **リアルタイムデータ**: 最新の決算発表を時系列で表示（開示時刻付き）
- **単独四半期算出**: Q2/Q3/Q4の累積値から単独四半期を自動計算
- **業績予想比較**: 会社予想・コンセンサスとの差異を可視化
- **前年同期比較**: YoY成長率を自動計算
- **適時開示追跡**: 決算発表・業績修正・配当発表を統合表示

### 技術スタック

- **バックエンド**: FastAPI（Python 3.10+）
- **テンプレート**: Jinja2
- **フロントエンド**: htmx（サーバー駆動UI）+ Alpine.js（クライアントステート）
- **スタイル**: カスタムCSS（ダークテーマ対応）

## 各スクリプトの使い方

### 株価取得 (fetch_prices.py)

```bash
# 全銘柄の最新データ
python3 scripts/fetch_prices.py

# 特定銘柄のみ
python3 scripts/fetch_prices.py --ticker 7203,6758,9984

# 過去30日分
python3 scripts/fetch_prices.py --days 30

# 全履歴
python3 scripts/fetch_prices.py --full
```

### 決算取得 (fetch_financials.py)

EDINETから有価証券報告書・半期報告書を取得します（正式版）。

```bash
# 直近7日分（有価証券報告書・半期報告書）
python3 scripts/fetch_financials.py

# 過去30日分
python3 scripts/fetch_financials.py --days 30

# 特定銘柄のみ
python3 scripts/fetch_financials.py --ticker 7203

# 処理済み書類も再取得（通常は自動スキップ）
python3 scripts/fetch_financials.py --force
```

**対応書類種別**:
- **有価証券報告書（docType=120）**: 通期決算（fiscal_quarter=FY）
- **半期報告書（docType=160）**: 上期決算（fiscal_quarter=Q2）

**パフォーマンス最適化**:
- EDINETコード→証券コードのマッピングを起動時に一括ロード
- 処理済み書類は自動スキップ（`--force`で無効化可能）
- API呼び出しをdocType統合で最適化

**対応業種（XBRLマッピング）**: 一般企業、建設業、銀行業、証券業、保険業、鉄道・不動産、電力・ガス、海運業、商社・サービス等。IFRS/US-GAAP企業にも対応。

### TDnet決算短信取得 (fetch_tdnet.py)

最新の決算短信をTDnetから取得します（速報版）。

```bash
# 本日分のTDnet決算短信を取得
python3 scripts/fetch_tdnet.py

# 過去7日分を取得
python3 scripts/fetch_tdnet.py --days 7

# 特定銘柄のみ
python3 scripts/fetch_tdnet.py --ticker 7203,6758

# 日付範囲指定
python3 scripts/fetch_tdnet.py --date-from 2024-02-01 --date-to 2024-02-05
```

**データソース戦略**:
- **TDnet**: 決算短信（速報版）、当日リアルタイム取得
- **EDINET**: 有価証券報告書（正式版）、数週間～数ヶ月遅れ
- **上書きルール**: EDINETがTDnetを上書き（正式版優先）

### 銘柄マスタ (init_companies.py)

```bash
# JPXから全銘柄取得
python3 scripts/init_companies.py

# サンプル銘柄のみ
python3 scripts/init_companies.py --sample

# CSVから読み込み
python3 scripts/init_companies.py --csv path/to/companies.csv
```

### マイグレーション管理 (migrate.py)

データベーススキーマの変更履歴を管理します。yoyo-migrationsを使用。

```bash
# マイグレーション状態確認
python3 scripts/migrate.py status

# マイグレーション適用（新規・既存環境どちらも対応）
python3 scripts/migrate.py apply

# 新規マイグレーション作成
python3 scripts/migrate.py new "add_news_table"
# → db/migrations/V002__add_news_table.sql
# → db/migrations/V002__add_news_table.rollback.sql

# ロールバック（開発環境のみ推奨）
python3 scripts/migrate.py rollback
```

**スキーマ変更フロー**:
1. `migrate.py new "説明"` で新規マイグレーション作成
2. 生成されたSQLファイルを編集
3. `migrate.py apply` でローカル適用・テスト
4. Git commit & push で本番環境に自動適用（GitHub Actions）

**既存環境への適用**:
- 初回のみ `migrate.py mark-baseline` でベースラインマークを適用
- その後は `migrate.py apply` で未適用マイグレーションを自動検出・適用

### EDINETコード欠損分析 (analyze_missing_edinet.py)

EDINETコード未登録銘柄を分析し、カテゴリ分類・優先度判定を行います。

```bash
# 基本実行（CSV出力）
python3 scripts/analyze_missing_edinet.py

# 統計情報付き
python3 scripts/analyze_missing_edinet.py --include-stats

# サマリーのみ表示
python3 scripts/analyze_missing_edinet.py --summary-only
```

### スキーマ検証 (validate_schema.py)

yfinanceから株価データを取得し、スキーマ適合性を検証します。

```bash
# 全銘柄を検証
python3 scripts/validate_schema.py

# 特定銘柄のみ
python3 scripts/validate_schema.py --ticker 7203

# ドライラン（DB挿入なし）
python3 scripts/validate_schema.py --dry-run
```

## データベーススキーマ

### テーブル一覧

| テーブル | 説明 |
|---------|------|
| companies | 銘柄マスタ |
| daily_prices | 日次株価 |
| stock_splits | 株式分割情報 |
| financials | 決算データ（announcement_time追加） |
| announcements | 適時開示（決算/業績修正/配当） |
| management_forecasts | 業績予想 |
| consensus_estimates | コンセンサス予想（スキーマのみ） |
| batch_logs | バッチ実行ログ |
| document_analyses | 決算資料分析（将来のAI分析用） |
| _yoyo_migration | マイグレーション履歴管理テーブル |

### 主なビュー

| ビュー | 説明 |
|--------|------|
| v_latest_prices | 各銘柄の最新株価（銘柄情報付き） |
| v_latest_financials | 各銘柄の最新決算（銘柄情報付き） |
| v_financials_yoy | 前年同期比較（LAGウィンドウ関数） |
| v_financials_qoq | 前四半期比較（LAGウィンドウ関数） |
| v_financials_standalone_quarter | 単独四半期算出（累積値から差分計算） |
| v_missing_financials | 決算データ欠損フィールド確認 |

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

## テスト

```bash
# 全テスト実行
python3 -m pytest tests/ -v

# カバレッジ付き
python3 -m pytest tests/ -v --cov=scripts --cov-report=term-missing

# 特定テストのみ
python3 -m pytest tests/test_db_utils.py -v
```

### テストファイル一覧

| テストファイル | テスト対象 |
|--------------|-----------|
| test_db_utils.py | DB操作ユーティリティ |
| test_init_companies.py | 銘柄マスタ初期化 |
| test_fetch_prices.py | 株価取得（実API統合テスト） |
| test_fetch_financials.py | 決算取得 |
| test_fetch_tdnet.py | TDnet取得 |
| test_update_edinet_codes.py | EDINETコード更新 |
| test_analyze_missing_edinet.py | EDINETコード欠損分析 |
| test_financial_views.py | YoY/QoQビュー |
| test_migrations.py | マイグレーション |

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
- **マイグレーション自動適用**（新規・既存環境どちらも対応）
  - 未適用のマイグレーションを自動検出・適用
  - スキーマ変更をgit pushだけで本番反映可能

### 除外されるファイル

- `.pem`ファイル（秘密鍵）
- `db/stock_agent.db`（DBファイル）
- `__pycache__/`
- `logs/`の中身
