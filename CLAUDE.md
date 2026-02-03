# 株式調査AIエージェント - Claude Code用プロジェクト設定

## プロジェクト概要

日本株の株価・決算データを収集し、AIエージェントで分析・レポート生成するシステム。

## 現在の状態

### 完了
- [x] SQLiteスキーマ設計（`db/schema.sql`）
- [x] DB操作ユーティリティ（`scripts/db_utils.py`）
- [x] 銘柄マスタ初期化スクリプト（`scripts/init_companies.py`）
- [x] 株価取得バッチ - Yahoo Finance（`scripts/fetch_prices.py`）
- [x] 決算取得バッチ - EDINET（`scripts/fetch_financials.py`）
- [x] 日次バッチ実行スクリプト（`scripts/run_daily_batch.py`）

### 未完了・要修正
- [ ] **外部キー制約の修正** - 銘柄マスタなしで株価投入できてしまう問題
  - `db_utils.py`の`bulk_insert_prices`で外部キーチェックが効いていない
  - 対応案A（推奨）: 存在しない銘柄は自動でマスタ登録してから株価挿入
  - 対応案B: 挿入前にマスタ存在チェック、なければスキップ

## ディレクトリ構成

```
stock_agent/
├── .claude/
│   ├── agents/
│   │   ├── test-runner.yml     # テスト実行サブエージェント
│   │   └── data-validator.yml  # データ検証サブエージェント
│   └── skills/
│       ├── test.md             # /test スキル
│       └── validate.md         # /validate スキル
├── db/
│   ├── schema.sql          # DBスキーマ定義
│   └── stock_agent.db      # SQLiteデータベース
├── scripts/
│   ├── db_utils.py         # DB操作ユーティリティ
│   ├── init_companies.py   # 銘柄マスタ初期化（JPX/サンプル）
│   ├── fetch_prices.py     # 株価取得（Yahoo Finance）
│   ├── fetch_financials.py # 決算取得（EDINET）
│   └── run_daily_batch.py  # 日次バッチメイン
├── tests/                  # テストコード
│   └── test_db_utils.py    # DB操作のテスト
├── logs/                   # ログ出力用
├── README.md
└── CLAUDE.md               # このファイル
```

## テーブル構成

| テーブル | 説明 | 主キー |
|---------|------|--------|
| companies | 銘柄マスタ | ticker_code |
| daily_prices | 日次株価（OHLCV+調整後終値） | id, UNIQUE(ticker_code, trade_date) |
| stock_splits | 株式分割情報 | id, UNIQUE(ticker_code, split_date) |
| financials | 決算データ | id, UNIQUE(ticker_code, fiscal_year, fiscal_quarter) |
| batch_logs | バッチ実行ログ | id |

## 依存パッケージ

```bash
pip install yfinance pandas requests openpyxl pytest pytest-cov
```

## 開発時のコマンド

```bash
# 初回セットアップ（サンプル銘柄でテスト）
python scripts/run_daily_batch.py --init --sample

# 銘柄マスタのみ初期化
python scripts/init_companies.py --sample

# 特定銘柄の株価取得
python scripts/fetch_prices.py --ticker 7203,6758 --days 30

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

## サブエージェント

コード変更後は以下のサブエージェントでテスト・検証を行う：

| エージェント | 用途 | 呼び出し方 |
|-------------|------|-----------|
| test-runner | pytestでテスト実行 | `test-runner`エージェントを使用 |
| data-validator | DB整合性チェック | `data-validator`エージェントを使用 |

### 使用例
- コード修正後: test-runnerでテスト実行
- バッチ実行後: data-validatorでデータ検証

## スキル（ショートカット）

| スキル | 説明 |
|--------|------|
| `/test` | テスト実行 |
| `/validate` | データ検証 |

## 次のタスク（優先順）

### 1. 外部キー制約の修正
`scripts/fetch_prices.py`を修正し、銘柄マスタにない銘柄は：
- Yahoo Financeから企業情報を取得
- companiesテーブルに自動登録
- その後株価を挿入

修正箇所: `fetch_all_prices()`関数内で、各銘柄処理前にマスタ存在チェック追加

### 2. エージェント構築（将来）
以下のエージェントを順次開発予定：

1. **上昇トレンド検出エージェント**
   - DBから株価抽出、移動平均・出来高等で判定

2. **テーマ・関連銘柄抽出エージェント**
   - Web検索で市場テーマを取得、関連銘柄を特定

3. **ニュース収集エージェント**
   - 日々のニュースを収集・要約

4. **材料調査エージェント**
   - 株価変動大の銘柄の材料を調査

5. **決算評価エージェント**
   - DBから決算抽出、前期比・予想比で評価

6. **統合レポートエージェント**
   - 上記エージェントの情報を統合、ユーザーに通知

## コーディング規約

- Python 3.10+
- 型ヒント推奨
- docstring必須（日本語OK）
- DB操作は`db_utils.py`の関数を使用
- エラーハンドリングはtry-exceptで、batch_logsに記録

## 注意事項

- Yahoo Finance API: 大量アクセス時はsleep入れる（0.3秒以上）
- EDINET API: APIキーは任意だが推奨
- SQLite: 同時書き込みに弱いので注意（日次バッチは単一プロセス想定）
