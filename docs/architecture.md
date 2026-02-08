# アーキテクチャ

## ディレクトリ構成

```
stock_agent/
├── .claude/
│   ├── agents/
│   │   ├── test-runner.yml          # テスト実行サブエージェント
│   │   ├── test-case-generator.yml  # テストケース生成エージェント（※未認識）
│   │   ├── test-reviewer.yml        # テストレビューエージェント（※未認識）
│   │   └── data-validator.yml       # データ検証サブエージェント
│   ├── rules/                       # プロジェクトルール（本ファイル含む）
│   │   ├── 01-architecture.md       # アーキテクチャ設計
│   │   ├── 02-coding-standards.md   # コーディング規約
│   │   ├── 03-commands.md           # 開発コマンド
│   │   ├── 04-workflow.md           # ワークフロー
│   │   └── 05-testing.md            # テスト作成ガイド
│   ├── skills/
│   │   ├── test.md                  # /test スキル
│   │   ├── test-workflow.md         # /test-workflow スキル（統合テストワークフロー）
│   │   └── validate.md              # /validate スキル
│   ├── test_progress.md             # テスト進捗管理・カバレッジ推移
│   └── settings.local.json          # ローカル設定
├── db/
│   ├── schema.sql                   # DBスキーマ定義
│   ├── stock_agent.db               # SQLiteデータベース
│   └── migrations/                  # DBマイグレーションファイル（将来）
├── lib/
│   └── xbrlp/                       # XBRLパーサーライブラリ（vendored）
│       ├── __init__.py              # Parser, Fact, QName等のexport
│       ├── file_loader.py           # ファイル取得・キャッシュ
│       └── parser.py                # XBRL/iXBRLパーサー本体
├── scripts/
│   ├── db_utils.py                  # DB操作ユーティリティ
│   ├── init_companies.py            # 銘柄マスタ初期化（JPX/サンプル）
│   ├── fetch_prices.py              # 株価取得（Yahoo Finance）
│   ├── fetch_financials.py          # 決算取得（EDINET + XBRLP）
│   ├── fetch_tdnet.py               # 決算短信取得（TDnet Webスクレイピング + XBRLP）
│   ├── update_edinet_codes.py       # EDINETコード一括更新
│   ├── migrate.py                   # データベースマイグレーション管理
│   ├── validate_schema.py           # スキーマ検証ツール
│   └── run_daily_batch.py           # 日次バッチメイン
├── tests/                           # テストコード
│   ├── conftest.py                  # pytest共通fixture（テストDB、サンプルデータ）
│   ├── test_db_utils.py             # DB操作のテスト
│   ├── test_init_companies.py       # 銘柄マスタ初期化のテスト
│   ├── test_fetch_prices.py         # 株価取得のテスト（実API統合）
│   ├── test_fetch_financials.py     # 決算取得のテスト（実API統合）
│   ├── test_fetch_tdnet.py          # TDnet取得のテスト（実API統合）
│   ├── test_financial_views.py      # YoY/QoQビューのテスト
│   └── test_migrations.py           # マイグレーションのテスト
├── data/                            # データキャッシュ（gitignore）
│   ├── xbrl_cache/                  # XBRLリモートファイルキャッシュ
│   └── pdf/                         # 決算短信PDF保存用（将来）
├── logs/                            # ログ出力用
├── .env                             # 環境変数（gitignore）
├── .env.example                     # 環境変数テンプレート
├── README.md
└── CLAUDE.md
```

## テーブル構成

| テーブル | 説明 | 主キー |
|---------|------|--------|
| companies | 銘柄マスタ | ticker_code |
| daily_prices | 日次株価（OHLCV+調整後終値） | id, UNIQUE(ticker_code, trade_date) |
| stock_splits | 株式分割情報 | id, UNIQUE(ticker_code, split_date) |
| financials | 決算データ | id, UNIQUE(ticker_code, fiscal_year, fiscal_quarter) |
| document_analyses | 決算資料AI分析（将来用） | id |
| batch_logs | バッチ実行ログ | id |

## ビュー構成

| ビュー | 説明 |
|--------|------|
| v_latest_prices | 最新株価（銘柄情報付き） |
| v_latest_financials | 最新決算（銘柄情報付き） |
| v_financials_yoy | 前年同期比較（LAGウィンドウ関数） |
| v_financials_qoq | 前四半期比較（LAGウィンドウ関数） |

