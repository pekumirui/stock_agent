# アーキテクチャ

## ディレクトリ構成

```
stock_agent/
├── .claude/
│   ├── agents/
│   │   ├── code-reviewer.md          # コードレビューエージェント
│   │   └── docs-updater.md           # ドキュメント更新エージェント
│   ├── rules/
│   │   └── coding-standards.md       # コーディング規約
│   ├── skills/
│   │   ├── test.md                   # /test スキル
│   │   ├── pr.md                     # /pr スキル（PR作成）
│   │   └── validate.md              # /validate スキル
│   └── settings.local.json          # ローカル設定
├── db/
│   ├── schema.sql                   # DBスキーマ定義
│   ├── stock_agent.db               # SQLiteデータベース
│   ├── yoyo.ini                     # yoyo-migrations設定
│   └── migrations/                  # DBマイグレーションファイル
│       └── V001__baseline.sql       # ベースラインマイグレーション
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
│   ├── fetch_tdnet.py               # 決算短信取得（TDnet + XBRLP）
│   ├── fetch_jquants_fins.py       # 決算取得（J-Quants API）
│   ├── update_edinet_codes.py       # EDINETコード一括更新
│   ├── analyze_missing_edinet.py    # EDINETコード欠損分析
│   ├── validate_schema.py           # スキーマ検証ツール
│   ├── migrate.py                   # DBマイグレーション管理
│   ├── migrate_tdnet_cache_layout.py # TDnet XBRLキャッシュ構造移行（フラット→日付フォルダ）
│   ├── run_price_batch.py           # 株価取得バッチ
│   └── run_disclosure_batch.py     # 開示データ取得バッチ（EDINET+TDnet）
├── web/                             # Webビューア
│   ├── app.py                       # FastAPIメインアプリ
│   ├── routers/
│   │   └── viewer.py                # ビューアルーター（4エンドポイント）
│   ├── services/
│   │   └── financial_service.py    # ビジネスロジック（get_financial_history含む）
│   ├── templates/                   # Jinja2テンプレート
│   │   ├── base.html
│   │   ├── viewer.html              # 3エリアGrid（メイン+AIコメント+業績詳細）
│   │   └── partials/                # htmx部分更新用
│   │       ├── table_body.html      # メインテーブル本体
│   │       ├── detail_row.html      # （旧）展開行パーシャル
│   │       └── financial_detail.html # 業績詳細パネル
│   └── static/
│       ├── css/viewer.css           # ダークテーマCSS
│       └── js/viewer.js             # Alpine.jsアプリ
├── tests/                           # テストコード
│   ├── conftest.py                  # pytest共通fixture
│   ├── test_db_utils.py             # DB操作のテスト
│   ├── test_init_companies.py       # 銘柄マスタ初期化のテスト
│   ├── test_fetch_prices.py         # 株価取得のテスト（実API統合）
│   ├── test_fetch_financials.py     # 決算取得のテスト
│   ├── test_fetch_tdnet.py          # TDnet取得のテスト
│   ├── test_fetch_jquants_fins.py   # J-Quants決算取得のテスト
│   ├── test_update_edinet_codes.py  # EDINETコード更新のテスト
│   ├── test_analyze_missing_edinet.py # 欠損分析のテスト
│   ├── test_financial_views.py      # YoY/QoQビューのテスト
│   └── test_migrations.py           # マイグレーションのテスト
├── data/                            # データキャッシュ（gitignore）
│   ├── xbrl_cache/                  # XBRLリモートファイルキャッシュ
│   ├── edinet_cache/                # EDINET XBRL ZIPキャッシュ
│   ├── tdnet_xbrl_cache/            # TDnet XBRL ZIPキャッシュ（日付フォルダ構造）
│   │   └── YYYY-MM-DD/              # 発表日フォルダ
│   │       ├── *.zip                # XBRL ZIPファイル
│   │       └── _complete.marker     # 当日分の取得完了マーカー
│   └── csv/                         # CSV出力用
├── docs/
│   ├── architecture.md              # アーキテクチャ（本ファイル）
│   └── commands.md                  # 開発コマンドリファレンス
├── logs/                            # ログ出力用
├── .env                             # 環境変数（gitignore）
├── requirements.txt                 # 依存パッケージ
├── README.md
└── CLAUDE.md
```

## テーブル構成

| テーブル | 説明 | 主キー |
|---------|------|--------|
| companies | 銘柄マスタ | ticker_code |
| daily_prices | 日次株価（OHLCV+調整後終値） | id, UNIQUE(ticker_code, trade_date) |
| stock_splits | 株式分割情報 | id, UNIQUE(ticker_code, split_date) |
| financials | 決算データ（fiscal_end_date必須化） | id, UNIQUE(ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date) |
| announcements | 適時開示（決算/業績修正/配当） | id, UNIQUE(ticker_code, announcement_date, type, fiscal_year, fiscal_quarter) |
| management_forecasts | 業績予想 | id, UNIQUE(ticker_code, fiscal_year, fiscal_quarter, announced_date) |
| consensus_estimates | コンセンサス予想（スキーマのみ） | id, UNIQUE(ticker_code, fiscal_year, fiscal_quarter, as_of_date) |
| document_analyses | 決算資料AI分析（将来用） | id |
| batch_logs | バッチ実行ログ | id |

## ビュー構成

| ビュー | 説明 |
|--------|------|
| v_latest_prices | 最新株価（銘柄情報付き） |
| v_latest_financials | 最新決算（銘柄情報付き） |
| v_financials_yoy | 前年同期比較（LAGウィンドウ関数） |
| v_financials_qoq | 前四半期比較（LAGウィンドウ関数） |
| v_financials_standalone_quarter | 単独四半期算出(累積値から差分計算、`has_prev_quarter`フラグで前四半期データ有無を判定) |

## TDnet XBRLキャッシュの仕組み

`data/tdnet_xbrl_cache/` は発表日ごとのフォルダ構造で管理される。

### フォルダ構造

```
tdnet_xbrl_cache/
└── 2025-11-14/
    ├── 12345678_0001.zip       # XBRL ZIPファイル（適時開示ID）
    ├── 98765432_0001.zip
    └── _complete.marker        # 当日分の取得完了マーカー
```

### `_complete.marker` の役割

- HTML取得フロー（TDnet Webスクレイピング）が正常完了した日に作成される
- `--ticker` 指定実行時（部分取得）ではマーカーは作成されない
- `--days N` 実行時、マーカーが存在する日はHTML取得をスキップしてキャッシュからDB投入する

### `--days N` 実行フロー

```
対象日ごとに:
  _complete.marker が存在する → キャッシュ内ZIPを直接パース → DB投入（HTML取得スキップ）
  _complete.marker が存在しない → TDnet HTMLスクレイピング → XBRL取得・保存 → DB投入 → マーカー作成
```

### 既存キャッシュの移行

過去にフラット構造（`data/tdnet_xbrl_cache/*.zip`）で保存されたZIPは `migrate_tdnet_cache_layout.py` で日付フォルダに移行できる。
移行手順は `docs/commands.md` の「TDnet XBRLキャッシュ移行」セクションを参照。

