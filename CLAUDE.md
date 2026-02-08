# 株式データ収集バッチ - Claude Code用プロジェクト設定

## プロジェクト概要

日本株の株価・決算データを収集・蓄積するバッチシステム。

詳細なガイドラインは `.claude/rules/` を参照
## 現在の状態

### 完了
- [x] SQLiteスキーマ設計（`db/schema.sql`）
- [x] DB操作ユーティリティ（`scripts/db_utils.py`）
- [x] 銘柄マスタ初期化スクリプト（`scripts/init_companies.py`）
- [x] 株価取得バッチ - Yahoo Finance（`scripts/fetch_prices.py`）
- [x] 決算取得バッチ - EDINET（`scripts/fetch_financials.py`）
- [x] 日次バッチ実行スクリプト（`scripts/run_daily_batch.py`）
- [x] XBRLパーサーをXBRLP（`lib/xbrlp/`）に置換
- [x] 前年同期比較（YoY）・前四半期比較（QoQ）のSQLビュー追加
- [x] 決算資料分析テーブル（`document_analyses`）追加（将来のPDF/AI分析用）
- [x] TDnet決算短信取得バッチ（`scripts/fetch_tdnet.py`）- Webスクレイピング + XBRL解析
- [x] EDINETコード一括更新スクリプト（`scripts/update_edinet_codes.py`）- ドキュメントリストAPIから収集
- [x] **EDINETコード更新の効率化**（`scripts/update_edinet_codes.py`）- 未登録銘柄のみを対象に事前フィルタリング
- [x] 実API統合テスト導入（`tests/test_fetch_prices.py`）- Yahoo Finance APIを実際に叩いてテスト
- [x] EDINETコード欠損分析スクリプト（`scripts/analyze_missing_edinet.py`）- カテゴリ分類・優先度判定・CSV出力
- [x] スキーマ検証ツール（`scripts/validate_schema.py`）- yfinanceデータでスキーマ適合性検証
- [x] IFRS/US-GAAP企業のP/L検出パターン修正（`scripts/fetch_tdnet.py`）- 全会計基準対応
- [x] DB操作ロジック改善（`scripts/db_utils.py`）- ticker_exists()追加、FOREIGN KEY違反の事前チェック
- [x] 包括的テストスイート（`tests/`）- 9テストファイル、実API統合テスト採用
- [x] **EDINET決算取得の信頼性改善**（`scripts/fetch_financials.py`）- マニフェスト誤選択・引数型・半期報告書対応を修正
- [x] **TDnet決算短信の日次バッチ統合**（`scripts/run_daily_batch.py`）- Q1/Q3法改正対応、`--skip-tdnet`フラグ追加

## 次のタスク（優先順）


## ワークフロー

### PR作成
- コード変更が完了したら、pull-request-creatorサブエージェントでPR作成を提案する
- `/pr` コマンドでも手動実行可能
