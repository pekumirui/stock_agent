# 株式データ収集バッチ - Claude Code用プロジェクト設定

## プロジェクト概要

日本株の株価・決算データを収集・蓄積するバッチシステム。

詳細なガイドラインは `.claude/rules/` を参照:
- `01-architecture.md` - ディレクトリ構成・テーブル構成
- `02-coding-standards.md` - コーディング規約・注意事項
- `03-commands.md` - 開発コマンド・テスト
- `04-workflow.md` - ワークフロー・スキル
- `05-testing.md` - テスト作成ガイド・実API統合テスト方針

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
- [x] テスト進捗管理（`.claude/test_progress.md`）- カバレッジ推移・テストケース一覧の可視化

### テスト進捗管理
- `.claude/test_progress.md` でカバレッジ推移・テストケース一覧を可視化
- 現在の総カバレッジ: **推定47%**（fetch_prices.py 完了により改善）


## 次のタスク（優先順）


