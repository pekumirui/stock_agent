# ワークフロー・Git/PR運用ルール

## PR作成
- プロジェクトは以下のファイルを変更したら、テスト・ドキュメント更新まで完了後に自動でPRを作成する
- PR作成にはpull-request-creatorサブエージェントを使う。`/pr` コマンドでも手動実行可能
- PRにはコード・テスト・ドキュメント変更を全て含めてから作成する（PR作成後にdocs追加しない）
- CLAUDE.md や rules/ のみの変更はPR不要（直接mainにコミット可）

## サブエージェントの使い分け
- `web/` 配下の変更（FastAPI, テンプレート, CSS, JS）→ **web-layer-builder** を使う
- `db/`, `scripts/` 配下の変更（スキーマ, マイグレーション, バッチ）→ **data-layer-builder** を使う
- ドキュメント更新 → **docs-updater** を使う
- PR作成 → **pull-request-creator** を使う
- 上記に該当しない汎用タスクのみ general-purpose を使う
- DB変更とWeb変更が両方必要な場合は data-layer-builder と web-layer-builder を**並行起動**する

## Codexレビュー
- **実装プラン策定時**: plan modeで実装方針を固めたら、ExitPlanMode前にCodexに設計レビューを依頼する
  - エッジケース・パフォーマンス・代替アプローチの観点でフィードバックを得る
  - 指摘があればプランに反映してからExitPlanModeする
- **PR作成前**: コード変更（テスト・ドキュメントのみの変更を除く）がある場合、PR作成前にCodexレビューを実施する
  - バグ・エッジケース・リグレッションリスク・テストカバレッジ不足の観点でレビュー
  - Critical/High指摘は修正してからPRを作成する
- サブエージェント（data-layer-builder, web-layer-builder）も各自の完了条件にCodexレビューを含む
- Codexスキル（`/codex`）を使用して実行する

## 検証時の禁止事項
- 検証・テスト目的でバッチスクリプト（fetch_*.py等）を実行してDBデータを追加・更新しないこと
- 本番DBへのデータ投入はユーザーが手動で行う