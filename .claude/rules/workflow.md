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

## 検証時の禁止事項
- 検証・テスト目的でバッチスクリプト（fetch_*.py等）を実行してDBデータを追加・更新しないこと
- 本番DBへのデータ投入はユーザーが手動で行う