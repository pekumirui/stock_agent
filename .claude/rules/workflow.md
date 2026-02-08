# ワークフロー・Git/PR運用ルール

## PR作成
- `scripts/`、`lib/`、`tests/`、`db/` 配下のファイルを変更したら、テスト・ドキュメント更新まで完了後に自動でPRを作成する
- PR作成にはpull-request-creatorサブエージェントを使う。`/pr` コマンドでも手動実行可能
- PRにはコード・テスト・ドキュメント変更を全て含めてから作成する（PR作成後にdocs追加しない）
- CLAUDE.md や rules/ のみの変更はPR不要（直接mainにコミット可）

## Git & GitHub Authentication
- GitHub Fine-grained PATs require explicit 'Contents: Read and write' permission for push operations. Never assume a token has push access—verify with `gh auth status` first.
- Always ensure `git config user.name` and `git config user.email` are set before committing.
- Prefer `gh auth login --with-token` for non-interactive auth. Do not attempt browser-based flows in this environment.
- Before pushing, verify auth with `gh auth status` and confirm push permissions.
