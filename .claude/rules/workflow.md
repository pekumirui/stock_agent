# ワークフロー・Git/PR運用ルール

## PR作成
- コード変更が完了したら、pull-request-creatorサブエージェントでPR作成を提案する
- `/pr` コマンドでも手動実行可能
- PRにはコード・テスト・ドキュメント変更を全て含めてから作成する（PR作成後にdocs追加しない）

## Git & GitHub Authentication
- GitHub Fine-grained PATs require explicit 'Contents: Read and write' permission for push operations. Never assume a token has push access—verify with `gh auth status` first.
- Always ensure `git config user.name` and `git config user.email` are set before committing.
- Prefer `gh auth login --with-token` for non-interactive auth. Do not attempt browser-based flows in this environment.
- Before pushing, verify auth with `gh auth status` and confirm push permissions.
