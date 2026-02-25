---
name: pr
description: Use when changes are ready for review - creates a pull request after verifying tests pass and docs are updated
---

<pr-skill>
pull-request-creatorサブエージェントを使用してプルリクエストを作成してください。

## PR作成前チェック
- ドキュメント更新が必要なら先にコミット済みであること
- CLAUDE.md や rules/ のみの変更はPR不要（直接mainにコミット可）

## 実行
Task toolを以下のパラメータで呼び出してください：
- subagent_type: "pull-request-creator"
- prompt: 現在のブランチの変更内容を確認し、mainブランチに対するPRを作成してください。PRにはコード・テスト・ドキュメント変更を全て含めてください。

## マージ後のブランチ削除
1. `git checkout main && git pull origin main`
2. `git fetch --prune && git branch --merged main | grep -v main | xargs git branch -d`
</pr-skill>
