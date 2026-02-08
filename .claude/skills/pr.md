---
description: プルリクエストを作成する
user-invocable: true
---

<pr-skill>
pull-request-creatorサブエージェントを使用してプルリクエストを作成してください。

Task toolを以下のパラメータで呼び出してください：
- subagent_type: "pull-request-creator"
- prompt: 現在の変更内容を確認し、適切なPRを作成してください

## マージ後のブランチ削除

PRがマージされた後は、以下の手順でブランチを削除してください：
1. mainブランチに切り替え: `git checkout main && git pull origin main`
2. マージ済みローカルブランチを一括削除: `git fetch --prune && git branch --merged main | grep -v main | xargs git branch -d`
</pr-skill>
