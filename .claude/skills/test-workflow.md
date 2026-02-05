# /test-workflow スキル

テストケース生成 → レビュー → 実行のフルワークフローを自動実行するスキルです。

## 使い方
```bash
/test-workflow <ファイルパス>        # 指定ファイルのテストを生成
/test-workflow scripts/fetch_prices.py  # fetch_prices.py のテスト生成例
/test-workflow --all                # 全未テストファイルを対象（P0優先）
```

## 実行内容

<test-workflow-skill>
テストワークフロースキルが呼び出されました。

### 実行フロー

1. **Phase 1: テストケース生成**
   - `test-case-generator` エージェントを起動
   - 変更ファイルまたは指定ファイルを分析
   - 既存テストパターンを学習
   - 網羅的なテストコードを生成（tests/test_xxx.py）

2. **Phase 2: テストレビュー**
   - `test-reviewer` エージェントを起動
   - 生成されたテストをチェックリスト評価
   - カバレッジ測定（pytest --cov）
   - **結果判定**:
     - 合格 → Phase 3 へ
     - 要改善 → Phase 1 へ戻る（フィードバック付き、最大3回）

3. **Phase 3: テスト実行**
   - `test-runner` エージェントを起動
   - pytest でテスト実行
   - カバレッジレポート生成

4. **Phase 4: 進捗記録**
   - `.claude/test_progress.md` に結果を追記
   - カバレッジ推移を更新
   - 実行履歴を記録

### カバレッジ目標

- **P0（完全未テスト）**: 60%以上
- **P1（低カバレッジ）**: 現状+30ポイント
- **新規コード**: 80%以上

### 失敗時の処理

- **テスト実行失敗**: エラーログを進捗ファイルに記録し、ユーザーに報告
- **レビュー不合格（3回連続）**: 生成されたテストを保存し、手動確認を促す
- **エージェント起動失敗**: エラーメッセージを表示し、中断

### 進捗確認

- `.claude/test_progress.md` を参照
- カバレッジ推移、テストケース一覧、実行履歴を可視化
- P0/P1 ファイルのステータス（Pending/In Progress/Completed）

### 注意事項

- 初回実行時は `.claude/test_progress.md` が存在することを確認
- **実API統合テスト方針**: Yahoo Finance、EDINET、TDnetは実際にAPIを叩いてテスト
- 実行には数分～十数分かかる場合があります（生成→レビュー→実行 + 実API呼び出し）
- API制限に注意: Yahoo Financeは頻繁なアクセスで制限される可能性あり
- 並行実行はサポートしていません（1ファイルずつ順次実行）

### ⚠️ カスタムエージェントの制限（重要）

**現状**: `.claude/agents/` に作成した `test-case-generator.yml` と `test-reviewer.yml` は**Claude Codeに認識されません**。

利用可能なエージェント：
- `Bash` - コマンド実行
- `general-purpose` - 汎用エージェント
- `Explore` - コードベース探索
- `Plan` - 実装計画

**対応**：サブエージェント方式ではなく、以下の手動ワークフローで実行してください。

### 手動実行ワークフロー

```
1. 対象ファイルを分析
   - Read scripts/fetch_prices.py
   - 主要な関数をリストアップ

2. 既存テストパターンを調査
   - Glob "tests/test_*.py"
   - Read tests/test_fetch_financials.py

3. テストコード生成
   - Write tests/test_xxx.py
   - 実API統合テスト方針に従う
   - conftest.py の fixture を活用

4. テスト実行
   - Bash: pytest tests/test_xxx.py -v

5. 進捗記録
   - Edit .claude/test_progress.md
   - カバレッジ推移を更新
```

詳細は [05-testing.md](.claude/rules/05-testing.md) を参照してください。

### 例

```bash
# fetch_prices.py のテスト生成
/test-workflow scripts/fetch_prices.py

# 実行結果イメージ:
# Phase 1: テストケース生成中...
# → 6ケース生成完了
# Phase 2: テストレビュー中...
# → 合格（チェックリスト 92%, カバレッジ 62%）
# Phase 3: テスト実行中...
# → 6/6 passed
# Phase 4: 進捗記録完了
# → .claude/test_progress.md 更新
```

### ワークフロー実装

以下の手順でワークフローを実行します:

```
ユーザー
  ↓ /test-workflow <file>
Claude
  ↓ (1) test-case-generator 起動
  ↓ テストコード生成
  ↓ (2) test-reviewer 起動
  ↓ レビュー → 合格/要改善判定
  ├─ 合格 → (3) test-runner 起動
  │   ↓ pytest 実行
  │   ↓ (4) test_progress.md 更新
  │   ↓ 完了
  └─ 要改善 → (1) へ戻る（最大3回）
      ↓ 3回失敗
      ↓ ユーザーにエスカレーション
```

### 自動再試行ロジック

レビューが不合格の場合、以下のロジックで自動再試行します:

1. **1回目**: test-case-generator が初回生成
2. **test-reviewer が要改善判定** → フィードバックを test-case-generator に渡す
3. **2回目**: フィードバックを反映してテスト再生成
4. **test-reviewer が要改善判定** → 再度フィードバック
5. **3回目**: さらに改善して再生成
6. **test-reviewer が要改善判定** → **ユーザーにエスカレーション**

### リソース

- エージェント設定: `.claude/agents/test-case-generator.yml`, `test-reviewer.yml`, `test-runner.yml`
- 進捗管理: `.claude/test_progress.md`
- 共通fixture: `tests/conftest.py`

</test-workflow-skill>

---

## 実装ガイド（Claude Code用）

このスキルが呼び出されたら、以下の手順を実行してください:

### Step 1: 引数解析
```python
# ユーザー入力から対象ファイルを特定
if args == "--all":
    # P0ファイル（fetch_prices.py, run_daily_batch.py, validate_schema.py）を順次処理
    targets = ["scripts/fetch_prices.py", "scripts/run_daily_batch.py", "scripts/validate_schema.py"]
else:
    targets = [args]  # 指定ファイル
```

### Step 2: 各ファイルに対してワークフロー実行
```python
for target_file in targets:
    retry_count = 0
    max_retries = 3
    feedback = None

    while retry_count < max_retries:
        # Phase 1: テストケース生成
        result = Task(
            subagent_type="test-case-generator",
            prompt=f"対象ファイル: {target_file}\n{feedback or '初回生成'}"
        )

        # Phase 2: テストレビュー
        review_result = Task(
            subagent_type="test-reviewer",
            prompt=f"生成されたテスト: tests/test_{basename}.py\nレビューしてください"
        )

        if review_result.status == "合格":
            # Phase 3: テスト実行
            test_result = Task(
                subagent_type="test-runner",
                prompt=f"tests/test_{basename}.py を実行してください"
            )

            # Phase 4: 進捗記録
            update_progress(target_file, test_result)
            break
        else:
            feedback = review_result.feedback
            retry_count += 1

    if retry_count == max_retries:
        # ユーザーにエスカレーション
        print(f"❌ {target_file} のテスト生成が3回失敗しました。手動確認が必要です。")
```

### Step 3: 進捗記録更新
```python
def update_progress(target_file, test_result):
    # .claude/test_progress.md に結果を追記
    # - テストケース一覧更新
    # - カバレッジ推移更新
    # - 実行履歴追記
    pass
```

---

## 参考: P0/P1 優先度

### P0（完全未テスト）- 最優先
1. `scripts/fetch_prices.py` (270行) - 目標: 60%
2. `scripts/run_daily_batch.py` (168行) - 目標: 60%
3. `scripts/validate_schema.py` (335行) - 目標: 60%

### P1（低カバレッジ）- 次優先
1. `scripts/db_utils.py` (253行) - 現状: 26%, 目標: 56%
2. `scripts/fetch_financials.py` (598行) - 現状: 30%, 目標: 60%
3. `scripts/fetch_tdnet.py` (543行) - 現状: 52%, 目標: 60%
