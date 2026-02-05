# /test スキル

テストを実行するスキルです。

## 使い方
- `/test` - 全テストを実行
- `/test db` - DB関連のテストのみ
- `/test fetch` - データ取得系のテストのみ
- `/test <ファイル名>` - 特定のテストファイルを実行

## 実行内容

<test-skill>
テスト実行スキルが呼び出されました。

### 手順
1. `tests/` ディレクトリのテストファイルを確認
2. 引数に応じてテスト範囲を決定
3. pytest でテストを実行
4. 結果をサマリーで報告

### コマンド
```bash
# 引数なしの場合
python -m pytest tests/ -v --tb=short

# 引数ありの場合
python -m pytest tests/test_${args}.py -v --tb=short
```

### 注意事項
- テストがない場合は `tests/` ディレクトリに作成を提案
- 失敗したテストがあれば原因を分析して報告
</test-skill>
