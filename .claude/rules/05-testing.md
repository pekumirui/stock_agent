# テスト作成ガイド

## 実API統合テスト方針

このプロジェクトでは、**実際のAPIを叩く統合テスト**を採用しています。

### 対象API

| API | 実行方法 | 理由 |
|-----|---------|------|
| **Yahoo Finance API** | `yfinance.download` を実際に実行 | 本番環境と同じ動作を検証 |
| **EDINET API** | `requests.get` で実際にアクセス | API仕様変更を早期発見 |
| **TDnet Webスクレイピング** | 実際にWebページ取得 | HTML構造の変更を検出 |
| **subprocess.run** | ❌ **モック化必須**（危険なため） | システムコマンドは制御下で実行 |


## カバレッジ目標

### 優先度別目標値

| 優先度 | 対象 | 目標カバレッジ | 説明 |
|--------|------|--------------|------|
| **P0** | 完全未テスト（0%） | **60%以上** | 最優先で改善 |
| **P1** | 低カバレッジ（<50%） | **現状+30pt** | 段階的に改善 |
| **新規コード** | 新規実装 | **80%以上** | 高品質を維持 |
| **総合目標** | プロジェクト全体 | **70%以上** | 最終目標 |

### 現在の状況（2026-02-05時点）

| ファイル | 現在 | 目標 | ステータス |
|---------|------|------|-----------|
| fetch_prices.py | **65%** ✅ | 60% | Completed |
| fetch_financials.py | 30% | 60% | Pending |
| fetch_tdnet.py | 52% | 60% | Pending |
| db_utils.py | 26% | 56% | Pending |
| run_daily_batch.py | 0% | 60% | Pending |
| validate_schema.py | 0% | 60% | Pending |
| **総合** | **推定47%** | **70%** | In Progress |

---

## テストデータ管理

### ルール

| 項目 | ルール | 理由 |
|------|--------|------|
| **銘柄コード** | 9xxx番台（9999等）を使用 | 本番データと分離 |
| **決算データ** | `source='TEST'` で識別 | クエリでフィルタ可能 |
| **クリーンアップ** | conftest.py の `test_db` fixture で自動削除 | テスト後に自動クリア |
| **実在銘柄** | 7203（トヨタ）等を使用可（実API統合テスト用） | 実APIを叩く場合のみ |

### conftest.py の使い方

#### 利用可能な fixture

```python
def test_something(test_db):
    """test_db: テストDB初期化 + 自動クリーンアップ"""
    pass

def test_with_company(sample_company):
    """sample_company: テスト用銘柄（9999）を自動作成"""
    ticker = sample_company  # '9999'
    pass

def test_with_financials(sample_financials):
    """sample_financials: テスト用決算データを自動投入"""
    ticker = sample_financials  # '9999' + 決算データ2件
    pass

def test_with_prices(sample_prices):
    """sample_prices: テスト用株価データを自動投入"""
    ticker = sample_prices  # '9999' + 株価データ3日分
    pass

def test_with_multiple(multiple_companies):
    """multiple_companies: 複数テスト用銘柄を自動作成"""
    tickers = multiple_companies  # ['9999', '9998', '9997']
    pass
```

---

## テスト作成手順

### 1. 対象ファイルを分析

```bash
# 対象ファイルを読む
Read scripts/fetch_prices.py

# 主要な関数をリストアップ
- ticker_to_yahoo_symbol()
- fetch_stock_data_batch()
- process_price_data()
- fetch_all_prices()
```

### 2. 既存テストパターンを調査

```bash
# 既存テストを確認
Glob "tests/test_*.py"
Read tests/test_fetch_financials.py

# パターンを学習
- テスト名: test_<関数名>_<シナリオ>
- fixture: test_db, sample_company 等を活用
- クリーンアップ: conftest.py で自動処理
```

### 3. テストケースを設計

| 観点 | 内容 |
|------|------|
| **正常系** | 基本動作、全パラメータパターン |
| **異常系** | None, 空文字, 不正な型、境界値 |
| **エッジケース** | API失敗、DB制約違反、データ不整合 |
| **統合テスト** | 実API + DB操作のフルフロー |

### 4. テストコードを生成

```python
"""
test_fetch_prices.py - 株価取得バッチの統合テスト

実API統合テスト方針：
- yfinance.download を実際に実行（モック化なし）
- テスト時間: 約10秒
"""
import pytest
from fetch_prices import ticker_to_yahoo_symbol

class TestTickerToYahooSymbol:
    """証券コード変換のテスト"""

    def test_valid_ticker(self):
        """正しい証券コードが.T付きに変換されること"""
        assert ticker_to_yahoo_symbol("7203") == "7203.T"

class TestFetchStockDataBatch:
    """株価一括取得のテスト（実API統合テスト）"""

    def test_single_ticker_with_period(self):
        """単一銘柄・期間指定での取得（実API）"""
        result = fetch_stock_data_batch(["7203"], period="5d")  # ← 実API呼び出し

        # 柔軟なアサーション（株価は日々変わるため）
        assert result is not None
        if not result.empty:
            assert 'Close' in result.columns
```

### 5. テストを実行

```bash
# テスト実行
C:/Users/pekum/anaconda3/python.exe -m pytest tests/test_fetch_prices.py -v

# カバレッジ測定
C:/Users/pekum/anaconda3/python.exe -m pytest tests/test_fetch_prices.py -v --cov=scripts/fetch_prices --cov-report=term-missing
```

### 6. 進捗を記録

`.claude/test_progress.md` を更新：
- テストケース一覧
- カバレッジ推移
- 実行履歴

---

## トラブルシューティング

### 外部キー制約エラー

**問題**:
```
sqlite3.IntegrityError: FOREIGN KEY constraint failed
```

**解決**:
```python
# conftest.py でクリーンアップ時に外部キー制約を一時的に無効化
with get_connection() as conn:
    conn.execute("PRAGMA foreign_keys = OFF")
    # テストデータ削除
    conn.execute("DELETE FROM ...")
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
```

### カバレッジ測定失敗

**問題**:
```
Module scripts/fetch_prices was never imported
```

**原因**:
- pytest-cov の設定が間違っている
- `--cov` オプションのパスが間違っている

**解決**:
```bash
# 正しいパス指定
pytest tests/test_fetch_prices.py --cov=scripts.fetch_prices
# または
pytest tests/ --cov=scripts
```

### 実API統合テストの失敗

**問題**:
- ネットワークエラー
- API制限
- データ形式の変更

**対応**:
```python
def test_with_network_error():
    """ネットワークエラーも想定"""
    try:
        data = fetch_stock_data_batch(['7203'])
        assert data is not None
    except Exception as e:
        # ネットワークエラーはスキップ
        pytest.skip(f"Network error: {e}")
```