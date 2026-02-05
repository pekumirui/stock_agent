# /validate スキル

データベースの状態とデータ整合性を検証するスキルです。

## 使い方
- `/validate` - 全体の検証
- `/validate db` - DB状態のみ確認
- `/validate prices` - 株価データの検証
- `/validate financials` - 決算データの検証

## 実行内容

<validate-skill>
データ検証スキルが呼び出されました。

### 検証項目
1. **銘柄マスタ（companies）**
   - 登録銘柄数
   - アクティブ銘柄数

2. **株価データ（daily_prices）**
   - 総レコード数
   - 最新日付
   - データ欠損チェック

3. **決算データ（financials）**
   - 総レコード数
   - 最新決算期

4. **整合性**
   - 外部キー参照の確認
   - 孤立データのチェック

### 検証コマンド
```python
from scripts.db_utils import get_connection

with get_connection() as conn:
    # 各テーブルの状態を確認
    companies = conn.execute("SELECT COUNT(*) FROM companies WHERE is_active=1").fetchone()[0]
    prices = conn.execute("SELECT COUNT(*), MAX(trade_date) FROM daily_prices").fetchone()
    financials = conn.execute("SELECT COUNT(*) FROM financials").fetchone()[0]

    # 孤立データチェック（株価に銘柄マスタがない）
    orphans = conn.execute("""
        SELECT COUNT(*) FROM daily_prices dp
        LEFT JOIN companies c ON dp.ticker_code = c.ticker_code
        WHERE c.ticker_code IS NULL
    """).fetchone()[0]
```
</validate-skill>
