# data-layer-builder エージェントメモリ

## プロジェクト固有規約（重要）

- DB: `db/stock_agent.db`（`stock_data.db`は旧DB）
- Python: `venv/bin/python`（`python`コマンドなし）
- テスト: `cd /home/pekumirui/stock_agent && venv/bin/python -m pytest tests/`
- xbrlp: `lib/xbrlp/`（pip管理外）

## テスト注意事項

- `insert_financial()` には `fiscal_end_date` 必須（NOT NULL制約）
- financials UNIQUE制約: `(ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date)` の4カラム
- management_forecasts UNIQUE制約: `(ticker_code, fiscal_year, fiscal_quarter, announced_date)` の4カラム
- `get_connection()` は自動commitしない。直接SQL実行時は `conn.commit()` 必須
- テストでは必ず `test_db` フィクスチャを使い本番DBに触れない
- テスト用銘柄コードは 9xxx 番台を使用

## SQLiteマイグレーション注意

- `_yoyo_migration` PRIMARY KEYは `migration_hash`（`migration_id`ではない）
- 手動マーク: `INSERT INTO _yoyo_migration (migration_hash, migration_id, applied_at_utc) VALUES ('V007__...', 'V007__...', '...')`
- `executescript()` はDDL途中失敗でロールバックしない

## SOURCE_PRIORITY

```python
SOURCE_PRIORITY = {'EDINET': 3, 'TDnet': 2, 'JQuants': 2, 'yfinance': 1}
```

## fetch_financials.py パターン

- 新関数の追加位置: `_parse_xbrl_legacy()` の前
- 予想用: `XBRL_FORECAST_MAPPING`, `_is_forecast_context()`, `parse_ixbrl_forecast()`, `extract_forecast_fiscal_year()`（xbrl_common.py）
- `_is_forecast_context()` は `NextYear`/`NextAccumulatedQ2` を含むコンテキストのみ対象
- `ForecastMember` 判定に加えて `NextYear` の有無も確認（前期予想を除外）

## parse_ixbrl_forecast() テスト方法

```python
from unittest.mock import patch, MagicMock
mock_parser = MagicMock()
mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
mock_parser.load_facts.return_value = mock_facts
with patch('fetch_financials.Parser', return_value=mock_parser):
    result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])
```
