"""
共通テストフィクスチャ

このファイルは全テストファイルで共有されるfixtureを定義します。
pytest は自動的にこのファイルを認識し、各テストで利用可能にします。

## テスト方針
このプロジェクトでは**実API統合テスト**を採用しています:
- Yahoo Finance API, EDINET API, TDnet Webスクレイピングは実際に叩く
- モック化は最小限（subprocess.run等の危険なシステムコマンドのみ）
- 理由: 本番環境と同じ動作を検証し、API仕様変更を早期発見するため
"""
import pytest
import sys
from pathlib import Path
from datetime import datetime

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import (
    get_connection,
    init_database,
    upsert_company,
    insert_financial,
    insert_daily_price,
)


@pytest.fixture(scope="function")
def test_db(tmp_path, monkeypatch):
    """
    テスト専用の一時DBを使用するfixture

    各テスト関数ごとに一時DBを作成し、本番DBに一切触れません。
    テスト終了後は tmp_path のクリーンアップで自動削除されます。

    使用例:
        def test_something(test_db):
            # テストコード（一時DB上で動作）
            pass
    """
    import db_utils
    test_db_path = tmp_path / "test_stock_agent.db"
    monkeypatch.setattr(db_utils, "DB_PATH", test_db_path)
    init_database()
    yield


@pytest.fixture
def sample_company(test_db):
    """
    テスト用銘柄データ（9999）を自動作成

    銘柄コード: 9999
    会社名: テスト株式会社
    EDINETコード: E99999

    使用例:
        def test_something(sample_company):
            ticker = sample_company
            # ticker = '9999' として利用可能
    """
    ticker = "9999"
    upsert_company(ticker, "テスト株式会社", edinet_code="E99999")
    return ticker


@pytest.fixture
def sample_company_2(test_db):
    """
    テスト用銘柄データ2（9998）を自動作成

    複数銘柄が必要なテストで使用します。

    銘柄コード: 9998
    会社名: テスト株式会社2
    EDINETコード: E99998
    """
    ticker = "9998"
    upsert_company(ticker, "テスト株式会社2", edinet_code="E99998")
    return ticker


@pytest.fixture
def sample_financials(sample_company):
    """
    テスト用決算データを自動投入

    sample_company (9999) に対して以下の決算データを投入:
    - 2023 Q1: 売上100.0億円、営業利益10.0億円、純利益7.0億円、EPS 50円
    - 2023 Q2: 売上110.0億円、営業利益12.0億円、純利益8.0億円、EPS 55円

    使用例:
        def test_something(sample_financials):
            ticker = sample_financials
            # ticker = '9999' で決算データが投入済み
    """
    ticker = sample_company
    test_data = [
        (
            "2023",
            "Q1",
            {
                "revenue": 100.0,
                "operating_income": 10.0,
                "net_income": 7.0,
                "eps": 50.0,
                "fiscal_end_date": "2022-06-30",
            },
        ),
        (
            "2023",
            "Q2",
            {
                "revenue": 110.0,
                "operating_income": 12.0,
                "net_income": 8.0,
                "eps": 55.0,
                "fiscal_end_date": "2022-09-30",
            },
        ),
    ]
    for year, quarter, data in test_data:
        insert_financial(ticker, year, quarter, **data, source="TEST")
    return ticker


@pytest.fixture
def sample_prices(sample_company):
    """
    テスト用株価データを自動投入

    sample_company (9999) に対して3日分の株価データを投入:
    - 2024-01-10: 終値1000円、高値1050円、安値980円、始値990円、出来高100万株
    - 2024-01-11: 終値1020円、高値1060円、安値1000円、始値1010円、出来高110万株
    - 2024-01-12: 終値1050円、高値1080円、安値1030円、始値1040円、出来高120万株

    使用例:
        def test_something(sample_prices):
            ticker = sample_prices
            # ticker = '9999' で株価データが投入済み
    """
    ticker = sample_company
    test_data = [
        ("2024-01-10", 1000.0, 1050.0, 980.0, 990.0, 1000000, 1000.0),
        ("2024-01-11", 1020.0, 1060.0, 1000.0, 1010.0, 1100000, 1020.0),
        ("2024-01-12", 1050.0, 1080.0, 1030.0, 1040.0, 1200000, 1050.0),
    ]
    for trade_date, close, high, low, open_, volume, adj_close in test_data:
        insert_daily_price(
            ticker_code=ticker,
            trade_date=trade_date,
            close_price=close,
            high_price=high,
            low_price=low,
            open_price=open_,
            volume=volume,
            adj_close=adj_close,
        )
    return ticker


@pytest.fixture
def multiple_companies(test_db):
    """
    複数テスト用銘柄データを自動作成

    3つの銘柄（9999, 9998, 9997）を作成します。
    バッチ処理やループ処理のテストに使用します。

    返り値: ['9999', '9998', '9997']

    使用例:
        def test_something(multiple_companies):
            tickers = multiple_companies
            # 3つの銘柄でテスト
            for ticker in tickers:
                # ...
    """
    tickers = [
        ("9999", "テスト株式会社", "E99999"),
        ("9998", "テスト株式会社2", "E99998"),
        ("9997", "テスト株式会社3", "E99997"),
    ]
    for ticker, name, edinet_code in tickers:
        upsert_company(ticker, name, edinet_code=edinet_code)
    return [t[0] for t in tickers]
