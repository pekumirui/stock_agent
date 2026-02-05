"""
db_utils.py のテスト
"""
import pytest
import sys
from pathlib import Path

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import get_connection, init_database, get_all_tickers


class TestDatabaseConnection:
    """DB接続のテスト"""

    def test_get_connection(self):
        """接続が取得できること"""
        with get_connection() as conn:
            assert conn is not None

    def test_init_database(self):
        """DB初期化が正常に実行されること"""
        # エラーが発生しなければOK
        init_database()


class TestCompanies:
    """銘柄マスタ関連のテスト"""

    def test_get_all_tickers(self):
        """銘柄コード一覧が取得できること"""
        tickers = get_all_tickers()
        assert isinstance(tickers, list)

    def test_ticker_format(self):
        """銘柄コードが4-5桁数字であること"""
        tickers = get_all_tickers()
        for ticker in tickers[:10]:  # 最初の10件をチェック
            assert 4 <= len(ticker) <= 5, f"銘柄コードは4-5桁: {ticker}"
            assert ticker.isdigit(), f"銘柄コードは数字のみ: {ticker}"


class TestDataIntegrity:
    """データ整合性のテスト"""

    def test_no_orphan_prices(self):
        """銘柄マスタにない株価データがないこと"""
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM daily_prices dp
                LEFT JOIN companies c ON dp.ticker_code = c.ticker_code
                WHERE c.ticker_code IS NULL
            """)
            orphan_count = cursor.fetchone()[0]
            assert orphan_count == 0, f"孤立した株価データ: {orphan_count}件"

    def test_no_orphan_financials(self):
        """銘柄マスタにない決算データがないこと"""
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM financials f
                LEFT JOIN companies c ON f.ticker_code = c.ticker_code
                WHERE c.ticker_code IS NULL
            """)
            orphan_count = cursor.fetchone()[0]
            assert orphan_count == 0, f"孤立した決算データ: {orphan_count}件"
