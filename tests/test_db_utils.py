"""
db_utils.py のテスト
"""
import pytest
import sys
from pathlib import Path

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import get_connection, init_database, get_all_tickers, is_valid_ticker_code


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


class TestTickerValidation:
    """証券コード検証のテスト"""

    def test_valid_4digit_numeric(self):
        """4桁数字コードは有効"""
        assert is_valid_ticker_code("7203") is True
        assert is_valid_ticker_code("6758") is True
        assert is_valid_ticker_code("1234") is True

    def test_valid_5digit_numeric(self):
        """5桁数字コードは有効"""
        assert is_valid_ticker_code("12345") is True
        assert is_valid_ticker_code("98765") is True

    def test_valid_alphanumeric(self):
        """4桁数字+英字1文字は有効"""
        assert is_valid_ticker_code("285A") is True
        assert is_valid_ticker_code("200A") is True
        assert is_valid_ticker_code("346A") is True
        assert is_valid_ticker_code("123Z") is True
        assert is_valid_ticker_code("9999B") is True

    def test_valid_lowercase_alpha(self):
        """4桁数字+小文字英字も有効"""
        assert is_valid_ticker_code("285a") is True
        assert is_valid_ticker_code("200a") is True

    def test_invalid_too_short(self):
        """3桁以下は無効"""
        assert is_valid_ticker_code("123") is False
        assert is_valid_ticker_code("12") is False
        assert is_valid_ticker_code("1") is False

    def test_invalid_too_long(self):
        """6桁以上は無効"""
        assert is_valid_ticker_code("123456") is False
        assert is_valid_ticker_code("12345A") is False  # 5桁数字+英字

    def test_invalid_pure_alpha(self):
        """英字のみは無効"""
        assert is_valid_ticker_code("ABCD") is False
        assert is_valid_ticker_code("ABCDE") is False

    def test_invalid_mixed_position(self):
        """英字が途中に入っているのは無効"""
        assert is_valid_ticker_code("12A34") is False
        assert is_valid_ticker_code("A1234") is False
        assert is_valid_ticker_code("1A234") is False

    def test_invalid_multiple_alpha(self):
        """英字が2文字以上は無効"""
        assert is_valid_ticker_code("123AB") is False
        assert is_valid_ticker_code("12ABC") is False

    def test_empty_and_none(self):
        """空文字・Noneは無効"""
        assert is_valid_ticker_code("") is False
        assert is_valid_ticker_code(None) is False

    def test_whitespace_trimmed(self):
        """空白は除去される"""
        assert is_valid_ticker_code(" 7203 ") is True
        assert is_valid_ticker_code(" 285A ") is True
        assert is_valid_ticker_code("   ") is False
