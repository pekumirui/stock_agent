"""
db_utils.py のテスト
"""
import pytest
import sys
from pathlib import Path

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import get_connection, init_database, get_all_tickers, is_valid_ticker_code, insert_financial


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


class TestTickerExistence:
    """ticker_exists() のテスト"""

    def test_ticker_exists_true(self, sample_company):
        """登録済み銘柄は True を返す"""
        from db_utils import ticker_exists
        assert ticker_exists("9999") is True

    def test_ticker_exists_false(self, test_db):
        """JPXリスト外の銘柄は False を返す"""
        from db_utils import ticker_exists
        assert ticker_exists("6655") is False

    def test_ticker_exists_empty_string(self, test_db):
        """空文字列は False を返す"""
        from db_utils import ticker_exists
        assert ticker_exists("") is False


class TestInsertFinancialWithOutOfScopeTicker:
    """JPXリスト外の銘柄に対する insert_financial() のテスト"""

    def test_insert_financial_out_of_scope_ticker(self, test_db):
        """JPXリスト外の銘柄は False を返し、データは挿入されない"""
        result = insert_financial(
            ticker_code='6655',  # 名証M銘柄
            fiscal_year='2024',
            fiscal_quarter='Q1',
            revenue=100.0,
            source='TDnet'
        )
        assert result is False

        # DBに保存されていないことを確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) as cnt FROM financials WHERE ticker_code='6655'"
            )
            row = cursor.fetchone()
            assert row['cnt'] == 0

    def test_insert_financial_registered_ticker(self, sample_company):
        """登録済み銘柄は True を返し、データが挿入される"""
        result = insert_financial(
            ticker_code='9999',  # 登録済み
            fiscal_year='2024',
            fiscal_quarter='Q1',
            revenue=100.0,
            source='TDnet'
        )
        assert result is True

        # DBに保存されていることを確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT revenue FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            )
            row = cursor.fetchone()
            assert row is not None
            assert row['revenue'] == 100.0


class TestInsertFinancialCoalesce:
    """insert_financial の COALESCE 動作テスト（Noneで既存データが上書きされないこと）"""

    def test_null_does_not_overwrite_existing(self, sample_company):
        """Noneフィールドが既存の非Null値を上書きしないこと"""
        # 1回目: 全フィールドを持つデータを挿入
        insert_financial(
            ticker_code='9999',
            fiscal_year='2024',
            fiscal_quarter='FY',
            revenue=1000.0,
            gross_profit=400.0,
            operating_income=200.0,
            ordinary_income=210.0,
            net_income=150.0,
            eps=100.0,
            source='TDnet',
        )

        # 2回目: 一部フィールドがNoneのデータで上書き（訂正版を想定）
        insert_financial(
            ticker_code='9999',
            fiscal_year='2024',
            fiscal_quarter='FY',
            revenue=1050.0,       # 訂正で変更
            gross_profit=None,    # 訂正版にはない
            operating_income=None,
            ordinary_income=220.0,  # 訂正で変更
            net_income=160.0,       # 訂正で変更
            eps=None,             # 訂正版にはない
            source='TDnet',
        )

        # 検証: Noneフィールドは元の値が保持され、非Noneフィールドは更新される
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='FY'"
            ).fetchone()
            assert row['revenue'] == 1050.0, "非Null値は更新されるべき"
            assert row['gross_profit'] == 400.0, "Noneで既存値が上書きされてはいけない"
            assert row['operating_income'] == 200.0, "Noneで既存値が上書きされてはいけない"
            assert row['ordinary_income'] == 220.0, "非Null値は更新されるべき"
            assert row['net_income'] == 160.0, "非Null値は更新されるべき"
            assert row['eps'] == 100.0, "Noneで既存値が上書きされてはいけない"

    def test_all_null_preserves_existing(self, sample_company):
        """全フィールドNoneでも既存データが保持されること"""
        insert_financial(
            ticker_code='9999',
            fiscal_year='2024',
            fiscal_quarter='Q2',
            revenue=500.0,
            gross_profit=200.0,
            operating_income=100.0,
            ordinary_income=105.0,
            net_income=70.0,
            eps=50.0,
            source='TDnet',
        )

        # 全財務フィールドNoneで上書き試行
        insert_financial(
            ticker_code='9999',
            fiscal_year='2024',
            fiscal_quarter='Q2',
            source='TDnet',
        )

        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q2'"
            ).fetchone()
            assert row['revenue'] == 500.0
            assert row['gross_profit'] == 200.0
            assert row['operating_income'] == 100.0
            assert row['ordinary_income'] == 105.0
            assert row['net_income'] == 70.0
            assert row['eps'] == 50.0


class TestBulkInsertPricesWithOutOfScopeTicker:
    """JPXリスト外の銘柄に対する bulk_insert_prices() のテスト"""

    def test_bulk_insert_prices_mixed(self, sample_company):
        """JPXリスト内外が混在する場合、JPXリスト内のみ挿入される"""
        from db_utils import bulk_insert_prices, get_connection

        test_date = '2099-12-31'  # 未来の日付（実データと重複しない）
        prices = [
            ('9999', test_date, 1000.0, 1050.0, 980.0, 990.0, 100000, 1000.0),  # JPXリスト内
            ('6655', test_date, 2000.0, 2050.0, 1980.0, 1990.0, 200000, 2000.0),  # JPXリスト外
        ]

        inserted = bulk_insert_prices(prices)

        # 1件のみ挿入されたことを確認
        assert inserted == 1

        # DBに9999のみ存在することを確認
        with get_connection() as conn:
            cursor = conn.execute(
                f"SELECT ticker_code FROM daily_prices WHERE trade_date = '{test_date}' ORDER BY ticker_code"
            )
            rows = cursor.fetchall()
            ticker_codes = [row['ticker_code'] for row in rows]
            assert len(ticker_codes) == 1, f"Expected 1, but got {len(ticker_codes)}: {ticker_codes}"
            assert ticker_codes[0] == '9999'

    def test_bulk_insert_prices_all_out_of_scope(self, test_db):
        """全てJPXリスト外の場合、0件挿入される"""
        from db_utils import bulk_insert_prices

        test_date = '2099-12-30'  # 未来の日付（実データと重複しない）
        prices = [
            ('6655', test_date, 2000.0, 2050.0, 1980.0, 1990.0, 200000, 2000.0),  # 名証M銘柄（未登録）
            ('1111', test_date, 3000.0, 3050.0, 2980.0, 2990.0, 300000, 3000.0),  # 架空の銘柄（未登録）
        ]

        inserted = bulk_insert_prices(prices)

        # 0件挿入
        assert inserted == 0

    def test_bulk_insert_prices_empty(self, test_db):
        """空リストの場合、0件挿入される"""
        from db_utils import bulk_insert_prices

        inserted = bulk_insert_prices([])
        assert inserted == 0


class TestSourcePriority:
    """データソース優先度のテスト (EDINET > TDnet > yfinance)"""

    def test_yfinance_skips_when_edinet_exists(self, sample_company):
        """yfinanceデータは既存EDINETデータを上書きしない"""
        insert_financial('9999', '2024', 'Q1', revenue=1000.0, source='EDINET')
        result = insert_financial('9999', '2024', 'Q1', revenue=900.0, source='yfinance')
        assert result is False

        with get_connection() as conn:
            row = conn.execute(
                "SELECT revenue, source FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            ).fetchone()
            assert row['revenue'] == 1000.0
            assert row['source'] == 'EDINET'

    def test_yfinance_skips_when_tdnet_exists(self, sample_company):
        """yfinanceデータは既存TDnetデータを上書きしない"""
        insert_financial('9999', '2024', 'Q1', revenue=1000.0, source='TDnet')
        result = insert_financial('9999', '2024', 'Q1', revenue=900.0, source='yfinance')
        assert result is False

        with get_connection() as conn:
            row = conn.execute(
                "SELECT source FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            ).fetchone()
            assert row['source'] == 'TDnet'

    def test_yfinance_inserts_new_data(self, sample_company):
        """yfinanceデータは新規レコードとして挿入できる"""
        result = insert_financial('9999', '2024', 'Q1', revenue=900.0, source='yfinance')
        assert result is True

        with get_connection() as conn:
            row = conn.execute(
                "SELECT revenue, source FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            ).fetchone()
            assert row['revenue'] == 900.0
            assert row['source'] == 'yfinance'

    def test_tdnet_overwrites_yfinance(self, sample_company):
        """TDnetデータはyfinanceデータを上書きする"""
        insert_financial('9999', '2024', 'Q1', revenue=900.0, source='yfinance')
        result = insert_financial('9999', '2024', 'Q1', revenue=1000.0, source='TDnet')
        assert result is True

        with get_connection() as conn:
            row = conn.execute(
                "SELECT revenue, source FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            ).fetchone()
            assert row['revenue'] == 1000.0
            assert row['source'] == 'TDnet'

    def test_edinet_overwrites_yfinance(self, sample_company):
        """EDINETデータはyfinanceデータを上書きする"""
        insert_financial('9999', '2024', 'Q1', revenue=900.0, source='yfinance')
        result = insert_financial('9999', '2024', 'Q1', revenue=1100.0, source='EDINET')
        assert result is True

        with get_connection() as conn:
            row = conn.execute(
                "SELECT revenue, source FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            ).fetchone()
            assert row['revenue'] == 1100.0
            assert row['source'] == 'EDINET'

    def test_tdnet_still_skips_edinet(self, sample_company):
        """TDnetデータは既存EDINETデータを上書きしない（既存動作維持）"""
        insert_financial('9999', '2024', 'Q1', revenue=1000.0, source='EDINET')
        result = insert_financial('9999', '2024', 'Q1', revenue=950.0, source='TDnet')
        assert result is False

        with get_connection() as conn:
            row = conn.execute(
                "SELECT revenue, source FROM financials WHERE ticker_code='9999' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            ).fetchone()
            assert row['revenue'] == 1000.0
            assert row['source'] == 'EDINET'
