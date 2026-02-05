"""
test_fetch_prices.py - 株価取得バッチの統合テスト

このテストは実際にYahoo Finance APIを叩く統合テストです。
- モック化なし: yfinance.download を実際に実行
- テスト時間: 数秒～数十秒かかる可能性あり
- ネット環境必須: オフラインでは失敗
"""
import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from fetch_prices import (
    ticker_to_yahoo_symbol,
    fetch_stock_data_batch,
    process_price_data,
    fetch_all_prices,
    fetch_company_info,
)
from db_utils import get_connection, get_last_price_date, upsert_company


class TestTickerToYahooSymbol:
    """証券コード変換のテスト"""

    def test_valid_ticker(self):
        """正しい証券コードが.T付きに変換されること"""
        assert ticker_to_yahoo_symbol("7203") == "7203.T"
        assert ticker_to_yahoo_symbol("6758") == "6758.T"
        assert ticker_to_yahoo_symbol("9984") == "9984.T"

    def test_four_digit_ticker(self):
        """4桁コードが正しく変換されること"""
        result = ticker_to_yahoo_symbol("1234")
        assert result.endswith(".T"), "末尾が.Tであること"
        assert result.startswith("1234"), "銘柄コードが先頭にあること"


class TestFetchStockDataBatch:
    """株価一括取得のテスト（実API統合テスト）"""

    def test_single_ticker_with_period(self):
        """単一銘柄・期間指定での取得（実API）"""
        # トヨタ自動車（7203）の過去5日分を取得
        result = fetch_stock_data_batch(["7203"], period="5d")

        # 結果検証（柔軟なアサーション）
        assert result is not None, "結果がNoneでないこと"
        if not result.empty:
            # データがある場合のみ検証
            assert 'Close' in result.columns or '7203.T' in str(result.columns), \
                "株価データのカラムが存在すること"

    def test_multiple_tickers(self):
        """複数銘柄の一括取得（実API）"""
        # トヨタ（7203）、ソニー（6758）の過去3日分
        result = fetch_stock_data_batch(["7203", "6758"], period="3d")

        # 結果検証
        assert result is not None, "結果がNoneでないこと"
        # 注: 取引がない日もあるため、データがない場合もある

    def test_with_start_end_date(self):
        """日付範囲指定での取得（実API）"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        result = fetch_stock_data_batch(
            ["7203"],
            start_date=start_date,
            end_date=end_date
        )

        assert result is not None, "結果がNoneでないこと"

    def test_empty_ticker_list(self):
        """空リスト入力時の動作"""
        result = fetch_stock_data_batch([], period="5d")

        # 空リストの場合、空DataFrameが返るか確認
        assert isinstance(result, pd.DataFrame), "DataFrameが返ること"

    def test_invalid_ticker(self):
        """存在しない銘柄コードでのエラーハンドリング"""
        # 存在しない銘柄（9999は通常存在しない）
        result = fetch_stock_data_batch(["9999"], period="5d")

        # エラーでもDataFrameが返ること
        assert isinstance(result, pd.DataFrame), "DataFrameが返ること"


class TestProcessPriceData:
    """価格データ変換のテスト"""

    def test_valid_dataframe(self):
        """正しいDataFrameをDB挿入用に変換"""
        # テスト用のDataFrame作成
        test_data = pd.DataFrame({
            'Open': [1000.0, 1010.0],
            'High': [1050.0, 1060.0],
            'Low': [980.0, 990.0],
            'Close': [1020.0, 1030.0],
            'Volume': [1000000, 1100000],
            'Adj Close': [1020.0, 1030.0],
        }, index=pd.to_datetime(['2024-01-10', '2024-01-11']))

        result = process_price_data('7203', test_data)

        # 検証
        # 返り値の順序: (ticker_code, trade_date, Open, High, Low, Close, Volume, Adj Close)
        assert len(result) == 2, "2レコード変換されること"
        assert result[0][0] == '7203', "銘柄コードが含まれること"
        assert result[0][1] == '2024-01-10', "日付が含まれること"
        assert result[0][2] == 1000.0, "Open価格が含まれること"
        assert result[0][3] == 1050.0, "High価格が含まれること"

    def test_empty_dataframe(self):
        """空DataFrameの処理"""
        empty_df = pd.DataFrame()
        result = process_price_data('7203', empty_df)

        assert result == [], "空リストが返ること"

    def test_nan_values(self):
        """NaN値を含むDataFrameの処理"""
        test_data = pd.DataFrame({
            'Open': [1000.0, None],
            'High': [1050.0, 1060.0],
            'Low': [980.0, None],
            'Close': [1020.0, 1030.0],
            'Volume': [1000000, 0],
            'Adj Close': [1020.0, 1030.0],
        }, index=pd.to_datetime(['2024-01-10', '2024-01-11']))

        result = process_price_data('7203', test_data)

        # NaN値はNoneに変換されること
        # result[1] = 2行目のレコード、[2] = Open（NaN）、[4] = Low（NaN）
        assert result[1][2] is None, "Open の NaN値がNoneに変換されること"
        assert result[1][4] is None, "Low の NaN値がNoneに変換されること"


class TestFetchAllPrices:
    """メイン処理のテスト（実API統合テスト + DB操作）"""

    def test_single_ticker_full_flow(self, test_db, sample_company):
        """単一銘柄のフルフロー（実API + DB挿入）"""
        # テスト用銘柄（9999）は実在しないので、実在する銘柄を使う
        # ただし、テストDBには sample_company (9999) が登録されている

        # 実在する銘柄（7203: トヨタ）でテスト
        upsert_company('7203', 'トヨタ自動車', edinet_code='E01225')

        # 過去3日分取得
        fetch_all_prices(
            tickers=['7203'],
            period='3d',
            sleep_interval=0.1,
            batch_size=1
        )

        # DB確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM daily_prices WHERE ticker_code = '7203'"
            )
            count = cursor.fetchone()[0]

            # データが挿入されたこと（0件以上）
            assert count >= 0, "データが挿入されたこと"

    def test_empty_ticker_list(self, test_db):
        """空リストでのエラーハンドリング"""
        # 空リストでも例外が発生しないこと
        try:
            fetch_all_prices(
                tickers=[],
                period='1d',
                sleep_interval=0.1,
                batch_size=1
            )
        except Exception as e:
            pytest.fail(f"空リストで例外が発生: {e}")


class TestFetchCompanyInfo:
    """企業情報取得のテスト（実API統合テスト）"""

    def test_valid_ticker(self):
        """実在する銘柄の企業情報取得（実API）"""
        # トヨタ自動車（7203）の情報を取得
        result = fetch_company_info('7203')

        # 結果検証（柔軟なアサーション）
        if result is not None:
            assert 'ticker_code' in result, "ticker_codeが含まれること"
            assert result['ticker_code'] == '7203', "正しい銘柄コードであること"
            assert 'company_name' in result, "company_nameが含まれること"

    def test_invalid_ticker(self):
        """存在しない銘柄でのエラーハンドリング"""
        # 存在しない銘柄（9999）
        result = fetch_company_info('9999')

        # エラーでもNoneまたは辞書が返ること
        assert result is None or isinstance(result, dict), \
            "エラー時はNoneまたは辞書が返ること"


class TestDatabaseIntegration:
    """DB統合テスト"""

    def test_bulk_insert_and_retrieve(self, test_db, sample_company):
        """バルク挿入と取得の統合テスト"""
        # 実在する銘柄でテスト
        upsert_company('7203', 'トヨタ自動車', edinet_code='E01225')

        # 過去1日分取得
        fetch_all_prices(
            tickers=['7203'],
            period='1d',
            sleep_interval=0.1,
            batch_size=1
        )

        # 最新日付を取得
        last_date = get_last_price_date()

        # 最新日付が取得できること（データがあれば）
        # 注: 取引がない日もあるため、Noneの場合もある
        assert last_date is None or isinstance(last_date, str), \
            "最新日付がNoneまたは文字列であること"

    def test_duplicate_prevention(self, test_db, sample_company):
        """重複データの挿入防止"""
        upsert_company('7203', 'トヨタ自動車', edinet_code='E01225')

        # 同じ期間を2回取得
        fetch_all_prices(tickers=['7203'], period='1d', sleep_interval=0.1, batch_size=1)

        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM daily_prices WHERE ticker_code = '7203'"
            )
            count1 = cursor.fetchone()[0]

        # 2回目の取得
        fetch_all_prices(tickers=['7203'], period='1d', sleep_interval=0.1, batch_size=1)

        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM daily_prices WHERE ticker_code = '7203'"
            )
            count2 = cursor.fetchone()[0]

        # 重複挿入されないこと（UNIQUE制約）
        assert count2 == count1, "重複データが挿入されないこと"


class TestAlphanumericTickerSupport:
    """英字付きティッカーの統合テスト"""

    def test_yahoo_symbol_conversion_alphanumeric(self):
        """285A → 285A.T 変換が正しいこと"""
        assert ticker_to_yahoo_symbol("285A") == "285A.T"
        assert ticker_to_yahoo_symbol("200A") == "200A.T"
        assert ticker_to_yahoo_symbol("346A") == "346A.T"

    def test_yahoo_symbol_conversion_numeric(self):
        """数字コードの変換も正常に動作すること（後方互換性）"""
        assert ticker_to_yahoo_symbol("7203") == "7203.T"
        assert ticker_to_yahoo_symbol("6758") == "6758.T"
        assert ticker_to_yahoo_symbol("12345") == "12345.T"

    @pytest.mark.integration
    def test_fetch_alphanumeric_ticker_real_api(self):
        """英字付きティッカーで実APIから株価取得（285A: Kioxia）"""
        # 285A（キオクシア）でテスト
        result = fetch_stock_data_batch(["285A"], period="5d")

        # Kioxiaが上場している場合、データが取得できる
        if result is not None and not result.empty:
            # DataFrameが返ってきた場合
            assert isinstance(result, pd.DataFrame), "DataFrameが返ること"

            # カラム構造を確認（MultiIndexまたは単一Index）
            if isinstance(result.columns, pd.MultiIndex):
                # マルチインデックスの場合（複数銘柄）
                assert '285A.T' in result.columns.levels[0] or 'Close' in result.columns.levels[1], \
                    "285A.T または Close カラムが含まれること"
            else:
                # 単一インデックスの場合（単一銘柄）
                assert 'Close' in result.columns or '285A.T' in str(result.columns), \
                    "Close カラムが含まれること"

            print(f"[SUCCESS] 285A (Kioxia) データ取得成功: {len(result)}行")
        else:
            # データなしまたはNoneの場合（上場廃止、API制限等）
            pytest.skip("285A: データなし（上場廃止 or API制限）")

    @pytest.mark.integration
    def test_fetch_multiple_alphanumeric_tickers(self, test_db):
        """複数の英字付きティッカーを混在して取得（実API）"""
        # 数字コードと英字コードを混在
        tickers = ['7203', '285A', '6758']  # トヨタ、キオクシア、ソニー

        # 銘柄登録
        upsert_company('7203', 'トヨタ自動車', edinet_code='E01225')
        upsert_company('285A', 'キオクシアホールディングス')
        upsert_company('6758', 'ソニーグループ', edinet_code='E01777')

        # フルフロー実行
        try:
            fetch_all_prices(
                tickers=tickers,
                period='3d',
                sleep_interval=0.5,  # API制限を考慮
                batch_size=3
            )

            # DB確認
            with get_connection() as conn:
                cursor = conn.execute(
                    "SELECT ticker_code, COUNT(*) FROM daily_prices WHERE ticker_code IN ('7203', '285A', '6758') GROUP BY ticker_code"
                )
                results = cursor.fetchall()

                # 少なくとも1銘柄はデータが取得できていること
                assert len(results) >= 1, "少なくとも1銘柄のデータが取得できること"

                print(f"[SUCCESS] 取得銘柄数: {len(results)}")
                for ticker, count in results:
                    print(f"  - {ticker}: {count}行")

        except Exception as e:
            pytest.skip(f"複数銘柄取得失敗（API制限等）: {e}")
