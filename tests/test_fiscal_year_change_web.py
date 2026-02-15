"""
決算期変更銘柄のWeb層クエリテスト

同一(ticker, fiscal_year, fiscal_quarter)に複数レコードが存在する場合、
最新のfiscal_end_dateのレコードを優先して返すことを確認。
"""
import pytest
import sys
import sqlite3
from pathlib import Path
from datetime import date

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent))

from db_utils import get_connection, upsert_company
from web.services.financial_service import get_viewer_data, get_detail_data


@pytest.fixture(autouse=True)
def setup_fiscal_change_data(test_db):
    """決算期変更銘柄のテストデータ（175A）をセットアップ"""
    # テスト用銘柄を登録
    upsert_company('175A', '決算期変更テスト株式会社')

    # 同一(ticker, year, quarter)に複数レコードを投入
    # fiscal_end_dateが異なる（決算期変更前後）
    with get_connection() as conn:
        # 旧決算期（12月期）の2024年度Q1: 2024-03-31期末
        conn.execute(
            """
            INSERT INTO financials (
                ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date,
                announcement_date, revenue, operating_income, net_income, eps, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                '175A', '2024', 'Q1', '2024-03-31',
                '2024-05-10', 100.0, 10.0, 7.0, 50.0, 'TEST_OLD'
            ]
        )

        # 新決算期（3月期）の2024年度Q1: 2024-06-30期末（決算期変更後）
        # 最新のfiscal_end_dateなのでこちらが優先されるべき
        conn.execute(
            """
            INSERT INTO financials (
                ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date,
                announcement_date, revenue, operating_income, net_income, eps, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                '175A', '2024', 'Q1', '2024-06-30',
                '2024-08-10', 150.0, 20.0, 15.0, 80.0, 'TEST_NEW'
            ]
        )

        # 前年同期（YoY計算用）
        conn.execute(
            """
            INSERT INTO financials (
                ticker_code, fiscal_year, fiscal_quarter, fiscal_end_date,
                announcement_date, revenue, operating_income, net_income, eps, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                '175A', '2023', 'Q1', '2023-03-31',
                '2023-05-10', 90.0, 9.0, 6.0, 45.0, 'TEST_PREV'
            ]
        )
        conn.commit()

    yield
    # 一時DBのため手動クリーンアップ不要


class TestFiscalYearChangeQueries:
    """決算期変更銘柄のクエリテスト"""

    def test_get_detail_data_selects_latest_fiscal_end_date(self):
        """get_detail_data()が最新fiscal_end_dateのレコードを返すこと"""
        result = get_detail_data('175A', '2024-08-10')

        # 累計実績が新決算期のデータ（revenue=150.0）を返すことを確認
        assert result['cumulative'] is not None
        assert result['cumulative']['revenue'] == 150.0
        assert result['cumulative']['operating_income'] == 20.0
        assert result['cumulative']['net_income'] == 15.0

    def test_direct_query_returns_latest_record(self):
        """financialsテーブルへの直接クエリでORDER BY fiscal_end_date DESCが動作すること"""
        with get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT revenue, operating_income, fiscal_end_date, source
                FROM financials
                WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                ORDER BY fiscal_end_date DESC LIMIT 1
                """,
                ['175A', '2024', 'Q1']
            )
            row = cursor.fetchone()

        # 最新のfiscal_end_date（2024-06-30）のレコードが返ること
        assert row['fiscal_end_date'] == '2024-06-30'
        assert row['source'] == 'TEST_NEW'
        assert row['revenue'] == 150.0
        assert row['operating_income'] == 20.0

    def test_view_query_with_order_by(self):
        """v_financials_yoyビューへのクエリでORDER BY fiscal_end_dateが動作すること"""
        with get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT revenue, fiscal_end_date
                FROM v_financials_yoy
                WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                ORDER BY fiscal_end_date DESC LIMIT 1
                """,
                ['175A', '2024', 'Q1']
            )
            row = cursor.fetchone()

        # ビューからも最新レコードが取得できること
        assert row['fiscal_end_date'] == '2024-06-30'
        assert row['revenue'] == 150.0

    def test_without_order_by_returns_undefined(self):
        """ORDER BYなしのクエリは不定のレコードを返すことを確認（検証用）"""
        with get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT revenue, source
                FROM financials
                WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                """,
                ['175A', '2024', 'Q1']
            )
            rows = cursor.fetchall()

        # 複数行が存在することを確認
        assert len(rows) == 2
        revenues = [r['revenue'] for r in rows]
        assert 100.0 in revenues
        assert 150.0 in revenues
