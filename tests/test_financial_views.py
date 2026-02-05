"""
決算比較ビュー（YoY/QoQ）のテスト
"""
import pytest
import sys
import sqlite3
import tempfile
from pathlib import Path

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import get_connection, init_database, insert_financial, upsert_company


@pytest.fixture(autouse=True)
def setup_test_data():
    """テスト用データのセットアップ"""
    init_database()

    # テスト用銘柄を登録
    upsert_company('9999', 'テスト株式会社')

    # 2年分の四半期データを投入
    test_data = [
        # 2023年度
        ('9999', '2023', 'Q1', {'revenue': 100.0, 'operating_income': 10.0, 'net_income': 7.0, 'eps': 50.0,
                                 'gross_profit': 30.0, 'ordinary_income': 9.0}),
        ('9999', '2023', 'Q2', {'revenue': 110.0, 'operating_income': 12.0, 'net_income': 8.0, 'eps': 55.0,
                                 'gross_profit': 33.0, 'ordinary_income': 11.0}),
        ('9999', '2023', 'Q3', {'revenue': 120.0, 'operating_income': 15.0, 'net_income': 10.0, 'eps': 60.0,
                                 'gross_profit': 36.0, 'ordinary_income': 13.0}),
        ('9999', '2023', 'FY', {'revenue': 450.0, 'operating_income': 50.0, 'net_income': 35.0, 'eps': 200.0,
                                 'gross_profit': 135.0, 'ordinary_income': 45.0}),
        # 2024年度
        ('9999', '2024', 'Q1', {'revenue': 120.0, 'operating_income': 14.0, 'net_income': 9.0, 'eps': 60.0,
                                 'gross_profit': 36.0, 'ordinary_income': 12.0}),
        ('9999', '2024', 'Q2', {'revenue': 130.0, 'operating_income': 16.0, 'net_income': 11.0, 'eps': 70.0,
                                 'gross_profit': 39.0, 'ordinary_income': 14.0}),
        ('9999', '2024', 'Q3', {'revenue': 140.0, 'operating_income': 18.0, 'net_income': 12.0, 'eps': 75.0,
                                 'gross_profit': 42.0, 'ordinary_income': 16.0}),
        ('9999', '2024', 'FY', {'revenue': 530.0, 'operating_income': 60.0, 'net_income': 42.0, 'eps': 250.0,
                                 'gross_profit': 159.0, 'ordinary_income': 55.0}),
    ]

    for ticker, year, quarter, data in test_data:
        insert_financial(
            ticker_code=ticker,
            fiscal_year=year,
            fiscal_quarter=quarter,
            **data,
            source='TEST'
        )

    yield

    # テストデータのクリーンアップ
    with get_connection() as conn:
        conn.execute("DELETE FROM financials WHERE ticker_code = '9999'")
        conn.execute("DELETE FROM companies WHERE ticker_code = '9999'")
        conn.commit()


class TestYoYView:
    """前年同期比較ビューのテスト"""

    def test_yoy_revenue_change(self):
        """YoY売上変化額が正しく計算されること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'Q1'"
            )
            row = dict(cursor.fetchone())

        # 2024 Q1: 120, 2023 Q1: 100 → 変化: +20
        assert row['revenue'] == 120.0
        assert row['revenue_prev_year'] == 100.0
        assert row['revenue_yoy_change'] == 20.0

    def test_yoy_percentage(self):
        """YoY変化率が正しく計算されること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'Q1'"
            )
            row = dict(cursor.fetchone())

        # 2024 Q1: 120, 2023 Q1: 100 → 変化率: +20%
        assert row['revenue_yoy_pct'] == 20.0

    def test_yoy_fy_comparison(self):
        """通期のYoY比較が正しいこと"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'FY'"
            )
            row = dict(cursor.fetchone())

        # FY: 530 vs 450 → +17.78%
        assert row['revenue'] == 530.0
        assert row['revenue_prev_year'] == 450.0
        assert abs(row['revenue_yoy_pct'] - 17.78) < 0.01

    def test_yoy_first_year_null(self):
        """初年度データは前年値がNULLであること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy WHERE ticker_code = '9999' AND fiscal_year = '2023' AND fiscal_quarter = 'Q1'"
            )
            row = dict(cursor.fetchone())

        assert row['revenue_prev_year'] is None
        assert row['revenue_yoy_change'] is None
        assert row['revenue_yoy_pct'] is None

    def test_yoy_operating_income(self):
        """営業利益のYoYが正しく計算されること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_yoy WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'Q2'"
            )
            row = dict(cursor.fetchone())

        # Q2: 16 vs 12 → +33.33%
        assert row['operating_income'] == 16.0
        assert row['operating_income_prev_year'] == 12.0
        assert abs(row['operating_income_yoy_pct'] - 33.33) < 0.01


class TestQoQView:
    """前四半期比較ビューのテスト"""

    def test_qoq_excludes_fy(self):
        """QoQビューが通期（FY）を除外すること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM v_financials_qoq WHERE ticker_code = '9999' AND fiscal_quarter = 'FY'"
            )
            count = cursor.fetchone()[0]

        assert count == 0

    def test_qoq_revenue_change(self):
        """QoQ売上変化が正しく計算されること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_qoq WHERE ticker_code = '9999' AND fiscal_year = '2023' AND fiscal_quarter = 'Q2'"
            )
            row = dict(cursor.fetchone())

        # 2023 Q2: 110, 2023 Q1: 100 → 変化: +10
        assert row['revenue'] == 110.0
        assert row['revenue_prev_quarter'] == 100.0
        assert row['revenue_qoq_change'] == 10.0

    def test_qoq_cross_year(self):
        """年度をまたぐQoQ（Q1 vs 前年Q3）が正しいこと"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_qoq WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'Q1'"
            )
            row = dict(cursor.fetchone())

        # 2024 Q1: 120, 前四半期 = 2023 Q3: 120 → 変化: 0
        assert row['revenue'] == 120.0
        assert row['revenue_prev_quarter'] == 120.0
        assert row['revenue_qoq_change'] == 0.0

    def test_qoq_first_quarter_null(self):
        """最初の四半期は前四半期がNULLであること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_qoq WHERE ticker_code = '9999' AND fiscal_year = '2023' AND fiscal_quarter = 'Q1'"
            )
            row = dict(cursor.fetchone())

        assert row['revenue_prev_quarter'] is None
        assert row['revenue_qoq_change'] is None

    def test_qoq_percentage(self):
        """QoQ変化率が正しく計算されること"""
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM v_financials_qoq WHERE ticker_code = '9999' AND fiscal_year = '2023' AND fiscal_quarter = 'Q2'"
            )
            row = dict(cursor.fetchone())

        # Q2: 110, Q1: 100 → +10%
        assert row['revenue_qoq_pct'] == 10.0
