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
def setup_test_data(test_db):
    """テスト用データのセットアップ（一時DB上で動作）"""
    # テスト用銘柄を登録
    upsert_company('9999', 'テスト株式会社')

    # 2年分の四半期データを投入
    test_data = [
        # 2023年度
        ('9999', '2023', 'Q1', {'revenue': 100.0, 'operating_income': 10.0, 'net_income': 7.0, 'eps': 50.0,
                                 'gross_profit': 30.0, 'ordinary_income': 9.0, 'fiscal_end_date': '2022-06-30'}),
        ('9999', '2023', 'Q2', {'revenue': 110.0, 'operating_income': 12.0, 'net_income': 8.0, 'eps': 55.0,
                                 'gross_profit': 33.0, 'ordinary_income': 11.0, 'fiscal_end_date': '2022-09-30'}),
        ('9999', '2023', 'Q3', {'revenue': 120.0, 'operating_income': 15.0, 'net_income': 10.0, 'eps': 60.0,
                                 'gross_profit': 36.0, 'ordinary_income': 13.0, 'fiscal_end_date': '2022-12-31'}),
        ('9999', '2023', 'FY', {'revenue': 450.0, 'operating_income': 50.0, 'net_income': 35.0, 'eps': 200.0,
                                 'gross_profit': 135.0, 'ordinary_income': 45.0, 'fiscal_end_date': '2023-03-31'}),
        # 2024年度
        ('9999', '2024', 'Q1', {'revenue': 120.0, 'operating_income': 14.0, 'net_income': 9.0, 'eps': 60.0,
                                 'gross_profit': 36.0, 'ordinary_income': 12.0, 'fiscal_end_date': '2023-06-30'}),
        ('9999', '2024', 'Q2', {'revenue': 130.0, 'operating_income': 16.0, 'net_income': 11.0, 'eps': 70.0,
                                 'gross_profit': 39.0, 'ordinary_income': 14.0, 'fiscal_end_date': '2023-09-30'}),
        ('9999', '2024', 'Q3', {'revenue': 140.0, 'operating_income': 18.0, 'net_income': 12.0, 'eps': 75.0,
                                 'gross_profit': 42.0, 'ordinary_income': 16.0, 'fiscal_end_date': '2023-12-31'}),
        ('9999', '2024', 'FY', {'revenue': 530.0, 'operating_income': 60.0, 'net_income': 42.0, 'eps': 250.0,
                                 'gross_profit': 159.0, 'ordinary_income': 55.0, 'fiscal_end_date': '2024-03-31'}),
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
    # 一時DBのため手動クリーンアップ不要


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


class TestFYQoQInViewer:
    """通期（FY）のQoQ計算がget_viewer_data()で動作することのテスト"""

    def test_fy_qoq_calculated(self, test_db):
        """通期のQoQ（Q4単独 vs Q3単独）が計算されること"""
        from web.services.financial_service import get_viewer_data

        # announcement_dateを設定してget_viewer_dataで取得できるようにする
        with get_connection() as conn:
            conn.execute(
                "UPDATE financials SET announcement_date = '2024-05-15' "
                "WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'FY'"
            )
            conn.commit()

        rows = get_viewer_data('2024-05-15')
        assert len(rows) >= 1

        fy_row = [r for r in rows if r['ticker_code'] == '9999' and r['fiscal_quarter'] == 'FY'][0]

        # FY累計530 - Q3累計140 = Q4単独390
        # Q3累計140 - Q2累計130 = Q3単独10 （Q3 standalone）
        # ※ 実際のstandalone計算はrevenue列で:
        #   Q3 standalone = Q3(140) - Q2(130) = 10
        #   Q4 standalone = FY(530) - Q3(140) = 390
        #   QoQ = (390 - 10) / |10| * 100 = +3800.0%
        assert fy_row['revenue_qoq'] is not None
        assert fy_row['revenue_qoq'] == 3800.0
        # 複数指標も検証
        # operating_income: Q4単独=FY(60)-Q3(18)=42, Q3単独=Q3(18)-Q2(16)=2 → (42-2)/|2|*100=2000%
        assert fy_row['operating_income_qoq'] == 2000.0
        # net_income: Q4単独=FY(42)-Q3(12)=30, Q3単独=Q3(12)-Q2(11)=1 → (30-1)/|1|*100=2900%
        assert fy_row['net_income_qoq'] == 2900.0

    def test_fy_qoq_not_confused_with_q4(self, test_db):
        """FYとQ4が共存する場合、FY行はFY由来のstandalone値を使うこと"""
        from web.services.financial_service import get_viewer_data

        # Q4レコードを追加（FYと異なるrevenue）
        insert_financial(
            ticker_code='9999', fiscal_year='2024', fiscal_quarter='Q4',
            revenue=600.0, operating_income=65.0, net_income=45.0, eps=260.0,
            gross_profit=180.0, ordinary_income=58.0,
            fiscal_end_date='2024-03-31', source='TEST',
        )
        with get_connection() as conn:
            conn.execute(
                "UPDATE financials SET announcement_date = '2024-05-15' "
                "WHERE ticker_code = '9999' AND fiscal_year = '2024' AND fiscal_quarter = 'FY'"
            )
            conn.commit()

        rows = get_viewer_data('2024-05-15')
        fy_row = [r for r in rows if r['ticker_code'] == '9999' and r['fiscal_quarter'] == 'FY'][0]

        # FY由来: Q4 standalone = FY(530) - Q3(140) = 390 → QoQ = (390-10)/|10|*100 = 3800%
        # Q4由来だと: Q4 standalone = Q4(600) - Q3(140) = 460 → QoQ = (460-10)/|10|*100 = 4500%
        # FY由来の値が使われていることを確認
        assert fy_row['revenue_qoq'] == 3800.0

    def test_fy_qoq_without_q3_data(self, test_db):
        """Q3データがない場合、通期のQoQはNoneであること"""
        from web.services.financial_service import get_viewer_data

        upsert_company('8888', 'QoQテスト株式会社')
        # Q3なし、FYのみ投入
        insert_financial(
            ticker_code='8888', fiscal_year='2024', fiscal_quarter='FY',
            revenue=500.0, operating_income=50.0, net_income=35.0, eps=200.0,
            fiscal_end_date='2024-03-31', source='TEST',
        )
        with get_connection() as conn:
            conn.execute(
                "UPDATE financials SET announcement_date = '2024-05-20' "
                "WHERE ticker_code = '8888' AND fiscal_year = '2024' AND fiscal_quarter = 'FY'"
            )
            conn.commit()

        rows = get_viewer_data('2024-05-20')
        fy_rows = [r for r in rows if r['ticker_code'] == '8888']
        assert len(fy_rows) == 1
        # Q3データがないのでQoQ計算不可
        assert fy_rows[0]['revenue_qoq'] is None
