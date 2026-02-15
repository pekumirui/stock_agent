"""
has_prev_quarter フラグと3か月決算の「-」表示テスト

前四半期の累計データが無い場合:
1. v_financials_standalone_quarter の has_prev_quarter が 0 になること
2. get_financial_history() が has_prev_quarter=0 の行の値を None にすること
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import get_connection, insert_financial, upsert_company
from web.services.financial_service import get_financial_history


class TestHasPrevQuarterFlag:
    """v_financials_standalone_quarter の has_prev_quarter フラグテスト"""

    def test_q1_always_has_prev(self, test_db):
        """Q1は常にhas_prev_quarter=1であること"""
        upsert_company("8001", "テスト社A")
        insert_financial("8001", "2024", "Q1",
                         revenue=100.0, operating_income=10.0,
                         ordinary_income=9.0, net_income=7.0, source="TEST")

        with get_connection() as conn:
            row = conn.execute(
                "SELECT has_prev_quarter FROM v_financials_standalone_quarter "
                "WHERE ticker_code = '8001' AND fiscal_quarter = 'Q1'"
            ).fetchone()
        assert row["has_prev_quarter"] == 1

    def test_q2_without_q1_no_prev(self, test_db):
        """Q1がない場合、Q2のhas_prev_quarter=0であること"""
        upsert_company("8002", "テスト社B")
        insert_financial("8002", "2024", "Q2",
                         revenue=200.0, operating_income=20.0,
                         ordinary_income=18.0, net_income=14.0, source="TEST")

        with get_connection() as conn:
            row = conn.execute(
                "SELECT has_prev_quarter, revenue_standalone FROM v_financials_standalone_quarter "
                "WHERE ticker_code = '8002' AND fiscal_quarter = 'Q2'"
            ).fetchone()
        assert row["has_prev_quarter"] == 0
        # standalone = cumulative（前四半期なしで引き算できない）
        assert row["revenue_standalone"] == 200.0

    def test_q2_with_q1_has_prev(self, test_db):
        """Q1がある場合、Q2のhas_prev_quarter=1であること"""
        upsert_company("8003", "テスト社C")
        insert_financial("8003", "2024", "Q1",
                         revenue=100.0, operating_income=10.0,
                         ordinary_income=9.0, net_income=7.0, source="TEST")
        insert_financial("8003", "2024", "Q2",
                         revenue=220.0, operating_income=22.0,
                         ordinary_income=20.0, net_income=15.0, source="TEST")

        with get_connection() as conn:
            row = conn.execute(
                "SELECT has_prev_quarter, revenue_standalone FROM v_financials_standalone_quarter "
                "WHERE ticker_code = '8003' AND fiscal_quarter = 'Q2'"
            ).fetchone()
        assert row["has_prev_quarter"] == 1
        assert row["revenue_standalone"] == pytest.approx(120.0)  # 220 - 100

    def test_q3_without_q2_no_prev(self, test_db):
        """Q2がない場合、Q3のhas_prev_quarter=0であること"""
        upsert_company("8004", "テスト社D")
        insert_financial("8004", "2024", "Q1",
                         revenue=100.0, operating_income=10.0,
                         ordinary_income=9.0, net_income=7.0, source="TEST")
        insert_financial("8004", "2024", "Q3",
                         revenue=300.0, operating_income=30.0,
                         ordinary_income=27.0, net_income=21.0, source="TEST")

        with get_connection() as conn:
            rows = conn.execute(
                "SELECT fiscal_quarter, has_prev_quarter "
                "FROM v_financials_standalone_quarter "
                "WHERE ticker_code = '8004' ORDER BY fiscal_quarter"
            ).fetchall()
        result = {r["fiscal_quarter"]: r["has_prev_quarter"] for r in rows}
        assert result["Q1"] == 1
        assert result["Q3"] == 0  # Q2が無いのでQ3は計算不能


class TestGetFinancialHistoryWithMissingPrev:
    """get_financial_history() での has_prev_quarter=0 行の「-」表示テスト"""

    def test_q2_only_shows_none_values(self, test_db):
        """Q2のみ（Q1なし）の場合、3か月決算の値がすべてNoneであること"""
        upsert_company("8010", "テスト社E")
        insert_financial("8010", "2024", "Q2",
                         revenue=500.0, operating_income=50.0,
                         ordinary_income=45.0, net_income=35.0,
                         eps=100.0, source="TEST")

        result = get_financial_history("8010")
        assert len(result["quarterly"]) == 1
        q = result["quarterly"][0]
        assert q["label"] == "24/2Q"
        assert q["revenue"] is None
        assert q["operating_income"] is None
        assert q["ordinary_income"] is None
        assert q["net_income"] is None
        assert q["eps"] is None

    def test_q1_and_q2_shows_standalone_values(self, test_db):
        """Q1+Q2がある場合、3か月決算に正しい差分値が表示されること"""
        upsert_company("8011", "テスト社F")
        insert_financial("8011", "2024", "Q1",
                         revenue=100.0, operating_income=10.0,
                         ordinary_income=9.0, net_income=7.0,
                         eps=50.0, source="TEST")
        insert_financial("8011", "2024", "Q2",
                         revenue=230.0, operating_income=25.0,
                         ordinary_income=22.0, net_income=17.0,
                         eps=85.0, source="TEST")

        result = get_financial_history("8011")
        labels = [r["label"] for r in result["quarterly"]]
        assert "24/1Q" in labels
        assert "24/2Q" in labels

        q2 = next(r for r in result["quarterly"] if r["label"] == "24/2Q")
        assert q2["revenue"] == pytest.approx(130.0)  # 230 - 100
        assert q2["operating_income"] == pytest.approx(15.0)  # 25 - 10
        assert q2["eps"] == pytest.approx(35.0)  # 85 - 50

    def test_mixed_prev_availability(self, test_db):
        """一部の年度でQ1あり、別の年度でQ1なしの場合、正しく混在表示されること"""
        upsert_company("8012", "テスト社G")
        # 2023: Q1+Q2あり（正常計算）
        insert_financial("8012", "2023", "Q1",
                         revenue=100.0, operating_income=10.0,
                         ordinary_income=9.0, net_income=7.0, source="TEST")
        insert_financial("8012", "2023", "Q2",
                         revenue=210.0, operating_income=21.0,
                         ordinary_income=19.0, net_income=14.0, source="TEST")
        # 2024: Q2のみ（Q1なし→「-」表示）
        insert_financial("8012", "2024", "Q2",
                         revenue=250.0, operating_income=25.0,
                         ordinary_income=23.0, net_income=17.0, source="TEST")

        result = get_financial_history("8012")
        quarterly = {r["label"]: r for r in result["quarterly"]}

        # 2023/Q1: 正常
        assert quarterly["23/1Q"]["revenue"] == pytest.approx(100.0)
        # 2023/Q2: Q1あり→正常な差分
        assert quarterly["23/2Q"]["revenue"] == pytest.approx(110.0)  # 210 - 100
        # 2024/Q2: Q1なし→None
        assert quarterly["24/2Q"]["revenue"] is None

    def test_yoy_not_calculated_for_missing_prev(self, test_db):
        """has_prev_quarter=0の行はQoQ計算の参照元にならないこと"""
        upsert_company("8013", "テスト社H")
        # 2023: Q2のみ（Q1なし）
        insert_financial("8013", "2023", "Q2",
                         revenue=200.0, operating_income=20.0,
                         ordinary_income=18.0, net_income=14.0, source="TEST")
        # 2024: Q1+Q2あり
        insert_financial("8013", "2024", "Q1",
                         revenue=120.0, operating_income=12.0,
                         ordinary_income=11.0, net_income=8.0, source="TEST")
        insert_financial("8013", "2024", "Q2",
                         revenue=260.0, operating_income=26.0,
                         ordinary_income=24.0, net_income=18.0, source="TEST")

        result = get_financial_history("8013")
        quarterly = {r["label"]: r for r in result["quarterly"]}

        # 2024/Q2 standalone = 260 - 120 = 140
        assert quarterly["24/2Q"]["revenue"] == pytest.approx(140.0)
        # QoQ計算: 2024/Q2 は 2024/Q1 と比較（両方とも has_prev_quarter=1）
        # revenue QoQ = (140 - 120) / 120 * 100 = 16.7%
        assert quarterly["24/2Q"]["revenue_yoy_pct"] == pytest.approx(16.7, abs=0.1)
