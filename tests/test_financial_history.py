"""
get_financial_history の累計データ表示ロジックのテスト
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import insert_financial, upsert_company
from web.services.financial_service import get_financial_history


@pytest.fixture
def ticker_with_3yr_data(test_db):
    """3年分の決算データ（FY + Q1/Q2/Q3）を持つテスト銘柄"""
    ticker = "9999"
    upsert_company(ticker, "テスト株式会社", edinet_code="E99999")

    data = [
        # 2022年度
        ("2022", "Q1", {"revenue": 80.0, "operating_income": 8.0, "ordinary_income": 7.0, "net_income": 5.0, "eps": 40.0}),
        ("2022", "Q2", {"revenue": 170.0, "operating_income": 17.0, "ordinary_income": 15.0, "net_income": 11.0, "eps": 85.0}),
        ("2022", "FY", {"revenue": 400.0, "operating_income": 40.0, "ordinary_income": 36.0, "net_income": 28.0, "eps": 180.0}),
        # 2023年度
        ("2023", "Q1", {"revenue": 100.0, "operating_income": 10.0, "ordinary_income": 9.0, "net_income": 7.0, "eps": 50.0}),
        ("2023", "Q2", {"revenue": 200.0, "operating_income": 20.0, "ordinary_income": 18.0, "net_income": 14.0, "eps": 100.0}),
        ("2023", "Q3", {"revenue": 310.0, "operating_income": 31.0, "ordinary_income": 28.0, "net_income": 21.0, "eps": 150.0}),
        ("2023", "FY", {"revenue": 450.0, "operating_income": 50.0, "ordinary_income": 45.0, "net_income": 35.0, "eps": 200.0}),
        # 2024年度
        ("2024", "Q1", {"revenue": 120.0, "operating_income": 14.0, "ordinary_income": 12.0, "net_income": 9.0, "eps": 60.0}),
        ("2024", "Q2", {"revenue": 240.0, "operating_income": 28.0, "ordinary_income": 24.0, "net_income": 18.0, "eps": 120.0}),
        ("2024", "FY", {"revenue": 530.0, "operating_income": 60.0, "ordinary_income": 55.0, "net_income": 42.0, "eps": 250.0}),
    ]
    for year, quarter, vals in data:
        insert_financial(ticker, year, quarter, **vals, source="TEST")
    return ticker


class TestCumulativeLatestQuarter:
    """累計データ: 最新四半期タイプのみ表示するテスト"""

    def test_returns_cumulative_list(self, ticker_with_3yr_data):
        """cumulativeキーがフラットリストで返されること"""
        result = get_financial_history(ticker_with_3yr_data)
        assert "cumulative" in result
        assert isinstance(result["cumulative"], list)

    def test_latest_fy_shows_fy_group(self, ticker_with_3yr_data):
        """最新がFYの場合、FYの年度推移が返ること"""
        # 最新は 2024 FY
        result = get_financial_history(ticker_with_3yr_data)
        assert result["cumulative_title"] == "通期"
        labels = [r["label"] for r in result["cumulative"]]
        assert labels == ["22/FY", "23/FY", "24/FY"]

    def test_fy_values_correct(self, ticker_with_3yr_data):
        """FY行の値が正しいこと"""
        result = get_financial_history(ticker_with_3yr_data)
        last_row = result["cumulative"][-1]  # 24/FY
        assert last_row["revenue"] == 530.0
        assert last_row["operating_income"] == 60.0
        assert last_row["eps"] == 250.0

    def test_yoy_calculation(self, ticker_with_3yr_data):
        """YoY%が前年同四半期と正しく比較されること"""
        result = get_financial_history(ticker_with_3yr_data)
        # 24/FY vs 23/FY: (530-450)/450*100 = 17.8%
        row_24 = result["cumulative"][2]
        assert row_24["revenue_yoy_pct"] == pytest.approx(17.8, abs=0.1)

    def test_first_year_no_yoy(self, ticker_with_3yr_data):
        """最古年のYoY%がNoneであること"""
        result = get_financial_history(ticker_with_3yr_data)
        first_row = result["cumulative"][0]  # 22/FY
        assert first_row["revenue_yoy_pct"] is None

    def test_latest_q2_shows_q2_group(self, test_db):
        """最新がQ2の場合、Q2の年度推移が返ること"""
        ticker = "9998"
        upsert_company(ticker, "テスト株式会社2", edinet_code="E99998")
        for year in ["2022", "2023", "2024"]:
            insert_financial(
                ticker, year, "Q2",
                revenue=float(int(year) * 10), operating_income=10.0,
                net_income=5.0, eps=1.0, source="TEST",
            )
            insert_financial(
                ticker, year, "FY",
                revenue=float(int(year) * 20), operating_income=20.0,
                net_income=10.0, eps=2.0, source="TEST",
            )
        # Q2を最新にするため2025 Q2を追加
        insert_financial(
            ticker, "2025", "Q2",
            revenue=25000.0, operating_income=100.0,
            net_income=50.0, eps=10.0, source="TEST",
        )
        result = get_financial_history(ticker)
        assert result["cumulative_title"] == "2Q累計"
        labels = [r["label"] for r in result["cumulative"]]
        assert labels == ["23/2Q", "24/2Q", "25/2Q"]

    def test_max_3_years(self, test_db):
        """4年以上あっても3年分のみ表示されること"""
        ticker = "9997"
        upsert_company(ticker, "テスト株式会社3", edinet_code="E99997")
        for year in range(2020, 2025):
            insert_financial(
                ticker, str(year), "FY",
                revenue=float(year), operating_income=10.0,
                net_income=5.0, eps=1.0, source="TEST",
            )
        result = get_financial_history(ticker)
        assert len(result["cumulative"]) == 3
        assert result["cumulative"][0]["label"] == "22/FY"
        assert result["cumulative"][2]["label"] == "24/FY"

    def test_short_label_format(self, ticker_with_3yr_data):
        """ラベルが短縮形式（YY/QQ）であること"""
        result = get_financial_history(ticker_with_3yr_data)
        for row in result["cumulative"]:
            assert "/" in row["label"]


class TestStandaloneQuarterlyLabels:
    """単独四半期ラベルの短縮形式テスト"""

    def test_quarterly_short_labels(self, ticker_with_3yr_data):
        """3か月決算のラベルも短縮形式であること"""
        result = get_financial_history(ticker_with_3yr_data)
        if result["quarterly"]:
            for row in result["quarterly"]:
                assert "/" in row["label"], f"Expected short label format, got: {row['label']}"
