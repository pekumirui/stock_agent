"""
get_financial_history の累計データ表示ロジックのテスト
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import insert_financial, insert_management_forecast, upsert_company
from web.services.financial_service import get_financial_history


@pytest.fixture
def ticker_with_3yr_data(test_db):
    """3年分の決算データ（FY + Q1/Q2/Q3）を持つテスト銘柄"""
    ticker = "9999"
    upsert_company(ticker, "テスト株式会社", edinet_code="E99999")

    data = [
        # 2022年度
        ("2022", "Q1", {"revenue": 80.0, "operating_income": 8.0, "ordinary_income": 7.0, "net_income": 5.0, "eps": 40.0, "fiscal_end_date": "2021-06-30"}),
        ("2022", "Q2", {"revenue": 170.0, "operating_income": 17.0, "ordinary_income": 15.0, "net_income": 11.0, "eps": 85.0, "fiscal_end_date": "2021-09-30"}),
        ("2022", "FY", {"revenue": 400.0, "operating_income": 40.0, "ordinary_income": 36.0, "net_income": 28.0, "eps": 180.0, "fiscal_end_date": "2022-03-31"}),
        # 2023年度
        ("2023", "Q1", {"revenue": 100.0, "operating_income": 10.0, "ordinary_income": 9.0, "net_income": 7.0, "eps": 50.0, "fiscal_end_date": "2022-06-30"}),
        ("2023", "Q2", {"revenue": 200.0, "operating_income": 20.0, "ordinary_income": 18.0, "net_income": 14.0, "eps": 100.0, "fiscal_end_date": "2022-09-30"}),
        ("2023", "Q3", {"revenue": 310.0, "operating_income": 31.0, "ordinary_income": 28.0, "net_income": 21.0, "eps": 150.0, "fiscal_end_date": "2022-12-31"}),
        ("2023", "FY", {"revenue": 450.0, "operating_income": 50.0, "ordinary_income": 45.0, "net_income": 35.0, "eps": 200.0, "fiscal_end_date": "2023-03-31"}),
        # 2024年度
        ("2024", "Q1", {"revenue": 120.0, "operating_income": 14.0, "ordinary_income": 12.0, "net_income": 9.0, "eps": 60.0, "fiscal_end_date": "2023-06-30"}),
        ("2024", "Q2", {"revenue": 240.0, "operating_income": 28.0, "ordinary_income": 24.0, "net_income": 18.0, "eps": 120.0, "fiscal_end_date": "2023-09-30"}),
        ("2024", "FY", {"revenue": 530.0, "operating_income": 60.0, "ordinary_income": 55.0, "net_income": 42.0, "eps": 250.0, "fiscal_end_date": "2024-03-31"}),
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
                fiscal_end_date=f"{int(year)-1}-09-30",
            )
            insert_financial(
                ticker, year, "FY",
                revenue=float(int(year) * 20), operating_income=20.0,
                net_income=10.0, eps=2.0, source="TEST",
                fiscal_end_date=f"{year}-03-31",
            )
        # Q2を最新にするため2025 Q2を追加
        insert_financial(
            ticker, "2025", "Q2",
            revenue=25000.0, operating_income=100.0,
            net_income=50.0, eps=10.0, source="TEST",
            fiscal_end_date="2024-09-30",
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
                fiscal_end_date=f"{year}-03-31",
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


# ============================================================
# 会社予想 + 進捗率テスト
# ============================================================

@pytest.fixture
def ticker_with_q3_and_forecast(test_db):
    """Q3累計実績 + FY予想を持つテスト銘柄"""
    ticker = "9990"
    upsert_company(ticker, "予想テスト株式会社", edinet_code="E99990")

    # 前年度FY実績
    insert_financial(
        ticker, "2024", "FY",
        revenue=400.0, operating_income=40.0, ordinary_income=36.0,
        net_income=28.0, eps=180.0, fiscal_end_date="2024-03-31", source="TEST",
    )
    # 今期 Q1-Q3 累計
    insert_financial(
        ticker, "2025", "Q1",
        revenue=120.0, operating_income=14.0, ordinary_income=12.0,
        net_income=9.0, eps=60.0, fiscal_end_date="2024-06-30", source="TEST",
    )
    insert_financial(
        ticker, "2025", "Q2",
        revenue=240.0, operating_income=28.0, ordinary_income=24.0,
        net_income=18.0, eps=120.0, fiscal_end_date="2024-09-30", source="TEST",
    )
    insert_financial(
        ticker, "2025", "Q3",
        revenue=360.0, operating_income=42.0, ordinary_income=36.0,
        net_income=27.0, eps=180.0, fiscal_end_date="2024-12-31", source="TEST",
    )
    # 今期FY予想
    insert_management_forecast(
        ticker_code=ticker, fiscal_year="2025", fiscal_quarter="FY",
        announced_date="2024-05-10", forecast_type="initial",
        revenue=500.0, operating_income=55.0, ordinary_income=48.0,
        net_income=36.0, eps=240.0, source="TEST",
    )
    return ticker


class TestForecastProgressRate:
    """会社予想行の進捗率・来期予想表示テスト"""

    def test_q3_shows_progress_rate(self, ticker_with_q3_and_forecast):
        """Q3累計表示時: 会社予想 + 進捗率が表示されること"""
        result = get_financial_history(ticker_with_q3_and_forecast)
        fc = result["forecast"]
        assert fc is not None
        assert fc["label"] == "会社予想"
        assert fc["pct_type"] == "progress"
        # revenue進捗: 360 / 500 * 100 = 72.0%
        assert fc["revenue_pct"] == pytest.approx(72.0, abs=0.1)
        # operating_income進捗: 42 / 55 * 100 = 76.4%
        assert fc["operating_income_pct"] == pytest.approx(76.4, abs=0.1)
        # net_income進捗: 27 / 36 * 100 = 75.0%
        assert fc["net_income_pct"] == pytest.approx(75.0, abs=0.1)
        # eps進捗: 180 / 240 * 100 = 75.0%
        assert fc["eps_pct"] == pytest.approx(75.0, abs=0.1)

    def test_q3_progress_baseline(self, ticker_with_q3_and_forecast):
        """Q3表示時のベースラインが75であること"""
        result = get_financial_history(ticker_with_q3_and_forecast)
        assert result["forecast"]["progress_baseline"] == 75.0

    def test_q3_forecast_values(self, ticker_with_q3_and_forecast):
        """予想の絶対値が正しく返されること"""
        result = get_financial_history(ticker_with_q3_and_forecast)
        fc = result["forecast"]
        assert fc["revenue"] == 500.0
        assert fc["operating_income"] == 55.0
        assert fc["eps"] == 240.0

    def test_fy_shows_next_year_forecast(self, ticker_with_3yr_data, test_db):
        """FY表示時: 来期予想 + YoY%が表示されること"""
        ticker = ticker_with_3yr_data
        # 来期(2025年度)予想を追加
        insert_management_forecast(
            ticker_code=ticker, fiscal_year="2025", fiscal_quarter="FY",
            announced_date="2024-05-10", forecast_type="initial",
            revenue=600.0, operating_income=70.0, ordinary_income=65.0,
            net_income=50.0, eps=300.0, source="TEST",
        )
        result = get_financial_history(ticker)
        fc = result["forecast"]
        assert fc is not None
        assert fc["label"] == "来期予想"
        assert fc["pct_type"] == "yoy"
        # revenue YoY: (600 - 530) / 530 * 100 = 13.2%
        assert fc["revenue_pct"] == pytest.approx(13.2, abs=0.1)

    def test_fy_without_next_forecast_returns_none(self, ticker_with_3yr_data):
        """FY表示時: 来期予想がなければforecast=Noneであること"""
        result = get_financial_history(ticker_with_3yr_data)
        assert result["forecast"] is None

    def test_q1_progress_small_value(self, test_db):
        """Q1累計でも進捗率が正しく計算されること（小さい値）"""
        ticker = "9989"
        upsert_company(ticker, "Q1テスト株式会社", edinet_code="E99989")
        insert_financial(
            ticker, "2025", "Q1",
            revenue=100.0, operating_income=10.0, ordinary_income=9.0,
            net_income=7.0, eps=50.0, fiscal_end_date="2024-06-30", source="TEST",
        )
        insert_management_forecast(
            ticker_code=ticker, fiscal_year="2025", fiscal_quarter="FY",
            announced_date="2024-05-10", forecast_type="initial",
            revenue=400.0, operating_income=40.0, ordinary_income=36.0,
            net_income=28.0, eps=200.0, source="TEST",
        )
        result = get_financial_history(ticker)
        fc = result["forecast"]
        assert fc is not None
        assert fc["label"] == "会社予想"
        assert fc["progress_baseline"] == 25.0
        # revenue進捗: 100 / 400 * 100 = 25.0%
        assert fc["revenue_pct"] == pytest.approx(25.0, abs=0.1)

    def test_negative_forecast_returns_none_pct(self, test_db):
        """赤字予想（負値）の場合、進捗率がNoneであること"""
        ticker = "9988"
        upsert_company(ticker, "赤字テスト株式会社", edinet_code="E99988")
        insert_financial(
            ticker, "2025", "Q3",
            revenue=300.0, operating_income=-5.0, ordinary_income=-3.0,
            net_income=-2.0, eps=-10.0, fiscal_end_date="2024-12-31", source="TEST",
        )
        insert_management_forecast(
            ticker_code=ticker, fiscal_year="2025", fiscal_quarter="FY",
            announced_date="2024-05-10", forecast_type="initial",
            revenue=400.0, operating_income=-10.0, ordinary_income=-5.0,
            net_income=-3.0, eps=-15.0, source="TEST",
        )
        result = get_financial_history(ticker)
        fc = result["forecast"]
        assert fc is not None
        # revenue は正値予想なので進捗率あり
        assert fc["revenue_pct"] == pytest.approx(75.0, abs=0.1)
        # 負値予想の項目は進捗率None
        assert fc["operating_income_pct"] is None
        assert fc["net_income_pct"] is None
        assert fc["eps_pct"] is None

    def test_no_forecast_data_returns_none(self, test_db):
        """予想データがない場合、forecast=Noneであること"""
        ticker = "9987"
        upsert_company(ticker, "予想なしテスト株式会社", edinet_code="E99987")
        insert_financial(
            ticker, "2025", "Q3",
            revenue=300.0, operating_income=30.0, ordinary_income=27.0,
            net_income=20.0, eps=130.0, fiscal_end_date="2024-12-31", source="TEST",
        )
        result = get_financial_history(ticker)
        assert result["forecast"] is None
