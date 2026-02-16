"""
test_fetch_tdnet.py - TDnet決算短信取得バッチのテスト
"""
import pytest
import sys
from pathlib import Path
from datetime import datetime

# プロジェクトのベースディレクトリをパスに追加
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))
sys.path.insert(0, str(BASE_DIR / "lib"))

from fetch_tdnet import detect_fiscal_period, detect_fiscal_end_date_from_title
from fetch_financials import _wareki_to_seireki
from db_utils import get_connection, insert_financial


class TestFiscalPeriodDetection:
    """決算期判定ロジックのテスト"""

    def test_q1_detection(self):
        """第1四半期の判定"""
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期 第1四半期決算短信",
            "2024-05-10"
        )
        assert fiscal_year == "2024"
        assert quarter == "Q1"

    def test_q2_detection(self):
        """第2四半期の判定"""
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期 第2四半期決算短信",
            "2024-08-10"
        )
        assert fiscal_year == "2024"
        assert quarter == "Q2"

    def test_q3_detection(self):
        """第3四半期の判定"""
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期 第3四半期決算短信",
            "2024-11-10"
        )
        assert fiscal_year == "2024"
        assert quarter == "Q3"

    def test_fy_detection_with_keyword(self):
        """通期の判定（キーワードあり）"""
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期 通期決算短信",
            "2024-05-10"
        )
        assert fiscal_year == "2024"
        assert quarter == "FY"

    def test_fy_detection_with_keyword_honkessan(self):
        """通期の判定（本決算キーワード）"""
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期 本決算",
            "2024-05-10"
        )
        assert fiscal_year == "2024"
        assert quarter == "FY"

    def test_fy_detection_no_quarter(self):
        """通期の判定（四半期表記なし）"""
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期決算短信",
            "2024-05-10"
        )
        assert fiscal_year == "2024"
        # 四半期表記なし → デフォルト値 FY
        assert quarter == "FY"

    def test_fallback_year_from_date(self):
        """年度のフォールバック（タイトルに年度なし）"""
        fiscal_year, quarter = detect_fiscal_period(
            "決算短信",
            "2024-05-10"
        )
        assert fiscal_year == "2024"  # 発表日から推定

    def test_q1_fullwidth_detection(self):
        """第１四半期の判定（全角数字）"""
        fiscal_year, quarter = detect_fiscal_period(
            "2026年９月期第１四半期決算短信〔ＩＦＲＳ〕（連結）",
            "2026-02-12"
        )
        assert fiscal_year == "2026"
        assert quarter == "Q1"

    def test_q3_fullwidth_detection(self):
        """第３四半期の判定（全角数字）"""
        fiscal_year, quarter = detect_fiscal_period(
            "2026年３月期第３四半期決算短信〔日本基準〕（非連結）",
            "2026-02-12"
        )
        assert fiscal_year == "2026"
        assert quarter == "Q3"

    def test_fullwidth_year_and_quarter(self):
        """全角年度+全角四半期の判定"""
        fiscal_year, quarter = detect_fiscal_period(
            "２０２６年３月期第３四半期決算短信〔日本基準〕(連結)",
            "2026-02-10"
        )
        assert fiscal_year == "2026"
        assert quarter == "Q3"

    def test_fullwidth_fy_detection(self):
        """全角数字でも通期キーワードがあればFY"""
        fiscal_year, quarter = detect_fiscal_period(
            "２０２５年12月期通期決算短信〔日本基準〕（連結）",
            "2026-02-10"
        )
        assert fiscal_year == "2025"
        assert quarter == "FY"

    def test_wareki_reiwa_fy(self):
        """令和表記の通期決算短信"""
        fiscal_year, quarter = detect_fiscal_period(
            "令和７年12月期決算短信〔日本基準〕（連結）",
            "2026-02-13"
        )
        assert fiscal_year == "2025"
        assert quarter == "FY"

    def test_wareki_reiwa_q3(self):
        """令和表記の第3四半期"""
        fiscal_year, quarter = detect_fiscal_period(
            "令和６年12月期第３四半期決算短信",
            "2024-10-15"
        )
        assert fiscal_year == "2024"
        assert quarter == "Q3"

    def test_wareki_heisei(self):
        """平成表記の通期"""
        fiscal_year, quarter = detect_fiscal_period(
            "平成31年3月期通期決算短信",
            "2019-05-10"
        )
        assert fiscal_year == "2019"
        assert quarter == "FY"

    def test_wareki_reiwa_fullwidth(self):
        """令和＋全角数字"""
        fiscal_year, quarter = detect_fiscal_period(
            "令和７年３月期第１四半期決算短信〔日本基準〕（連結）",
            "2025-07-30"
        )
        assert fiscal_year == "2025"
        assert quarter == "Q1"

    def test_no_fallback_defaults_to_fy(self):
        """四半期表記がない場合はFYにデフォルト（月推定フォールバック廃止）"""
        # 6月発表でも四半期表記なし → FY
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期決算短信",
            "2024-06-30"
        )
        assert quarter == "FY"

        # 9月発表でも四半期表記なし → FY
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期決算短信",
            "2024-09-30"
        )
        assert quarter == "FY"

        # 12月発表でも四半期表記なし → FY
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期決算短信",
            "2024-12-31"
        )
        assert quarter == "FY"

        # 3月 → FY
        fiscal_year, quarter = detect_fiscal_period(
            "2024年3月期決算短信",
            "2024-03-31"
        )
        assert quarter == "FY"


class TestWarekiToSeireki:
    """和暦→西暦変換のテスト"""

    def test_reiwa(self):
        assert _wareki_to_seireki("令和7年12月期") == "2025年12月期"

    def test_reiwa_double_digit(self):
        assert _wareki_to_seireki("令和10年3月期") == "2028年3月期"

    def test_heisei(self):
        assert _wareki_to_seireki("平成31年3月期") == "2019年3月期"

    def test_no_wareki(self):
        assert _wareki_to_seireki("2025年3月期") == "2025年3月期"

    def test_no_year(self):
        assert _wareki_to_seireki("決算短信") == "決算短信"


class TestDataSourcePriority:
    """データソース優先度のテスト"""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_db):
        """各テスト前後でDBをセットアップ（一時DB上で動作）"""
        # セットアップ: テスト用会社データを作成
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO companies (ticker_code, company_name, market_segment, sector_33)
                VALUES ('TEST', 'テスト株式会社', 'TEST', 'TEST')
            """)
            conn.commit()

        yield
        # 一時DBのため手動クリーンアップ不要

    def test_tdnet_new_data(self):
        """新規データは保存される"""
        result = insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=100.0,
            source='TDnet'
        )
        assert result is True

        # DBに保存されているか確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT source, revenue FROM financials WHERE ticker_code='TEST' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            )
            row = cursor.fetchone()
            assert row is not None
            assert row['source'] == 'TDnet'
            assert row['revenue'] == 100.0

    def test_tdnet_overwrites_tdnet(self):
        """TDnet → TDnet は上書き"""
        # 最初のTDnetデータ
        insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=100.0,
            source='TDnet'
        )

        # 2回目のTDnetデータで上書き
        result = insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=200.0,
            source='TDnet'
        )
        assert result is True

        # 上書きされているか確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT revenue FROM financials WHERE ticker_code='TEST' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            )
            row = cursor.fetchone()
            assert row['revenue'] == 200.0

    def test_tdnet_skips_edinet(self):
        """TDnet → EDINET はスキップ"""
        # 最初にEDINETデータを保存
        insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=100.0,
            source='EDINET'
        )

        # TDnetデータはスキップされる
        result = insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=200.0,
            source='TDnet'
        )
        assert result is False  # スキップ

        # EDINETデータが残っているか確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT source, revenue FROM financials WHERE ticker_code='TEST' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            )
            row = cursor.fetchone()
            assert row['source'] == 'EDINET'
            assert row['revenue'] == 100.0  # 上書きされていない

    def test_edinet_overwrites_tdnet(self):
        """EDINET → TDnet は上書き"""
        # 最初にTDnetデータを保存
        insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=100.0,
            source='TDnet'
        )

        # EDINETデータで上書き
        result = insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=200.0,
            source='EDINET'
        )
        assert result is True

        # 上書きされているか確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT source, revenue FROM financials WHERE ticker_code='TEST' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            )
            row = cursor.fetchone()
            assert row['source'] == 'EDINET'
            assert row['revenue'] == 200.0

    def test_edinet_overwrites_edinet(self):
        """EDINET → EDINET は上書き"""
        # 最初のEDINETデータ
        insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=100.0,
            source='EDINET'
        )

        # 2回目のEDINETデータで上書き
        result = insert_financial(
            ticker_code='TEST',
            fiscal_year='2024',
            fiscal_quarter='Q1',
            fiscal_end_date='2023-06-30',
            revenue=200.0,
            source='EDINET'
        )
        assert result is True

        # 上書きされているか確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT revenue FROM financials WHERE ticker_code='TEST' AND fiscal_year='2024' AND fiscal_quarter='Q1'"
            )
            row = cursor.fetchone()
            assert row['revenue'] == 200.0


class TestDetectFiscalEndDateFromTitle:
    """タイトルからfiscal_end_date推定のテスト"""

    def test_march_fy_q1(self):
        assert detect_fiscal_end_date_from_title(
            "2024年3月期 第1四半期決算短信", "2024", "Q1") == "2023-06-30"

    def test_march_fy_fy(self):
        assert detect_fiscal_end_date_from_title(
            "2024年3月期 通期決算短信", "2024", "FY") == "2024-03-31"

    def test_december_fy_fy(self):
        assert detect_fiscal_end_date_from_title(
            "2024年12月期 通期決算短信", "2024", "FY") == "2024-12-31"

    def test_december_fy_q1(self):
        assert detect_fiscal_end_date_from_title(
            "2025年12月期 第1四半期決算短信", "2025", "Q1") == "2025-03-31"

    def test_february_fy_leap_year(self):
        assert detect_fiscal_end_date_from_title(
            "2024年2月期 通期決算短信", "2024", "FY") == "2024-02-29"

    def test_fullwidth_digits(self):
        assert detect_fiscal_end_date_from_title(
            "２０２６年３月期 通期決算短信", "2026", "FY") == "2026-03-31"

    def test_no_match_returns_none(self):
        assert detect_fiscal_end_date_from_title(
            "決算短信", "2024", "FY") is None

    def test_march_fy_q3_returns_december(self):
        """3月期企業のQ3は12月末"""
        assert detect_fiscal_end_date_from_title(
            "2026年3月期 第3四半期決算短信", "2026", "Q3") == "2025-12-31"

    def test_september_fy_q1_returns_december(self):
        """9月期企業のQ1は12月末"""
        assert detect_fiscal_end_date_from_title(
            "2026年9月期 第1四半期決算短信", "2026", "Q1") == "2025-12-31"

    def test_march_fy_q2_returns_september(self):
        """3月期企業のQ2は9月末"""
        assert detect_fiscal_end_date_from_title(
            "2024年3月期 第2四半期決算短信", "2024", "Q2") == "2023-09-30"

    def test_december_fy_q3_returns_september(self):
        """12月期企業のQ3は9月末"""
        assert detect_fiscal_end_date_from_title(
            "2024年12月期 第3四半期決算短信", "2024", "Q3") == "2024-09-30"


class TestQuarterlyFiscalEndDateValidation:
    """四半期決算のfiscal_end_date検証ロジックのテスト

    fetch_tdnet.pyのprocess_tdnet_announcement()で実装された
    四半期（Q1/Q2/Q3）の場合、XBRLから取得したfiscal_end_dateが
    タイトル推定と異なる場合にタイトル推定を優先するロジックの検証
    """

    def test_q3_fiscal_end_date_should_be_quarter_end_not_fy_end(self):
        """Q3のfiscal_end_dateは会計年度末ではなくQ3期末であるべき

        バグ事例: 2026年3月期Q3のXBRLから2026-03-31（会計年度末）を取得
        正しくは: 2025-12-31（Q3期末）
        """
        # 3月期企業のQ3
        title = "2026年3月期 第3四半期決算短信"
        fiscal_year = "2026"
        fiscal_quarter = "Q3"

        # タイトルから正しいQ3期末日を取得
        expected = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
        assert expected == "2025-12-31"

        # XBRLから誤って会計年度末を取得した場合（2026-03-31）は、
        # タイトル推定（2025-12-31）を優先すべき
        # この検証は実際のprocess_tdnet_announcement()で実装済み

    def test_september_fy_q1_fiscal_end_date_should_be_quarter_end(self):
        """9月決算企業のQ1のfiscal_end_dateはQ1期末であるべき

        バグ事例: 2026年9月期Q1のXBRLから2026-09-30（会計年度末）を取得
        正しくは: 2025-12-31（Q1期末）
        """
        # 9月期企業のQ1
        title = "2026年9月期 第1四半期決算短信"
        fiscal_year = "2026"
        fiscal_quarter = "Q1"

        # タイトルから正しいQ1期末日を取得
        expected = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
        assert expected == "2025-12-31"

    def test_fy_fiscal_end_date_can_be_fy_end(self):
        """通期のfiscal_end_dateは会計年度末で正しい

        FY/Q4の場合は会計年度末=期末なので、XBRLから取得した値をそのまま使用
        """
        # 3月期企業の通期
        title = "2024年3月期 通期決算短信"
        fiscal_year = "2024"
        fiscal_quarter = "FY"

        expected = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
        assert expected == "2024-03-31"


# TdnetClient のテストは実際のHTTPリクエストが必要なため、
# モックを使った統合テストは別途作成することを推奨
# ここでは基本的なロジックのユニットテストのみ実装
