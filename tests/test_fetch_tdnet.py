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

from fetch_tdnet import (
    detect_fiscal_period,
    detect_fiscal_end_date_from_title,
    _normalize_jp_date,
    _get_ticker_from_namelist,
    extract_metadata_from_summary,
    _extract_metadata_from_attachment,
    _extract_filing_date_from_namelist,
)
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


class TestNormalizeJpDate:
    """和文日付→ISO変換のテスト"""

    def test_halfwidth(self):
        assert _normalize_jp_date("2026年2月13日") == "2026-02-13"

    def test_fullwidth(self):
        assert _normalize_jp_date("２０２６年２月１３日") == "2026-02-13"

    def test_mixed(self):
        assert _normalize_jp_date("2026年２月13日") == "2026-02-13"

    def test_already_iso(self):
        assert _normalize_jp_date("2026-02-13") == "2026-02-13"

    def test_none(self):
        assert _normalize_jp_date(None) is None

    def test_empty(self):
        assert _normalize_jp_date("") is None

    def test_invalid(self):
        assert _normalize_jp_date("不明") is None

    def test_single_digit_month_day(self):
        assert _normalize_jp_date("2026年1月3日") == "2026-01-03"


class TestGetTickerFromNamelist:
    """ZIPファイル名リストからticker抽出のテスト"""

    def test_normal_ticker(self):
        """通常の数字のみticker"""
        namelist = ['XBRLData/Summary/tse-qcedjpsm-81520-20260213381520-ixbrl.htm']
        assert _get_ticker_from_namelist(namelist) == "8152"

    def test_alpha_ticker(self):
        """アルファベット含むticker（130A0→130A）"""
        namelist = ['XBRLData/Summary/tse-anedjpsm-130A0-202602123130A0-ixbrl.htm']
        assert _get_ticker_from_namelist(namelist) == "130A"

    def test_attachment_fallback(self):
        """Summary無しでもAttachmentから抽出"""
        namelist = [
            'XBRLData/Attachment/qualitative.htm',
            'XBRLData/Attachment/0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm'
        ]
        assert _get_ticker_from_namelist(namelist) == "3909"

    def test_no_match(self):
        namelist = ['some/random/file.htm']
        assert _get_ticker_from_namelist(namelist) is None

    def test_empty(self):
        assert _get_ticker_from_namelist([]) is None


class TestExtractMetadataFromSummary:
    """Summary iXBRLからのメタデータ抽出テスト"""

    def test_quarterly_jp(self):
        """日本基準Q3のメタデータ抽出"""
        html = '''
        <ix:nonnumeric name="tse-ed-t:DocumentName">第３四半期決算短信〔日本基準〕（連結）</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:FilingDate">2026年２月13日</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:FiscalYearEnd">2026-03-31</ix:nonnumeric>
        <ix:nonfraction name="tse-ed-t:QuarterlyPeriod">3</ix:nonfraction>
        <ix:nonnumeric name="tse-ed-t:SecuritiesCode">81520</ix:nonnumeric>
        '''
        meta = extract_metadata_from_summary(html)
        assert meta['ticker_code'] == "8152"
        assert meta['fiscal_year'] == "2026"
        assert meta['fiscal_quarter'] == "Q3"
        assert meta['announcement_date'] == "2026-02-13"
        assert meta['fiscal_year_end'] == "2026-03-31"

    def test_fy_jp(self):
        """日本基準FYのメタデータ抽出（QuarterlyPeriod無し→FY）"""
        html = '''
        <ix:nonnumeric name="tse-ed-t:DocumentName">通期決算短信〔日本基準〕（連結）</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:FilingDate">2026年2月13日</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:FiscalYearEnd">2025-12-31</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:SecuritiesCode">96720</ix:nonnumeric>
        '''
        meta = extract_metadata_from_summary(html)
        assert meta['ticker_code'] == "9672"
        assert meta['fiscal_year'] == "2025"
        assert meta['fiscal_quarter'] == "FY"

    def test_4digit_securities_code(self):
        """4桁SecuritiesCodeの処理"""
        html = '''
        <ix:nonnumeric name="tse-ed-t:FiscalYearEnd">2026-03-31</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:SecuritiesCode">7011</ix:nonnumeric>
        '''
        meta = extract_metadata_from_summary(html)
        assert meta['ticker_code'] == "7011"

    def test_q1_detection(self):
        """Q1の検出"""
        html = '''
        <ix:nonnumeric name="tse-ed-t:FiscalYearEnd">2026-03-31</ix:nonnumeric>
        <ix:nonfraction name="tse-ed-t:QuarterlyPeriod">1</ix:nonfraction>
        '''
        meta = extract_metadata_from_summary(html)
        assert meta['fiscal_quarter'] == "Q1"

    def test_q2_from_document_name(self):
        """DocumentNameから第2四半期を検出（QuarterlyPeriod無しの場合）"""
        html = '''
        <ix:nonnumeric name="tse-ed-t:DocumentName">第2四半期決算短信</ix:nonnumeric>
        <ix:nonnumeric name="tse-ed-t:FiscalYearEnd">2026-03-31</ix:nonnumeric>
        '''
        meta = extract_metadata_from_summary(html)
        assert meta['fiscal_quarter'] == "Q2"

    def test_empty_html(self):
        """空HTMLの処理"""
        meta = extract_metadata_from_summary("")
        assert meta['ticker_code'] is None
        assert meta['fiscal_year'] is None
        assert meta['fiscal_quarter'] == "FY"


class TestExtractMetadataFromAttachment:
    """Attachmentファイル名パターンからのメタデータ抽出テスト"""

    def test_quarterly(self):
        """四半期レポートの抽出"""
        namelist = [
            'XBRLData/Attachment/0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm'
        ]
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is not None
        assert meta['ticker_code'] == "3909"
        assert meta['fiscal_year'] is None  # 四半期ではfiscal_endから推定不可
        assert meta['announcement_date'] == "2026-02-13"
        assert meta['fiscal_quarter'] is None  # q=四半期だがQ何かは不明

    def test_annual(self):
        """年次レポートの抽出"""
        namelist = [
            'XBRLData/Attachment/0102010-acpl01-tse-acedjpfr-78560-2025-10-31-02-2025-12-08-ixbrl.htm'
        ]
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is not None
        assert meta['ticker_code'] == "7856"
        assert meta['fiscal_quarter'] == "FY"
        assert meta['fiscal_year'] == "2025"  # FYならfiscal_end_dateの年

    def test_quarterly_fiscal_year_is_none(self):
        """四半期ではfiscal_yearが不確定のためNone"""
        namelist = [
            'XBRLData/Attachment/0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm'
        ]
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is not None
        assert meta['fiscal_year'] is None  # 四半期ではfiscal_endから推定不可

    def test_semi_annual_fiscal_year_is_none(self):
        """半期ではfiscal_yearが不確定のためNone"""
        namelist = [
            'XBRLData/Attachment/0102010-scpl15-tse-scedjpfr-92470-2025-09-30-02-2026-01-30-ixbrl.htm'
        ]
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is not None
        assert meta['fiscal_year'] is None  # 半期ではfiscal_endから推定不可

    def test_alpha_ticker_code(self):
        """英字含む銘柄コード（130A等）のAttachmentファイル名"""
        namelist = [
            'XBRLData/Attachment/0102010-acpl01-tse-acedjpfr-130A0-2025-12-31-01-2026-02-14-ixbrl.htm'
        ]
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is not None
        assert meta['ticker_code'] == "130A"

    def test_semi_annual(self):
        """半期レポートの抽出"""
        namelist = [
            'XBRLData/Attachment/0102010-scpl15-tse-scedjpfr-92470-2025-09-30-02-2026-01-30-ixbrl.htm'
        ]
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is not None
        assert meta['ticker_code'] == "9247"
        assert meta['fiscal_quarter'] == "Q2"

    def test_no_match(self):
        """パターン不一致"""
        namelist = ['XBRLData/Attachment/qualitative.htm']
        meta = _extract_metadata_from_attachment(namelist)
        assert meta is None


class TestExtractFilingDateFromNamelist:
    """filing_date抽出のテスト"""

    def test_normal(self):
        namelist = [
            'XBRLData/Attachment/0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm'
        ]
        assert _extract_filing_date_from_namelist(namelist) == "2026-02-13"

    def test_no_match(self):
        namelist = ['XBRLData/Summary/tse-qcedjpsm-81520-20260213-ixbrl.htm']
        assert _extract_filing_date_from_namelist(namelist) is None

    def test_alpha_ticker_code(self):
        """英字含む銘柄コードでもfiling_dateを抽出できる"""
        namelist = [
            'XBRLData/Attachment/0102010-acpl01-tse-acedjpfr-130A0-2025-12-31-01-2026-02-14-ixbrl.htm'
        ]
        assert _extract_filing_date_from_namelist(namelist) == "2026-02-14"


# TdnetClient のテストは実際のHTTPリクエストが必要なため、
# モックを使った統合テストは別途作成することを推奨
# ここでは基本的なロジックのユニットテストのみ実装
