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

import json
from unittest.mock import MagicMock, patch
from fetch_tdnet import (
    detect_fiscal_period,
    detect_fiscal_end_date_from_title,
    compute_fiscal_end_date,
    _normalize_jp_date,
    _get_ticker_from_namelist,
    extract_metadata_from_summary,
    _extract_metadata_from_attachment,
    _extract_filing_date_from_namelist,
    _pick_ix_value,
    _load_or_fetch_announcements,
)
from fetch_financials import _wareki_to_seireki
from db_utils import get_connection, insert_financial, insert_announcement


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


def _make_mock_zf(dei_html: str = '') -> MagicMock:
    """テスト用のZipFileモックを作成（DEI要素を含むiXBRL HTMLを返す）"""
    zf = MagicMock()
    zf.read.return_value = dei_html.encode('utf-8')
    return zf


class TestExtractMetadataFromAttachment:
    """Attachmentファイル名+iXBRL DEI要素からのメタデータ抽出テスト"""

    def test_quarterly_with_dei(self):
        """四半期レポート: DEIからQ3を正しく取得"""
        namelist = [
            'XBRLData/Attachment/0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm'
        ]
        dei_html = '''
        <ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">Q3</ix:nonNumeric>
        <ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant">2026-03-31</ix:nonNumeric>
        '''
        zf = _make_mock_zf(dei_html)
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['ticker_code'] == "3909"
        assert meta['fiscal_quarter'] == "Q3"
        assert meta['fiscal_year'] == "2026"
        assert meta['announcement_date'] == "2026-02-13"

    def test_quarterly_q4_dei(self):
        """Q4（12月決算企業の第4四半期）をDEIから取得"""
        namelist = [
            'XBRLData/Attachment/0101010-qcbs01-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm'
        ]
        dei_html = '''
        <ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">Q4</ix:nonNumeric>
        <ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant">2025-03-31</ix:nonNumeric>
        '''
        zf = _make_mock_zf(dei_html)
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['fiscal_quarter'] == "Q4"
        assert meta['fiscal_year'] == "2025"

    def test_semi_annual_hy_dei(self):
        """半期レポート: DEIのHYをQ2にマッピング"""
        namelist = [
            'XBRLData/Attachment/0102010-scpl15-tse-scedjpfr-92470-2025-09-30-02-2026-01-30-ixbrl.htm'
        ]
        dei_html = '''
        <ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">HY</ix:nonNumeric>
        <ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant">2026-03-31</ix:nonNumeric>
        '''
        zf = _make_mock_zf(dei_html)
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['ticker_code'] == "9247"
        assert meta['fiscal_quarter'] == "Q2"
        assert meta['fiscal_year'] == "2026"

    def test_annual_with_dei(self):
        """年次レポート: DEIからFYを取得"""
        namelist = [
            'XBRLData/Attachment/0102010-acpl01-tse-acedjpfr-78560-2025-10-31-02-2025-12-08-ixbrl.htm'
        ]
        dei_html = '''
        <ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">FY</ix:nonNumeric>
        <ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant">2025-10-31</ix:nonNumeric>
        '''
        zf = _make_mock_zf(dei_html)
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['ticker_code'] == "7856"
        assert meta['fiscal_quarter'] == "FY"
        assert meta['fiscal_year'] == "2025"

    def test_dei_missing_fallback_annual(self):
        """DEI欠損時: taxonomy prefix a → FY フォールバック"""
        namelist = [
            'XBRLData/Attachment/0102010-acpl01-tse-acedjpfr-78560-2025-10-31-02-2025-12-08-ixbrl.htm'
        ]
        zf = _make_mock_zf('')  # DEI要素なし
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['fiscal_quarter'] == "FY"
        assert meta['fiscal_year'] == "2025"

    def test_dei_missing_fallback_semi_annual(self):
        """DEI欠損時: taxonomy prefix s → Q2 フォールバック"""
        namelist = [
            'XBRLData/Attachment/0102010-scpl15-tse-scedjpfr-92470-2025-09-30-02-2026-01-30-ixbrl.htm'
        ]
        zf = _make_mock_zf('')  # DEI要素なし
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['fiscal_quarter'] == "Q2"

    def test_alpha_ticker_code(self):
        """英字含む銘柄コード（130A等）のAttachmentファイル名"""
        namelist = [
            'XBRLData/Attachment/0102010-acpl01-tse-acedjpfr-130A0-2025-12-31-01-2026-02-14-ixbrl.htm'
        ]
        dei_html = '''
        <ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">FY</ix:nonNumeric>
        <ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant">2025-12-31</ix:nonNumeric>
        '''
        zf = _make_mock_zf(dei_html)
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['ticker_code'] == "130A"

    def test_no_match(self):
        """パターン不一致"""
        namelist = ['XBRLData/Attachment/qualitative.htm']
        zf = _make_mock_zf('')
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is None

    def test_dei_spread_across_files(self):
        """DEI要素が複数ファイルに分散している場合も集約できること"""
        namelist = [
            'XBRLData/Attachment/0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm',
            'XBRLData/Attachment/0101010-qcbs01-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm',
        ]
        # ファイル1: TypeOfCurrentPeriodDEIのみ、ファイル2: CurrentFiscalYearEndDateDEIのみ
        html_pl = '<ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">Q3</ix:nonNumeric>'
        html_bs = '<ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant">2026-03-31</ix:nonNumeric>'
        zf = MagicMock()
        zf.read.side_effect = lambda name: {
            namelist[0]: html_pl.encode('utf-8'),
            namelist[1]: html_bs.encode('utf-8'),
        }[name]
        meta = _extract_metadata_from_attachment(zf, namelist)
        assert meta is not None
        assert meta['fiscal_quarter'] == "Q3"
        assert meta['fiscal_year'] == "2026"


class TestPickIxValue:
    """_pick_ix_value ヘルパーのテスト"""

    def test_normal(self):
        html = '<ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">Q3</ix:nonNumeric>'
        assert _pick_ix_value(html, 'jpdei_cor:TypeOfCurrentPeriodDEI') == 'Q3'

    def test_with_nested_tags(self):
        html = '<ix:nonNumeric name="jpdei_cor:CurrentFiscalYearEndDateDEI" contextRef="FilingDateInstant"><div>2026-03-31</div></ix:nonNumeric>'
        assert _pick_ix_value(html, 'jpdei_cor:CurrentFiscalYearEndDateDEI') == '2026-03-31'

    def test_not_found(self):
        html = '<ix:nonNumeric name="other:Tag">value</ix:nonNumeric>'
        assert _pick_ix_value(html, 'jpdei_cor:TypeOfCurrentPeriodDEI') is None

    def test_empty_value(self):
        html = '<ix:nonNumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant"></ix:nonNumeric>'
        assert _pick_ix_value(html, 'jpdei_cor:TypeOfCurrentPeriodDEI') is None

    def test_single_quote(self):
        """シングルクオートのname属性にも対応"""
        html = "<ix:nonnumeric name='jpdei_cor:TypeOfCurrentPeriodDEI' contextRef='FilingDateInstant'>Q3</ix:nonnumeric>"
        assert _pick_ix_value(html, 'jpdei_cor:TypeOfCurrentPeriodDEI') == 'Q3'

    def test_lowercase_tag(self):
        """小文字タグ名にも対応"""
        html = '<ix:nonnumeric name="jpdei_cor:TypeOfCurrentPeriodDEI" contextRef="FilingDateInstant">Q1</ix:nonnumeric>'
        assert _pick_ix_value(html, 'jpdei_cor:TypeOfCurrentPeriodDEI') == 'Q1'


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


class TestLoadOrFetchAnnouncements:
    """_load_or_fetch_announcements() のテスト"""

    SAMPLE_ANNOUNCEMENTS = [
        {
            'ticker_code': '7203',
            'company_name': 'トヨタ自動車',
            'title': '2026年3月期 第3四半期決算短信',
            'announcement_date': '2026-02-14',
            'announcement_time': '15:00',
            'announcement_type': 'earnings',
            'xbrl_zip_url': 'https://example.com/test.zip',
            'document_url': 'https://example.com/test.pdf',
        },
        {
            'ticker_code': '6758',
            'company_name': 'ソニーグループ',
            'title': '業績予想の修正に関するお知らせ',
            'announcement_date': '2026-02-14',
            'announcement_time': '16:00',
            'announcement_type': 'revision',
            'xbrl_zip_url': None,
            'document_url': 'https://example.com/rev.pdf',
        },
    ]

    def test_no_cache_fetches_and_saves_json(self, tmp_path):
        """JSONキャッシュなし → client呼び出し + JSON保存"""
        cache_dir = tmp_path / "2026-02-14"
        client = MagicMock()
        client.get_announcements.return_value = self.SAMPLE_ANNOUNCEMENTS

        result = _load_or_fetch_announcements(client, "2026-02-14", cache_dir)

        assert result == self.SAMPLE_ANNOUNCEMENTS
        client.get_announcements.assert_called_once_with("2026-02-14")
        manifest = cache_dir / "_announcements.json"
        assert manifest.exists()
        saved = json.loads(manifest.read_text(encoding="utf-8"))
        assert len(saved) == 2
        assert saved[0]['ticker_code'] == '7203'

    def test_cache_hit_skips_client(self, tmp_path):
        """JSONキャッシュあり → JSON読み込み（client呼ばない）"""
        cache_dir = tmp_path / "2026-02-14"
        cache_dir.mkdir()
        manifest = cache_dir / "_announcements.json"
        manifest.write_text(
            json.dumps(self.SAMPLE_ANNOUNCEMENTS, ensure_ascii=False),
            encoding="utf-8"
        )
        client = MagicMock()

        result = _load_or_fetch_announcements(client, "2026-02-14", cache_dir)

        assert result == self.SAMPLE_ANNOUNCEMENTS
        client.get_announcements.assert_not_called()

    def test_corrupted_cache_refetches(self, tmp_path):
        """JSON破損 → 削除して再取得"""
        cache_dir = tmp_path / "2026-02-14"
        cache_dir.mkdir()
        manifest = cache_dir / "_announcements.json"
        manifest.write_text("invalid json{{{", encoding="utf-8")
        client = MagicMock()
        client.get_announcements.return_value = self.SAMPLE_ANNOUNCEMENTS

        result = _load_or_fetch_announcements(client, "2026-02-14", cache_dir)

        assert result == self.SAMPLE_ANNOUNCEMENTS
        client.get_announcements.assert_called_once()
        # 破損ファイルは削除され、正しいJSONが書き直される
        saved = json.loads(manifest.read_text(encoding="utf-8"))
        assert len(saved) == 2

    def test_force_ignores_cache(self, tmp_path):
        """force=True → JSON無視して再取得"""
        cache_dir = tmp_path / "2026-02-14"
        cache_dir.mkdir()
        manifest = cache_dir / "_announcements.json"
        manifest.write_text(json.dumps([]), encoding="utf-8")
        client = MagicMock()
        client.get_announcements.return_value = self.SAMPLE_ANNOUNCEMENTS

        result = _load_or_fetch_announcements(
            client, "2026-02-14", cache_dir, force=True
        )

        assert result == self.SAMPLE_ANNOUNCEMENTS
        client.get_announcements.assert_called_once()

    def test_client_exception_returns_none(self, tmp_path):
        """client例外 → None返却、JSON未作成"""
        cache_dir = tmp_path / "2026-02-14"
        client = MagicMock()
        client.get_announcements.side_effect = ConnectionError("network error")

        result = _load_or_fetch_announcements(client, "2026-02-14", cache_dir)

        assert result is None
        manifest = cache_dir / "_announcements.json"
        assert not manifest.exists()

    def test_empty_list_saved_as_cache_for_past_date(self, tmp_path):
        """過去日の空リスト（開示なし日）もJSONキャッシュとして保存"""
        cache_dir = tmp_path / "2020-01-01"
        client = MagicMock()
        client.get_announcements.return_value = []

        result = _load_or_fetch_announcements(client, "2020-01-01", cache_dir)

        assert result == []
        manifest = cache_dir / "_announcements.json"
        assert manifest.exists()
        assert json.loads(manifest.read_text(encoding="utf-8")) == []

    def test_today_not_cached(self, tmp_path):
        """当日分はJSONキャッシュを作成しない（開示追加の可能性）"""
        today = datetime.now().strftime("%Y-%m-%d")
        cache_dir = tmp_path / today
        client = MagicMock()
        client.get_announcements.return_value = self.SAMPLE_ANNOUNCEMENTS

        result = _load_or_fetch_announcements(client, today, cache_dir)

        assert result == self.SAMPLE_ANNOUNCEMENTS
        manifest = cache_dir / "_announcements.json"
        assert not manifest.exists()


class TestInsertAnnouncementUpsert:
    """insert_announcement() UPSERT動作のテスト"""

    def test_earnings_dedup(self, test_db, sample_company):
        """同じearnings 2回insert → 1行のみ"""
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='15:00', announcement_type='earnings',
            title='2026年3月期 第3四半期決算短信',
            fiscal_year='2026', fiscal_quarter='Q3',
        )
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='15:00', announcement_type='earnings',
            title='2026年3月期 第3四半期決算短信',
            fiscal_year='2026', fiscal_quarter='Q3',
        )
        with get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM announcements WHERE ticker_code = '9999'"
            ).fetchone()[0]
        assert count == 1

    def test_revision_dedup_null_fiscal(self, test_db, sample_company):
        """同じrevision 2回insert（fiscal_year/quarter=NULL）→ 1行のみ"""
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='16:00', announcement_type='revision',
            title='業績予想の修正に関するお知らせ',
        )
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='16:00', announcement_type='revision',
            title='業績予想の修正に関するお知らせ',
        )
        with get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM announcements "
                "WHERE ticker_code = '9999' AND announcement_type = 'revision'"
            ).fetchone()[0]
        assert count == 1

    def test_upsert_updates_fields(self, test_db, sample_company):
        """UPSERTでannouncement_time等が更新されること"""
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='15:00', announcement_type='earnings',
            title='2026年3月期 第3四半期決算短信',
            fiscal_year='2026', fiscal_quarter='Q3',
        )
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='15:30', announcement_type='earnings',
            title='2026年3月期 第3四半期決算短信',
            fiscal_year='2026', fiscal_quarter='Q3',
            document_url='https://example.com/updated.pdf',
        )
        with get_connection() as conn:
            row = conn.execute(
                "SELECT announcement_time, document_url FROM announcements "
                "WHERE ticker_code = '9999'"
            ).fetchone()
        assert row[0] == '15:30'
        assert row[1] == 'https://example.com/updated.pdf'

    def test_upsert_preserves_existing_on_null(self, test_db, sample_company):
        """UPSERTでNULL値は既存値を上書きしない（COALESCEで保護）"""
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time='15:00', announcement_type='earnings',
            title='2026年3月期 第3四半期決算短信',
            fiscal_year='2026', fiscal_quarter='Q3',
            document_url='https://example.com/original.pdf',
        )
        # 2回目はNULLで上書き試行
        insert_announcement(
            ticker_code='9999', announcement_date='2026-02-14',
            announcement_time=None, announcement_type='earnings',
            title='2026年3月期 第3四半期決算短信',
            fiscal_year=None, fiscal_quarter=None,
            document_url=None,
        )
        with get_connection() as conn:
            row = conn.execute(
                "SELECT announcement_time, fiscal_year, fiscal_quarter, document_url "
                "FROM announcements WHERE ticker_code = '9999'"
            ).fetchone()
        # 既存値が保持されること
        assert row[0] == '15:00'
        assert row[1] == '2026'
        assert row[2] == 'Q3'
        assert row[3] == 'https://example.com/original.pdf'


class TestComputeFiscalEndDate:
    """FiscalYearEnd + fiscal_quarter → fiscal_end_date計算のテスト"""

    def test_march_fy_q1(self):
        """3月期Q1 → 6月末"""
        assert compute_fiscal_end_date("2026-03-31", "Q1") == "2025-06-30"

    def test_march_fy_q2(self):
        """3月期Q2 → 9月末"""
        assert compute_fiscal_end_date("2026-03-31", "Q2") == "2025-09-30"

    def test_march_fy_q3(self):
        """3月期Q3 → 12月末"""
        assert compute_fiscal_end_date("2026-03-31", "Q3") == "2025-12-31"

    def test_march_fy_fy(self):
        """3月期FY → 3月末"""
        assert compute_fiscal_end_date("2026-03-31", "FY") == "2026-03-31"

    def test_december_fy_q1(self):
        """12月期Q1 → 3月末"""
        assert compute_fiscal_end_date("2025-12-31", "Q1") == "2025-03-31"

    def test_december_fy_q3(self):
        """12月期Q3 → 9月末"""
        assert compute_fiscal_end_date("2025-12-31", "Q3") == "2025-09-30"

    def test_september_fy_q1(self):
        """9月期Q1 → 12月末"""
        assert compute_fiscal_end_date("2026-09-30", "Q1") == "2025-12-31"

    def test_february_fy(self):
        """2月期FY → 2月末"""
        assert compute_fiscal_end_date("2026-02-28", "FY") == "2026-02-28"

    def test_none_input(self):
        """None入力"""
        assert compute_fiscal_end_date(None, "Q3") is None

    def test_invalid_format(self):
        """不正フォーマット"""
        assert compute_fiscal_end_date("2026/03/31", "Q3") is None

    def test_invalid_quarter(self):
        """不正四半期"""
        assert compute_fiscal_end_date("2026-03-31", "Q5") is None


class TestFiscalEndDateCorrectionWithFiscalYearEnd:
    """FiscalYearEndからfiscal_end_date補正のテスト

    キャッシュ経路でtitleに「YYYY年M月期」が含まれない場合に、
    FiscalYearEnd + QuarterlyPeriodから正しいfiscal_end_dateを計算するロジックの検証
    """

    def test_q3_title_without_fiscal_period_uses_fiscal_year_end(self):
        """タイトルに「YYYY年M月期」が無い場合、FiscalYearEndから計算される"""
        title = "第３四半期決算短信〔ＩＦＲＳ〕（連結）"
        fiscal_year = "2026"
        fiscal_quarter = "Q3"
        fiscal_year_end = "2026-03-31"

        # タイトルからは推定不可
        title_result = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
        assert title_result is None

        # FiscalYearEndからは正しく計算
        computed = compute_fiscal_end_date(fiscal_year_end, fiscal_quarter)
        assert computed == "2025-12-31"

    def test_title_with_fiscal_period_takes_priority(self):
        """タイトルに「YYYY年M月期」がある場合はタイトル推定が優先"""
        title = "2026年3月期 第3四半期決算短信〔IFRS〕(連結)"
        fiscal_year = "2026"
        fiscal_quarter = "Q3"

        title_result = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
        assert title_result == "2025-12-31"

    def test_q1_title_without_fiscal_period(self):
        """Q1でもタイトルに「YYYY年M月期」が無い場合はFiscalYearEndから計算"""
        title = "第１四半期決算短信〔日本基準〕（連結）"
        fiscal_year = "2026"
        fiscal_quarter = "Q1"
        fiscal_year_end = "2026-03-31"

        assert detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter) is None
        assert compute_fiscal_end_date(fiscal_year_end, fiscal_quarter) == "2025-06-30"
