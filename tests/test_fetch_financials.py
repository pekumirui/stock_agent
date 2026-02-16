"""
fetch_financials.py のテスト
"""
import pytest
import sys
import zipfile
import io
import tempfile
import shutil
from pathlib import Path
from decimal import Decimal

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from fetch_financials import (
    XBRL_FACT_MAPPING,
    XBRL_FACT_MAPPING_IFRS,
    XBRL_TEMPLATE_TO_QUARTER,
    _is_jppfs_namespace,
    _is_supported_namespace,
    _is_current_period_context,
    _detect_fiscal_year,
    _detect_quarter_from_xbrl_filename,
    _extract_fiscal_end_date_from_xbrl,
    extract_edinet_zip,
)
from xbrlp import QName


class TestXbrlFactMapping:
    """XBRL Fact → DBフィールドマッピングのテスト"""

    def test_revenue_mapping(self):
        """売上高の各バリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['NetSales'] == 'revenue'
        assert XBRL_FACT_MAPPING['Revenue'] == 'revenue'
        assert XBRL_FACT_MAPPING['OperatingRevenue'] == 'revenue'

    def test_operating_income_mapping(self):
        """営業利益のバリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['OperatingIncome'] == 'operating_income'
        assert XBRL_FACT_MAPPING['OperatingProfit'] == 'operating_income'

    def test_net_income_mapping(self):
        """当期純利益のバリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['ProfitLoss'] == 'net_income'
        assert XBRL_FACT_MAPPING['NetIncome'] == 'net_income'
        assert XBRL_FACT_MAPPING['ProfitLossAttributableToOwnersOfParent'] == 'net_income'

    def test_eps_mapping(self):
        """EPSのバリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['BasicEarningsLossPerShare'] == 'eps'
        assert XBRL_FACT_MAPPING['EarningsPerShare'] == 'eps'
        # EDINET有報・半期報の要素名
        assert XBRL_FACT_MAPPING['BasicEarningsLossPerShareSummaryOfBusinessResults'] == 'eps'
        assert XBRL_FACT_MAPPING['DilutedEarningsPerShareSummaryOfBusinessResults'] == 'eps'

    def test_eps_mapping_ifrs(self):
        """IFRS EPSのバリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING_IFRS['BasicEarningsLossPerShareIFRS'] == 'eps'
        assert XBRL_FACT_MAPPING_IFRS['BasicEarningsLossPerShareIFRSSummaryOfBusinessResults'] == 'eps'
        assert XBRL_FACT_MAPPING_IFRS['DilutedEarningsLossPerShareIFRSSummaryOfBusinessResults'] == 'eps'

    def test_eps_mapping_tdnet(self):
        """TDnet EPS要素（tse-ed-t/jpigp_cor名前空間）が正しくマッピングされること"""
        tdnet_eps_elements = [
            'NetIncomePerShare',
            'DilutedNetIncomePerShare',
            'DilutedEarningsPerShareIFRS',
            'DilutedEarningsLossPerShareIFRS',
            'NetIncomePerShareUS',
            'BasicAndDilutedEarningsLossPerShareIFRS',
        ]
        for elem in tdnet_eps_elements:
            assert XBRL_FACT_MAPPING_IFRS[elem] == 'eps', f"{elem} should map to eps"

    def test_tdnet_revenue_mapping(self):
        """TDnet売上要素（tse-ed-t/jpigp_cor名前空間）が正しくマッピングされること"""
        tdnet_revenue_elements = [
            'OperatingRevenues', 'OrdinaryRevenuesBK', 'OrdinaryRevenuesIN',
            'OperatingRevenuesSE', 'NetSalesIFRS', 'OperatingRevenuesIFRS',
            'NetSalesUS',
        ]
        for elem in tdnet_revenue_elements:
            assert XBRL_FACT_MAPPING_IFRS[elem] == 'revenue', f"{elem} should map to revenue"

    def test_tdnet_net_income_mapping(self):
        """TDnet純利益要素（tse-ed-t/jpigp_cor名前空間）が正しくマッピングされること"""
        assert XBRL_FACT_MAPPING_IFRS['ProfitAttributableToOwnersOfParent'] == 'net_income'
        assert XBRL_FACT_MAPPING_IFRS['ProfitLossIFRS'] == 'net_income'
        assert XBRL_FACT_MAPPING_IFRS['ProfitIFRS'] == 'net_income'
        assert XBRL_FACT_MAPPING_IFRS['NetIncomeUS'] == 'net_income'
        assert XBRL_FACT_MAPPING_IFRS['OperatingIncomeUS'] == 'operating_income'

    def test_revenue_industry_variants(self):
        """業種別売上高バリエーションが正しくマッピングされること"""
        industry_variants = [
            'OperatingRevenue1', 'OperatingRevenue2',
            'NetSalesOfCompletedConstructionContracts',
            'NetSalesOfCompletedConstructionContractsCNS',
            'NetSalesAndOperatingRevenue',
            'BusinessRevenue', 'TotalOperatingRevenue',
            'OrdinaryIncomeBNK',
            'OperatingRevenueINV', 'OperatingRevenueIVT', 'OperatingRevenueCMD',
            'GrossOperatingRevenue',  # 営業総収入（コンビニ・小売・サービス業）
            'GrossSales',             # 総売上高（広告代理店等）
            'RevenueRevOA',           # 収益（博報堂DY等）
        ]
        for variant in industry_variants:
            assert XBRL_FACT_MAPPING[variant] == 'revenue', f"{variant} should map to revenue"

    def test_revenue_summary_variants(self):
        """有報SummaryOfBusinessResults売上高が正しくマッピングされること"""
        summary_variants = [
            'NetSalesSummaryOfBusinessResults',
            'OperatingRevenue1SummaryOfBusinessResults',
            'RevenueIFRSSummaryOfBusinessResults',
            'RevenuesUSGAAPSummaryOfBusinessResults',
            'GrossOperatingRevenueSummaryOfBusinessResults',  # 営業総収入（コンビニ・小売等）
            'OperatingRevenuesSummaryOfBusinessResults',      # 営業収益（航空業等）
        ]
        for variant in summary_variants:
            assert XBRL_FACT_MAPPING[variant] == 'revenue', f"{variant} should map to revenue"

    def test_gross_profit_construction(self):
        """建設業の売上総利益バリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['GrossProfitOnCompletedConstructionContracts'] == 'gross_profit'
        assert XBRL_FACT_MAPPING['GrossProfitOnCompletedConstructionContractsCNS'] == 'gross_profit'

    def test_gross_profit_industry_variants(self):
        """業種別売上総利益バリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['NetOperatingRevenueSEC'] == 'gross_profit'   # 第一種金融商品取引業
        assert XBRL_FACT_MAPPING['OperatingGrossProfit'] == 'gross_profit'     # 一般商工業（営業総利益）
        assert XBRL_FACT_MAPPING['OperatingGrossProfitWAT'] == 'gross_profit'  # 海運業
        assert XBRL_FACT_MAPPING['GrossProfitBusiness'] == 'gross_profit'      # 事業利益（航空業等）

    def test_ifrs_revenue_variants(self):
        """IFRS売上高バリエーションが正しくマッピングされること"""
        assert XBRL_FACT_MAPPING_IFRS['RevenueFromContractsWithCustomers'] == 'revenue'
        assert XBRL_FACT_MAPPING_IFRS['RevenueIFRS'] == 'revenue'
        assert XBRL_FACT_MAPPING_IFRS['OperatingRevenueIFRS'] == 'revenue'

    def test_ifrs_jpigp_operating_income(self):
        """jpigp_cor用IFRS営業利益が正しくマッピングされること"""
        assert XBRL_FACT_MAPPING_IFRS['OperatingProfitLossIFRS'] == 'operating_income'
        assert XBRL_FACT_MAPPING_IFRS['ProfitLossBeforeTaxIFRS'] == 'ordinary_income'
        assert XBRL_FACT_MAPPING_IFRS['ProfitLossAttributableToOwnersOfParentIFRS'] == 'net_income'

    def test_ifrs_summary_operating_income(self):
        """IFRS営業利益（有報/半期報サマリー）が正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['OperatingProfitLossIFRSSummaryOfBusinessResults'] == 'operating_income'

    def test_ifrs_summary_ordinary_income(self):
        """IFRS税引前利益（有報/半期報サマリー）が経常利益としてマッピングされること"""
        assert XBRL_FACT_MAPPING['ProfitLossBeforeTaxIFRSSummaryOfBusinessResults'] == 'ordinary_income'

    def test_ifrs_summary_net_income(self):
        """IFRS純利益（有報/半期報サマリー）が正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults'] == 'net_income'

    def test_ifrs_summary_in_jppfs_mapping(self):
        """IFRS Summary要素がXBRL_FACT_MAPPING側にあること（jpcrp_cor名前空間のため）"""
        ifrs_summary_keys = [
            'OperatingProfitLossIFRSSummaryOfBusinessResults',
            'ProfitLossBeforeTaxIFRSSummaryOfBusinessResults',
            'ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults',
        ]
        for key in ifrs_summary_keys:
            assert key in XBRL_FACT_MAPPING, f"{key} should be in XBRL_FACT_MAPPING"
            assert key not in XBRL_FACT_MAPPING_IFRS, f"{key} should NOT be in XBRL_FACT_MAPPING_IFRS"

    def test_insurance_revenue(self):
        """保険業の経常収益・営業収益が正しくマッピングされること"""
        assert XBRL_FACT_MAPPING['OrdinaryIncomeINS'] == 'revenue'
        assert XBRL_FACT_MAPPING['OperatingIncomeINS'] == 'revenue'
        assert XBRL_FACT_MAPPING['OrdinaryIncomeINSSummaryOfBusinessResults'] == 'revenue'

    def test_usgaap_summary_mappings(self):
        """US-GAAP有報サマリーのマッピング確認"""
        assert XBRL_FACT_MAPPING['ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults'] == 'ordinary_income'
        assert XBRL_FACT_MAPPING['NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults'] == 'net_income'
        assert XBRL_FACT_MAPPING['BasicEarningsLossPerShareUSGAAPSummaryOfBusinessResults'] == 'eps'
        assert XBRL_FACT_MAPPING['DilutedEarningsLossPerShareUSGAAPSummaryOfBusinessResults'] == 'eps'

    def test_usgaap_summary_in_jppfs_mapping(self):
        """US-GAAPサマリーもjpcrp_corなのでXBRL_FACT_MAPPING側にあること"""
        usgaap_keys = [
            'ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults',
            'NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults',
            'BasicEarningsLossPerShareUSGAAPSummaryOfBusinessResults',
        ]
        for key in usgaap_keys:
            assert key in XBRL_FACT_MAPPING, f"{key} should be in XBRL_FACT_MAPPING"
            assert key not in XBRL_FACT_MAPPING_IFRS, f"{key} should NOT be in XBRL_FACT_MAPPING_IFRS"

    def test_all_db_fields_covered(self):
        """全DBフィールドがマッピングに含まれること"""
        expected_fields = {'revenue', 'gross_profit', 'operating_income',
                          'ordinary_income', 'net_income', 'eps'}
        mapped_fields = set(XBRL_FACT_MAPPING.values())
        assert expected_fields == mapped_fields


class TestNamespaceDetection:
    """jppfs名前空間の判定テスト"""

    def test_jppfs_cor_prefix(self):
        """jppfs_corプレフィックスが正しく判定されること"""
        qname = QName(local_name='NetSales', prefix='jppfs_cor')
        assert _is_jppfs_namespace(qname) is True

    def test_jppfs_namespace_uri(self):
        """jppfs名前空間URIが正しく判定されること"""
        qname = QName(
            local_name='NetSales',
            namespace_uri='http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/2023-11-01/jppfs_cor'
        )
        assert _is_jppfs_namespace(qname) is True

    def test_jpcrp_cor_prefix(self):
        """jpcrp_corプレフィックス（EDINET開示タクソノミ）がjppfsとして認識されること"""
        qname = QName(local_name='BasicEarningsLossPerShareSummaryOfBusinessResults', prefix='jpcrp_cor')
        assert _is_jppfs_namespace(qname) is True

    def test_non_jppfs_prefix(self):
        """jppfs/jpcrp以外のプレフィックスが除外されること"""
        qname = QName(local_name='SomeElement', prefix='jpigp_cor')
        assert _is_jppfs_namespace(qname) is False

    def test_no_prefix_no_uri(self):
        """プレフィックスもURIもない場合はFalse"""
        qname = QName(local_name='SomeElement')
        assert _is_jppfs_namespace(qname) is False


class TestContextFiltering:
    """コンテキスト判定のテスト"""

    def test_current_year_duration(self):
        assert _is_current_period_context('CurrentYearDuration') is True

    def test_current_quarter_duration(self):
        assert _is_current_period_context('CurrentQuarterDuration') is True

    def test_current_year_instant(self):
        assert _is_current_period_context('CurrentYearInstant') is True

    def test_prior_year_duration(self):
        """前年コンテキストが除外されること"""
        assert _is_current_period_context('Prior1YearDuration') is False

    def test_prior_quarter_duration(self):
        assert _is_current_period_context('Prior1QuarterDuration') is False

    def test_unrelated_context(self):
        """関係ないコンテキストが除外されること"""
        assert _is_current_period_context('SomeRandomContext') is False


class TestUnitConversion:
    """単位変換のテスト"""

    def test_yen_to_million(self):
        """円から百万円への変換"""
        value_yen = Decimal('123456789000')
        value_million = float(value_yen / 1_000_000)
        assert abs(value_million - 123456.789) < 0.001

    def test_eps_no_conversion(self):
        """EPSは単位変換しないこと"""
        eps_value = Decimal('150.5')
        assert float(eps_value) == 150.5


class TestExtractEdinetZip:
    """ZIP展開のテスト"""

    def test_extract_with_manifest(self):
        """manifest.xmlを含むZIPが正しく展開されること"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('XBRL/PublicDoc/manifest_LocalDoc.xml', '<manifest><ixbrl>test.htm</ixbrl></manifest>')
            zf.writestr('XBRL/PublicDoc/test.htm', '<html></html>')

        result = extract_edinet_zip(buf.getvalue())
        try:
            assert result is not None
            assert isinstance(result, list)
            assert 'manifest' in result[0].name.lower()
            assert result[0].exists()
        finally:
            self._cleanup_result(result)

    def test_extract_legacy_xbrl(self):
        """旧形式の.xbrlファイルがフォールバックで検出されること"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('XBRL/PublicDoc/report.xbrl', '<xbrli:xbrl></xbrli:xbrl>')

        result = extract_edinet_zip(buf.getvalue())
        try:
            assert result is not None
            assert isinstance(result, list)
            assert result[0].suffix == '.xbrl'
        finally:
            self._cleanup_result(result)

    def test_extract_empty_zip(self):
        """XBRL関連ファイルのないZIPはNoneを返すこと"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('README.txt', 'empty')

        result = extract_edinet_zip(buf.getvalue())
        assert result is None

    def test_invalid_zip(self):
        """不正なZIPデータはNoneを返すこと"""
        result = extract_edinet_zip(b'not a zip file')
        assert result is None

    def _cleanup_result(self, result):
        """テスト用: extract_edinet_zipの結果を一時ディレクトリごと削除"""
        if result:
            temp_dir = result[0]
            while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
                temp_dir = temp_dir.parent
            if str(temp_dir.name).startswith("edinet_"):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def test_tdnet_jgaap_attachment_pl_detected(self):
        """J-GAAP TDnet ZIPのAttachment P/Lファイル（acedjppl）が検出されること"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('XBRLData/Summary/tse-acedjpsm-12345-ixbrl.htm', '<html></html>')
            zf.writestr('XBRLData/Attachment/tse-acedjppl-12345-ixbrl.htm', '<html></html>')
            zf.writestr('XBRLData/Attachment/tse-acedjpbs-12345-ixbrl.htm', '<html></html>')

        result = extract_edinet_zip(buf.getvalue())
        try:
            assert result is not None
            assert len(result) == 2  # Summary + P/Lのみ（B/Sは含まない）
            filenames = [p.name for p in result]
            assert any('sm' in fn for fn in filenames)
            assert any('pl' in fn for fn in filenames)
        finally:
            self._cleanup_result(result)

    def test_tdnet_ifrs_attachment_pl_detected(self):
        """IFRS TDnet ZIPのAttachment P/Lファイル（acifrspl）が検出されること"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('XBRLData/Summary/tse-acedifsm-12345-ixbrl.htm', '<html></html>')
            zf.writestr('XBRLData/Attachment/tse-acifrspl-12345-ixbrl.htm', '<html></html>')

        result = extract_edinet_zip(buf.getvalue())
        try:
            assert result is not None
            assert len(result) == 2
            filenames = [p.name for p in result]
            assert any('sm' in fn for fn in filenames)
            assert any('pl' in fn for fn in filenames)
        finally:
            self._cleanup_result(result)

    def test_tdnet_summary_before_attachment(self):
        """Summaryファイルが常にAttachmentより先にリストされること"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            # ZIP内の順序を意図的に逆にする
            zf.writestr('XBRLData/Attachment/tse-acedjppl-12345-ixbrl.htm', '<html></html>')
            zf.writestr('XBRLData/Summary/tse-acedjpsm-12345-ixbrl.htm', '<html></html>')

        result = extract_edinet_zip(buf.getvalue())
        try:
            assert result is not None
            assert len(result) == 2
            assert 'Summary' in str(result[0])
            assert 'Attachment' in str(result[1])
        finally:
            self._cleanup_result(result)


class TestDetectQuarterFromXbrlFilename:
    """XBRLファイル名テンプレートコードからのfiscal_quarter判定テスト"""

    def _make_paths(self, filename):
        """テスト用Pathリストを生成"""
        return [Path(f'/tmp/{filename}')]

    def test_q1r(self):
        paths = self._make_paths('jpcrp040300-q1r-001_E01234-000_2023-06-30_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'Q1'

    def test_q2r(self):
        paths = self._make_paths('jpcrp040300-q2r-001_E01234-000_2023-09-30_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'Q2'

    def test_q3r(self):
        paths = self._make_paths('jpcrp040300-q3r-001_E01234-000_2023-12-31_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'Q3'

    def test_q4r(self):
        paths = self._make_paths('jpcrp040300-q4r-001_E01234-000_2024-03-31_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'Q4'

    def test_asr(self):
        paths = self._make_paths('jpcrp030000-asr-001_E01234-000_2024-03-31_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'FY'

    def test_ssr(self):
        paths = self._make_paths('jpcrp040300-ssr-001_E01234-000_2024-09-30_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'Q2'

    def test_esr(self):
        paths = self._make_paths('jpcrp030000-esr-001_E01234-000_2024-03-31_ixbrl.htm')
        assert _detect_quarter_from_xbrl_filename(paths) == 'FY'

    def test_no_match(self):
        paths = self._make_paths('some_random_file.htm')
        assert _detect_quarter_from_xbrl_filename(paths) is None

    def test_empty_paths(self):
        assert _detect_quarter_from_xbrl_filename([]) is None

    def test_multiple_files_first_match(self):
        """複数ファイルの場合、最初にマッチしたものを使用"""
        paths = [
            Path('/tmp/0000000_header_jpcrp040300-q3r-001_E01234_ixbrl.htm'),
            Path('/tmp/0104010_honbun_jpcrp040300-q3r-001_E01234_ixbrl.htm'),
        ]
        assert _detect_quarter_from_xbrl_filename(paths) == 'Q3'

    def test_manifest_only_with_sibling_xbrl(self, tmp_path):
        """manifestのみの場合、兄弟ファイルからテンプレートコード検出"""
        pub_dir = tmp_path / "XBRL" / "PublicDoc"
        pub_dir.mkdir(parents=True)
        manifest = pub_dir / "manifest_PublicDoc.xml"
        manifest.write_text("<manifest/>")
        xbrl_file = pub_dir / "jpcrp040300-ssr-001_E01321-000_2025-09-30_01_2025-11-12.xbrl"
        xbrl_file.write_text("")
        assert _detect_quarter_from_xbrl_filename([manifest]) == 'Q2'

    def test_manifest_only_without_sibling_xbrl(self, tmp_path):
        """manifestのみで兄弟にjpcrpファイルがない場合 → None"""
        pub_dir = tmp_path / "XBRL" / "PublicDoc"
        pub_dir.mkdir(parents=True)
        manifest = pub_dir / "manifest_PublicDoc.xml"
        manifest.write_text("<manifest/>")
        other_file = pub_dir / "some_other_file.xml"
        other_file.write_text("")
        assert _detect_quarter_from_xbrl_filename([manifest]) is None


class TestDetectFiscalYear:
    """EDINET書類情報からのfiscal_year判定テスト"""

    def test_fiscal_year_from_doc_description(self):
        """docDescriptionから年度を抽出（2024年3月期）"""
        doc = {
            'periodEnd': '2023-06-30',
            'docDescription': '2024年3月期 第1四半期報告書',
        }
        assert _detect_fiscal_year(doc) == '2024'

    def test_fiscal_year_from_doc_description_december(self):
        """12月決算企業（2024年12月期）"""
        doc = {
            'periodEnd': '2024-03-31',
            'docDescription': '2024年12月期 第1四半期報告書',
        }
        assert _detect_fiscal_year(doc) == '2024'

    def test_fiscal_year_fallback_to_period_start(self):
        """periodStart=4月→翌年3月期"""
        doc = {
            'periodStart': '2025-04-01',
            'periodEnd': '2025-06-30',
            'docDescription': '四半期報告書',
        }
        assert _detect_fiscal_year(doc) == '2026'

    def test_fiscal_year_december_with_period_start(self):
        """periodStart=1月→同年12月期"""
        doc = {
            'periodStart': '2025-01-01',
            'periodEnd': '2025-03-31',
            'docDescription': '第1四半期報告書',
        }
        assert _detect_fiscal_year(doc) == '2025'

    def test_fiscal_year_fallback_to_period_end(self):
        """periodStartもdocDescriptionもない場合、periodEnd[:4]"""
        doc = {
            'periodEnd': '2023-06-30',
            'docDescription': '',
        }
        assert _detect_fiscal_year(doc) == '2023'

    def test_fiscal_year_fallback_to_submit_date(self):
        """periodEndもない場合、submitDateTime[:4]"""
        doc = {
            'docDescription': '',
            'submitDateTime': '2023-08-10 10:00:00',
        }
        assert _detect_fiscal_year(doc) == '2023'


class TestExtractFiscalEndDateFromXbrl:
    """iXBRL Context要素からのfiscal_end_date抽出テスト"""

    def _write_ixbrl(self, tmp_path, content):
        """テスト用iXBRLファイルを作成してPathを返す"""
        p = tmp_path / "test.htm"
        p.write_text(content, encoding="utf-8")
        return p

    def test_current_year_instant(self, tmp_path):
        """CurrentYearInstantのinstantから決算期末日を取得"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentYearInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E00012</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2026-03-31</xbrli:instant></xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2026-03-31"

    def test_current_year_duration_fallback(self, tmp_path):
        """CurrentYearDurationのendDateにフォールバック"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentYearDuration">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E00012</xbrli:identifier></xbrli:entity>
  <xbrli:period>
    <xbrli:startDate>2025-04-01</xbrli:startDate>
    <xbrli:endDate>2026-03-31</xbrli:endDate>
  </xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2026-03-31"

    def test_skip_scenario_context(self, tmp_path):
        """scenario付きContextはスキップし、scenario無しを返す"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentYearInstant_segment1">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E00012</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2026-03-31</xbrli:instant></xbrli:period>
  <xbrli:scenario><xbrli:identifier>segment</xbrli:identifier></xbrli:scenario>
</xbrli:context>
<xbrli:context id="CurrentYearInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E00012</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2026-03-31</xbrli:instant></xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2026-03-31"

    def test_no_context_returns_none(self, tmp_path):
        """Context要素がない場合はNoneを返す"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header><ix:resources></ix:resources></ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) is None

    def test_december_fiscal_year(self, tmp_path):
        """12月決算企業のQ3: instant=2025-12-31"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentYearInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E00012</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        result = _extract_fiscal_end_date_from_xbrl([p])
        assert result == "2025-12-31"
        assert result[:4] == "2025"  # fiscal_year導出の確認

    def test_current_quarter_instant(self, tmp_path):
        """四半期報告書のCurrentQuarterInstantから期末日を取得"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentQuarterInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E02144</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2024-09-30</xbrli:instant></xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2024-09-30"

    def test_interim_instant(self, tmp_path):
        """半期報告書のInterimInstantから期末日を取得"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="InterimInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E02144</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2025-09-30</xbrli:instant></xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2025-09-30"

    def test_current_ytd_duration_fallback(self, tmp_path):
        """CurrentYTDDurationのendDateにフォールバック"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentYTDDuration">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E02144</xbrli:identifier></xbrli:entity>
  <xbrli:period>
    <xbrli:startDate>2025-04-01</xbrli:startDate>
    <xbrli:endDate>2025-09-30</xbrli:endDate>
  </xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2025-09-30"

    def test_quarter_instant_priority_over_year_instant(self, tmp_path):
        """CurrentQuarterInstantとCurrentYearInstantが両方ある場合、Quarterが優先"""
        ixbrl = '''<?xml version="1.0" encoding="UTF-8"?>
<html xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<ix:header>
<ix:resources>
<xbrli:context id="CurrentYearInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E02144</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2026-03-31</xbrli:instant></xbrli:period>
</xbrli:context>
<xbrli:context id="CurrentQuarterInstant">
  <xbrli:entity><xbrli:identifier scheme="http://info.edinet-fsa.go.jp">E02144</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:instant>2025-09-30</xbrli:instant></xbrli:period>
</xbrli:context>
</ix:resources>
</ix:header>
</body>
</html>'''
        p = self._write_ixbrl(tmp_path, ixbrl)
        assert _extract_fiscal_end_date_from_xbrl([p]) == "2025-09-30"
