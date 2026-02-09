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
    _is_jppfs_namespace,
    _is_supported_namespace,
    _is_current_period_context,
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
