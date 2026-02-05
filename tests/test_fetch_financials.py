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
    _is_jppfs_namespace,
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

    def test_non_jppfs_prefix(self):
        """jppfs以外のプレフィックスが除外されること"""
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
            assert 'manifest' in result.name.lower()
            assert result.exists()
        finally:
            # クリーンアップ
            if result:
                temp_dir = result
                while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
                    temp_dir = temp_dir.parent
                if str(temp_dir.name).startswith("edinet_"):
                    shutil.rmtree(temp_dir, ignore_errors=True)

    def test_extract_legacy_xbrl(self):
        """旧形式の.xbrlファイルがフォールバックで検出されること"""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('XBRL/PublicDoc/report.xbrl', '<xbrli:xbrl></xbrli:xbrl>')

        result = extract_edinet_zip(buf.getvalue())
        try:
            assert result is not None
            assert result.suffix == '.xbrl'
        finally:
            if result:
                temp_dir = result
                while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
                    temp_dir = temp_dir.parent
                if str(temp_dir.name).startswith("edinet_"):
                    shutil.rmtree(temp_dir, ignore_errors=True)

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
