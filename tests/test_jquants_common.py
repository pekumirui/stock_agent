"""jquants_common.py のユニットテスト"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from jquants_common import (
    QUARTER_PREFIX,
    detect_quarter,
    to_million,
    to_float,
    format_date,
    fiscal_year_from_fy_end,
)


class TestQuarterPrefix:
    def test_has_expected_keys(self):
        assert set(QUARTER_PREFIX.keys()) == {'FY', '1Q', '2Q', '3Q'}


class TestDetectQuarter:
    def test_fy(self):
        assert detect_quarter('FYFinancialStatements_Consolidated_JP') == 'FY'

    def test_q1(self):
        assert detect_quarter('1QFinancialStatements_Consolidated_IFRS') == 'Q1'

    def test_q2(self):
        assert detect_quarter('2QFinancialStatements_NonConsolidated_JP') == 'Q2'

    def test_q3(self):
        assert detect_quarter('3QFinancialStatements_Consolidated_JP') == 'Q3'

    def test_unknown(self):
        assert detect_quarter('DividendForecastRevision') is None

    def test_none_input(self):
        assert detect_quarter(None) is None

    def test_nan_input(self):
        assert detect_quarter(float('nan')) is None


class TestToMillion:
    def test_basic(self):
        assert to_million(1_000_000) == 1.0

    def test_none(self):
        assert to_million(None) is None

    def test_empty_string(self):
        assert to_million('') is None

    def test_string_number(self):
        assert to_million('2000000') == 2.0


class TestToFloat:
    def test_basic(self):
        assert to_float(1.5) == 1.5

    def test_none(self):
        assert to_float(None) is None

    def test_string(self):
        assert to_float('3.14') == 3.14


class TestFormatDate:
    def test_string(self):
        assert format_date('2024-03-31') == '2024-03-31'

    def test_none(self):
        assert format_date(None) is None

    def test_empty_string(self):
        assert format_date('') is None


class TestFiscalYearFromFyEnd:
    def test_basic(self):
        assert fiscal_year_from_fy_end('2024-03-31') == '2024'

    def test_none(self):
        assert fiscal_year_from_fy_end(None) is None
