"""
fetch_jquants_fins.py のユニットテスト
"""
import sys
from pathlib import Path

import pytest

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from fetch_jquants_fins import (
    _detect_quarter,
    _is_target_row,
    _is_consolidated,
    _to_million,
    _to_float,
    _format_date,
    _format_time,
    _fiscal_year_from_fy_end,
    map_to_financial,
)


class TestDetectQuarter:
    def test_fy_jp(self):
        assert _detect_quarter('FYFinancialStatements_Consolidated_JP') == 'FY'

    def test_fy_ifrs(self):
        assert _detect_quarter('FYFinancialStatements_Consolidated_IFRS') == 'FY'

    def test_1q(self):
        assert _detect_quarter('1QFinancialStatements_Consolidated_JP') == 'Q1'

    def test_2q(self):
        assert _detect_quarter('2QFinancialStatements_Consolidated_IFRS') == 'Q2'

    def test_3q(self):
        assert _detect_quarter('3QFinancialStatements_NonConsolidated_JP') == 'Q3'

    def test_other_period(self):
        assert _detect_quarter('OtherPeriodFinancialStatements_Consolidated_JP') is None

    def test_dividend_forecast(self):
        assert _detect_quarter('DividendForecastRevision') is None

    def test_earn_forecast(self):
        assert _detect_quarter('EarnForecastRevision') is None


class TestIsTargetRow:
    def test_consolidated_fy(self):
        assert _is_target_row('FYFinancialStatements_Consolidated_JP') is True

    def test_non_consolidated(self):
        assert _is_target_row('FYFinancialStatements_NonConsolidated_JP') is True

    def test_dividend_forecast_excluded(self):
        assert _is_target_row('DividendForecastRevision') is False

    def test_earn_forecast_excluded(self):
        assert _is_target_row('EarnForecastRevision') is False

    def test_reit_dividend_excluded(self):
        assert _is_target_row('REITDividendForecastRevision') is False

    def test_reit_fy(self):
        # REIT決算短信はFinancialStatementsを含むので対象
        assert _is_target_row('FYFinancialStatements_Consolidated_REIT') is True


class TestIsConsolidated:
    def test_consolidated(self):
        assert _is_consolidated('FYFinancialStatements_Consolidated_JP') is True

    def test_non_consolidated(self):
        assert _is_consolidated('FYFinancialStatements_NonConsolidated_JP') is False


class TestToMillion:
    def test_normal(self):
        assert _to_million(48_036_704_000_000) == pytest.approx(48_036_704.0)

    def test_none(self):
        assert _to_million(None) is None

    def test_empty_string(self):
        assert _to_million('') is None

    def test_string_number(self):
        assert _to_million('1000000') == pytest.approx(1.0)

    def test_nan(self):
        import math
        assert _to_million(float('nan')) is None


class TestToFloat:
    def test_normal(self):
        assert _to_float(179.47) == pytest.approx(179.47)

    def test_none(self):
        assert _to_float(None) is None

    def test_empty_string(self):
        assert _to_float('') is None

    def test_string_number(self):
        assert _to_float('365.94') == pytest.approx(365.94)

    def test_nan(self):
        assert _to_float(float('nan')) is None


class TestFormatDate:
    def test_string(self):
        assert _format_date('2024-03-31') == '2024-03-31'

    def test_timestamp(self):
        import pandas as pd
        ts = pd.Timestamp('2024-03-31')
        assert _format_date(ts) == '2024-03-31'

    def test_none(self):
        assert _format_date(None) is None

    def test_empty_string(self):
        assert _format_date('') is None

    def test_nat(self):
        import pandas as pd
        assert _format_date(pd.NaT) is None


class TestFormatTime:
    def test_full_time(self):
        assert _format_time('13:55:00') == '13:55'

    def test_short_time(self):
        assert _format_time('13:55') == '13:55'

    def test_none(self):
        assert _format_time(None) is None

    def test_nan_string(self):
        assert _format_time('nan') is None


class TestFiscalYearFromFyEnd:
    def test_march_end(self):
        import pandas as pd
        assert _fiscal_year_from_fy_end(pd.Timestamp('2024-03-31')) == '2024'

    def test_december_end(self):
        assert _fiscal_year_from_fy_end('2024-12-31') == '2024'

    def test_none(self):
        assert _fiscal_year_from_fy_end(None) is None


class TestMapToFinancial:
    """map_to_financial()のテスト"""

    def _make_row(self, **overrides):
        """テスト用の行データを生成"""
        import pandas as pd
        base = {
            'Code': '72030',
            'DocType': 'FYFinancialStatements_Consolidated_IFRS',
            'CurFYEn': pd.Timestamp('2025-03-31'),
            'CurPerEn': pd.Timestamp('2025-03-31'),
            'DiscDate': pd.Timestamp('2025-05-08'),
            'DiscTime': '13:55:00',
            'Sales': 48_036_704_000_000,
            'OP': 4_795_586_000_000,
            'OdP': '',  # IFRSなので空
            'NP': 4_765_086_000_000,
            'EPS': 359.56,
        }
        base.update(overrides)
        return base

    def test_basic_mapping(self):
        result = map_to_financial(self._make_row())
        assert result is not None
        assert result['ticker_code'] == '7203'
        assert result['fiscal_year'] == '2025'
        assert result['fiscal_quarter'] == 'FY'
        assert result['fiscal_end_date'] == '2025-03-31'
        assert result['announcement_date'] == '2025-05-08'
        assert result['announcement_time'] == '13:55'
        assert result['revenue'] == pytest.approx(48_036_704.0)
        assert result['operating_income'] == pytest.approx(4_795_586.0)
        assert result['ordinary_income'] is None  # IFRS
        assert result['net_income'] == pytest.approx(4_765_086.0)
        assert result['eps'] == pytest.approx(359.56)
        assert result['source'] == 'JQuants'

    def test_quarterly(self):
        result = map_to_financial(self._make_row(
            DocType='1QFinancialStatements_Consolidated_IFRS',
        ))
        assert result['fiscal_quarter'] == 'Q1'

    def test_4digit_code(self):
        result = map_to_financial(self._make_row(Code='7203'))
        assert result['ticker_code'] == '7203'

    def test_excluded_doc_type(self):
        result = map_to_financial(self._make_row(DocType='DividendForecastRevision'))
        assert result is None

    def test_other_period_excluded(self):
        result = map_to_financial(self._make_row(
            DocType='OtherPeriodFinancialStatements_Consolidated_JP',
        ))
        assert result is None

    def test_jp_gaap_with_ordinary_income(self):
        result = map_to_financial(self._make_row(
            DocType='FYFinancialStatements_Consolidated_JP',
            OdP=500_000_000_000,
        ))
        assert result['ordinary_income'] == pytest.approx(500_000.0)
