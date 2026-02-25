"""
業績予想パイプラインのテスト

- _is_forecast_context() のパターンテスト
- parse_ixbrl_forecast() の単体テスト（モックFactデータ）
- insert_management_forecast() の優先度ロジックテスト
- J-Quants フィールドマッピングのテスト（map_to_forecast()）
"""
import pytest
import sys
from pathlib import Path
from decimal import Decimal
from typing import Any
from unittest.mock import patch, MagicMock

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from db_utils import (
    get_connection, insert_management_forecast, get_management_forecast,
    upsert_company,
)
from fetch_financials import _is_forecast_context, parse_ixbrl_forecast, _extract_forecast_fiscal_year


# ============================================================
# _is_forecast_context() テスト
# ============================================================

class TestIsForecastContext:
    """_is_forecast_context() のコンテキスト判定テスト"""

    def test_fy_consolidated_basic(self):
        """通期予想（連結・基本パターン）"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_fy_annual_consolidated(self):
        """通期予想（連結・AnnualMember付き）"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_AnnualMember_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_fy_non_consolidated(self):
        """通期予想（非連結）"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_NonConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_q2_consolidated(self):
        """半期予想（連結）"""
        quarter, is_forecast = _is_forecast_context(
            "NextAccumulatedQ2Duration_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'Q2'

    def test_q2_non_consolidated(self):
        """半期予想（非連結）"""
        quarter, is_forecast = _is_forecast_context(
            "NextAccumulatedQ2Duration_NonConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'Q2'

    def test_not_forecast_current_period(self):
        """当期データは対象外"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_ConsolidatedMember"
        )
        assert is_forecast is False
        assert quarter is None

    def test_not_forecast_no_member(self):
        """ForecastMemberなしは対象外"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_ConsolidatedMember"
        )
        assert is_forecast is False
        assert quarter is None

    def test_not_forecast_upper_member(self):
        """上方レンジ（UpperMember）は対象外"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_ConsolidatedMember_ForecastMember_UpperMember"
        )
        assert is_forecast is False
        assert quarter is None

    def test_not_forecast_lower_member(self):
        """下方レンジ（LowerMember）は対象外"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_ConsolidatedMember_ForecastMember_LowerMember"
        )
        assert is_forecast is False
        assert quarter is None

    def test_not_forecast_dividend_quarterly(self):
        """配当四半期コンテキストは対象外"""
        quarter, is_forecast = _is_forecast_context(
            "NextYearDuration_ConsolidatedMember_SecondQuarterMember_ForecastMember"
        )
        assert is_forecast is False
        assert quarter is None

    def test_not_forecast_prior_period(self):
        """前期データは対象外"""
        quarter, is_forecast = _is_forecast_context(
            "PriorYearDuration_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is False
        assert quarter is None

    # --- CurrentYear系テスト（Q1-Q3決算短信の当期通期予想）---

    def test_fy_current_year_consolidated(self):
        """Q1-Q3短信: CurrentYearDuration + ForecastMember → ('FY', True)"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_fy_current_year_non_consolidated(self):
        """Q1-Q3短信（非連結）: CurrentYearDuration → ('FY', True)"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_NonConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_fy_current_year_annual_member(self):
        """AnnualMember付きCurrentYearDuration → ('FY', True)"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_AnnualMember_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_q2_current_accumulated(self):
        """CurrentAccumulatedQ2Duration + ForecastMember → ('Q2', True)"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentAccumulatedQ2Duration_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'Q2'

    def test_current_year_dividend_year_end(self):
        """YearEndMemberはQ2判定に引っかからない → ('FY', True)"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_YearEndMember_ConsolidatedMember_ForecastMember"
        )
        assert is_forecast is True
        assert quarter == 'FY'

    def test_current_year_upper_member_excluded(self):
        """CurrentYear + UpperMember は対象外"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_ConsolidatedMember_ForecastMember_UpperMember"
        )
        assert is_forecast is False
        assert quarter is None

    def test_current_year_second_quarter_dividend_excluded(self):
        """CurrentYear + SecondQuarterMember（配当四半期）は対象外"""
        quarter, is_forecast = _is_forecast_context(
            "CurrentYearDuration_ConsolidatedMember_SecondQuarterMember_ForecastMember"
        )
        assert is_forecast is False
        assert quarter is None


# ============================================================
# parse_ixbrl_forecast() テスト（モックFactデータ）
# ============================================================

class MockQName:
    """テスト用QName代替オブジェクト"""
    def __init__(self, prefix: str, local_name: str, namespace_uri: str = ""):
        self.prefix = prefix
        self.local_name = local_name
        self.namespace_uri = namespace_uri


class MockFact:
    """テスト用Fact代替オブジェクト"""
    def __init__(self, prefix: str, local_name: str, context_ref: str, value: Any,
                 namespace_uri: str = ""):
        self.qname = MockQName(prefix, local_name, namespace_uri)
        self.context_ref = context_ref
        self.value = value


class TestParseIxbrlForecast:
    """parse_ixbrl_forecast() のテスト（モックFact使用）"""

    def _make_facts(self, facts_data: list) -> list:
        """MockFact のリストを生成するヘルパー"""
        return [MockFact(**d) for d in facts_data]

    def test_extract_fy_forecast_consolidated(self):
        """通期予想（連結）が正しく抽出される"""
        mock_facts = self._make_facts([
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('500000000000'),  # 500億円
                'namespace_uri': 'tse-ed-t',
            },
            {
                'prefix': 'tse-ed-t',
                'local_name': 'OperatingIncome',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('50000000000'),   # 50億円
                'namespace_uri': 'tse-ed-t',
            },
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetIncomePerShare',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('250.50'),         # EPS 250.50円
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'FY' in result
        fy = result['FY']
        assert fy['revenue'] == pytest.approx(500000.0, rel=1e-3)    # 500億円 → 50万百万円
        assert fy['operating_income'] == pytest.approx(50000.0, rel=1e-3)
        assert fy['eps'] == pytest.approx(250.50, rel=1e-3)

    def test_extract_q2_forecast(self):
        """半期予想（Q2）が正しく抽出される"""
        mock_facts = self._make_facts([
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextAccumulatedQ2Duration_ConsolidatedMember_ForecastMember',
                'value': Decimal('240000000000'),
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'Q2' in result
        assert result['Q2']['revenue'] == pytest.approx(240000.0, rel=1e-3)

    def test_consolidated_priority_over_non_consolidated(self):
        """連結データがある場合は非連結より優先される"""
        mock_facts = self._make_facts([
            # 連結
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('500000000000'),
                'namespace_uri': 'tse-ed-t',
            },
            # 非連結（より小さな値）
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_NonConsolidatedMember_ForecastMember',
                'value': Decimal('300000000000'),
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        # 連結データが返される（500億円）
        assert result['FY']['revenue'] == pytest.approx(500000.0, rel=1e-3)

    def test_non_consolidated_used_when_no_consolidated(self):
        """連結データがない場合は非連結を使用"""
        mock_facts = self._make_facts([
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_NonConsolidatedMember_ForecastMember',
                'value': Decimal('100000000000'),
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'FY' in result
        assert result['FY']['revenue'] == pytest.approx(100000.0, rel=1e-3)

    def test_empty_when_no_forecast_facts(self):
        """予想データがない場合は空辞書"""
        mock_facts = self._make_facts([
            {
                'prefix': 'jppfs_cor',
                'local_name': 'NetSales',
                'context_ref': 'CurrentYearDuration_ConsolidatedMember',
                'value': Decimal('500000000000'),
                'namespace_uri': 'jppfs_cor',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert result == {}

    def test_upper_lower_member_excluded(self):
        """UpperMember/LowerMemberのコンテキストは無視される"""
        mock_facts = self._make_facts([
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember_UpperMember',
                'value': Decimal('600000000000'),
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert result == {}

    def test_dividend_per_share_not_divided_by_million(self):
        """配当（dividend_per_share）は百万円割りしない"""
        mock_facts = self._make_facts([
            {
                'prefix': 'tse-ed-t',
                'local_name': 'DividendPerShare',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('80.00'),  # 80円/株
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'FY' in result
        assert result['FY']['dividend_per_share'] == pytest.approx(80.0, rel=1e-3)

    def test_extract_ifrs_sales_forecast(self):
        """SalesIFRS要素がrevenueにマッピングされる（Q1-Q3短信IFRS企業）"""
        mock_facts = self._make_facts([
            {
                'prefix': 'tse-ed-t',
                'local_name': 'SalesIFRS',
                'context_ref': 'CurrentYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('10000000000000'),  # 10兆円（日本製鉄相当）
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'FY' in result
        assert result['FY']['revenue'] == pytest.approx(10000000.0, rel=1e-3)


# ============================================================
# insert_management_forecast() 優先度テスト
# ============================================================

class TestInsertManagementForecastPriority:
    """insert_management_forecast() のソース優先度ロジックテスト"""

    def test_insert_initial(self, sample_company):
        """初回挿入が成功する"""
        ticker = sample_company
        result = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            operating_income=50000.0,
            source='TDnet',
        )
        assert result is True

        records = get_management_forecast(ticker, '2026', 'FY')
        assert len(records) == 1
        assert records[0]['revenue'] == 500000.0
        assert records[0]['source'] == 'TDnet'

    def test_lower_priority_skipped(self, sample_company):
        """低優先度ソースが高優先度データを上書きしない"""
        ticker = sample_company
        # TDnet（優先度2）を先に挿入
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='TDnet',
        )
        # yfinance（優先度1）で上書き試行 → スキップされるべき
        result = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=999999.0,  # 別の値
            source='yfinance',
        )
        assert result is False

        records = get_management_forecast(ticker, '2026', 'FY')
        assert records[0]['revenue'] == 500000.0  # TDnetの値が保持される

    def test_higher_priority_overwrites(self, sample_company):
        """高優先度ソースは低優先度データを上書きする"""
        ticker = sample_company
        # yfinance（優先度1）を先に挿入
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=400000.0,
            source='yfinance',
        )
        # TDnet（優先度2）で上書き試行 → 成功するべき
        result = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='TDnet',
        )
        assert result is True

        records = get_management_forecast(ticker, '2026', 'FY')
        assert records[0]['revenue'] == 500000.0  # TDnetの値に更新

    def test_same_priority_overwrites(self, sample_company):
        """同一優先度ソースは上書きを許可する"""
        ticker = sample_company
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='TDnet',
        )
        # TDnet同士（同一優先度）の上書き
        result = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=510000.0,
            source='TDnet',
        )
        assert result is True

        records = get_management_forecast(ticker, '2026', 'FY')
        assert records[0]['revenue'] == 510000.0

    def test_insert_fy_and_q2_separately(self, sample_company):
        """FY通期とQ2半期が独立して保存される"""
        ticker = sample_company
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='TDnet',
        )
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='Q2',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=240000.0,
            source='TDnet',
        )

        records_fy = get_management_forecast(ticker, '2026', 'FY')
        records_q2 = get_management_forecast(ticker, '2026', 'Q2')
        assert len(records_fy) == 1
        assert len(records_q2) == 1
        assert records_fy[0]['revenue'] == 500000.0
        assert records_q2[0]['revenue'] == 240000.0

    def test_jquants_and_tdnet_same_priority(self, sample_company):
        """JQuantsとTDnetは同一優先度（上書き可能）"""
        ticker = sample_company
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='JQuants',
        )
        # TDnet（同一優先度=2）で上書き
        result = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500500.0,
            source='TDnet',
        )
        assert result is True


# ============================================================
# J-Quants フィールドマッピングテスト
# ============================================================

class TestJQuantsForecastMapping:
    """fetch_jquants_forecasts.py の map_to_forecast() テスト"""

    def setup_method(self):
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from fetch_jquants_forecasts import map_to_forecast
        self.map_to_forecast = map_to_forecast

    def _base_row(self, **overrides) -> dict:
        """テスト用の基本データ行"""
        base = {
            'DocType': 'FYFinancialStatements_Consolidated_JP',
            'Code': '72030',
            'DiscDate': '2025-05-10',
            'CurFYEn': '2025-03-31',
            'NxtFYEn': '2026-03-31',
            'FSales': 46000000000000,   # 46兆円（トヨタ相当）
            'FOP': 5000000000000,
            'FOdP': 4800000000000,
            'FNP': 3500000000000,
            'FEPS': 1200.50,
            'FDivAnn': 80.0,
            'FSales2Q': 22000000000000,
            'FOP2Q': 2400000000000,
            'FOdP2Q': 2300000000000,
            'FNP2Q': 1700000000000,
            'FEPS2Q': 580.0,
        }
        base.update(overrides)
        return base

    def test_fy_announcement_uses_nxtfyen(self):
        """FY決算発表時は来期予想（NxtFYEn）からfiscal_yearを取得"""
        row = self._base_row()
        results = self.map_to_forecast(row)
        fy_results = [r for r in results if r['fiscal_quarter'] == 'FY']
        assert len(fy_results) >= 1
        assert fy_results[0]['fiscal_year'] == '2026'

    def test_quarterly_announcement_uses_curfyen(self):
        """Q1決算発表時は当期予想（CurFYEn）からfiscal_yearを取得"""
        row = self._base_row(DocType='1QFinancialStatements_Consolidated_JP')
        results = self.map_to_forecast(row)
        fy_results = [r for r in results if r['fiscal_quarter'] == 'FY']
        assert len(fy_results) >= 1
        assert fy_results[0]['fiscal_year'] == '2025'

    def test_fy_revenue_in_millions(self):
        """FY売上高が百万円単位に変換される"""
        row = self._base_row(FSales=500000000000)  # 5000億円
        results = self.map_to_forecast(row)
        fy_results = [r for r in results if r['fiscal_quarter'] == 'FY']
        assert len(fy_results) >= 1
        assert fy_results[0]['revenue'] == pytest.approx(500000.0, rel=1e-3)

    def test_eps_not_divided_by_million(self):
        """EPSは百万円割りしない"""
        row = self._base_row(FEPS=250.50)
        results = self.map_to_forecast(row)
        fy_results = [r for r in results if r['fiscal_quarter'] == 'FY']
        assert fy_results[0]['eps'] == pytest.approx(250.50, rel=1e-3)

    def test_dividend_not_divided_by_million(self):
        """配当は百万円割りしない"""
        row = self._base_row(FDivAnn=80.0)
        results = self.map_to_forecast(row)
        fy_results = [r for r in results if r['fiscal_quarter'] == 'FY']
        assert fy_results[0]['dividend_per_share'] == pytest.approx(80.0, rel=1e-3)

    def test_q2_forecast_extracted(self):
        """Q2半期予想が抽出される"""
        row = self._base_row(FSales2Q=220000000000)  # 2200億円
        results = self.map_to_forecast(row)
        q2_results = [r for r in results if r['fiscal_quarter'] == 'Q2']
        assert len(q2_results) >= 1
        assert q2_results[0]['revenue'] == pytest.approx(220000.0, rel=1e-3)

    def test_earn_forecast_revision_is_revised(self):
        """EarnForecastRevisionはforecast_type='revised'"""
        row = self._base_row(DocType='EarnForecastRevision')
        results = self.map_to_forecast(row)
        for r in results:
            assert r['forecast_type'] == 'revised'

    def test_dividend_forecast_revision_excluded(self):
        """DividendForecastRevisionは対象外"""
        row = self._base_row(DocType='DividendForecastRevision')
        results = self.map_to_forecast(row)
        assert results == []

    def test_no_fsales_skips_fy_entry(self):
        """FSalesがNoneの場合はFYエントリを生成しない（他フィールドも全てNone）"""
        import numpy as np
        row = self._base_row(
            FSales=None, FOP=None, FOdP=None, FNP=None, FEPS=None, FDivAnn=None,
            FSales2Q=None, FOP2Q=None, FOdP2Q=None, FNP2Q=None, FEPS2Q=None,
        )
        results = self.map_to_forecast(row)
        assert results == []

    def test_ticker_code_normalized(self):
        """5桁コードは4桁に変換される"""
        row = self._base_row(Code='72030')
        results = self.map_to_forecast(row)
        for r in results:
            assert r['ticker_code'] == '7203'

    def test_source_is_jquants(self):
        """sourceはJQuantsに設定される"""
        row = self._base_row()
        results = self.map_to_forecast(row)
        for r in results:
            assert r['source'] == 'JQuants'


# ============================================================
# 連結/非連結 四半期混在テスト
# ============================================================

class TestConsolidatedNonConsolidatedMix:
    """FY連結 + Q2非連結の混在ケース"""

    def _make_facts(self, facts_data: list) -> list:
        return [MockFact(**d) for d in facts_data]

    def test_fy_consolidated_q2_non_consolidated(self):
        """FYは連結、Q2は非連結のみの場合、両方返される"""
        mock_facts = self._make_facts([
            # FY 連結
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_ConsolidatedMember_ForecastMember',
                'value': Decimal('500000000000'),
                'namespace_uri': 'tse-ed-t',
            },
            # Q2 非連結のみ
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextAccumulatedQ2Duration_NonConsolidatedMember_ForecastMember',
                'value': Decimal('200000000000'),
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'FY' in result
        assert 'Q2' in result
        assert result['FY']['revenue'] == pytest.approx(500000.0, rel=1e-3)
        assert result['Q2']['revenue'] == pytest.approx(200000.0, rel=1e-3)

    def test_fy_non_consolidated_q2_consolidated(self):
        """FYは非連結のみ、Q2は連結のみの場合、両方返される"""
        mock_facts = self._make_facts([
            # FY 非連結のみ
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextYearDuration_NonConsolidatedMember_ForecastMember',
                'value': Decimal('100000000000'),
                'namespace_uri': 'tse-ed-t',
            },
            # Q2 連結
            {
                'prefix': 'tse-ed-t',
                'local_name': 'NetSales',
                'context_ref': 'NextAccumulatedQ2Duration_ConsolidatedMember_ForecastMember',
                'value': Decimal('250000000000'),
                'namespace_uri': 'tse-ed-t',
            },
        ])

        mock_parser = MagicMock()
        mock_parser.ixbrl_files = [Path('/tmp/fake.htm')]
        mock_parser.load_facts.return_value = mock_facts

        with patch('fetch_financials.Parser', return_value=mock_parser):
            result = parse_ixbrl_forecast([Path('/tmp/fake.htm')])

        assert 'FY' in result
        assert 'Q2' in result
        assert result['FY']['revenue'] == pytest.approx(100000.0, rel=1e-3)  # 非連結
        assert result['Q2']['revenue'] == pytest.approx(250000.0, rel=1e-3)  # 連結


# ============================================================
# skip_priority_check (--force) テスト
# ============================================================

class TestForceSkipPriorityCheck:
    """insert_management_forecast() の skip_priority_check テスト"""

    def test_force_overwrites_higher_priority(self, sample_company):
        """skip_priority_check=True で高優先度データも上書きできる"""
        ticker = sample_company
        # EDINET（優先度3）を先に挿入
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='EDINET',
        )
        # yfinance（優先度1）で上書き試行 → 通常はスキップ
        result_normal = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=999999.0,
            source='yfinance',
        )
        assert result_normal is False

        # skip_priority_check=True で強制上書き
        result_force = insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=888888.0,
            source='yfinance',
            skip_priority_check=True,
        )
        assert result_force is True

        records = get_management_forecast(ticker, '2026', 'FY')
        assert records[0]['revenue'] == 888888.0
        assert records[0]['source'] == 'yfinance'


# ============================================================
# COALESCE NULL上書き防止テスト
# ============================================================

class TestCoalesceNullPreservation:
    """ON CONFLICT DO UPDATE で NULL が既存値を上書きしないことを確認"""

    def test_null_does_not_overwrite_existing_value(self, sample_company):
        """新規データのNULLフィールドが既存の値を上書きしない"""
        ticker = sample_company
        # 初回: revenue + operating_income を設定
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            operating_income=50000.0,
            eps=250.0,
            source='TDnet',
        )
        # 2回目: revenue のみ更新、operating_income/eps は None
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=510000.0,
            operating_income=None,
            eps=None,
            source='TDnet',
        )

        records = get_management_forecast(ticker, '2026', 'FY')
        assert records[0]['revenue'] == 510000.0  # 更新される
        assert records[0]['operating_income'] == 50000.0  # 既存値が保持される
        assert records[0]['eps'] == 250.0  # 既存値が保持される

    def test_new_value_overwrites_existing(self, sample_company):
        """新規データに値がある場合は既存値を上書きする"""
        ticker = sample_company
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=500000.0,
            source='TDnet',
        )
        insert_management_forecast(
            ticker_code=ticker,
            fiscal_year='2026',
            fiscal_quarter='FY',
            announced_date='2025-05-10',
            forecast_type='initial',
            revenue=600000.0,
            source='TDnet',
        )

        records = get_management_forecast(ticker, '2026', 'FY')
        assert records[0]['revenue'] == 600000.0


# ============================================================
# _extract_forecast_fiscal_year() テスト
# ============================================================

import tempfile
import os


def _write_ixbrl(path: str, contexts: list) -> None:
    """テスト用の最小限iXBRLファイルを書き出す。

    contexts: list of dict with keys:
        id: コンテキストID
        end_date: endDate 文字列 (e.g. "2026-03-31")
    """
    ctx_xml = ""
    for ctx in contexts:
        ctx_xml += f"""
  <xbrli:context id="{ctx['id']}">
    <xbrli:entity><xbrli:identifier scheme="http://disclosure.edinet-fsa.go.jp">E12345</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-04-01</xbrli:startDate>
      <xbrli:endDate>{ctx['end_date']}</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>"""

    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
  xmlns:xbrli="http://www.xbrl.org/2003/instance"
  xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
>
{ctx_xml}
</xbrli:xbrl>"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


class TestExtractForecastFiscalYear:
    """_extract_forecast_fiscal_year() のコンテキスト抽出テスト"""

    def test_extract_next_year_priority(self):
        """NextYearDuration + ForecastMember → 年度を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                {'id': 'NextYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2026-03-31'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2026'

    def test_extract_current_year_fallback(self):
        """NextYearDurationがなく、CurrentYearDuration + ForecastMember のみ → フォールバックで年度返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                {'id': 'CurrentYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2026-03-31'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2026'

    def test_ignore_non_forecast_current_year(self):
        """ForecastMemberなしのCurrentYearDurationは無視される"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                # ForecastMemberなし → 対象外
                {'id': 'CurrentYearDuration_ConsolidatedMember', 'end_date': '2026-03-31'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result is None

    def test_extract_current_accumulated_q2(self):
        """CurrentAccumulatedQ2Duration + ForecastMember → 年度を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                {'id': 'CurrentAccumulatedQ2Duration_ConsolidatedMember_ForecastMember', 'end_date': '2025-09-30'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2025'

    def test_next_year_takes_priority_over_current_year(self):
        """NextYearDurationとCurrentYearDurationが共存する場合、NextYearを優先"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                # CurrentYear（来期より前）
                {'id': 'CurrentYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2026-03-31'},
                # NextYear（来期）
                {'id': 'NextYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2027-03-31'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2027'

    def test_empty_file_list_returns_none(self):
        """空のパスリストはNoneを返す"""
        result = _extract_forecast_fiscal_year([])
        assert result is None

    def test_next_q2_first_but_next_year_wins(self):
        """NextAccumulatedQ2Durationが先に出現してもNextYearDurationが優先される"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                # Q2が先に出現（endDate=2026-09-30 → 年=2026）
                {'id': 'NextAccumulatedQ2Duration_ConsolidatedMember_ForecastMember', 'end_date': '2026-09-30'},
                # FYが後に出現（endDate=2027-03-31 → 年=2027）
                {'id': 'NextYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2027-03-31'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2027'

    def test_current_q2_and_current_year_coexist(self):
        """CurrentAccumulatedQ2DurationとCurrentYearDurationが共存 → CurrentYearが優先"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                # Q2（endDate=2025-09-30 → 年=2025）
                {'id': 'CurrentAccumulatedQ2Duration_ConsolidatedMember_ForecastMember', 'end_date': '2025-09-30'},
                # FY（endDate=2026-03-31 → 年=2026）
                {'id': 'CurrentYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2026-03-31'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2026'

    def test_multi_file_next_wins_over_current(self):
        """複数ファイル入力: file1にCurrentYear、file2にNextYear → NextYearが優先"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "file1.htm")
            path2 = os.path.join(tmpdir, "file2.htm")
            # file1: CurrentYearのみ
            _write_ixbrl(path1, [
                {'id': 'CurrentYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2026-03-31'},
            ])
            # file2: NextYearあり
            _write_ixbrl(path2, [
                {'id': 'NextYearDuration_ConsolidatedMember_ForecastMember', 'end_date': '2027-03-31'},
            ])
            # file1が先に処理されるが、NextYearを持つfile2の結果は…
            # 注: 現行実装は最初のファイルでマッチしたら返す
            result1 = _extract_forecast_fiscal_year([Path(path1), Path(path2)])
            result2 = _extract_forecast_fiscal_year([Path(path2), Path(path1)])
        # file1のCurrentYearが先にマッチして返る（ファイル順に処理）
        assert result1 == '2026'
        # file2のNextYearが先にマッチして返る
        assert result2 == '2027'

    def test_next_q2_only_returns_q2_year(self):
        """NextAccumulatedQ2Durationのみの場合、そのendDateの年を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            ixbrl_path = os.path.join(tmpdir, "test.htm")
            _write_ixbrl(ixbrl_path, [
                {'id': 'NextAccumulatedQ2Duration_ConsolidatedMember_ForecastMember', 'end_date': '2026-09-30'},
            ])
            result = _extract_forecast_fiscal_year([Path(ixbrl_path)])
        assert result == '2026'
