"""
init_companies.py のテスト
"""
import pytest
import sys
from pathlib import Path
import pandas as pd

# scriptsディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from init_companies import parse_jpx_data, init_companies_from_sample


class TestParseJpxData:
    """JPXデータパースのテスト"""

    def test_parse_valid_data(self):
        """正しい形式のデータがパースできること"""
        # テスト用のDataFrame作成
        df = pd.DataFrame({
            'コード': ['7203', '6758', '9984'],
            '銘柄名': ['トヨタ自動車', 'ソニーグループ', 'ソフトバンクグループ'],
            '市場・商品区分': ['プライム', 'プライム', 'プライム'],
            '33業種区分': ['輸送用機器', '電気機器', '情報・通信業'],
            '17業種区分': ['自動車', '電機', 'IT'],
        })

        companies = parse_jpx_data(df)

        assert len(companies) == 3
        assert companies[0]['ticker_code'] == '7203'
        assert companies[0]['company_name'] == 'トヨタ自動車'

    def test_skip_invalid_ticker(self):
        """無効な銘柄コードはスキップされること"""
        df = pd.DataFrame({
            'コード': ['7203', 'ABCD', '12345', ''],
            '銘柄名': ['トヨタ', '無効1', '無効2', '無効3'],
        })

        companies = parse_jpx_data(df)

        # 4桁数字のもののみ
        assert len(companies) == 1
        assert companies[0]['ticker_code'] == '7203'

    def test_code_column_exact_match(self):
        """「コード」完全一致で銘柄コードを取得すること（33業種コード等を除外）"""
        df = pd.DataFrame({
            'コード': ['7203'],
            '銘柄名': ['トヨタ自動車'],
            '33業種コード': ['50'],
            '17業種コード': ['10'],
            '規模コード': ['1'],
        })

        companies = parse_jpx_data(df)

        assert len(companies) == 1
        # ticker_codeが「7203」であること（「50」や「1」ではない）
        assert companies[0]['ticker_code'] == '7203'


class TestSampleCompanies:
    """サンプル銘柄のテスト"""

    def test_sample_count(self):
        """サンプル銘柄が31件あること"""
        companies = init_companies_from_sample()
        assert len(companies) == 31

    def test_sample_has_major_stocks(self):
        """主要銘柄が含まれていること"""
        companies = init_companies_from_sample()
        tickers = [c['ticker_code'] for c in companies]

        assert '7203' in tickers  # トヨタ
        assert '6758' in tickers  # ソニー
        assert '9984' in tickers  # ソフトバンクG
