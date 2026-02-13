"""
test_analyze_missing_edinet.py - EDINET未登録銘柄分析スクリプトのテスト

実APIなし・モック不要のユニット/統合テスト
"""
import pytest
import csv
from pathlib import Path
from datetime import datetime

# テスト対象モジュール
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))
from analyze_missing_edinet import (
    categorize_company,
    calculate_statistics,
    export_to_csv,
    generate_summary_report,
    fetch_missing_edinet_companies
)

from db_utils import get_connection, upsert_company


class TestCategorizationLogic:
    """カテゴリ分類ロジックのテスト"""

    def test_categorize_etf(self):
        """ETF判定テスト"""
        result = categorize_company("ETF・ETN")
        assert result['category'] == 'ETF'
        assert result['priority'] == 'LOW'
        assert result['likely_cause'] == 'EDINET登録不要（投資信託）'
        assert result['recommendation'] == '対応不要'

    def test_categorize_reit(self):
        """REIT判定テスト"""
        result = categorize_company("REIT・ベンチャーファンド・カントリーファンド・インフラファンド")
        assert result['category'] == 'REIT'
        assert result['priority'] == 'LOW'
        assert '不動産投資信託' in result['likely_cause']

    def test_categorize_pro_market(self):
        """PRO Market判定テスト"""
        result = categorize_company("PRO Market")
        assert result['category'] == 'PRO_MARKET'
        assert result['priority'] == 'MEDIUM'
        assert 'プロ投資家向け市場' in result['likely_cause']

    def test_categorize_prime(self):
        """プライム市場判定テスト"""
        result = categorize_company("プライム（内国株式）")
        assert result['category'] == 'REGULAR_STOCK'
        assert result['priority'] == 'HIGH'
        assert result['recommendation'] == 'API再実行 or 手動登録'

    def test_categorize_standard(self):
        """スタンダード市場判定テスト"""
        result = categorize_company("スタンダード（内国株式）")
        assert result['category'] == 'REGULAR_STOCK'
        assert result['priority'] == 'HIGH'

    def test_categorize_growth(self):
        """グロース市場判定テスト"""
        result = categorize_company("グロース（内国株式）")
        assert result['category'] == 'REGULAR_STOCK'
        assert result['priority'] == 'HIGH'

    def test_categorize_none(self):
        """市場区分なしの判定テスト"""
        result = categorize_company(None)
        assert result['category'] == 'OTHER'
        assert result['priority'] == 'MEDIUM'

    def test_categorize_empty(self):
        """空文字の判定テスト"""
        result = categorize_company("")
        assert result['category'] == 'OTHER'
        assert result['priority'] == 'MEDIUM'

    def test_categorize_unknown(self):
        """不明な市場区分の判定テスト"""
        result = categorize_company("出資証券")
        assert result['category'] == 'OTHER'
        assert result['priority'] == 'MEDIUM'


class TestStatisticsCalculation:
    """統計計算のテスト"""

    def test_calculate_statistics_basic(self):
        """基本統計計算テスト"""
        companies = [
            {'category': 'ETF', 'priority': 'LOW', 'market_segment': 'ETF・ETN'},
            {'category': 'ETF', 'priority': 'LOW', 'market_segment': 'ETF・ETN'},
            {'category': 'REGULAR_STOCK', 'priority': 'HIGH', 'market_segment': 'プライム（内国株式）'},
        ]

        stats = calculate_statistics(companies)

        assert stats['total'] == 3
        assert stats['by_category']['ETF'] == 2
        assert stats['by_category']['REGULAR_STOCK'] == 1
        assert stats['by_priority']['LOW'] == 2
        assert stats['by_priority']['HIGH'] == 1
        assert stats['by_market_segment']['ETF・ETN'] == 2
        assert stats['by_market_segment']['プライム（内国株式）'] == 1

    def test_calculate_statistics_with_data_availability(self):
        """データ有無を含む統計計算テスト"""
        companies = [
            {'category': 'ETF', 'priority': 'LOW', 'market_segment': 'ETF・ETN',
             'price_count': 10, 'financial_count': 0},
            {'category': 'REGULAR_STOCK', 'priority': 'HIGH', 'market_segment': 'プライム（内国株式）',
             'price_count': 0, 'financial_count': 5},
            {'category': 'REGULAR_STOCK', 'priority': 'HIGH', 'market_segment': 'プライム（内国株式）',
             'price_count': 0, 'financial_count': 0},
        ]

        stats = calculate_statistics(companies)

        assert stats['with_price_data'] == 1
        assert stats['with_financial_data'] == 1

    def test_calculate_statistics_empty(self):
        """空リストの統計計算テスト"""
        stats = calculate_statistics([])

        assert stats['total'] == 0
        assert stats['by_category'] == {}
        assert stats['by_priority'] == {}


class TestCSVExport:
    """CSV出力のテスト"""

    def test_csv_export_basic(self, tmp_path):
        """基本CSV出力テスト"""
        companies = [
            {
                'ticker_code': '9999',
                'company_name': 'テスト株式会社',
                'market_segment': 'プライム（内国株式）',
                'sector_33': 'テスト業種',
                'category': 'REGULAR_STOCK',
                'likely_cause': 'APIマッチング失敗',
                'priority': 'HIGH',
                'recommendation': 'API再実行',
                'price_count': 0,
                'financial_count': 0,
                'created_at': '2026-02-08 12:00:00'
            }
        ]

        output_path = tmp_path / "test_output.csv"
        export_to_csv(companies, output_path)

        # ファイルが作成されたことを確認
        assert output_path.exists()

        # CSV内容を確認
        with open(output_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            assert rows[0]['ticker_code'] == '9999'
            assert rows[0]['company_name'] == 'テスト株式会社'
            assert rows[0]['category'] == 'REGULAR_STOCK'
            assert rows[0]['priority'] == 'HIGH'
            assert rows[0]['has_price_data'] == '0'
            assert rows[0]['has_financial_data'] == '0'

    def test_csv_export_multiple_companies(self, tmp_path):
        """複数銘柄のCSV出力テスト"""
        companies = [
            {
                'ticker_code': '9999',
                'company_name': 'テスト1',
                'market_segment': 'ETF・ETN',
                'sector_33': '-',
                'category': 'ETF',
                'likely_cause': 'EDINET登録不要',
                'priority': 'LOW',
                'recommendation': '対応不要',
                'price_count': 10,
                'financial_count': 0,
                'created_at': '2026-02-08 12:00:00'
            },
            {
                'ticker_code': '9998',
                'company_name': 'テスト2',
                'market_segment': 'プライム（内国株式）',
                'sector_33': 'テスト業種',
                'category': 'REGULAR_STOCK',
                'likely_cause': 'APIマッチング失敗',
                'priority': 'HIGH',
                'recommendation': 'API再実行',
                'price_count': 0,
                'financial_count': 0,
                'created_at': '2026-02-08 12:00:00'
            }
        ]

        output_path = tmp_path / "test_output.csv"
        export_to_csv(companies, output_path)

        # CSV内容を確認
        with open(output_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 2
            assert rows[0]['ticker_code'] == '9999'
            assert rows[1]['ticker_code'] == '9998'
            assert rows[0]['has_price_data'] == '1'  # price_count > 0
            assert rows[1]['has_price_data'] == '0'

    def test_csv_encoding_excel_compatible(self, tmp_path):
        """Excel互換エンコーディング（BOM付きUTF-8）テスト"""
        companies = [
            {
                'ticker_code': '9999',
                'company_name': '日本語テスト株式会社',
                'market_segment': 'プライム（内国株式）',
                'sector_33': 'テスト業種',
                'category': 'REGULAR_STOCK',
                'likely_cause': 'APIマッチング失敗',
                'priority': 'HIGH',
                'recommendation': 'API再実行',
                'price_count': 0,
                'financial_count': 0,
                'created_at': '2026-02-08 12:00:00'
            }
        ]

        output_path = tmp_path / "test_output.csv"
        export_to_csv(companies, output_path)

        # BOMが含まれているか確認
        with open(output_path, 'rb') as f:
            first_bytes = f.read(3)
            assert first_bytes == b'\xef\xbb\xbf'  # UTF-8 BOM


class TestSummaryReport:
    """サマリーレポートのテスト"""

    def test_summary_report_generation(self, tmp_path, test_db):
        """サマリーレポート生成テスト"""
        # テスト用銘柄を追加（EDINETコードなし）
        with get_connection() as conn:
            # 未登録銘柄を2件追加
            upsert_company('9999', 'テストETF', market_segment='ETF・ETN')
            upsert_company('9998', 'テスト株式会社', market_segment='プライム（内国株式）')

        companies = [
            {
                'ticker_code': '9999',
                'company_name': 'テストETF',
                'market_segment': 'ETF・ETN',
                'sector_33': '-',
                'category': 'ETF',
                'likely_cause': 'EDINET登録不要',
                'priority': 'LOW',
                'recommendation': '対応不要',
                'created_at': '2026-02-08 12:00:00'
            },
            {
                'ticker_code': '9998',
                'company_name': 'テスト株式会社',
                'market_segment': 'プライム（内国株式）',
                'sector_33': 'テスト業種',
                'category': 'REGULAR_STOCK',
                'likely_cause': 'APIマッチング失敗',
                'priority': 'HIGH',
                'recommendation': 'API再実行',
                'created_at': '2026-02-08 12:00:00'
            }
        ]

        stats = calculate_statistics(companies)
        output_path = tmp_path / "test_summary.txt"

        generate_summary_report(companies, stats, output_path)

        # ファイルが作成されたことを確認
        assert output_path.exists()

        # レポート内容を確認
        content = output_path.read_text(encoding='utf-8')

        assert 'EDINET Code Missing Analysis Report' in content
        assert '1. OVERVIEW' in content
        assert '2. BREAKDOWN BY CATEGORY' in content
        assert '3. PRIORITY ACTION ITEMS' in content
        assert 'ETF (投資信託)' in content
        assert 'REGULAR_STOCK (通常株式)' in content
        assert 'HIGH Priority' in content or 'LOW Priority' in content


class TestIntegration:
    """統合テスト"""

    def test_fetch_missing_edinet_companies(self, test_db):
        """DB取得の統合テスト"""
        # EDINETコードなしの銘柄を作成
        upsert_company('9999', 'テスト株式会社')
        companies = fetch_missing_edinet_companies(include_stats=False)

        # 少なくとも1件は取得できるはず
        assert len(companies) > 0

        # 必須カラムが含まれているか確認
        company = companies[0]
        assert 'ticker_code' in company
        assert 'company_name' in company
        assert 'market_segment' in company

    def test_fetch_with_stats(self, test_db):
        """株価・決算データ有無を含む取得テスト"""
        # EDINETコードなしの銘柄を作成
        upsert_company('9999', 'テスト株式会社')
        companies = fetch_missing_edinet_companies(include_stats=True)

        assert len(companies) > 0

        # 統計カラムが含まれているか確認
        company = companies[0]
        assert 'price_count' in company
        assert 'financial_count' in company

    def test_full_workflow(self, test_db, tmp_path):
        """フルワークフローテスト（DB → CSV → Summary）"""
        # テスト用銘柄を追加（EDINETコードなし）
        upsert_company('9999', 'テストETF', market_segment='ETF・ETN')
        upsert_company('9998', 'テスト株式会社', market_segment='プライム（内国株式）')
        upsert_company('9997', 'テストREIT', market_segment='REIT・ベンチャーファンド・カントリーファンド・インフラファンド')

        # 1. データ取得
        companies = fetch_missing_edinet_companies(include_stats=True)
        assert len(companies) >= 3

        # 2. 分類処理
        for company in companies:
            result = categorize_company(
                company.get('market_segment'),
                company.get('company_name')
            )
            company.update(result)

        # カテゴリが付与されたか確認
        categories = {c['category'] for c in companies}
        assert 'ETF' in categories or 'REGULAR_STOCK' in categories or 'REIT' in categories

        # 3. 統計計算
        stats = calculate_statistics(companies)
        assert stats['total'] >= 3
        assert 'by_category' in stats
        assert 'by_priority' in stats

        # 4. CSV出力
        csv_path = tmp_path / "test_full_workflow.csv"
        export_to_csv(companies, csv_path)
        assert csv_path.exists()

        # 5. サマリー出力
        summary_path = tmp_path / "test_full_workflow_summary.txt"
        generate_summary_report(companies, stats, summary_path)
        assert summary_path.exists()

        # CSVの内容確認
        with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) >= 3

        # サマリーの内容確認
        summary_content = summary_path.read_text(encoding='utf-8')
        assert 'OVERVIEW' in summary_content
        assert 'BREAKDOWN BY CATEGORY' in summary_content

    def test_priority_filtering(self, test_db):
        """優先度フィルタのテスト"""
        # テスト用銘柄を追加
        upsert_company('9999', 'テストETF', market_segment='ETF・ETN')
        upsert_company('9998', 'テスト株式会社', market_segment='プライム（内国株式）')

        companies = fetch_missing_edinet_companies(include_stats=False)

        # 分類処理
        for company in companies:
            result = categorize_company(
                company.get('market_segment'),
                company.get('company_name')
            )
            company.update(result)

        # HIGH優先度のみフィルタ
        high_priority = [c for c in companies if c['priority'] == 'HIGH']

        # プライム市場のテスト株式会社が含まれているはず
        ticker_codes = {c['ticker_code'] for c in high_priority}
        assert '9998' in ticker_codes

        # ETFは含まれないはず
        assert '9999' not in ticker_codes


class TestEdgeCases:
    """エッジケースのテスト"""

    def test_null_market_segment(self, test_db):
        """市場区分がNULLの銘柄のテスト"""
        # 市場区分がNULLの銘柄を追加
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO companies (ticker_code, company_name, market_segment, edinet_code)
                VALUES ('9996', 'テスト銘柄（市場区分なし）', NULL, NULL)
            """)
            conn.commit()

        companies = fetch_missing_edinet_companies(include_stats=False)

        # 分類処理
        for company in companies:
            if company['ticker_code'] == '9996':
                result = categorize_company(
                    company.get('market_segment'),
                    company.get('company_name')
                )
                company.update(result)

                # NULLの場合はOTHERに分類されるはず
                assert company['category'] == 'OTHER'
                assert company['priority'] == 'MEDIUM'

    def test_long_company_name(self, tmp_path):
        """長い企業名のCSV出力テスト"""
        long_name = '株式会社' + 'あ' * 100  # 103文字の企業名

        companies = [
            {
                'ticker_code': '9999',
                'company_name': long_name,
                'market_segment': 'プライム（内国株式）',
                'sector_33': 'テスト業種',
                'category': 'REGULAR_STOCK',
                'likely_cause': 'APIマッチング失敗',
                'priority': 'HIGH',
                'recommendation': 'API再実行',
                'price_count': 0,
                'financial_count': 0,
                'created_at': '2026-02-08 12:00:00'
            }
        ]

        output_path = tmp_path / "test_long_name.csv"
        export_to_csv(companies, output_path)

        # CSV読み込み確認（エラーが起きないこと）
        with open(output_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 1
            assert rows[0]['company_name'] == long_name
