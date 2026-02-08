"""
EDINET Code 未登録銘柄の抽出・原因分析

EDINETコードが未登録の銘柄をDBから抽出し、
カテゴリ分類・優先度判定を行い、CSV形式で出力する

使用方法:
    python scripts/analyze_missing_edinet.py
    python scripts/analyze_missing_edinet.py --include-stats
    python scripts/analyze_missing_edinet.py --output-dir exports
    python scripts/analyze_missing_edinet.py --priority HIGH
    python scripts/analyze_missing_edinet.py --category REGULAR_STOCK
    python scripts/analyze_missing_edinet.py --summary-only
"""
import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from db_utils import get_connection, log_batch_start, log_batch_end


# 基本パス設定
BASE_DIR = Path(__file__).parent.parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "csv"


def fetch_missing_edinet_companies(include_stats: bool = False) -> List[Dict]:
    """
    EDINETコード未登録企業を全件取得

    Args:
        include_stats: 株価・決算データ有無を集計するか

    Returns:
        未登録企業のリスト（辞書形式）
    """
    with get_connection() as conn:
        if include_stats:
            # 株価・決算データ有無を含む
            query = """
                SELECT
                    c.ticker_code,
                    c.company_name,
                    c.market_segment,
                    c.sector_33,
                    c.created_at,
                    -- データ有無チェック
                    (SELECT COUNT(*) FROM daily_prices dp
                     WHERE dp.ticker_code = c.ticker_code) as price_count,
                    (SELECT COUNT(*) FROM financials f
                     WHERE f.ticker_code = c.ticker_code) as financial_count
                FROM companies c
                WHERE c.edinet_code IS NULL
                ORDER BY c.market_segment, c.ticker_code
            """
        else:
            # 基本情報のみ
            query = """
                SELECT
                    c.ticker_code,
                    c.company_name,
                    c.market_segment,
                    c.sector_33,
                    c.created_at
                FROM companies c
                WHERE c.edinet_code IS NULL
                ORDER BY c.market_segment, c.ticker_code
            """

        cursor = conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]


def categorize_company(market_segment: str, company_name: str = None) -> Dict:
    """
    企業を分類し、原因・優先度・推奨アクションを判定

    Args:
        market_segment: 市場区分
        company_name: 企業名（オプション）

    Returns:
        {
            'category': str,           # ETF, REIT, PRO_MARKET, REGULAR_STOCK, OTHER
            'likely_cause': str,       # 推定原因
            'priority': str,           # LOW, MEDIUM, HIGH
            'recommendation': str      # 推奨アクション
        }
    """
    if not market_segment:
        return {
            'category': 'OTHER',
            'likely_cause': '市場区分不明',
            'priority': 'MEDIUM',
            'recommendation': '個別調査'
        }

    segment_str = str(market_segment)

    # ETF・ETN判定
    if 'ETF' in segment_str or 'ETN' in segment_str:
        return {
            'category': 'ETF',
            'likely_cause': 'EDINET登録不要（投資信託）',
            'priority': 'LOW',
            'recommendation': '対応不要'
        }

    # REIT等判定
    if 'REIT' in segment_str or 'ベンチャーファンド' in segment_str or \
       'カントリーファンド' in segment_str or 'インフラファンド' in segment_str:
        return {
            'category': 'REIT',
            'likely_cause': 'EDINET登録不要（不動産投資信託等）',
            'priority': 'LOW',
            'recommendation': '対応不要'
        }

    # PRO Market判定
    if 'PRO Market' in segment_str:
        return {
            'category': 'PRO_MARKET',
            'likely_cause': 'プロ投資家向け市場（開示義務限定）',
            'priority': 'MEDIUM',
            'recommendation': '手動調査推奨'
        }

    # 通常株式判定（プライム・スタンダード・グロース）
    if '内国株式' in segment_str or 'プライム' in segment_str or \
       'スタンダード' in segment_str or 'グロース' in segment_str:
        return {
            'category': 'REGULAR_STOCK',
            'likely_cause': 'APIマッチング失敗 or 最近上場',
            'priority': 'HIGH',
            'recommendation': 'API再実行 or 手動登録'
        }

    # その他
    return {
        'category': 'OTHER',
        'likely_cause': '特殊証券（要調査）',
        'priority': 'MEDIUM',
        'recommendation': '個別調査'
    }


def calculate_statistics(companies: List[Dict]) -> Dict:
    """
    カテゴリ別・優先度別の統計を算出

    Args:
        companies: 銘柄リスト（分類済み）

    Returns:
        統計情報の辞書
    """
    stats = {
        'total': len(companies),
        'by_category': {},
        'by_priority': {},
        'by_market_segment': {}
    }

    # カテゴリ別集計
    for company in companies:
        category = company['category']
        priority = company['priority']
        market_segment = company.get('market_segment', '不明')

        # カテゴリ別
        if category not in stats['by_category']:
            stats['by_category'][category] = 0
        stats['by_category'][category] += 1

        # 優先度別
        if priority not in stats['by_priority']:
            stats['by_priority'][priority] = 0
        stats['by_priority'][priority] += 1

        # 市場区分別
        if market_segment not in stats['by_market_segment']:
            stats['by_market_segment'][market_segment] = 0
        stats['by_market_segment'][market_segment] += 1

    # データ有無集計（include_stats=True の場合）
    if companies and 'price_count' in companies[0]:
        stats['with_price_data'] = sum(1 for c in companies if c.get('price_count', 0) > 0)
        stats['with_financial_data'] = sum(1 for c in companies if c.get('financial_count', 0) > 0)

    return stats


def export_to_csv(companies: List[Dict], output_path: Path):
    """
    詳細CSVを出力

    Args:
        companies: 銘柄リスト（分類済み）
        output_path: 出力先パス
    """
    # CSV列定義
    fieldnames = [
        'ticker_code',
        'company_name',
        'market_segment',
        'sector_33',
        'category',
        'likely_cause',
        'priority',
        'recommendation',
        'has_price_data',
        'has_financial_data',
        'created_at'
    ]

    # 出力ディレクトリ作成
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # CSV出力（Excel互換：BOM付きUTF-8）
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for company in companies:
            # has_price_data / has_financial_data の計算
            has_price = 1 if company.get('price_count', 0) > 0 else 0
            has_financial = 1 if company.get('financial_count', 0) > 0 else 0

            row = {
                'ticker_code': company['ticker_code'],
                'company_name': company['company_name'],
                'market_segment': company.get('market_segment', ''),
                'sector_33': company.get('sector_33', ''),
                'category': company['category'],
                'likely_cause': company['likely_cause'],
                'priority': company['priority'],
                'recommendation': company['recommendation'],
                'has_price_data': has_price,
                'has_financial_data': has_financial,
                'created_at': company.get('created_at', '')
            }
            writer.writerow(row)

    print(f"  → {output_path}")


def generate_summary_report(companies: List[Dict], stats: Dict, output_path: Path):
    """
    テキストサマリーレポートを生成

    Args:
        companies: 銘柄リスト（分類済み）
        stats: 統計情報
        output_path: 出力先パス
    """
    # 出力ディレクトリ作成
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # レポート生成
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("EDINET Code Missing Analysis Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")

        # 1. 概要
        f.write("1. OVERVIEW\n")
        f.write("-" * 60 + "\n")

        # DB総数取得
        with get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) as total, COUNT(edinet_code) as with_edinet FROM companies")
            row = cursor.fetchone()
            total_companies = row['total']
            with_edinet = row['with_edinet']

        f.write(f"Total companies in DB: {total_companies:,}\n")
        f.write(f"Companies with EDINET code: {with_edinet:,}\n")
        f.write(f"Companies without EDINET code: {stats['total']:,}\n")
        coverage = (with_edinet / total_companies * 100) if total_companies > 0 else 0
        f.write(f"Coverage rate: {coverage:.1f}%\n\n")

        # 2. カテゴリ別内訳
        f.write("2. BREAKDOWN BY CATEGORY\n")
        f.write("-" * 60 + "\n")

        category_names = {
            'ETF': 'ETF (投資信託)',
            'REIT': 'REIT (不動産投資信託等)',
            'PRO_MARKET': 'PRO_MARKET (プロ向け市場)',
            'REGULAR_STOCK': 'REGULAR_STOCK (通常株式)',
            'OTHER': 'OTHER (その他)'
        }

        priority_map = {'ETF': 'LOW', 'REIT': 'LOW', 'PRO_MARKET': 'MEDIUM', 'REGULAR_STOCK': 'HIGH', 'OTHER': 'MEDIUM'}

        for category in ['ETF', 'REIT', 'PRO_MARKET', 'REGULAR_STOCK', 'OTHER']:
            count = stats['by_category'].get(category, 0)
            if count > 0:
                pct = (count / stats['total'] * 100) if stats['total'] > 0 else 0
                priority = priority_map.get(category, 'MEDIUM')
                f.write(f"{category_names[category]:<30}: {count:4} ({pct:5.1f}%)  [Priority: {priority}]\n")
        f.write("\n")

        # 3. 優先度別アクションアイテム
        f.write("3. PRIORITY ACTION ITEMS\n")
        f.write("-" * 60 + "\n")

        # HIGH優先度の詳細
        high_priority = stats['by_priority'].get('HIGH', 0)
        if high_priority > 0:
            f.write(f"HIGH Priority (要対応): {high_priority}件\n")

            # 市場区分別の内訳
            high_companies = [c for c in companies if c['priority'] == 'HIGH']
            high_segments = {}
            for c in high_companies:
                seg = c.get('market_segment', '不明')
                high_segments[seg] = high_segments.get(seg, 0) + 1

            for seg, count in sorted(high_segments.items(), key=lambda x: -x[1]):
                f.write(f"  - {seg}: {count}件\n")

            f.write("\n  推奨アクション:\n")
            f.write("  1. update_edinet_codes.py を --days 180 で再実行\n")
            f.write("  2. 残存銘柄は手動でEDINET検索\n")
            f.write("  3. 英字付き証券コード (285A等) の特別処理検討\n\n")

        # MEDIUM優先度
        medium_priority = stats['by_priority'].get('MEDIUM', 0)
        if medium_priority > 0:
            f.write(f"MEDIUM Priority (調査推奨): {medium_priority}件\n")
            medium_companies = [c for c in companies if c['priority'] == 'MEDIUM']
            medium_categories = {}
            for c in medium_companies:
                cat = c.get('category', 'OTHER')
                medium_categories[cat] = medium_categories.get(cat, 0) + 1

            for cat, count in sorted(medium_categories.items(), key=lambda x: -x[1]):
                f.write(f"  - {cat}: {count}件\n")
            f.write("\n")

        # LOW優先度
        low_priority = stats['by_priority'].get('LOW', 0)
        if low_priority > 0:
            f.write(f"LOW Priority (対応不要): {low_priority}件\n")
            low_companies = [c for c in companies if c['priority'] == 'LOW']
            low_categories = {}
            for c in low_companies:
                cat = c.get('category', 'OTHER')
                low_categories[cat] = low_categories.get(cat, 0) + 1

            for cat, count in sorted(low_categories.items(), key=lambda x: -x[1]):
                f.write(f"  - {cat}: {count}件 → EDINET登録不要\n")
            f.write("\n")

        # 4. データ有無（include_stats=True の場合）
        if 'with_price_data' in stats:
            f.write("4. DATA AVAILABILITY\n")
            f.write("-" * 60 + "\n")
            f.write(f"High priority companies with stock price data: {stats.get('with_price_data', 0):,} / {high_priority:,}\n")
            f.write(f"High priority companies with financial data: {stats.get('with_financial_data', 0):,} / {high_priority:,}\n\n")

        # 5. 根本原因仮説
        f.write("5. ROOT CAUSE HYPOTHESIS\n")
        f.write("-" * 60 + "\n")

        etf_count = stats['by_category'].get('ETF', 0)
        reit_count = stats['by_category'].get('REIT', 0)
        pro_count = stats['by_category'].get('PRO_MARKET', 0)
        regular_count = stats['by_category'].get('REGULAR_STOCK', 0)

        etf_reit_total = etf_count + reit_count
        etf_reit_pct = (etf_reit_total / stats['total'] * 100) if stats['total'] > 0 else 0

        f.write(f"A. ETF/REIT ({etf_reit_total}件, {etf_reit_pct:.1f}%)\n")
        f.write("   - EDINETは企業開示システムであり、投資信託・REITは対象外\n")
        f.write("   - これらは投資信託協会等の別の報告体系を使用\n\n")

        if pro_count > 0:
            pro_pct = (pro_count / stats['total'] * 100) if stats['total'] > 0 else 0
            f.write(f"B. PRO Market ({pro_count}件, {pro_pct:.1f}%)\n")
            f.write("   - プロ投資家向け市場は開示義務が緩和されている\n")
            f.write("   - 一部企業はEDINET登録しているが、義務ではない\n\n")

        if regular_count > 0:
            regular_pct = (regular_count / stats['total'] * 100) if stats['total'] > 0 else 0
            f.write(f"C. Regular Stocks ({regular_count}件, {regular_pct:.1f}%)\n")
            f.write("   - EDINET API の document-based マッチングの限界\n")
            f.write("   - 過去90日間で書類提出がない企業はマッチ不可\n")
            f.write("   - 英字付き証券コード (285A等) のパース問題の可能性\n")
            f.write("   - 最近上場した企業でまだ決算未発表\n\n")

        # 6. 次のステップ
        f.write("6. NEXT STEPS\n")
        f.write("-" * 60 + "\n")
        f.write("1. Immediate Actions:\n")
        f.write("   - Run: python scripts/update_edinet_codes.py --days 180\n")
        f.write("   - Export HIGH priority list for manual review\n\n")
        f.write("2. Medium-term:\n")
        f.write("   - Implement EDINET code list API (Type 1) integration\n")
        f.write("   - Add support for letter-suffix ticker codes (285A)\n")
        f.write("   - Create manual registration workflow\n\n")
        f.write("3. Long-term:\n")
        f.write("   - Consider alternative data sources for PRO Market\n")
        f.write("   - Build ticker code normalization library\n")

    print(f"  → {output_path}")


def main():
    parser = argparse.ArgumentParser(description='EDINET Code Missing Analysis')
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR),
                       help='出力ディレクトリ (default: data/csv)')
    parser.add_argument('--include-stats', action='store_true',
                       help='株価・決算データの有無を集計（処理時間増加）')
    parser.add_argument('--priority',
                       help='優先度フィルタ (HIGH,MEDIUM,LOW)')
    parser.add_argument('--category',
                       help='カテゴリフィルタ (ETF,REIT,PRO_MARKET,REGULAR_STOCK,OTHER)')
    parser.add_argument('--summary-only', action='store_true',
                       help='サマリーレポートのみ生成（CSV出力なし）')
    args = parser.parse_args()

    print("EDINET Code Missing Analysis")
    print("-" * 60)

    # バッチログ開始
    log_id = log_batch_start("analyze_missing_edinet")

    try:
        # 1. データ取得
        print("\nデータ取得中...")
        companies = fetch_missing_edinet_companies(include_stats=args.include_stats)
        print(f"  取得完了: {len(companies)}件")

        if not companies:
            print("\n[INFO] EDINETコード未登録の銘柄はありません")
            log_batch_end(log_id, "success", 0)
            return

        # 2. 分類処理
        print("\n分類処理中...")
        for company in companies:
            result = categorize_company(
                company.get('market_segment'),
                company.get('company_name')
            )
            company.update(result)

        # 統計計算
        stats = calculate_statistics(companies)

        # カテゴリ別集計表示
        for category, count in stats['by_category'].items():
            print(f"  {category}: {count}件")

        # 3. フィルタ適用
        filtered_companies = companies

        if args.priority:
            priorities = [p.strip().upper() for p in args.priority.split(',')]
            filtered_companies = [c for c in filtered_companies if c['priority'] in priorities]
            print(f"\n優先度フィルタ適用: {len(filtered_companies)}件")

        if args.category:
            categories = [c.strip().upper() for c in args.category.split(',')]
            filtered_companies = [c for c in filtered_companies if c['category'] in categories]
            print(f"\nカテゴリフィルタ適用: {len(filtered_companies)}件")

        # 4. 出力
        output_dir = Path(args.output_dir)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if not args.summary_only:
            # CSV出力
            print("\nCSV出力中...")
            csv_path = output_dir / f"missing_edinet_detailed_{timestamp}.csv"
            export_to_csv(filtered_companies, csv_path)

        # サマリーレポート生成
        print("\nサマリーレポート生成中...")
        summary_path = output_dir / f"missing_edinet_summary_{timestamp}.txt"
        generate_summary_report(companies, stats, summary_path)  # フィルタ前の全データで統計

        # 5. コンソール出力
        print("\n" + "-" * 60)
        print(f"完了: {len(companies)}件を分析しました")
        print("\n優先度別サマリー:")
        for priority in ['HIGH', 'MEDIUM', 'LOW']:
            count = stats['by_priority'].get(priority, 0)
            if count > 0:
                desc = {'HIGH': '要対応', 'MEDIUM': '調査推奨', 'LOW': '対応不要'}
                print(f"  {priority} ({desc[priority]}): {count}件")

        print("\n次のステップ:")
        high_count = stats['by_priority'].get('HIGH', 0)
        if high_count > 0:
            print(f"1. HIGH優先度の{high_count}件を確認")
            print("2. update_edinet_codes.py を --days 180 で再実行")
            print("3. 残存銘柄は手動でEDINET検索を検討")
        else:
            print("1. MEDIUM優先度の銘柄を個別調査")
            print("2. 必要に応じてEDINET Code List API (Type 1) の実装検討")

        # バッチログ終了
        log_batch_end(log_id, "success", len(companies))

    except Exception as e:
        log_batch_end(log_id, "failed", 0, str(e))
        print(f"\n[ERROR] 処理失敗: {e}")
        raise


if __name__ == "__main__":
    main()
