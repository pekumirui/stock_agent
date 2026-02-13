#!/usr/bin/env python3
"""
開示データ取得バッチスクリプト

EDINET（有報・半期報・四半期報告書）とTDnet（決算短信）から決算データを取得・更新する。

データソース:
    - EDINET: 有価証券報告書・半期報告書・四半期報告書
    - TDnet: Q1-Q4決算短信（※Q1/Q3は法改正によりTDnetのみ）

使用方法:
    python run_disclosure_batch.py                # 通常実行（直近7日分）
    python run_disclosure_batch.py --days 30      # 直近30日分を取得
    python run_disclosure_batch.py --skip-edinet  # TDnetのみ
    python run_disclosure_batch.py --skip-tdnet   # EDINETのみ

cronでの設定例（平日22:00に実行）:
    0 22 * * 1-5 cd /path/to/stock_agent && venv/bin/python scripts/run_disclosure_batch.py >> logs/batch.log 2>&1
"""
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from db_utils import init_database, get_connection, get_all_tickers


def run_command(cmd: list, description: str) -> bool:
    """コマンドを実行"""
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {description}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            capture_output=False,
            text=True
        )
        return result.returncode == 0
    except Exception as e:
        print(f"[ERROR] {e}")
        return False


def check_prerequisites():
    """必要なパッケージがインストールされているか確認"""
    required = ['requests', 'bs4']
    missing = []

    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)

    if missing:
        print(f"[ERROR] 以下のパッケージが必要です: {', '.join(missing)}")
        print(f"インストール: pip install {' '.join(missing)}")
        return False

    return True


def show_summary():
    """決算関連のDBサマリーを表示"""
    print(f"\n{'='*60}")
    print("データベース状態（決算）")
    print(f"{'='*60}")

    with get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM companies WHERE is_active = 1")
        company_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM financials")
        financial_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT MAX(fiscal_year_end) FROM financials")
        latest_fiscal = cursor.fetchone()[0] or 'なし'

    print(f"  銘柄数:         {company_count:,}")
    print(f"  決算レコード:   {financial_count:,}")
    print(f"  最新決算期末:   {latest_fiscal}")


def main():
    parser = argparse.ArgumentParser(description='開示データ取得バッチ（EDINET + TDnet）')
    parser.add_argument('--init', action='store_true', help='初回セットアップ（銘柄マスタ初期化含む）')
    parser.add_argument('--days', type=int, default=7, help='検索日数（デフォルト: 7日）')
    parser.add_argument('--skip-edinet', action='store_true', help='EDINET取得をスキップ')
    parser.add_argument('--skip-tdnet', action='store_true', help='TDnet取得をスキップ')
    parser.add_argument('--edinet-api-key', help='EDINET APIキー')
    parser.add_argument('--sample', action='store_true', help='サンプル銘柄のみ（テスト用）')
    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"# 開示データ取得バッチ（EDINET + TDnet）")
    print(f"# 実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    if not check_prerequisites():
        sys.exit(1)

    # DB初期化
    print("\n[1/4] データベース初期化...")
    init_database()

    # 銘柄マスタ
    if args.init:
        print("\n[2/4] 銘柄マスタ初期化...")
        cmd = [sys.executable, 'init_companies.py']
        if args.sample:
            cmd.append('--sample')
        run_command(cmd, "銘柄マスタ初期化")
    else:
        tickers = get_all_tickers()
        if not tickers:
            print("\n[WARNING] 銘柄マスタが空です。--init オプションで初期化してください。")
            print("          または: python scripts/init_companies.py --sample")
            sys.exit(1)
        print(f"\n[2/4] 銘柄マスタ: {len(tickers)}銘柄登録済み")

    # EDINET決算取得
    if not args.skip_edinet:
        print(f"\n[3/4] 決算データ取得（EDINET）... 直近{args.days}日")
        cmd = [sys.executable, 'fetch_financials.py', '--days', str(args.days)]
        if args.edinet_api_key:
            cmd.extend(['--api-key', args.edinet_api_key])
        run_command(cmd, "決算データ取得（EDINET: 有報・半期報・四半期報）")
    else:
        print("\n[3/4] EDINET決算取得: スキップ")

    # TDnet決算短信取得
    if not args.skip_tdnet:
        print(f"\n[4/4] 決算短信取得（TDnet）... 直近{args.days}日")
        cmd = [sys.executable, 'fetch_tdnet.py', '--days', str(args.days)]
        run_command(cmd, "決算短信取得（TDnet: Q1-Q4決算短信）")
    else:
        print("\n[4/4] TDnet決算短信取得: スキップ")

    show_summary()

    print(f"\n{'#'*60}")
    print(f"# 開示データ取得バッチ完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
