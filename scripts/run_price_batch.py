#!/usr/bin/env python3
"""
株価取得バッチスクリプト

Yahoo Financeから日次株価データを取得・更新する。

使用方法:
    python run_price_batch.py           # 通常実行（差分取得）
    python run_price_batch.py --init    # 初回セットアップ（銘柄マスタ初期化含む）
    python run_price_batch.py --full    # 全履歴取得（初回用）

cronでの設定例（平日18:00に実行）:
    0 18 * * 1-5 cd /path/to/stock_agent && venv/bin/python scripts/run_price_batch.py >> logs/batch.log 2>&1
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
    required = ['yfinance', 'pandas']
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
    """株価関連のDBサマリーを表示"""
    print(f"\n{'='*60}")
    print("データベース状態（株価）")
    print(f"{'='*60}")

    with get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM companies WHERE is_active = 1")
        company_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM daily_prices")
        price_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT MAX(trade_date) FROM daily_prices")
        latest_price = cursor.fetchone()[0] or 'なし'

        cursor = conn.execute("SELECT COUNT(*) FROM stock_splits")
        split_count = cursor.fetchone()[0]

    print(f"  銘柄数:         {company_count:,}")
    print(f"  株価レコード:   {price_count:,}")
    print(f"  最新株価日:     {latest_price}")
    print(f"  株式分割情報:   {split_count:,}")


def main():
    parser = argparse.ArgumentParser(description='株価取得バッチ')
    parser.add_argument('--init', action='store_true', help='初回セットアップ（銘柄マスタ初期化含む）')
    parser.add_argument('--full', action='store_true', help='全履歴取得')
    parser.add_argument('--sample', action='store_true', help='サンプル銘柄のみ（テスト用）')
    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"# 株価取得バッチ")
    print(f"# 実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    if not check_prerequisites():
        sys.exit(1)

    # DB初期化
    print("\n[1/3] データベース初期化...")
    init_database()

    # 銘柄マスタ
    if args.init:
        print("\n[2/3] 銘柄マスタ初期化...")
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
        print(f"\n[2/3] 銘柄マスタ: {len(tickers)}銘柄登録済み")

    # 株価取得
    print("\n[3/3] 株価取得...")
    cmd = [sys.executable, 'fetch_prices.py']
    if args.full:
        cmd.append('--full')
    run_command(cmd, "株価データ取得（Yahoo Finance）")

    show_summary()

    print(f"\n{'#'*60}")
    print(f"# 株価取得バッチ完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
