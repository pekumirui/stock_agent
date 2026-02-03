#!/usr/bin/env python3
"""
株式調査AIエージェント - 日次バッチ実行スクリプト

毎日実行して株価と決算データを更新する

使用方法:
    python run_daily_batch.py           # 通常実行
    python run_daily_batch.py --init    # 初回セットアップ（銘柄マスタ初期化含む）
    python run_daily_batch.py --full    # 全履歴取得（初回用）

cronでの設定例（毎日18:00に実行）:
    0 18 * * 1-5 cd /path/to/stock_agent && python scripts/run_daily_batch.py >> logs/batch.log 2>&1
"""
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# スクリプトのディレクトリをパスに追加
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
    required = ['yfinance', 'pandas', 'requests']
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
    """DBの状態サマリーを表示"""
    print(f"\n{'='*60}")
    print("データベース状態")
    print(f"{'='*60}")
    
    with get_connection() as conn:
        # 銘柄数
        cursor = conn.execute("SELECT COUNT(*) FROM companies WHERE is_active = 1")
        company_count = cursor.fetchone()[0]
        
        # 株価レコード数
        cursor = conn.execute("SELECT COUNT(*) FROM daily_prices")
        price_count = cursor.fetchone()[0]
        
        # 最新株価日
        cursor = conn.execute("SELECT MAX(trade_date) FROM daily_prices")
        latest_price = cursor.fetchone()[0] or 'なし'
        
        # 決算レコード数
        cursor = conn.execute("SELECT COUNT(*) FROM financials")
        financial_count = cursor.fetchone()[0]
        
        # 分割情報数
        cursor = conn.execute("SELECT COUNT(*) FROM stock_splits")
        split_count = cursor.fetchone()[0]
    
    print(f"  銘柄数:         {company_count:,}")
    print(f"  株価レコード:   {price_count:,}")
    print(f"  最新株価日:     {latest_price}")
    print(f"  決算レコード:   {financial_count:,}")
    print(f"  株式分割情報:   {split_count:,}")


def main():
    parser = argparse.ArgumentParser(description='日次バッチ実行')
    parser.add_argument('--init', action='store_true', help='初回セットアップ')
    parser.add_argument('--full', action='store_true', help='全履歴取得')
    parser.add_argument('--sample', action='store_true', help='サンプル銘柄のみ（テスト用）')
    parser.add_argument('--skip-prices', action='store_true', help='株価取得をスキップ')
    parser.add_argument('--skip-financials', action='store_true', help='決算取得をスキップ')
    parser.add_argument('--edinet-api-key', help='EDINET APIキー')
    args = parser.parse_args()
    
    print(f"\n{'#'*60}")
    print(f"# 株式調査AIエージェント - 日次バッチ")
    print(f"# 実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    # 前提条件チェック
    if not check_prerequisites():
        sys.exit(1)
    
    # DB初期化
    print("\n[1/4] データベース初期化...")
    init_database()
    
    # 初回セットアップ時は銘柄マスタを初期化
    if args.init:
        print("\n[2/4] 銘柄マスタ初期化...")
        cmd = [sys.executable, 'init_companies.py']
        if args.sample:
            cmd.append('--sample')
        run_command(cmd, "銘柄マスタ初期化")
    else:
        # 銘柄が登録されているか確認
        tickers = get_all_tickers()
        if not tickers:
            print("\n[WARNING] 銘柄マスタが空です。--init オプションで初期化してください。")
            print("          または: python scripts/init_companies.py --sample")
            sys.exit(1)
        print(f"\n[2/4] 銘柄マスタ: {len(tickers)}銘柄登録済み")
    
    # 株価取得
    if not args.skip_prices:
        print("\n[3/4] 株価取得...")
        cmd = [sys.executable, 'fetch_prices.py']
        if args.full:
            cmd.append('--full')
        run_command(cmd, "株価データ取得")
    else:
        print("\n[3/4] 株価取得: スキップ")
    
    # 決算取得
    if not args.skip_financials:
        print("\n[4/4] 決算データ取得...")
        cmd = [sys.executable, 'fetch_financials.py', '--days', '7']
        if args.edinet_api_key:
            cmd.extend(['--api-key', args.edinet_api_key])
        run_command(cmd, "決算データ取得（EDINET）")
    else:
        print("\n[4/4] 決算取得: スキップ")
    
    # サマリー表示
    show_summary()
    
    print(f"\n{'#'*60}")
    print(f"# バッチ完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
