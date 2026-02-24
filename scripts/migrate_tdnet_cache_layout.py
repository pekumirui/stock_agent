"""
TDnet XBRLキャッシュのフラット構造→日付フォルダ構造への移行スクリプト

既存の data/tdnet_xbrl_cache/*.zip を data/tdnet_xbrl_cache/YYYY-MM-DD/*.zip に移動する。
filing_dateはZIP内のAttachmentファイル名パターンから抽出する。

使用方法:
    python migrate_tdnet_cache_layout.py --dry-run   # 移動先を表示（実行しない）
    python migrate_tdnet_cache_layout.py --execute    # 実際に移動を実行
"""
import argparse
import shutil
import sys
import zipfile
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "lib"))

from fetch_tdnet import (
    TDNET_XBRL_CACHE_DIR,
    _extract_filing_date_from_namelist,
    _get_ticker_from_namelist,
)


def migrate(dry_run: bool = True):
    """フラットZIPを日付フォルダに移行"""
    zip_files = sorted(TDNET_XBRL_CACHE_DIR.glob('*.zip'))
    if not zip_files:
        print("移行対象のZIPファイルがありません")
        return

    print(f"移行対象: {len(zip_files)}ファイル")
    print(f"モード: {'dry-run（確認のみ）' if dry_run else '実行'}")
    print("-" * 50)

    moved = 0
    failed = 0
    date_counts = Counter()

    for zip_path in zip_files:
        try:
            with zipfile.ZipFile(zip_path) as zf:
                namelist = zf.namelist()

            filing_date = _extract_filing_date_from_namelist(namelist)
            if not filing_date:
                print(f"  [SKIP] filing_date抽出失敗: {zip_path.name}")
                failed += 1
                continue

            ticker = _get_ticker_from_namelist(namelist)
            dest_dir = TDNET_XBRL_CACHE_DIR / filing_date
            dest_path = dest_dir / zip_path.name

            if dest_path.exists():
                print(f"  [SKIP] 移動先に既に存在: {dest_path.relative_to(TDNET_XBRL_CACHE_DIR)}")
                continue

            if dry_run:
                print(f"  {zip_path.name} → {filing_date}/ (ticker={ticker})")
            else:
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(zip_path), str(dest_path))

            date_counts[filing_date] += 1
            moved += 1

        except (zipfile.BadZipFile, Exception) as e:
            print(f"  [ERROR] {zip_path.name}: {e}")
            failed += 1

    print("-" * 50)
    print(f"{'移動予定' if dry_run else '移動完了'}: {moved}ファイル → {len(date_counts)}日付フォルダ")
    if failed > 0:
        print(f"失敗: {failed}ファイル")

    # 日付フォルダごとに _complete.marker を作成（実行モードのみ）
    if not dry_run and date_counts:
        for date_str in date_counts:
            marker = TDNET_XBRL_CACHE_DIR / date_str / '_complete.marker'
            marker.touch()
        print(f"_complete.marker 作成: {len(date_counts)}フォルダ")

    # サマリー
    if date_counts:
        print(f"\n日付分布:")
        for date_str in sorted(date_counts):
            print(f"  {date_str}: {date_counts[date_str]}件")


def main():
    parser = argparse.ArgumentParser(
        description='TDnet XBRLキャッシュを日付フォルダ構造に移行'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dry-run', action='store_true',
                       help='移動先を表示（実行しない）')
    group.add_argument('--execute', action='store_true',
                       help='実際に移動を実行')
    args = parser.parse_args()

    migrate(dry_run=not args.execute)


if __name__ == "__main__":
    main()
