"""J-Quants vs TDnet 取得結果比較スクリプト

DBに保存済みのTDnetデータとJ-Quants APIから取得した生データを突合し、
フィールドごとの差異をレポートする。

使用方法:
    python compare_sources.py --date 2026-02-13
    python compare_sources.py --date 2026-02-13 --ticker 8001,1721
"""
import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

BASE_DIR = Path(__file__).parent.parent

from fetch_jquants_fins import (
    _load_env,
    _is_target_row,
    _select_best_rows,
    map_to_financial,
)
from db_utils import DB_PATH, ticker_exists

# 比較対象フィールド
COMPARE_FIELDS = ['revenue', 'operating_income', 'ordinary_income', 'net_income', 'eps']

# 差異判定の許容値
TOLERANCE_MILLION = 1.0   # 百万円単位フィールド
TOLERANCE_EPS = 0.01      # EPS（円単位）


def _values_match(a: Optional[float], b: Optional[float], field: str) -> bool:
    """2つの値が許容範囲内で一致するか判定する。"""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    tol = TOLERANCE_EPS if field == 'eps' else TOLERANCE_MILLION
    return abs(a - b) < tol


def fetch_db_from_db(date: str, tickers: list[str] = None) -> dict:
    """DBからTDnet+EDINETデータを取得して辞書に変換する。

    EDINET(priority=3)がTDnet(priority=2)を上書きしたレコードも含める。

    Returns:
        {(ticker_code, fiscal_year, fiscal_quarter): {field: value, ...}}
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sql = """
        SELECT ticker_code, fiscal_year, fiscal_quarter,
               revenue, operating_income, ordinary_income, net_income, eps, source
        FROM financials
        WHERE announcement_date = ? AND source IN ('TDnet', 'EDINET')
    """
    params = [date]
    if tickers:
        placeholders = ','.join('?' * len(tickers))
        sql += f" AND ticker_code IN ({placeholders})"
        params.extend(tickers)

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    result = {}
    for row in rows:
        key = (row['ticker_code'], row['fiscal_year'], row['fiscal_quarter'])
        result[key] = {f: row[f] for f in COMPARE_FIELDS}
        result[key]['_source'] = row['source']
    return result


def fetch_jquants_raw(date: str, tickers: list[str] = None) -> dict:
    """J-Quants APIから生データを取得してmap_to_financial形式の辞書に変換する。
    DB挿入は行わない。

    Returns:
        {(ticker_code, fiscal_year, fiscal_quarter): {field: value, ...}}
    """
    import jquantsapi

    client = jquantsapi.ClientV2()
    date_fmt = date.replace('-', '')
    print(f"  J-Quants API呼び出し中 (date={date_fmt}) ...")

    try:
        df = client.get_fin_summary(date_yyyymmdd=date_fmt)
    except Exception as e:
        print(f"  [ERROR] API呼び出し失敗: {e}")
        return {}

    if df.empty:
        print("  J-Quants: データなし")
        return {}

    # 対象行のみフィルタ
    df = df[df['DocType'].apply(_is_target_row)]
    if df.empty:
        print("  J-Quants: 対象データなし")
        return {}

    # 連結優先で重複排除
    df = _select_best_rows(df)

    result = {}
    for _, row in df.iterrows():
        mapped = map_to_financial(row.to_dict())
        if mapped is None:
            continue
        tc = mapped['ticker_code']
        if tickers and tc not in tickers:
            continue
        if not ticker_exists(tc):
            continue
        key = (tc, mapped['fiscal_year'], mapped['fiscal_quarter'])
        result[key] = {f: mapped.get(f) for f in COMPARE_FIELDS}
    return result


def compare_and_report(tdnet: dict, jquants: dict, date: str):
    """突合してレポートを出力する。"""
    tdnet_keys = set(tdnet.keys())
    jquants_keys = set(jquants.keys())
    common_keys = tdnet_keys & jquants_keys
    tdnet_only = tdnet_keys - jquants_keys
    jquants_only = jquants_keys - tdnet_keys

    print(f"\n{'=' * 60}")
    print(f" J-Quants vs DB(TDnet+EDINET) 比較レポート ({date})")
    print(f"{'=' * 60}")
    print(f"\n突合結果:")
    print(f"  DB件数:       {len(tdnet_keys):>6}")
    print(f"  JQuants件数:  {len(jquants_keys):>6}")
    print(f"  突合キー一致: {len(common_keys):>6}")
    print(f"  DBのみ:       {len(tdnet_only):>6}")
    print(f"  JQuantsのみ:  {len(jquants_only):>6}")

    if not common_keys:
        print("\n  突合対象なし。比較できるデータがありません。")
        return

    # フィールド比較
    match_count = 0
    diffs = []  # [(key, field, tdnet_val, jquants_val, diff)]

    for key in sorted(common_keys):
        td = tdnet[key]
        jq = jquants[key]
        row_match = True
        for field in COMPARE_FIELDS:
            tv = td.get(field)
            jv = jq.get(field)
            if not _values_match(tv, jv, field):
                row_match = False
                diff = None
                if tv is not None and jv is not None:
                    diff = abs(tv - jv)
                diffs.append((key, field, tv, jv, diff))
        if row_match:
            match_count += 1

    diff_count = len(common_keys) - match_count
    pct = match_count / len(common_keys) * 100 if common_keys else 0

    print(f"\n完全一致: {match_count} / {len(common_keys)} ({pct:.1f}%)")
    print(f"差異あり: {diff_count} / {len(common_keys)}")

    if diffs:
        print(f"\n--- 差異詳細 ---")
        print(f"{'ticker':<8} {'year':<6} {'qtr':<4} {'field':<20} {'DB':>14} {'JQuants':>14} {'diff':>12}")
        print("-" * 80)
        for key, field, tv, jv, diff in diffs:
            tv_str = f"{tv:>14.2f}" if tv is not None else f"{'NULL':>14}"
            jv_str = f"{jv:>14.2f}" if jv is not None else f"{'NULL':>14}"
            diff_str = f"{diff:>12.2f}" if diff is not None else f"{'N/A':>12}"
            print(f"{key[0]:<8} {key[1]:<6} {key[2]:<4} {field:<20} {tv_str} {jv_str} {diff_str}")

    if jquants_only:
        print(f"\n--- JQuantsのみ（DBになし）: {len(jquants_only)}件 ---")
        for key in sorted(jquants_only):
            print(f"  {key[0]}  {key[1]}/{key[2]}")

    if tdnet_only and len(tdnet_only) <= 30:
        print(f"\n--- TDnetのみ（JQuantsになし（XBRL解析失敗等））: {len(tdnet_only)}件 ---")
        for key in sorted(tdnet_only):
            print(f"  {key[0]}  {key[1]}/{key[2]}")
    elif tdnet_only:
        print(f"\n--- TDnetのみ（JQuantsになし（XBRL解析失敗等））: {len(tdnet_only)}件（多いため省略）---")


def main():
    _load_env()

    parser = argparse.ArgumentParser(description='J-Quants vs TDnet 取得結果比較')
    parser.add_argument('--date', required=True, help='比較対象日 (YYYY-MM-DD)')
    parser.add_argument('--ticker', '-t', help='特定銘柄のみ比較（カンマ区切り）')
    args = parser.parse_args()

    # 日付バリデーション
    try:
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print(f"[ERROR] 日付形式が不正: {args.date} (YYYY-MM-DD)")
        sys.exit(1)

    tickers = None
    if args.ticker:
        tickers = [t.strip() for t in args.ticker.split(',')]

    print(f"=== J-Quants vs DB(TDnet+EDINET) 比較 ({args.date}) ===")
    if tickers:
        print(f"  対象銘柄: {', '.join(tickers)}")

    # 1. DB(TDnet+EDINET)データを取得
    print("\n[1/3] DBデータ(TDnet+EDINET)を取得中...")
    tdnet_data = fetch_db_from_db(args.date, tickers)
    print(f"  {len(tdnet_data)}件取得")

    # 2. JQuantsデータをAPIから取得
    print("\n[2/3] JQuantsデータをAPIから取得中...")
    jquants_data = fetch_jquants_raw(args.date, tickers)
    print(f"  {len(jquants_data)}件取得")

    # 3. 比較レポート
    print("\n[3/3] 比較中...")
    compare_and_report(tdnet_data, jquants_data, args.date)


if __name__ == '__main__':
    main()
