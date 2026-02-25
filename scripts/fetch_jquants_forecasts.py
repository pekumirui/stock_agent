"""
業績予想取得バッチ - J-Quants API

J-Quants APIのfin-summaryエンドポイントから業績予想データを取得し、
management_forecastsテーブルに保存する。
TDnetのフォールバックではなく、独立したスクリプトとして動作する。

前提: pip install jquants-api-client

使用方法:
    python fetch_jquants_forecasts.py                 # 過去7日分の開示を取得
    python fetch_jquants_forecasts.py --days 30       # 過去30日分
    python fetch_jquants_forecasts.py --ticker 7203   # 特定銘柄
    python fetch_jquants_forecasts.py --force         # 既存データも上書き
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

BASE_DIR = Path(__file__).parent.parent

from db_utils import (
    get_all_tickers, insert_management_forecast,
    log_batch_start, log_batch_end,
)
from env_utils import load_env
from jquants_common import (
    QUARTER_PREFIX,
    detect_quarter,
    to_million,
    to_float,
    format_date,
    fiscal_year_from_fy_end,
)

# Lightプラン: 60件/分 → 安全マージンとして1.5秒間隔
API_SLEEP_SEC = 1.5

# 業績予想修正の DocType パターン
FORECAST_REVISION_DOC_TYPES = {
    'EarnForecastRevision',
    'REITEarnForecastRevision',
}

# 除外する DocType パターン（配当予想修正等）
EXCLUDED_DOC_TYPES = {
    'DividendForecastRevision',
    'REITDividendForecastRevision',
}

# J-Quants フィールド → (quarter, db_column) のマッピング
# FY通期予想フィールド
FY_FORECAST_FIELDS = {
    'FSales': 'revenue',
    'FOP': 'operating_income',
    'FOdP': 'ordinary_income',
    'FNP': 'net_income',
    'FEPS': 'eps',
    'FDivAnn': 'dividend_per_share',
}

# Q2半期予想フィールド
Q2_FORECAST_FIELDS = {
    'FSales2Q': 'revenue',
    'FOP2Q': 'operating_income',
    'FOdP2Q': 'ordinary_income',
    'FNP2Q': 'net_income',
    'FEPS2Q': 'eps',
}



def _is_target_row(doc_type: str) -> bool:
    """対象レコードかどうか判定する。

    決算短信（FinancialStatements）または業績予想修正（EarnForecastRevision）が対象。
    配当予想修正（DividendForecastRevision）・REITは除外。
    """
    if doc_type in EXCLUDED_DOC_TYPES:
        return False
    # 業績予想修正
    if doc_type in FORECAST_REVISION_DOC_TYPES:
        return True
    # 決算短信（予想フィールドを含む）
    return 'FinancialStatements' in doc_type



def map_to_forecast(row: dict) -> list:
    """J-Quants fin-summaryの1行をinsert_management_forecast用の引数リストに変換する。

    1行から複数の予想レコード（FY通期 + Q2半期）が生成される場合がある。

    Args:
        row: DataFrameの行（dict形式）

    Returns:
        insert_management_forecast()に渡すkwargs辞書のリスト
        変換不能時は空リスト
    """
    doc_type = row.get('DocType', '')

    if not _is_target_row(doc_type):
        return []

    # ticker_code: 5桁→4桁
    code = str(row.get('Code', ''))
    if len(code) >= 5:
        ticker_code = code[:4]
    else:
        ticker_code = code

    if not ticker_code:
        return []

    announced_date = format_date(row.get('DiscDate'))
    if not announced_date:
        return []

    # forecast_type: EarnForecastRevision は 'revised'、それ以外は 'initial'
    forecast_type = 'revised' if doc_type in FORECAST_REVISION_DOC_TYPES else 'initial'

    # fiscal_year判定:
    # - FY決算発表時（FY*): 来期予想 → NxtFYEn使用
    # - Q1/Q2/Q3決算発表時: 当期予想 → CurFYEn使用
    # - EarnForecastRevision: 当期予想修正 → CurFYEn使用
    fiscal_quarter_of_doc = detect_quarter(doc_type)
    if fiscal_quarter_of_doc == 'FY' and doc_type not in FORECAST_REVISION_DOC_TYPES:
        # 通期決算発表時: 来期予想 → NxtFYEn
        fiscal_year_fy = fiscal_year_from_fy_end(row.get('NxtFYEn'))
        fiscal_year_q2 = fiscal_year_fy  # Q2半期予想も来期が対象
    else:
        # Q1/Q2/Q3決算発表時または予想修正: 当期予想 → CurFYEn
        fiscal_year_fy = fiscal_year_from_fy_end(row.get('CurFYEn'))
        fiscal_year_q2 = fiscal_year_fy

    results = []

    # FY通期予想
    if fiscal_year_fy:
        fy_data = {}
        for field, db_col in FY_FORECAST_FIELDS.items():
            val = row.get(field)
            if db_col in ('eps', 'dividend_per_share'):
                converted = to_float(val)
            else:
                converted = to_million(val)
            if converted is not None:
                fy_data[db_col] = converted

        if fy_data:
            results.append({
                'ticker_code': ticker_code,
                'fiscal_year': fiscal_year_fy,
                'fiscal_quarter': 'FY',
                'announced_date': announced_date,
                'forecast_type': forecast_type,
                'source': 'JQuants',
                **fy_data,
            })

    # Q2半期予想（Q2フィールドが存在する場合）
    if fiscal_year_q2:
        q2_data = {}
        for field, db_col in Q2_FORECAST_FIELDS.items():
            val = row.get(field)
            if db_col == 'eps':
                converted = to_float(val)
            else:
                converted = to_million(val)
            if converted is not None:
                q2_data[db_col] = converted

        if q2_data:
            results.append({
                'ticker_code': ticker_code,
                'fiscal_year': fiscal_year_q2,
                'fiscal_quarter': 'Q2',
                'announced_date': announced_date,
                'forecast_type': forecast_type,
                'source': 'JQuants',
                **q2_data,
            })

    return results


def _process_rows(df: pd.DataFrame, force: bool = False,
                  valid_tickers: set = None) -> int:
    """DataFrameの各行をmanagement_forecastsテーブルに保存する。

    Args:
        df: J-Quants fin-summary DataFrame
        force: Trueなら優先度チェックをスキップして上書き
        valid_tickers: 有効な銘柄コードのSet（Noneならチェックスキップ）

    Returns:
        保存件数
    """
    saved = 0
    for _, row in df.iterrows():
        mapped_list = map_to_forecast(row.to_dict())
        for mapped in mapped_list:
            ticker_code = mapped.pop('ticker_code')
            fiscal_year = mapped.pop('fiscal_year')
            fiscal_quarter = mapped.pop('fiscal_quarter')
            announced_date = mapped.pop('announced_date')
            forecast_type = mapped.pop('forecast_type')
            source = mapped.pop('source')

            if valid_tickers is not None and ticker_code not in valid_tickers:
                continue

            try:
                result = insert_management_forecast(
                    ticker_code=ticker_code,
                    fiscal_year=fiscal_year,
                    fiscal_quarter=fiscal_quarter,
                    announced_date=announced_date,
                    forecast_type=forecast_type,
                    source=source,
                    skip_priority_check=force,
                    **mapped,
                )
                if result:
                    saved += 1
            except Exception as e:
                print(f"  [ERROR] {ticker_code} {fiscal_year} {fiscal_quarter}: {e}")

    return saved


def fetch_by_ticker(client, tickers: list, force: bool = False,
                    valid_tickers: set = None) -> int:
    """銘柄コード指定で全履歴を取得する。

    Args:
        client: jquantsapi.ClientV2インスタンス
        tickers: 証券コードのリスト
        force: Trueなら優先度チェックをスキップして上書き
        valid_tickers: 有効な銘柄コードのSet

    Returns:
        保存件数
    """
    saved_count = 0

    for i, ticker in enumerate(tickers):
        if i > 0:
            time.sleep(API_SLEEP_SEC)
        print(f"\n--- {ticker} ---")
        try:
            df = client.get_fin_summary(code=ticker)
        except Exception as e:
            print(f"  [ERROR] API呼び出し失敗: {e}")
            continue

        if df.empty:
            print("  データなし")
            continue

        df = df[df['DocType'].apply(_is_target_row)]
        if df.empty:
            print("  対象データなし")
            continue

        count = _process_rows(df, force=force, valid_tickers=valid_tickers)
        saved_count += count
        print(f"  {ticker}: {count}件保存")

    return saved_count


def fetch_by_date(client, days: int, force: bool = False,
                  valid_tickers: set = None) -> int:
    """日付指定で過去N日分の開示を取得する。

    Args:
        client: jquantsapi.ClientV2インスタンス
        days: 過去何日分を取得するか
        force: Trueなら優先度チェックをスキップして上書き
        valid_tickers: 有効な銘柄コードのSet

    Returns:
        保存件数
    """
    saved_count = 0
    today = datetime.now()

    for i in range(days):
        if i > 0:
            time.sleep(API_SLEEP_SEC)
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime('%Y%m%d')
        print(f"\n--- {target_date.strftime('%Y-%m-%d')} ---")

        try:
            df = client.get_fin_summary(date_yyyymmdd=date_str)
        except Exception as e:
            print(f"  [ERROR] API呼び出し失敗: {e}")
            continue

        if df.empty:
            print("  開示なし")
            continue

        df = df[df['DocType'].apply(_is_target_row)]
        if df.empty:
            print("  対象データなし")
            continue

        count = _process_rows(df, force=force, valid_tickers=valid_tickers)
        saved_count += count
        print(f"  {count}件保存")

    return saved_count


def main():
    load_env()

    parser = argparse.ArgumentParser(description='J-Quants APIから業績予想データを取得')
    parser.add_argument('--days', type=int, default=7, help='過去N日分を取得（デフォルト: 7）')
    parser.add_argument('--ticker', '-t', help='特定銘柄のみ取得（カンマ区切り）')
    parser.add_argument('--force', action='store_true', help='既存データも上書き')
    args = parser.parse_args()

    # jquantsapi は環境変数 JQUANTS_API_KEY を自動読み込み
    import jquantsapi
    try:
        client = jquantsapi.ClientV2()
    except ValueError as e:
        print(f"[ERROR] J-Quants API キーが未設定: {e}")
        print("  .env に JQUANTS_API_KEY を設定してください")
        sys.exit(1)

    valid_tickers = set(get_all_tickers(active_only=False))

    log_id = log_batch_start('fetch_jquants_forecasts')
    print(f"=== J-Quants 業績予想データ取得開始 ===")
    print(f"  日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        if args.ticker:
            tickers = [t.strip() for t in args.ticker.split(',')]
            print(f"  モード: 銘柄指定 ({', '.join(tickers)})")
            saved_count = fetch_by_ticker(client, tickers, force=args.force,
                                          valid_tickers=valid_tickers)
        else:
            print(f"  モード: 日付指定 (過去{args.days}日)")
            saved_count = fetch_by_date(client, args.days, force=args.force,
                                        valid_tickers=valid_tickers)

        print(f"\n=== 完了: {saved_count}件保存 ===")
        log_batch_end(log_id, 'success', records_processed=saved_count)

    except Exception as e:
        print(f"\n[ERROR] バッチ失敗: {e}")
        log_batch_end(log_id, 'failed', error_message=str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
