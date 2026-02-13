"""
決算データ取得バッチ - J-Quants API

J-Quants APIのfin-summaryエンドポイントから決算データを取得し、
financialsテーブルに保存する。EDINET/TDnetの補完・代替手段として利用。

前提: pip install jquants-api-client

使用方法:
    python fetch_jquants_fins.py                  # 過去7日分の開示を取得
    python fetch_jquants_fins.py --days 30        # 過去30日分
    python fetch_jquants_fins.py --ticker 7203    # 特定銘柄の全履歴
    python fetch_jquants_fins.py --ticker 7203,6758  # 複数銘柄
    python fetch_jquants_fins.py --force          # 既存データも上書き
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
    get_all_tickers, insert_financial,
    log_batch_start, log_batch_end,
    ticker_exists,
)

# DocType → fiscal_quarter マッピング
QUARTER_PREFIX = {
    'FY': 'FY',
    '1Q': 'Q1',
    '2Q': 'Q2',
    '3Q': 'Q3',
}

# 除外する DocType パターン
EXCLUDED_DOC_TYPES = {
    'DividendForecastRevision',
    'EarnForecastRevision',
    'REITDividendForecastRevision',
    'REITEarnForecastRevision',
}


def _load_env():
    """プロジェクトルートの.envファイルから環境変数を読み込む"""
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)


def _detect_quarter(doc_type: str) -> Optional[str]:
    """DocTypeからfiscal_quarterを判定する。

    Args:
        doc_type: J-Quants DocType (例: 'FYFinancialStatements_Consolidated_IFRS')

    Returns:
        'FY', 'Q1', 'Q2', 'Q3' または None（対象外）
    """
    for prefix, quarter in QUARTER_PREFIX.items():
        if doc_type.startswith(prefix):
            return quarter
    return None


def _is_target_row(doc_type: str) -> bool:
    """対象レコードかどうか判定する。

    決算短信（FinancialStatements）のみ対象。
    配当予想修正・業績予想修正・REITは除外。
    """
    if doc_type in EXCLUDED_DOC_TYPES:
        return False
    return 'FinancialStatements' in doc_type


def _is_consolidated(doc_type: str) -> bool:
    """連結決算かどうか"""
    return '_Consolidated_' in doc_type


def _to_million(value) -> Optional[float]:
    """円単位の値を百万円単位に変換する。None/NaN/空文字はNoneを返す。"""
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == '':
            return None
        try:
            value = float(value)
        except (ValueError, TypeError):
            return None
    if pd.isna(value):
        return None
    return float(value) / 1_000_000


def _to_float(value) -> Optional[float]:
    """値をfloatに変換する。None/NaN/空文字はNoneを返す。"""
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    if pd.isna(value):
        return None
    return float(value)


def _format_date(value) -> Optional[str]:
    """pandas Timestamp/datetime/文字列をYYYY-MM-DD形式にする。"""
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == '':
            return None
        return value[:10]
    if isinstance(value, (pd.Timestamp, datetime)):
        if pd.isna(value):
            return None
        return value.strftime('%Y-%m-%d')
    return None


def _format_time(value) -> Optional[str]:
    """DiscTime値をHH:MM形式にする。"""
    if value is None:
        return None
    s = str(value).strip()
    if s == '' or s == 'nan' or s == 'NaT':
        return None
    # "13:55:00" → "13:55"
    if len(s) >= 5:
        return s[:5]
    return s


def _fiscal_year_from_fy_end(fy_end) -> Optional[str]:
    """CurFYEn（決算期末日）からfiscal_yearを抽出する。"""
    date_str = _format_date(fy_end)
    if date_str is None:
        return None
    return date_str[:4]


def map_to_financial(row: dict) -> Optional[dict]:
    """J-Quants fin-summaryの1行をinsert_financial用のkwargsに変換する。

    Args:
        row: DataFrameの行（dict形式）

    Returns:
        insert_financial()に渡すkwargs辞書、またはNone（変換不能時）
    """
    doc_type = row.get('DocType', '')

    # 対象外の行をスキップ
    if not _is_target_row(doc_type):
        return None

    # fiscal_quarter判定
    fiscal_quarter = _detect_quarter(doc_type)
    if fiscal_quarter is None:
        return None

    # ticker_code: 5桁→4桁
    code = str(row.get('Code', ''))
    if len(code) >= 5:
        ticker_code = code[:4]
    else:
        ticker_code = code

    if not ticker_code:
        return None

    # fiscal_year
    fiscal_year = _fiscal_year_from_fy_end(row.get('CurFYEn'))
    if fiscal_year is None:
        return None

    # 財務データ
    result = {
        'ticker_code': ticker_code,
        'fiscal_year': fiscal_year,
        'fiscal_quarter': fiscal_quarter,
        'fiscal_end_date': _format_date(row.get('CurPerEn')),
        'announcement_date': _format_date(row.get('DiscDate')),
        'announcement_time': _format_time(row.get('DiscTime')),
        'revenue': _to_million(row.get('Sales')),
        'operating_income': _to_million(row.get('OP')),
        'ordinary_income': _to_million(row.get('OdP')),
        'net_income': _to_million(row.get('NP')),
        'eps': _to_float(row.get('EPS')),
        'source': 'JQuants',
    }

    return result


def _select_best_rows(df: pd.DataFrame) -> pd.DataFrame:
    """同一 (Code, CurFYEn, quarter) で連結/非連結が重複する場合、連結を優先する。
    さらに同一グループ内ではDiscNo（開示番号）が大きいものを採用。
    """
    if df.empty:
        return df

    # quarter列を追加
    df = df.copy()
    df['_quarter'] = df['DocType'].apply(_detect_quarter)
    df['_is_consolidated'] = df['DocType'].apply(_is_consolidated)

    # 連結フラグ降順（True=1 > False=0）、DiscNo降順でソート
    sort_cols = ['Code', '_quarter']
    if 'DiscNo' in df.columns:
        df['_disc_no'] = pd.to_numeric(df['DiscNo'], errors='coerce').fillna(0)
        df.sort_values(
            sort_cols + ['_is_consolidated', '_disc_no'],
            ascending=[True, True, False, False],
            inplace=True,
        )
    else:
        df.sort_values(
            sort_cols + ['_is_consolidated'],
            ascending=[True, True, False],
            inplace=True,
        )

    # CurFYEnをグループキーに使用（日付文字列に変換）
    df['_fy_key'] = df['CurFYEn'].apply(_format_date)

    # 同一 (Code, _fy_key, _quarter) で最初の行を取る
    deduped = df.drop_duplicates(subset=['Code', '_fy_key', '_quarter'], keep='first')

    return deduped


def fetch_by_ticker(client, tickers: list[str]) -> int:
    """銘柄コード指定で全履歴を取得する。"""
    saved_count = 0

    for ticker in tickers:
        print(f"\n--- {ticker} ---")
        try:
            df = client.get_fin_summary(code=ticker)
        except Exception as e:
            print(f"  [ERROR] API呼び出し失敗: {e}")
            continue

        if df.empty:
            print("  データなし")
            continue

        # 対象行のみフィルタ
        df = df[df['DocType'].apply(_is_target_row)]
        if df.empty:
            print("  対象データなし")
            continue

        # 連結優先で重複排除
        df = _select_best_rows(df)

        count = _process_rows(df, ticker)
        saved_count += count
        print(f"  {ticker}: {count}件保存")

    return saved_count


def fetch_by_date(client, days: int) -> int:
    """日付指定で過去N日分の開示を取得する。"""
    saved_count = 0
    today = datetime.now()

    for i in range(days):
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

        # 対象行のみフィルタ
        df = df[df['DocType'].apply(_is_target_row)]
        if df.empty:
            print("  対象データなし")
            continue

        # 連結優先で重複排除
        df = _select_best_rows(df)

        count = _process_rows(df)
        saved_count += count
        print(f"  {count}件保存")

    return saved_count


def _process_rows(df: pd.DataFrame, label: str = "") -> int:
    """DataFrameの各行をfinancials テーブルに保存する。"""
    saved = 0
    for _, row in df.iterrows():
        mapped = map_to_financial(row.to_dict())
        if mapped is None:
            continue

        ticker_code = mapped.pop('ticker_code')
        fiscal_year = mapped.pop('fiscal_year')
        fiscal_quarter = mapped.pop('fiscal_quarter')

        if not ticker_exists(ticker_code):
            continue

        try:
            result = insert_financial(ticker_code, fiscal_year, fiscal_quarter, **mapped)
            if result:
                saved += 1
        except Exception as e:
            print(f"  [ERROR] {ticker_code} {fiscal_year} {fiscal_quarter}: {e}")

    return saved


def main():
    _load_env()

    parser = argparse.ArgumentParser(description='J-Quants APIから決算データを取得')
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

    log_id = log_batch_start('fetch_jquants_fins')
    print(f"=== J-Quants 決算データ取得開始 ===")
    print(f"  日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        if args.ticker:
            tickers = [t.strip() for t in args.ticker.split(',')]
            print(f"  モード: 銘柄指定 ({', '.join(tickers)})")
            saved_count = fetch_by_ticker(client, tickers)
        else:
            print(f"  モード: 日付指定 (過去{args.days}日)")
            saved_count = fetch_by_date(client, args.days)

        print(f"\n=== 完了: {saved_count}件保存 ===")
        log_batch_end(log_id, 'success', records_processed=saved_count)

    except Exception as e:
        print(f"\n[ERROR] バッチ失敗: {e}")
        log_batch_end(log_id, 'error', error_message=str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
