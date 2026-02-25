"""J-Quants API バッチスクリプト共通ユーティリティ

fetch_jquants_fins.py と fetch_jquants_forecasts.py で共通の
定数・変換関数を提供する。
"""
from datetime import datetime
from typing import Optional

import pandas as pd

# DocType → fiscal_quarter マッピング
QUARTER_PREFIX = {
    'FY': 'FY',
    '1Q': 'Q1',
    '2Q': 'Q2',
    '3Q': 'Q3',
}


def detect_quarter(doc_type) -> Optional[str]:
    """DocTypeからfiscal_quarterを判定する。

    Args:
        doc_type: J-Quants DocType (例: 'FYFinancialStatements_Consolidated_IFRS')

    Returns:
        'FY', 'Q1', 'Q2', 'Q3' または None（対象外・非文字列）
    """
    if not isinstance(doc_type, str):
        return None
    for prefix, quarter in QUARTER_PREFIX.items():
        if doc_type.startswith(prefix):
            return quarter
    return None


def to_million(value) -> Optional[float]:
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


def to_float(value) -> Optional[float]:
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


def format_date(value) -> Optional[str]:
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


def fiscal_year_from_fy_end(fy_end) -> Optional[str]:
    """期末日（YYYY-MM-DD）からfiscal_yearを抽出する。"""
    date_str = format_date(fy_end)
    if date_str is None:
        return None
    return date_str[:4]
