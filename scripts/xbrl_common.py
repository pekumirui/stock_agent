"""XBRL解析共通ユーティリティ

fetch_financials.py と fetch_tdnet.py で共有する
XBRL関連のユーティリティ関数を提供する。
"""
import re
import xml.etree.ElementTree as ET
from typing import Optional


# 和暦→西暦変換マップ
WAREKI_MAP = {
    '令和': 2018,  # 令和N年 = 2018 + N
    '平成': 1988,  # 平成N年 = 1988 + N
}


def wareki_to_seireki(text: str) -> str:
    """和暦表記を西暦に変換する。

    例: "令和7年12月期" → "2025年12月期"
    """
    for era, offset in WAREKI_MAP.items():
        match = re.search(rf'{era}(\d{{1,2}})年', text)
        if match:
            year = offset + int(match.group(1))
            return text[:match.start()] + f'{year}年' + text[match.end():]
    return text


def resolve_fiscal_end_date(
    *,
    xbrl_fiscal_end: Optional[str],
    fiscal_year: str,
    fiscal_quarter: str,
    title_fiscal_end: Optional[str] = None,
    computed_fiscal_end: Optional[str] = None,
) -> tuple[Optional[str], str, list[str]]:
    """fiscal_end_dateの補正・フォールバック解決

    XBRLから取得したfiscal_end_dateを、タイトル推定やFiscalYearEnd計算で
    検証・補正する。Q1/Q2/Q3ではタイトル推定を最優先し、FY/Q4ではXBRL値から
    fiscal_yearを補正する。

    Args:
        xbrl_fiscal_end: XBRLパース結果のfiscal_end_date (None可)
        fiscal_year: 決算年度 (例: "2026")
        fiscal_quarter: 四半期 (Q1/Q2/Q3/Q4/FY)
        title_fiscal_end: タイトル推定のfiscal_end_date (None可)
        computed_fiscal_end: FiscalYearEnd計算のfiscal_end_date (None可)

    Returns:
        (resolved_fiscal_end_date, resolved_fiscal_year, log_messages)
        - resolved_fiscal_end_date: 補正後のfiscal_end_date (Noneの場合は解決不能)
        - resolved_fiscal_year: 補正後のfiscal_year
        - log_messages: 補正/補完ログメッセージのリスト
    """
    logs = []

    if fiscal_quarter in ('Q1', 'Q2', 'Q3'):
        corrected = title_fiscal_end or computed_fiscal_end

        if xbrl_fiscal_end and corrected and xbrl_fiscal_end != corrected:
            source_label = "タイトル推定" if title_fiscal_end else "FiscalYearEnd計算"
            logs.append(f"[補正] fiscal_end_date: XBRL={xbrl_fiscal_end} → {source_label}={corrected}")
            xbrl_fiscal_end = corrected
        elif not xbrl_fiscal_end and corrected:
            source_label = "タイトル推定" if title_fiscal_end else "FiscalYearEnd計算"
            xbrl_fiscal_end = corrected
            logs.append(f"[補完] fiscal_end_date: {source_label}={xbrl_fiscal_end}")

    elif fiscal_quarter in ('FY', 'Q4'):
        is_valid_format = bool(
            xbrl_fiscal_end and re.fullmatch(r'\d{4}-\d{2}-\d{2}', xbrl_fiscal_end)
        )
        if is_valid_format:
            xbrl_fiscal_year = xbrl_fiscal_end[:4]
            if xbrl_fiscal_year != fiscal_year:
                logs.append(f"[補正] fiscal_year: タイトル={fiscal_year} → XBRL={xbrl_fiscal_year}")
                fiscal_year = xbrl_fiscal_year
        elif title_fiscal_end:
            xbrl_fiscal_end = title_fiscal_end
            logs.append(f"[補完] fiscal_end_date: タイトルから推定={xbrl_fiscal_end}")

    return xbrl_fiscal_end, fiscal_year, logs


def extract_forecast_fiscal_year(ixbrl_paths: list) -> Optional[str]:
    """iXBRLのContext要素から予想対象の年度を抽出する。

    NextYearDuration / CurrentYearDuration を含む予想コンテキスト
    （ForecastMember必須）の endDate を取得し、
    その年を fiscal_year 文字列として返す。
    NextYear を優先し、なければ CurrentYear（Q1-Q3短信）にフォールバックする。

    Args:
        ixbrl_paths: iXBRLファイルのパスリスト

    Returns:
        年度文字列 (例: "2026") or None
    """
    for ixbrl_path in ixbrl_paths:
        xbrli_ns = None
        # 4バケット: NextYear / NextQ2 / CurrentYear / CurrentQ2 を個別管理
        found_next_year = None
        found_next_q2 = None
        found_current_year = None
        found_current_q2 = None

        for event, elem in ET.iterparse(str(ixbrl_path), events=["start-ns", "end"]):
            if event == "start-ns":
                prefix, uri = elem
                if uri == "http://www.xbrl.org/2003/instance":
                    xbrli_ns = uri
            elif event == "end" and xbrli_ns:
                if elem.tag != f"{{{xbrli_ns}}}context":
                    continue
                ctx_id = elem.get("id", "")
                # ForecastMember + Duration を含むコンテキストのみ対象
                if "ForecastMember" not in ctx_id or "Duration" not in ctx_id:
                    continue
                period = elem.find(f"{{{xbrli_ns}}}period")
                if period is None:
                    continue
                end_date = period.find(f"{{{xbrli_ns}}}endDate")
                if end_date is None or not end_date.text:
                    continue
                date_text = end_date.text
                # startswith で厳密にバケット振り分け（順序重要: Q2を先に判定）
                if ctx_id.startswith("NextAccumulatedQ2Duration"):
                    if not found_next_q2:
                        found_next_q2 = date_text
                elif ctx_id.startswith("NextYearDuration"):
                    if not found_next_year:
                        found_next_year = date_text
                elif ctx_id.startswith("CurrentAccumulatedQ2Duration"):
                    if not found_current_q2:
                        found_current_q2 = date_text
                elif ctx_id.startswith("CurrentYearDuration"):
                    if not found_current_year:
                        found_current_year = date_text
                if found_next_year:
                    break  # NextYearDuration = 最優先、早期終了

        # 優先順位: NextYear > CurrentYear > NextQ2 > CurrentQ2
        # Q2 endDateは年度末ではないため FY/CurrentYear を優先
        best_date = found_next_year or found_current_year or found_next_q2 or found_current_q2
        if best_date:
            return best_date[:4]

    return None
