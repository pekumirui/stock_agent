"""決算データ取得・計算サービス"""
import sqlite3
from pathlib import Path
from typing import Optional


DB_PATH = Path(__file__).parent.parent.parent / "db" / "stock_agent.db"


def get_db():
    """SQLite接続を取得"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_viewer_data(
    target_date: str,
    types: list[str] | None = None,
    sort: str = "time",
    order: str = "desc",
) -> list[dict]:
    """
    指定日の決算発表一覧データを取得。

    QoQ計算: 単独四半期の前年同期比（v_financials_standalone_quarter使用）
    YoY計算: 累計ベースの前年同期比（v_financials_yoy使用）

    注意: announcementsテーブルが存在しない場合（Agent A未完了）は、
    financialsテーブルのannouncement_dateで代替する。
    """
    conn = get_db()
    try:
        rows = []

        # まずannouncementsテーブルの存在確認
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='announcements'"
        )
        has_announcements = cursor.fetchone() is not None

        announcement_rows = []

        if has_announcements:
            # announcementsテーブルからデータ取得
            query = """
                SELECT a.ticker_code, a.announcement_time, a.announcement_type, a.title,
                       a.fiscal_year, a.fiscal_quarter,
                       c.company_name
                FROM announcements a
                JOIN companies c ON a.ticker_code = c.ticker_code
                WHERE a.announcement_date = ?
            """
            params: list = [target_date]
            if types:
                placeholders = ",".join("?" * len(types))
                query += f" AND a.announcement_type IN ({placeholders})"
                params.extend(types)

            cursor = conn.execute(query, params)
            announcement_rows = cursor.fetchall()

        if not announcement_rows:
            # フォールバック: financialsテーブルから取得
            # (announcementsテーブルが存在しない場合、またはデータが空の場合)
            query = """
                SELECT f.ticker_code, NULL as announcement_time,
                       'earnings' as announcement_type,
                       NULL as title, f.fiscal_year, f.fiscal_quarter,
                       c.company_name
                FROM financials f
                JOIN companies c ON f.ticker_code = c.ticker_code
                WHERE f.announcement_date = ?
            """
            params = [target_date]
            cursor = conn.execute(query, params)
            announcement_rows = cursor.fetchall()

        for row in announcement_rows:
            ticker = row["ticker_code"]
            fy = row["fiscal_year"]
            fq = row["fiscal_quarter"]

            data = {
                "ticker_code": ticker,
                "company_name": row["company_name"],
                "announcement_time": row["announcement_time"] or "",
                "announcement_type": row["announcement_type"],
                "title": row["title"] or "",
                "fiscal_year": fy,
                "fiscal_quarter": fq,
                # QoQ, YoY, Consensus はNoneで初期化
                "revenue_qoq": None,
                "gross_profit_qoq": None,
                "operating_income_qoq": None,
                "ordinary_income_qoq": None,
                "net_income_qoq": None,
                "revenue_yoy": None,
                "operating_income_yoy": None,
                "ordinary_income_yoy": None,
                "net_income_yoy": None,
                "revenue_con": None,
                "operating_income_con": None,
                "ordinary_income_con": None,
                "net_income_con": None,
                "dividend_info": None,
            }

            # YoY計算（v_financials_yoy ビューを使用）
            if fy and fq:
                yoy_cursor = conn.execute(
                    """
                    SELECT revenue_yoy_pct, operating_income_yoy_pct, net_income_yoy_pct,
                           ordinary_income, ordinary_income_prev_year
                    FROM v_financials_yoy
                    WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                """,
                    [ticker, fy, fq],
                )
                yoy_row = yoy_cursor.fetchone()
                if yoy_row:
                    data["revenue_yoy"] = yoy_row["revenue_yoy_pct"]
                    data["operating_income_yoy"] = yoy_row["operating_income_yoy_pct"]
                    data["net_income_yoy"] = yoy_row["net_income_yoy_pct"]
                    # 経常利益YoY（ビューにないので計算）
                    oi = yoy_row["ordinary_income"]
                    oi_prev = yoy_row["ordinary_income_prev_year"]
                    if oi is not None and oi_prev is not None and oi_prev != 0:
                        data["ordinary_income_yoy"] = round(
                            (oi - oi_prev) * 100.0 / abs(oi_prev), 1
                        )

            # QoQ計算（v_financials_standalone_quarter 使用 - Agent A作成予定）
            # ビューが存在しない場合はスキップ
            try:
                if fy and fq and fq != "FY":
                    # 前年同期の単独四半期と比較
                    prev_fy = str(int(fy) - 1)

                    cur_cursor = conn.execute(
                        """
                        SELECT revenue_standalone, gross_profit_standalone,
                               operating_income_standalone, ordinary_income_standalone,
                               net_income_standalone
                        FROM v_financials_standalone_quarter
                        WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                    """,
                        [ticker, fy, fq],
                    )
                    cur_sq = cur_cursor.fetchone()

                    prev_cursor = conn.execute(
                        """
                        SELECT revenue_standalone, gross_profit_standalone,
                               operating_income_standalone, ordinary_income_standalone,
                               net_income_standalone
                        FROM v_financials_standalone_quarter
                        WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                    """,
                        [ticker, prev_fy, fq],
                    )
                    prev_sq = prev_cursor.fetchone()

                    if cur_sq and prev_sq:
                        for field, key in [
                            ("revenue_standalone", "revenue_qoq"),
                            ("gross_profit_standalone", "gross_profit_qoq"),
                            ("operating_income_standalone", "operating_income_qoq"),
                            ("ordinary_income_standalone", "ordinary_income_qoq"),
                            ("net_income_standalone", "net_income_qoq"),
                        ]:
                            cur_val = cur_sq[field]
                            prev_val = prev_sq[field]
                            if (
                                cur_val is not None
                                and prev_val is not None
                                and prev_val != 0
                            ):
                                data[key] = round(
                                    (cur_val - prev_val) * 100.0 / abs(prev_val), 1
                                )
            except Exception:
                pass  # ビュー未作成時は無視

            # 配当情報（management_forecastsから取得 - Agent A作成予定）
            try:
                div_cursor = conn.execute(
                    """
                    SELECT dividend_per_share FROM management_forecasts
                    WHERE ticker_code = ? AND fiscal_year = ?
                    ORDER BY announced_date DESC LIMIT 1
                """,
                    [ticker, fy],
                )
                div_row = div_cursor.fetchone()
                if div_row and div_row["dividend_per_share"]:
                    data["dividend_info"] = (
                        f"合計{div_row['dividend_per_share']:.2f}円"
                    )
            except Exception:
                pass  # テーブル未作成時は無視

            rows.append(data)

        # ソート
        if sort == "time":
            rows.sort(
                key=lambda x: x.get("announcement_time") or "",
                reverse=(order == "desc"),
            )
        elif sort == "ticker":
            rows.sort(
                key=lambda x: x.get("ticker_code") or "",
                reverse=(order == "desc"),
            )

        return rows
    finally:
        conn.close()


def get_detail_data(ticker_code: str, target_date: str) -> dict:
    """展開行用の詳細データ取得"""
    conn = get_db()
    try:
        result = {
            "ticker_code": ticker_code,
            "cumulative": None,  # 累計実績
            "forecast": None,  # 会社予想（最新）
            "initial_forecast": None,  # 期初予想
            "q4_standalone": None,  # 4Q単体予想
        }

        # 該当発表の fiscal_year, fiscal_quarter を取得
        # announcementsテーブル or financials テーブルから
        fy, fq = None, None
        try:
            cursor = conn.execute(
                """
                SELECT fiscal_year, fiscal_quarter FROM announcements
                WHERE ticker_code = ? AND announcement_date = ?
                LIMIT 1
            """,
                [ticker_code, target_date],
            )
            row = cursor.fetchone()
            if row:
                fy, fq = row["fiscal_year"], row["fiscal_quarter"]
        except Exception:
            pass

        if not fy:
            cursor = conn.execute(
                """
                SELECT fiscal_year, fiscal_quarter FROM financials
                WHERE ticker_code = ? AND announcement_date = ?
                LIMIT 1
            """,
                [ticker_code, target_date],
            )
            row = cursor.fetchone()
            if row:
                fy, fq = row["fiscal_year"], row["fiscal_quarter"]

        if not fy:
            return result

        result["fiscal_year"] = fy
        result["fiscal_quarter"] = fq

        # 累計実績
        cursor = conn.execute(
            """
            SELECT revenue, gross_profit, operating_income, ordinary_income, net_income
            FROM financials
            WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
        """,
            [ticker_code, fy, fq],
        )
        cum_row = cursor.fetchone()
        if cum_row:
            # 前年同期の累計
            prev_fy = str(int(fy) - 1)
            prev_cursor = conn.execute(
                """
                SELECT revenue, gross_profit, operating_income, ordinary_income, net_income
                FROM financials
                WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
            """,
                [ticker_code, prev_fy, fq],
            )
            prev_cum = prev_cursor.fetchone()

            result["cumulative"] = _calc_changes(cum_row, prev_cum)

        # 会社予想（最新）
        try:
            cursor = conn.execute(
                """
                SELECT revenue, operating_income, ordinary_income, net_income,
                       dividend_per_share, forecast_type, revision_direction
                FROM management_forecasts
                WHERE ticker_code = ? AND fiscal_year = ?
                ORDER BY announced_date DESC LIMIT 1
            """,
                [ticker_code, fy],
            )
            fc_row = cursor.fetchone()
            if fc_row:
                result["forecast"] = dict(fc_row)

            # 期初予想
            cursor = conn.execute(
                """
                SELECT revenue, operating_income, ordinary_income, net_income
                FROM management_forecasts
                WHERE ticker_code = ? AND fiscal_year = ? AND forecast_type = 'initial'
                ORDER BY announced_date ASC LIMIT 1
            """,
                [ticker_code, fy],
            )
            init_row = cursor.fetchone()
            if init_row:
                result["initial_forecast"] = dict(init_row)
        except Exception:
            pass

        return result
    finally:
        conn.close()


def get_available_dates() -> list[str]:
    """決算発表が存在する日付の一覧を取得（降順）"""
    conn = get_db()
    try:
        cursor = conn.execute(
            """
            SELECT DISTINCT announcement_date
            FROM financials
            WHERE announcement_date IS NOT NULL
            ORDER BY announcement_date DESC
            LIMIT 60
        """
        )
        return [row["announcement_date"] for row in cursor.fetchall()]
    finally:
        conn.close()


def _calc_changes(current_row, prev_row) -> dict:
    """当期と前期の変化率を計算"""
    fields = [
        "revenue",
        "gross_profit",
        "operating_income",
        "ordinary_income",
        "net_income",
    ]
    result = {}
    for f in fields:
        cur = current_row[f] if current_row else None
        prev = prev_row[f] if prev_row else None
        result[f] = cur
        result[f"{f}_prev"] = prev
        if cur is not None and prev is not None and prev != 0:
            result[f"{f}_pct"] = round((cur - prev) * 100.0 / abs(prev), 1)
        else:
            result[f"{f}_pct"] = None
    return result
